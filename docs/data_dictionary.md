# Dictionnaire de données

## Sources (schéma `bronze` — extrait de l'OLTP)

| Table | Grain | Lignes |
|---|---|---|
| `customers` | 1 compte client | 99 441 |
| `sellers` | 1 vendeur | 3 095 |
| `products` | 1 produit | 32 951 |
| `category_translation` | 1 catégorie (PT → EN) | 71 |
| `orders` | 1 commande | 99 441 |
| `order_items` | 1 unité produit vendue | 112 650 |
| `order_payments` | 1 paiement | 103 886 |
| `order_reviews` | 1 avis | 99 224 |
| `geolocation` | 1 coordonnée de code postal | 1 000 163 |

## Staging (`staging` — vues nettoyées)

| Modèle | Traitement appliqué |
|---|---|
| `stg_customers` | ville en minuscules, état en majuscules, trim |
| `stg_sellers` | idem |
| `stg_products` | catégorie traduite EN, repli PT puis `unknown` (610 produits sans catégorie) |
| `stg_orders` | statut en minuscules, typage des timestamps |
| `stg_order_items` | cast `numeric(12,2)` sur prix/fret |
| `stg_order_payments` | statut de paiement en minuscules |
| `stg_order_reviews` | commentaires vides → `NULL` |
| `stg_geolocation` | city/state normalisés |
| `stg_category_translation` | copie à l'identique (dictionnaire) |

## Intermediate (`intermediate` — agrégations et déduplication)

| Modèle | Grain | Logique |
|---|---|---|
| `int_order_payments_agg` | commande | somme/nombre de paiements, mode de paiement dominant |
| `int_order_reviews_agg` | commande | dédup (`row_number` par `review_id, order_id`) puis moyenne/min/max des notes |
| `int_order_items` | ligne de commande | jointures commande + produit (catégorie EN) |
| `int_orders` | commande | fusion paiements + avis + agrégats lignes, calcul des délais de livraison |
| `int_customers_enriched` | client | agrégats de rétention : nombre de commandes, 1ère/ dernière commande, CA cumulé |

## Marts (`marts` — modèle en étoile)

### Dimensions

| Table | PK | Colonnes clés |
|---|---|---|
| `dim_date` | `date_id` (AAAAMMJJ) | `date_day`, `year_number`, `month_number`, `quarter_number`, `iso_day_of_week`, `day_name`, `month_name`, `year_month`, `is_weekend` |
| `dim_customers` | `customer_id` | `customer_unique_id`, `customer_zip_code_prefix`, `customer_city`, `customer_state`, `order_count`, `first_order_date`, `last_order_date`, `lifetime_revenue` |
| `dim_products` | `product_id` | `product_category_english`, `product_category`, dimensions physiques, poids, longueurs de description |
| `dim_sellers` | `seller_id` | `seller_zip_code_prefix`, `seller_city`, `seller_state` |

### Faits

| Table | PK | Mesures |
|---|---|---|
| `fct_orders` | `order_id` | `items_revenue`, `freight_total`, `order_gross_revenue`, `payment_total`, `review_avg_score`, `delivery_delay_days`, `delivery_duration_days`, `is_delivered` |
| `fct_order_items` | (`order_id`, `order_item_id`) | `price`, `freight_value`, `item_total` + catégorisation produit |
| `fct_revenue` | `year_month` | `revenue`, `freight_revenue`, `gross_revenue`, `order_count`, `avg_payment_per_order`, `delivered_orders` |

### Clés étrangères

```
fct_orders.customer_id      → dim_customers.customer_id
fct_orders.order_date_id    → dim_date.date_id
fct_order_items.order_id    → fct_orders.order_id
fct_order_items.product_id  → dim_products.product_id
fct_order_items.seller_id   → dim_sellers.seller_id
fct_order_items.order_date_id → dim_date.date_id
```

## Jeux de tests (71)

| Type | Exemples |
|---|---|
| `unique` / `not_null` | PK de chaque dimension et fait |
| `relationships` | FK faits → dimensions, lignes → commandes |
| `accepted_values` | `order_status` (8), `payment_type` (5), `review_score` (1-5) |
| Tests métier SQL | montants > 0, pas de commande future, réconciliation CA mensuel ⇄ lignes, unicité clé composite, commande sans paiement (`severity: warn`) |
| Fraîcheur source | `bronze.orders` : warn 4 000 j / error 5 000 j (jeu historique 2016-2018) |

## Valeurs de contrôle

| KPI | Valeur |
|---|---|
| CA total | 13 591 643,70 R$ |
| Commandes | 99 441 |
| Panier moyen | 136,68 R$ |
| Taux de livraison | 97,0 % |
| Délai moyen | 12,5 jours |
