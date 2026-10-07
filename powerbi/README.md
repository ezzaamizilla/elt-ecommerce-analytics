# Power BI — Dashboard e-commerce (Olist)

## 1. Connexion

| Paramètre | Valeur |
|---|---|
| Serveur | `localhost` |
| Port | `5434` |
| Base | `olist_warehouse` |
| Schéma | `marts` |
| Authentification | Mot de passe (`dbt` / `warehouse_secret`, voir `.env`) |

Power BI Desktop → **Obtenir des données → PostgreSQL** → `localhost:5434` →
base `olist_warehouse` → sélectionner les 7 tables du schéma `marts` → **Charger**
(mode Import).

## 2. Modélisation (étoile)

Onglet **Modélisation** — liaisons à vérifier/créer :

| Table many | Colonne | Table one | Cardinalité |
|---|---|---|---|
| fct_orders | customer_id | dim_customers | Many-to-one, filtre vers le bas |
| fct_orders | order_date_id | dim_date | Many-to-one, filtre vers le bas |
| fct_order_items | order_id | fct_orders | Many-to-one, filtre vers le bas |
| fct_order_items | product_id | dim_products | Many-to-one, filtre vers le bas |
| fct_order_items | seller_id | dim_sellers | Many-to-one, filtre vers le bas |
| fct_order_items | order_date_id | dim_date | Many-to-one, filtre vers le bas |

> Les dates sont des clés entières `AAAAMMJJ` : si Power BI crée une relation
> `fct_revenue ↔ dim_date` sur autre chose que `year_month`, la supprimer
> (`fct_revenue` reste utilisable seul pour la courbe de CA).

## 3. Mesures DAX

Créer un **nouveau groupe de mesures** nommé `Measures`, puis :

```dax
CA Total = SUM ( fct_orders[items_revenue] )

CA Livré = CALCULATE ( [CA Total], fct_orders[is_delivered] = TRUE () )

Commandes = DISTINCTCOUNT ( fct_orders[order_id] )

Panier Moyen = DIVIDE ( [CA Total], [Commandes] )

Produits Vendus = COUNTROWS ( fct_order_items )

Taux Livraison = DIVIDE (
    CALCULATE ( [Commandes], fct_orders[is_delivered] = TRUE () ),
    [Commandes]
)

Note Moyenne = AVERAGE ( fct_orders[review_avg_score] )

Délai Livraison Moyen (j) = AVERAGE ( fct_orders[delivery_duration_days] )

Clients Actifs = DISTINCTCOUNT ( fct_orders[customer_id] )

Nouveaux Clients =
VAR FirstMonth = MIN ( dim_date[date_day] )
RETURN
    CALCULATE (
        [Clients Actifs],
        FILTER (
            ALL ( dim_customers ),
            dim_customers[first_order_date] <= FirstMonth
            && dim_customers[first_order_date] > EDATE ( FirstMonth, -1 )
        )
    )

CA Top Catégorie =
VAR TopCat =
    TOPN ( 1, VALUES ( dim_products[product_category_english] ), [CA Total], DESC )
RETURN
    CALCULATE ( [CA Total], TopCat )
```

## 4. Visuels (page unique « Vue d'ensemble »)

| # | Visuel | Champs |
|---|---|---|
| 1 | Graphique linéaire — **CA par mois** | Axe : `dim_date[year_month]` · Valeur : `[CA Total]` |
| 2 | Barres horizontales — **Top 10 catégories** | Axe : `dim_products[product_category_english]` · Valeur : `[CA Total]` · Top 10 par valeur |
| 3 | Cartes (KPI) | `[CA Total]` · `[Panier Moyen]` · `[Commandes]` · `[Taux Livraison]` |
| 4 | Graphique combiné — **Rétention clients** | Axe : `dim_date[year_month]` · Ligne : `[Clients Actifs]` et `[Nouveaux Clients]` |
| 5 (bonus) | Matrice — **CA par état** | Lignes : `dim_customers[customer_state]` · Valeur : `[CA Total]` |

Filtres page : `dim_date[year_number]` (slicer), `fct_orders[order_status]`.

## 5. Livrable

Enregistrer sous `powerbi/olist_dashboard.pbix` (dossier de ce dépôt), puis
ajouter les captures d'écran dans `powerbi/screenshots/`.

## Valeurs de contrôle (pour vérifier les chiffres affichés)

| KPI | Valeur attendue |
|---|---|
| CA Total | 13 591 643,70 R$ |
| Commandes | 99 441 |
| Panier moyen (CA/commande) | 136,68 R$ |
| Taux de livraison | 97,0 % |
| Délai moyen de livraison | 12,5 jours |
| Top catégorie | health_beauty (~1 259 k R$) |
| Dernière période | 2018-10 |
