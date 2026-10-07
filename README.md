# End-to-End ELT Pipeline for E-Commerce Analytics

Pipeline ELT de bout en bout sur le jeu de données **Olist** (100 k commandes
e-commerce brésiliennes) : extraction OLTP orchestrée par **Airflow**,
transformations modélisées en étoile avec **dbt** (71 tests de qualité
intégrés au DAG), et dashboards **Power BI** sur les KPIs commerciaux.
Environnement 100 % reproductible via **Docker Compose**.

## Architecture

```
Sources e-commerce       EXTRACT                 TRANSFORM                LOAD / CONSOMMATION
┌─────────────────┐   ┌──────────────┐     ┌───────────────────┐     ┌──────────────┐
│ PostgreSQL OLTP │──▶│ Airflow      │────▶│ dbt               │────▶│ Schéma marts │
│ (commandes,     │   │ (orchestre   │     │ (staging →        │     │ (faits/dims) │
│  clients,       │   │  l'extraction│     │  tests +          │     └──────┬───────┘
│  produits)      │   │  ELT)        │     │  modélisation)    │            │
└─────────────────┘   └──────────────┘     └───────────────────┘            ▼
┌─────────────────┐                                                      ┌──────────────┐
│ CSV Olist       │  (miroir public S3 / GitHub)                         │ Power BI     │
└─────────────────┘                                                      │ (dashboards) │
                                                                         └──────────────┘
```

Détail du flux :

1. **OLTP** (`postgres:5432`, base `olist_oltp`) : schéma normalisé chargé à
   partir des CSV (1,55 M lignes, contrôles référentiels à 0 orphelin).
2. **Airflow** (`localhost:8080`) : DAG `elt_pipeline` — réinitialise le schéma
   `bronze` puis extrait les 9 tables en streaming (curseur serveur + COPY),
   avec comptage source/cible et idempotence.
3. **dbt** : `bronze → staging (vues) → intermediate (agrégats/dédup) →
   marts (tables en étoile)`, avec `source freshness` avant chaque run.
4. **Qualité** : 71 tests dbt (unicité, non-nul, référentiel, valeurs
   acceptées, tests métier SQL) s'exécutent comme task du DAG — **un échec de
   test fait échouer le DAG** (bloquage de la livraison).
5. **Warehouse** (`localhost:5434`, base `olist_warehouse`, schémas
   `bronze/staging/intermediate/marts`) → connecté à Power BI.

## Démarrage rapide

```bash
cp .env.example .env                 # identifiants de développement
docker compose up -d --build         # 6 services (~3 min au 1er lancement)

# Données source (une seule fois)
docker compose run --rm --entrypoint python elt-dbt /opt/scripts/fetch_olist.py
docker compose run --rm --entrypoint python elt-dbt /opt/scripts/load_oltp.py

# Pipeline complet (ou bouton "Trigger DAG" dans l'UI)
# UI : http://localhost:8080  (admin / admin)
```

dbt manuellement :

```bash
docker compose run --rm --entrypoint dbt elt-dbt build \
  --project-dir /opt/dbt_project --profiles-dir /opt/dbt_project
```

## Résultat du pipeline

```
elt_pipeline: init_bronze → 9× extract → dbt_source_freshness → dbt_run → dbt_test
Durée ~90 s | 12 tasks | dbt: 21 modèles, 71 tests (91 PASS / 1 WARN / 0 ERROR)
```

## Contenu du dépôt

```
.
├── docker-compose.yml          # Airflow + 3 PostgreSQL (OLTP, warehouse, méta) + dbt
├── .env.example                # identifiants (copier en .env)
├── airflow/
│   ├── Dockerfile              # image Airflow + dbt-postgres + providers
│   ├── dags/elt_pipeline.py    # DAG principal (ELT complet)
│   └── requirements.txt
├── data/raw/                   # CSV Olist (téléchargés, gitignorés)
├── dbt_project/
│   ├── models/staging/         # 9 vues nettoyées
│   ├── models/intermediate/    # agrégats paiements/avis, jointures ligne de commande
│   ├── models/marts/           # dim_date, dim_customers/products/sellers,
│   │                           # fct_orders, fct_order_items, fct_revenue
│   ├── tests/                  # 5 tests métier SQL (singular)
│   └── macros/                 # schémas exacts staging/intermediate/marts
├── scripts/                    # fetch_olist.py (miroir public), load_oltp.py
├── powerbi/                    # guide DAX + .pbix + captures
└── docs/                       # architecture + dictionnaire de données
```

## Ce que ce projet démontre

| Brique | Preuve |
|---|---|
| PostgreSQL (OLTP) | schéma normalisé, PK/FK, chargement idempotent, contrôles référentiels |
| Airflow | DAG, scheduling quotidien, retries, callbacks d'échec, idempotence, logging |
| dbt | ELT, SQL versionné, 71 tests, doc auto (`dbt docs`), source freshness, lineage |
| Power BI | modèle en étoile, DAX (CA, panier moyen, rétention), dashboards métier |
| Docker | environnement reproductible en une commande |
| Qualité données | test cassé volontairement → DAG rouge → livraison bloquée |

## Données

[Jeu Olist — Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(CC BY-NC-SA 4.0), récupéré via miroir public (S3 Le Wagon, repli GitHub),
sans compte Kaggle.
