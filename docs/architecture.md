# Architecture

## Vue d'ensemble

```
                    ┌────────────────────────────────────────────────────────┐
                    │                  docker compose                        │
                    │                                                        │
 CSV Olist ──┐      │  ┌──────────────┐   schéma bronze   ┌───────────────┐  │
 (S3/GitHub) │      │  │              │  ┌──────────────┐ │               │  │
             ▼      │  │  Airflow     │  │  dbt         │ │  Warehouse    │  │
      scripts/      │  │  webserver   │─▶│  freshness   │─▶│  PostgreSQL   │──┼──▶ Power BI
      fetch_olist   │  │  scheduler   │  │  dbt run     │ │  :5434        │  │    localhost:5434
             │      │  │  :8080       │  │  dbt test    │ │  marts        │  │
             ▼      │  └──────┬───────┘  └──────────────┘ └───────────────┘  │
      data/raw/*.csv│         │ extracts (streaming COPY)                    │
                    │         ▼                                              │
                    │  ┌──────────────┐                                      │
                    │  │  OLTP        │  :5432  (schéma normalisé, 9 tables)  │
                    │  │  PostgreSQL  │                                      │
                    │  └──────────────┘                                      │
                    └────────────────────────────────────────────────────────┘
```

## Les 3 bases PostgreSQL

 | Base | Port hôte | Rôle | Schémas |
 |---|---|---|---|
 | `olist_oltp` | 5432 | source transactionnelle (normalisée, PK/FK) | `public` |
 | `olist_warehouse` | 5434 | cible analytics (Power BI se connecte ici) | `bronze`, `staging`, `intermediate`, `marts` |
 | `airflow` | 5435 | métadonnées Airflow (tasks, runs, états) | `public` |

## DAG `elt_pipeline`

```
init_bronze                DROP/CREATE schema bronze (idempotence)
    │
    ├── extract_customers ─┐
    ├── extract_sellers    │   9 tasks parallèles :
    ├── extract_products   │   curseur serveur côté OLTP,
    ├── extract_category…  ├─▶ lots de 20 000 lignes,
    ├── extract_orders     │   COPY vers le warehouse,
    ├── extract_order_items│   contrôle count(source) = count(cible),
    ├── extract_order_pay… │   2 retries, callback d'échec.
    ├── extract_order_rev… │
    └── extract_geolocation┘
              │
              ▼
dbt_source_freshness       contrôle de fraîcheur des sources (bronze)
              │
              ▼
dbt_run                     staging → intermediate → marts (21 modèles)
              │
              ▼
dbt_test                    71 tests : UN ÉCHEC FAIT ÉCHOUER LE DAG
```

- `schedule = @daily`, `catchup = False`, `max_active_runs = 1`.
- `retries = 2`, `retry_delay = 2 min`, `execution_timeout = 30 min`.
- Toute task en échec appelle `failure_callback` → log ERROR dans Airflow.

## Choix de conception

**Idempotence de l'extraction.** Chaque run recrée `bronze` (DROP/CREATE) puis
recharge tout : un run interrompu ou relancé produit toujours le même état.

**Streaming plutôt que `INSERT ... SELECT`.** L'OLTP et le warehouse sont deux
bases distinctes (impossible en SQL natif) : le DAG lit via un curseur serveur
(par lots de 20 000 lignes, sans tout charger en mémoire) puis écrit avec
`COPY` en CSV mémoire. La table `geolocation` (1 M lignes) tourne en ~8 s.

**dbt dans le conteneur Airflow.** Même image, même montage `/opt/dbt_project` :
le DAG exécute `dbt run`/`dbt test` en `BashOperator`, et un conteneur dédié
`elt-dbt` (profil `tools`) permet les runs manuels sans Airflow.

**Qualité = critère d'acceptation.** `dbt_test` est la dernière task : une
régression dans les données (clé dupliquée, FK rompue, valeur hors domaine)
bloque la livraison. Démontré en Phase 5 : test cassé → DAG rouge ; test
restauré → DAG vert.

**Test en `severity: warn`.** `assert_orders_have_payment` documente une
anomalie réelle du jeu Olist (1 commande `delivered` sans paiement) sans
bloquer le pipeline.

## Échelonnement (dimension date)

`dim_date` est générée par `generate_series` sur la plage des commandes
(2016-09-04 → 2018-10-17) : clé `date_id` entière `AAAAMMJJ`, attributs
calendaires (année, trimestre, mois, jour ISO, fin de semaine) utilisés comme
axe de temps dans Power BI.
