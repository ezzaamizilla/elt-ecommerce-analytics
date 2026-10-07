"""Pipeline ELT e-commerce : extract OLTP -> bronze, puis dbt (Phase 5).

- Extraction idempotente : le schéma `bronze` du warehouse est recréé à chaque run.
- Scheduling : quotidien, sans rattrapage (catchup=False), 2 retries.
- Echec d'une task => notification dans les logs Airflow (callback).
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOGGER = logging.getLogger("airflow.elt_pipeline")

SOURCE_TABLES = [
    "customers",
    "sellers",
    "products",
    "category_translation",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "geolocation",
]

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def failure_callback(context) -> None:
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else "?"
    task_id = context.get("task_instance", {}).task_id
    exception = context.get("exception")
    LOGGER.error("ECHEC %s.%s: %s", dag_id, task_id, exception)


def reset_bronze(**_) -> None:
    """Recree le schema bronze : garantit l'idempotence du run."""
    warehouse = PostgresHook(postgres_conn_id="POSTGRES_WAREHOUSE")
    warehouse.run("DROP SCHEMA IF EXISTS bronze CASCADE")
    warehouse.run("CREATE SCHEMA bronze")
    LOGGER.info("schema bronze cree")


def extract_table(table: str):
    """Extrait une table OLTP -> warehouse (bronze) en streaming par lots.

    Le warehouse et l'OLTP sont deux bases distinctes : impossible de faire un
    INSERT ... SELECT inter-bases, donc on streame par curseur serveur puis COPY.
    Retourne le nombre de lignes extraites (vérifié contre la source).
    """

    def _extract(**_) -> None:
        oltp = PostgresHook(postgres_conn_id="POSTGRES_OLTP")
        warehouse = PostgresHook(postgres_conn_id="POSTGRES_WAREHOUSE")

        src_count = oltp.get_first(f"SELECT count(*) FROM public.{table}")[0]
        warehouse.run(f"DROP TABLE IF EXISTS bronze.{table}")

        col_names = [
            row[0]
            for row in oltp.get_records(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=%s "
                "ORDER BY ordinal_position",
                parameters=(table,),
            )
        ]
        col_list = ", ".join(f'"{c}"' for c in col_names)

        wh_conn = warehouse.get_conn()
        wh_conn.autocommit = False
        with wh_conn.cursor() as wh_cur:
            wh_cur.execute(f'CREATE TABLE bronze."{table}" ({_ddl_types(oltp, table)})')
            wh_conn.commit()

            # curseur serveur : pas de chargement du resultat entier en memoire
            with oltp.get_conn().cursor(name=f"extract_{table}") as cur:
                cur.itersize = 20_000
                cur.execute(f'SELECT {col_list} FROM public."{table}"')
                total = 0
                while True:
                    rows = cur.fetchmany(20_000)
                    if not rows:
                        break
                    _copy_rows(wh_conn, wh_cur, table, col_names, rows)
                    total += len(rows)

        if total != src_count:
            raise ValueError(
                f"{table}: extrait {total} lignes mais la source en a {src_count}"
            )
        LOGGER.info("%s: %s lignes extraites -> bronze.%s", table, total, table)

    return _extract


def _ddl_types(oltp: PostgresHook, table: str) -> str:
    """Recopie la definition de colonnes (type PostgreSQL) depuis l'OLTP."""
    rows = oltp.get_records(
        "SELECT column_name, data_type, character_maximum_length, numeric_precision, "
        "numeric_scale FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position",
        parameters=(table,),
    )
    parts = []
    for name, data_type, char_len, precision, scale in rows:
        if char_len:
            decl = f"{data_type}({char_len})"
        elif data_type == "numeric" and precision:
            decl = f"numeric({precision},{scale or 0})"
        else:
            decl = data_type
        parts.append(f'"{name}" {decl}')
    return ", ".join(parts)


def _copy_rows(wh_conn, wh_cur, table: str, col_names: list, rows: list) -> None:
    """COPY un lot depuis la memoire vers le warehouse (format CSV en memoire)."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    for row in rows:
        writer.writerow(["" if v is None else v for v in row])
    buf.seek(0)
    col_list = ", ".join(f'"{c}"' for c in col_names)
    wh_cur.copy_expert(
        f'COPY bronze."{table}" ({col_list}) FROM STDIN WITH (FORMAT csv)',
        buf,
    )
    wh_conn.commit()


with DAG(
    dag_id="elt_pipeline",
    description="ELT e-commerce : OLTP -> bronze -> dbt",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["elt", "olist", "portfolio"],
) as dag:

    init_bronze = PythonOperator(
        task_id="init_bronze",
        python_callable=reset_bronze,
        on_failure_callback=failure_callback,
    )

    extracts = [
        PythonOperator(
            task_id=f"extract_{table}",
            python_callable=extract_table(table),
            on_failure_callback=failure_callback,
        )
        for table in SOURCE_TABLES
    ]

    # --- dbt : qualite des sources, transformations, tests de donnees ---
    dbt_cmd = "dbt {command} --project-dir $DBT_PROJECT_DIR --profiles-dir $DBT_PROJECT_DIR"

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=dbt_cmd.format(command="source freshness"),
        on_failure_callback=failure_callback,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=dbt_cmd.format(command="run"),
        on_failure_callback=failure_callback,
    )

    # Un echec de test dbt fait echouer le DAG : pas de livraison de donnees
    # de mauvaise qualite dans les marts.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_cmd.format(command="test"),
        on_failure_callback=failure_callback,
    )

    init_bronze >> extracts >> dbt_source_freshness >> dbt_run >> dbt_test
