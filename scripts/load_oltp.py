"""Charge les CSV Olist dans PostgreSQL OLTP (schéma normalisé).

Usage (dans le réseau Docker):
    python scripts/load_olist.py   -> scripts/load_oltp.py

Idempotent : le schéma public est recréé à chaque exécution.
"""

import os
import pathlib
import sys
import time

import psycopg2
from psycopg2 import sql

RAW_DIR = pathlib.Path(__file__).resolve().parents[1] / "data" / "raw"

CONN_PARAMS = {
    "host": os.environ.get("POSTGRES_OLTP_HOST", "postgres-oltp"),
    "port": os.environ.get("POSTGRES_OLTP_PORT", "5432"),
    "user": os.environ.get("POSTGRES_OLTP_USER", "olist"),
    "password": os.environ.get("POSTGRES_OLTP_PASSWORD", "olist_secret"),
    "dbname": os.environ.get("POSTGRES_OLTP_DB", "olist_oltp"),
}

# table -> (fichier CSV, colonnes explicites)
TABLES = {
    "customers": (
        "olist_customers_dataset.csv",
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"],
    ),
    "sellers": (
        "olist_sellers_dataset.csv",
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    ),
    "products": (
        "olist_products_dataset.csv",
        ["product_id", "product_category_name", "product_name_length",
         "product_description_length", "product_photos_qty", "product_weight_g",
         "product_length_cm", "product_height_cm", "product_width_cm"],
    ),
    "category_translation": (
        "product_category_name_translation.csv",
        ["product_category_name", "product_category_name_english"],
    ),
    "orders": (
        "olist_orders_dataset.csv",
        ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
         "order_approved_at", "order_delivered_carrier_date",
         "order_delivered_customer_date", "order_estimated_delivery_date"],
    ),
    "order_items": (
        "olist_order_items_dataset.csv",
        ["order_id", "order_item_id", "product_id", "seller_id",
         "shipping_limit_date", "price", "freight_value"],
    ),
    "order_payments": (
        "olist_order_payments_dataset.csv",
        ["order_id", "payment_sequential", "payment_type",
         "payment_installments", "payment_value"],
    ),
    "order_reviews": (
        "olist_order_reviews_dataset.csv",
        ["review_id", "order_id", "review_score", "review_comment_title",
         "review_comment_message", "review_creation_date",
         "review_answer_timestamp"],
    ),
    "geolocation": (
        "olist_geolocation_dataset.csv",
        ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
         "geolocation_city", "geolocation_state"],
    ),
}

DDL = [
    """
    CREATE TABLE customers (
        customer_id               TEXT PRIMARY KEY,
        customer_unique_id        TEXT NOT NULL,
        customer_zip_code_prefix  INTEGER,
        customer_city             TEXT,
        customer_state            TEXT
    )
    """,
    """
    CREATE TABLE sellers (
        seller_id                 TEXT PRIMARY KEY,
        seller_zip_code_prefix    INTEGER,
        seller_city               TEXT,
        seller_state              TEXT
    )
    """,
    """
    CREATE TABLE products (
        product_id                    TEXT PRIMARY KEY,
        product_category_name         TEXT,
        product_name_length           INTEGER,
        product_description_length    INTEGER,
        product_photos_qty            INTEGER,
        product_weight_g              NUMERIC(12,2),
        product_length_cm             NUMERIC(10,2),
        product_height_cm             NUMERIC(10,2),
        product_width_cm              NUMERIC(10,2)
    )
    """,
    """
    CREATE TABLE category_translation (
        product_category_name          TEXT PRIMARY KEY,
        product_category_name_english  TEXT
    )
    """,
    """
    CREATE TABLE orders (
        order_id                     TEXT PRIMARY KEY,
        customer_id                  TEXT NOT NULL REFERENCES customers(customer_id),
        order_status                 TEXT NOT NULL,
        order_purchase_timestamp     TIMESTAMP,
        order_approved_at            TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP
    )
    """,
    """
    CREATE TABLE order_items (
        order_id            TEXT NOT NULL REFERENCES orders(order_id),
        order_item_id       INTEGER NOT NULL,
        product_id          TEXT NOT NULL REFERENCES products(product_id),
        seller_id           TEXT NOT NULL REFERENCES sellers(seller_id),
        shipping_limit_date TIMESTAMP,
        price               NUMERIC(12,2),
        freight_value       NUMERIC(12,2),
        PRIMARY KEY (order_id, order_item_id)
    )
    """,
    """
    CREATE TABLE order_payments (
        order_id              TEXT NOT NULL REFERENCES orders(order_id),
        payment_sequential    INTEGER NOT NULL,
        payment_type          TEXT,
        payment_installments  INTEGER,
        payment_value         NUMERIC(12,2),
        PRIMARY KEY (order_id, payment_sequential)
    )
    """,
    # Pas de PK : le dataset réel contient des doublons (dédup géré par dbt).
    """
    CREATE TABLE order_reviews (
        review_id               TEXT,
        order_id                TEXT,
        review_score            INTEGER,
        review_comment_title    TEXT,
        review_comment_message  TEXT,
        review_creation_date    DATE,
        review_answer_timestamp TIMESTAMP
    )
    """,
    # Pas de PK : plusieurs coordonnées par code postal.
    """
    CREATE TABLE geolocation (
        geolocation_zip_code_prefix  INTEGER,
        geolocation_lat              NUMERIC(10,6),
        geolocation_lng              NUMERIC(10,6),
        geolocation_city             TEXT,
        geolocation_state            TEXT
    )
    """,
]

POST_LOAD = [
    "CREATE INDEX idx_orders_customer_id   ON orders(customer_id)",
    "CREATE INDEX idx_orders_status        ON orders(order_status)",
    "CREATE INDEX idx_orders_purchase_date ON orders(order_purchase_timestamp)",
    "CREATE INDEX idx_items_product        ON order_items(product_id)",
    "CREATE INDEX idx_items_seller         ON order_items(seller_id)",
    "CREATE INDEX idx_reviews_order        ON order_reviews(order_id)",
    "CREATE INDEX idx_geo_zip              ON geolocation(geolocation_zip_code_prefix)",
    "CREATE INDEX idx_products_category    ON products(product_category_name)",
    "ANALYZE",
]


def wait_for_db(conn_params: dict, retries: int = 30) -> psycopg2.extensions.connection:
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(**conn_params)
        except psycopg2.OperationalError as exc:
            last_exc = exc
            print(f"  attente base ({attempt}/{retries})...")
            time.sleep(2)
    raise last_exc


def copy_csv(cur, table: str, columns: list[str], path: pathlib.Path) -> int:
    col_list = sql.SQL(", ").join(map(sql.Identifier, columns))
    stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
        sql.Identifier(table), col_list
    )
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        cur.copy_expert(stmt.as_string(cur), fh)
    cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table)))
    return cur.fetchone()[0]


def main() -> int:
    missing = [f for _, (f, _) in TABLES.items() if not (RAW_DIR / f).exists()]
    if missing:
        print(f"ERREUR: CSV manquants dans {RAW_DIR}: {missing}")
        print("Lance d'abord: python scripts/fetch_olist.py")
        return 1

    print(f"Connexion a {CONN_PARAMS['host']}:{CONN_PARAMS['port']}/{CONN_PARAMS['dbname']}")
    conn = wait_for_db(CONN_PARAMS)
    conn.autocommit = False
    cur = conn.cursor()

    print("Recreation du schema public (idempotent)...")
    cur.execute("DROP SCHEMA public CASCADE")
    cur.execute("CREATE SCHEMA public")

    print("Creation des tables...")
    for ddl in DDL:
        cur.execute(ddl)

    print("Chargement des CSV...")
    total = 0
    for table, (filename, columns) in TABLES.items():
        rows = copy_csv(cur, table, columns, RAW_DIR / filename)
        total += rows
        print(f"  {table:<22} {rows:>9,} lignes")

    print("Index + statistiques...")
    for stmt in POST_LOAD:
        cur.execute(stmt)

    conn.commit()
    print(f"OK: {total:,} lignes chargees dans {CONN_PARAMS['dbname']}")

    # --- controles de qualite ---
    cur.execute("""
        SELECT
          (SELECT count(*) FROM orders o LEFT JOIN customers c
             ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL) AS orders_orphelines,
          (SELECT count(*) FROM order_items i LEFT JOIN orders o
             ON i.order_id = o.order_id WHERE o.order_id IS NULL)          AS items_orphelins,
          (SELECT count(*) FROM order_items i LEFT JOIN products p
             ON i.product_id = p.product_id WHERE p.product_id IS NULL)    AS items_produit_inconnu,
          (SELECT count(*) FROM order_items i LEFT JOIN sellers s
             ON i.seller_id = s.seller_id WHERE s.seller_id IS NULL)       AS items_vendeur_inconnu,
          (SELECT count(*) FROM order_payments pay LEFT JOIN orders o
             ON pay.order_id = o.order_id WHERE o.order_id IS NULL)        AS paiements_orphelins,
          (SELECT count(*) FROM order_reviews r LEFT JOIN orders o
             ON r.order_id = o.order_id WHERE o.order_id IS NULL)          AS reviews_orphelines,
          (SELECT count(*) - count(DISTINCT review_id || order_id)
             FROM order_reviews)                                           AS reviews_doublons
    """)
    res = cur.fetchone()
    labels = ["orders_orphelines", "items_orphelins", "items_produit_inconnu",
              "items_vendeur_inconnu", "paiements_orphelins", "reviews_orphelines",
              "reviews_doublons"]
    print("Controles referentiels:")
    for label, val in zip(labels, res):
        print(f"  {label:<26} {val}")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
