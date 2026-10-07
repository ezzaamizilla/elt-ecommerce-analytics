"""Télécharge le dataset Olist (miroir public) dans data/raw/.

Usage:
    python scripts/fetch_olist.py

Priorité : miroir S3 Le Wagon (zip, 9 CSV) -> repli GitHub raw.
Idempotent : saute le téléchargement si les 9 CSV sont déjà présents.
"""

import pathlib
import sys
import time
import urllib.request
import zipfile

RAW_DIR = pathlib.Path(__file__).resolve().parents[1] / "data" / "raw"

CSV_FILES = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]

PRIMARY_ZIP = "https://wagon-public-datasets.s3.amazonaws.com/olist/olist.zip"
GITHUB_BASE = "https://raw.githubusercontent.com/Kaaykun/OlistAnalysis/master/data/csv/"


def already_downloaded() -> bool:
    return all(
        (RAW_DIR / name).exists() and (RAW_DIR / name).stat().st_size > 0
        for name in CSV_FILES
    )


def download(url: str, dest: pathlib.Path, retries: int = 3, timeout: int = 120) -> None:
    for attempt in range(1, retries + 1):
        try:
            print(f"  download ({attempt}/{retries}) {url}")
            with urllib.request.urlopen(url, timeout=timeout) as resp, open(dest, "wb") as out:
                out.write(resp.read())
            return
        except Exception as exc:  # noqa: BLE001 - retry sur toute erreur réseau
            print(f"  echec: {exc}")
            if attempt == retries:
                raise
            time.sleep(3)


def fetch_from_zip() -> bool:
    zip_path = RAW_DIR / "olist.zip"
    try:
        download(PRIMARY_ZIP, zip_path)
    except Exception as exc:  # noqa: BLE001
        print(f"[fallback] miroir S3 indisponible ({exc}) -> GitHub raw")
        return False

    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            if member.endswith(".csv"):
                target = RAW_DIR / pathlib.Path(member).name
                with zf.open(member) as src, open(target, "wb") as out:
                    out.write(src.read())
    zip_path.unlink(missing_ok=True)
    return True


def fetch_from_github() -> None:
    for name in CSV_FILES:
        download(GITHUB_BASE + name, RAW_DIR / name)


def main() -> int:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if already_downloaded():
        print(f"OK: 9 CSV déjà présents dans {RAW_DIR}")
        return 0

    print("Téléchargement du dataset Olist...")
    if not fetch_from_zip():
        fetch_from_github()

    missing = [n for n in CSV_FILES if not (RAW_DIR / n).exists()]
    if missing:
        print(f"ERREUR: fichiers manquants: {missing}")
        return 1

    for name in CSV_FILES:
        size = (RAW_DIR / name).stat().st_size
        print(f"  {name:<45} {size:>10,} octets")
    print("OK: dataset Olist prêt dans data/raw/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
