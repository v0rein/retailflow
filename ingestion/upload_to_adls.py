import os
import shutil
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Olist file -> table folder under bronze/ (and the Silver table name).
# The Spark job imports this too, so the two layers can't drift apart.
# The geolocation CSV is skipped: no Silver or Gold model uses it.
BRONZE_TABLES = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_order_payments_dataset.csv": "payments",
    "olist_order_reviews_dataset.csv": "reviews",
    "product_category_name_translation.csv": "product_category_translation",
}


def upload_raw_file(local_path: str, layer: str, subpath: str) -> bool:
    src = Path(local_path)
    if not src.exists():
        raise FileNotFoundError(f"Source file not found: {local_path}")

    use_local = os.getenv("USE_LOCAL_STORAGE", "true").lower() == "true"
    if use_local:
        root = Path(os.getenv("LOCAL_LAKEHOUSE_ROOT", "./data/lakehouse"))
        target = root / layer / subpath
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, target)
        return True

    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container = os.getenv("AZURE_CONTAINER_NAME", "retailflow")
    if not conn_str:
        raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING")

    # ponytail: lazy import azure sdk to avoid hard dependency when USE_LOCAL_STORAGE=true
    from azure.storage.blob import BlobServiceClient

    blob_name = f"{layer}/{subpath}".replace("\\", "/")
    client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = client.get_blob_client(container=container, blob=blob_name)
    with open(src, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    return True


def upload_directory(local_dir: str, layer: str) -> dict[str, bool]:
    """Upload each Olist CSV to <layer>/<table>/<file>. A missing file raises."""
    source = Path(local_dir)
    if not source.is_dir():
        raise NotADirectoryError(f"Source directory not found: {local_dir}")

    return {
        file_name: upload_raw_file(
            str(source / file_name), layer=layer, subpath=f"{table}/{file_name}"
        )
        for file_name, table in BRONZE_TABLES.items()
    }


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print(
            "Usage:\n"
            "  python -m ingestion.upload_to_adls <file_path> <layer> <subpath>\n"
            "  python -m ingestion.upload_to_adls all <layer> <source_dir>\n"
            "'all' copies each Olist CSV to <layer>/<table>/<file>,\n"
            "e.g. bronze/orders/olist_orders_dataset.csv"
        )
        sys.exit(1)

    if sys.argv[1] == "all":
        if len(sys.argv) < 4:
            print("Usage: python -m ingestion.upload_to_adls all <layer> <source_dir>")
            sys.exit(1)
        uploaded = upload_directory(sys.argv[3], layer=sys.argv[2])
        print(f"Uploaded {len(uploaded)} files to {sys.argv[2]}/<table>/")
    else:
        if len(sys.argv) < 4:
            print("Usage: python -m ingestion.upload_to_adls <file_path> <layer> <subpath>")
            sys.exit(1)
        upload_raw_file(sys.argv[1], sys.argv[2], sys.argv[3])
