from __future__ import annotations

import argparse
import os
from pathlib import Path

from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ingestion.upload_to_adls import BRONZE_TABLES

# ponytail: hardcoded validation rules; upgrade to config-driven rule engine when onboarding more bronze tables
VALID_ORDER_STATUSES = {
    "created",
    "approved",
    "delivered",
    "shipped",
    "canceled",
    "invoiced",
    "processing",
    "unavailable",
}

VALID_PAYMENT_TYPES = {
    "credit_card",
    "boleto",
    "voucher",
    "debit_card",
    "not_defined",
}

ORDER_TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def _cast_present(df: DataFrame, columns: dict[str, str]) -> DataFrame:
    """Cast columns that exist in the frame and record values that failed to cast.

    Spark silently turns uncastable text (price="abc") into NULL, so before
    casting we note each column that has a value now but would be NULL after.
    Empty values are not flagged here; required columns use _null_flags.
    """
    present = {column: data_type for column, data_type in columns.items() if column in df.columns}
    cast_errors = [
        F.when(
            F.col(column).isNotNull() & F.col(column).cast(data_type).isNull(),
            F.lit(f"invalid_{column}"),
        )
        for column, data_type in present.items()
    ]
    df = df.withColumn("_cast_errors", F.array(*cast_errors).cast("array<string>"))
    for column, data_type in present.items():
        df = df.withColumn(column, F.col(column).cast(data_type))
    return df


def _flag_and_split(
    df: DataFrame,
    flags: list,
    key_columns: list[str],
) -> tuple[DataFrame, DataFrame]:
    """Split rows into clean and quarantine. Every input row lands in exactly one."""
    # A value like review_score="x" fails both the cast and the range rule, so
    # array_distinct keeps the reason from appearing twice. concat_ws skips NULLs.
    reasons = F.array_distinct(F.concat(F.col("_cast_errors"), F.array(*flags)))
    data_columns = [column for column in df.columns if column != "_cast_errors"]
    flagged = df.withColumn("quarantine_reason", F.concat_ws(";", reasons)).drop("_cast_errors")

    # Keep one passing row per key; extra copies go to quarantine instead of
    # vanishing. Passing rows (empty reason) sort first in each key group, and
    # sorting by every column makes the kept row the same on every run.
    per_key = Window.partitionBy(*key_columns).orderBy("quarantine_reason", *data_columns)
    is_duplicate = (F.col("quarantine_reason") == "") & (F.row_number().over(per_key) > 1)
    flagged = flagged.withColumn(
        "quarantine_reason",
        F.when(is_duplicate, F.lit("duplicate_key")).otherwise(F.col("quarantine_reason")),
    )

    clean_df = flagged.filter(F.col("quarantine_reason") == "").drop("quarantine_reason")
    quarantine_df = flagged.filter(F.col("quarantine_reason") != "")
    return clean_df, quarantine_df


def _null_flags(columns: list[str]) -> list:
    return [
        F.when(F.col(column).isNull(), F.lit(f"null_{column}"))
        for column in columns
    ]


def clean_orders_df(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    # Approval/carrier/delivery timestamps are NULL by design for orders that
    # have not reached that step yet, so only uncastable values are flagged.
    df = _cast_present(df, {column: "timestamp" for column in ORDER_TIMESTAMP_COLUMNS})
    flags = [
        *_null_flags(["order_id", "customer_id"]),
        F.when(
            F.col("order_status").isNull()
            | ~F.col("order_status").isin(list(VALID_ORDER_STATUSES)),
            F.lit("invalid_order_status"),
        ),
    ]
    return _flag_and_split(df, flags, ["order_id"])


def clean_order_items_df(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = _cast_present(
        df,
        {
            "order_item_id": "int",
            "shipping_limit_date": "timestamp",
            "price": "double",
            "freight_value": "double",
        },
    )
    flags = [
        *_null_flags(["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"]),
        F.when(F.col("price") < 0, F.lit("negative_price")),
        F.when(F.col("freight_value") < 0, F.lit("negative_freight_value")),
    ]
    return _flag_and_split(df, flags, ["order_id", "order_item_id"])


def clean_customers_df(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = _cast_present(df, {"customer_zip_code_prefix": "string"})
    flags = _null_flags(["customer_id", "customer_unique_id"])
    return _flag_and_split(df, flags, ["customer_id"])


def clean_products_df(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = _cast_present(
        df,
        {
            "product_name_lenght": "int",
            "product_description_lenght": "int",
            "product_photos_qty": "int",
            "product_weight_g": "double",
            "product_length_cm": "double",
            "product_height_cm": "double",
            "product_width_cm": "double",
        },
    )
    flags = _null_flags(["product_id"])
    return _flag_and_split(df, flags, ["product_id"])


def clean_sellers_df(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = _cast_present(df, {"seller_zip_code_prefix": "string"})
    flags = _null_flags(["seller_id"])
    return _flag_and_split(df, flags, ["seller_id"])


def clean_payments_df(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = _cast_present(
        df,
        {
            "payment_sequential": "int",
            "payment_installments": "int",
            "payment_value": "double",
        },
    )
    flags = [
        *_null_flags(["order_id", "payment_sequential"]),
        F.when(
            F.col("payment_type").isNull()
            | ~F.col("payment_type").isin(list(VALID_PAYMENT_TYPES)),
            F.lit("invalid_payment_type"),
        ),
        F.when(
            F.col("payment_value").isNull() | (F.col("payment_value") < 0),
            F.lit("invalid_payment_value"),
        ),
    ]
    return _flag_and_split(df, flags, ["order_id", "payment_sequential"])


def clean_reviews_df(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = _cast_present(
        df,
        {
            "review_score": "int",
            "review_creation_date": "timestamp",
            "review_answer_timestamp": "timestamp",
        },
    )
    flags = [
        *_null_flags(["review_id", "order_id"]),
        F.when(
            F.col("review_score").isNull() | ~F.col("review_score").between(1, 5),
            F.lit("invalid_review_score"),
        ),
    ]
    # Review comments are NULL for most real reviews and stay allowed.
    # One review_id can cover several orders in Olist, so the key is the pair.
    return _flag_and_split(df, flags, ["review_id", "order_id"])


# Silver table -> cleaning rules. The category translation is a small lookup
# with no business keys to check, so it has no cleaner and is copied as is.
CLEANERS = {
    "orders": clean_orders_df,
    "order_items": clean_order_items_df,
    "customers": clean_customers_df,
    "products": clean_products_df,
    "sellers": clean_sellers_df,
    "payments": clean_payments_df,
    "reviews": clean_reviews_df,
}


def load_raw_csv(spark: SparkSession, path: Path) -> DataFrame:
    # Read everything as text so leading-zero zip codes survive and malformed
    # values are handled by the per-table casting/quarantine rules.
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(path))
    )


def _write_delta(df: DataFrame, path: Path) -> None:
    # Delta commits through _delta_log only after every file is written, so a
    # crashed overwrite leaves readers on the previous version. Plain Parquet
    # overwrite deletes the old files first and can leave an empty table.
    df.write.format("delta").mode("overwrite").save(str(path))


def run_bronze_to_silver(
    spark: SparkSession,
    raw_dir: str | Path,
    lakehouse_root: str | Path,
) -> dict[str, dict[str, int]]:
    """Clean every Bronze table (bronze/<table>/<file>.csv) into Silver + quarantine Delta tables."""
    raw_dir = Path(raw_dir)
    lakehouse_root = Path(lakehouse_root)
    silver_root = lakehouse_root / "silver"
    quarantine_root = lakehouse_root / "bronze" / "quarantine"

    stats: dict[str, dict[str, int]] = {}

    for file_name, table_name in BRONZE_TABLES.items():
        source = raw_dir / table_name / file_name
        if not source.exists():
            raise FileNotFoundError(f"Missing Bronze input: {source}")

        raw_df = load_raw_csv(spark, source)
        if table_name not in CLEANERS:
            _write_delta(raw_df, silver_root / table_name)
            stats[table_name] = {"clean": raw_df.count(), "quarantined": 0}
            continue

        clean_df, quarantine_df = CLEANERS[table_name](raw_df)
        clean_count = clean_df.count()
        quarantine_count = quarantine_df.count()
        _write_delta(clean_df, silver_root / table_name)
        # Written even when empty, so a rerun replaces the last run's bad rows.
        _write_delta(quarantine_df, quarantine_root / table_name)

        stats[table_name] = {
            "clean": clean_count,
            "quarantined": quarantine_count,
        }

    return stats


def build_session(master: str = "local[*]") -> SparkSession:
    builder = (
        SparkSession.builder.master(master)
        .appName("retailflow-bronze-to-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    # Adds the delta-spark jar matching the pip package (downloaded once from Maven).
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> None:
    parser = argparse.ArgumentParser(description="RetailFlow Bronze -> Silver")
    parser.add_argument(
        "--lakehouse-root",
        default=os.getenv("LOCAL_LAKEHOUSE_ROOT", "data/lakehouse"),
    )
    parser.add_argument(
        "--raw-dir",
        help="Bronze folder holding <table>/<file>.csv (default: <lakehouse-root>/bronze)",
    )
    args = parser.parse_args()
    raw_dir = args.raw_dir or Path(args.lakehouse_root) / "bronze"

    spark = build_session()
    try:
        stats = run_bronze_to_silver(spark, raw_dir, args.lakehouse_root)
    finally:
        spark.stop()

    for table, counts in stats.items():
        print(f"{table}: {counts['clean']} clean, {counts['quarantined']} quarantined")


if __name__ == "__main__":
    main()
