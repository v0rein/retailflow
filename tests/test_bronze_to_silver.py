import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spark_jobs.bronze_to_silver import (
    build_session,
    clean_customers_df,
    clean_order_items_df,
    clean_orders_df,
    clean_payments_df,
    clean_reviews_df,
    run_bronze_to_silver,
)
from ingestion.upload_to_adls import BRONZE_TABLES

# Spark on Windows needs Hadoop's winutils (HADOOP_HOME) to write files.
# Tests that write real tables skip there and run in Docker/CI instead.
NO_FILE_WRITES = sys.platform == "win32" and not os.environ.get("HADOOP_HOME")
needs_file_writes = pytest.mark.skipif(
    NO_FILE_WRITES, reason="Spark cannot write files on Windows without HADOOP_HOME/winutils"
)


@pytest.fixture(scope="session")
def spark():
    if NO_FILE_WRITES:
        # Loading the Delta jar also needs winutils, and the in-memory tests
        # that still run here don't touch Delta, so use a plain session.
        session = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    else:
        # Same Delta-enabled session as the real job, on one core for speed.
        session = build_session("local[1]")
    session.conf.set("spark.sql.shuffle.partitions", "1")
    return session


def test_clean_orders_splits_valid_and_quarantine(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
    ])
    rows = [
        ("o1", "c1", "delivered", "2023-01-01 10:00:00"),
        (None, "c2", "delivered", "2023-01-01 11:00:00"),
        ("o3", None, "shipped", "2023-01-01 12:00:00"),
        ("o4", "c4", "invalid_status", "2023-01-01 13:00:00"),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    clean_df, quarantine_df = clean_orders_df(df)

    assert clean_df.count() == 1
    assert clean_df.first()["order_id"] == "o1"
    assert quarantine_df.count() == 3
    quarantine_reasons = [r["quarantine_reason"] for r in quarantine_df.collect()]
    assert any("null_order_id" in reason for reason in quarantine_reasons)
    assert any("null_customer_id" in reason for reason in quarantine_reasons)
    assert any("invalid_order_status" in reason for reason in quarantine_reasons)


def test_clean_orders_deduplicates_and_casts_timestamp(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
    ])
    rows = [
        ("o1", "c1", "delivered", "2023-01-01 10:00:00"),
        ("o1", "c1", "delivered", "2023-01-01 10:00:00"),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    clean_df, quarantine_df = clean_orders_df(df)

    assert clean_df.count() == 1
    assert quarantine_df.count() == 1
    assert quarantine_df.first()["quarantine_reason"] == "duplicate_key"
    assert isinstance(clean_df.schema["order_purchase_timestamp"].dataType, TimestampType)


def test_clean_order_items_quarantines_missing_keys_and_negative_values(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_item_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("price", StringType(), True),
        StructField("freight_value", StringType(), True),
    ])
    rows = [
        ("o1", "1", "p1", "s1", "100.0", "10.0"),
        ("o2", "1", None, "s1", "50.0", "5.0"),
        ("o3", "1", "p1", "s1", "-1.0", "5.0"),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    clean_df, quarantine_df = clean_order_items_df(df)

    assert clean_df.count() == 1
    assert quarantine_df.count() == 2
    reasons = " ".join(r["quarantine_reason"] for r in quarantine_df.collect())
    assert "null_product_id" in reasons
    assert "negative_price" in reasons
    assert isinstance(clean_df.schema["price"].dataType, DoubleType)


def test_clean_payments_quarantines_invalid_type_and_value(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_value", StringType(), True),
    ])
    rows = [
        ("o1", "1", "credit_card", "10.0"),
        ("o2", "1", "crypto", "10.0"),
        ("o3", "1", "boleto", "-2.0"),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    clean_df, quarantine_df = clean_payments_df(df)

    assert clean_df.count() == 1
    assert quarantine_df.count() == 2


def test_clean_reviews_quarantines_invalid_score(spark):
    schema = StructType([
        StructField("review_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("review_score", StringType(), True),
    ])
    rows = [
        ("r1", "o1", "5"),
        ("r2", "o2", "6"),
        (None, "o3", "3"),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    clean_df, quarantine_df = clean_reviews_df(df)

    assert clean_df.count() == 1
    assert quarantine_df.count() == 2
    reasons = " ".join(r["quarantine_reason"] for r in quarantine_df.collect())
    assert "invalid_review_score" in reasons
    assert "null_review_id" in reasons


def test_clean_customers_deduplicates_and_keeps_zip_as_string(spark):
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", StringType(), True),
    ])
    rows = [
        ("c1", "u1", "01001"),
        ("c1", "u1", "01001"),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    clean_df, quarantine_df = clean_customers_df(df)

    assert clean_df.count() == 1
    assert quarantine_df.count() == 1
    assert quarantine_df.first()["quarantine_reason"] == "duplicate_key"
    assert clean_df.first()["customer_zip_code_prefix"] == "01001"


def test_every_bronze_row_lands_in_clean_or_quarantine(spark):
    schema = StructType([
        StructField("review_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("review_score", StringType(), True),
    ])
    rows = [
        ("r1", "o1", "5"),
        ("r1", "o2", "4"),  # same review_id on another order: real Olist pattern, keep
        ("r2", "o3", "3"),
        ("r2", "o3", "3"),  # exact duplicate key: quarantine
        ("r3", "o4", "9"),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    clean_df, quarantine_df = clean_reviews_df(df)

    assert clean_df.count() + quarantine_df.count() == len(rows)
    assert clean_df.count() == 3
    reasons = sorted(r["quarantine_reason"] for r in quarantine_df.collect())
    assert reasons == ["duplicate_key", "invalid_review_score"]


def test_uncastable_and_null_values_are_quarantined(spark):
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("order_delivered_customer_date", StringType(), True),
    ])
    orders = spark.createDataFrame([
        ("o1", "c1", "shipped", "2023-01-01 10:00:00", None),  # undelivered: stays clean
        ("o2", "c2", None, "2023-01-01 10:00:00", None),
        ("o3", "c3", "delivered", "not-a-date", None),
    ], schema=orders_schema)
    clean_df, quarantine_df = clean_orders_df(orders)
    assert [r["order_id"] for r in clean_df.collect()] == ["o1"]
    reasons = {r["order_id"]: r["quarantine_reason"] for r in quarantine_df.collect()}
    assert reasons == {
        "o2": "invalid_order_status",
        "o3": "invalid_order_purchase_timestamp",
    }

    items_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_item_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("price", StringType(), True),
        StructField("freight_value", StringType(), True),
    ])
    items = spark.createDataFrame([("o1", "1", "p1", "s1", "abc", "1.0")], schema=items_schema)
    clean_df, quarantine_df = clean_order_items_df(items)
    assert clean_df.count() == 0
    assert "invalid_price" in quarantine_df.first()["quarantine_reason"]

    payments_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_value", StringType(), True),
    ])
    payments = spark.createDataFrame([("o1", "1", None, "10.0")], schema=payments_schema)
    clean_df, quarantine_df = clean_payments_df(payments)
    assert clean_df.count() == 0
    assert quarantine_df.first()["quarantine_reason"] == "invalid_payment_type"

    reviews_schema = StructType([
        StructField("review_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("review_score", StringType(), True),
    ])
    reviews = spark.createDataFrame([("r1", "o1", "x")], schema=reviews_schema)
    clean_df, quarantine_df = clean_reviews_df(reviews)
    assert clean_df.count() == 0
    assert quarantine_df.first()["quarantine_reason"] == "invalid_review_score"


# One small CSV per Bronze table: (header, rows). Each table has one bad row,
# except the translation lookup, which is copied without rules.
BRONZE_FIXTURE = {
    "orders": ("order_id,customer_id,order_status", ["o1,c1,delivered", "o2,c2,lost"]),
    "order_items": (
        "order_id,order_item_id,product_id,seller_id,price,freight_value",
        ["o1,1,p1,s1,10.0,1.0", "o1,2,p1,s1,-5.0,1.0"],
    ),
    "customers": ("customer_id,customer_unique_id", ["c1,u1", "c1,u1"]),
    "products": ("product_id,product_category_name", ["p1,beleza_saude", ",x"]),
    "sellers": ("seller_id,seller_zip_code_prefix", ["s1,01001", ",02002"]),
    "payments": (
        "order_id,payment_sequential,payment_type,payment_value",
        ["o1,1,boleto,11.0", "o1,2,crypto,1.0"],
    ),
    "reviews": ("review_id,order_id,review_score", ["r1,o1,5", "r2,o1,9"]),
    "product_category_translation": (
        "product_category_name,product_category_name_english",
        ["beleza_saude,health_beauty"],
    ),
}


@needs_file_writes
def test_job_reads_bronze_tables_and_reconciles(spark, tmp_path):
    bronze = tmp_path / "bronze"
    for file_name, table in BRONZE_TABLES.items():
        header, rows = BRONZE_FIXTURE[table]
        (bronze / table).mkdir(parents=True)
        (bronze / table / file_name).write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")

    run_bronze_to_silver(spark, bronze, tmp_path)
    # A second run must overwrite cleanly (idempotent) and add Delta version 1.
    stats = run_bronze_to_silver(spark, bronze, tmp_path)

    for table, (_, rows) in BRONZE_FIXTURE.items():
        counts = stats[table]
        # Reconciliation: every Bronze row is either clean or quarantined.
        assert counts["clean"] + counts["quarantined"] == len(rows), table
        silver_path = tmp_path / "silver" / table
        assert (silver_path / "_delta_log").is_dir(), table
        assert spark.read.format("delta").load(str(silver_path)).count() == counts["clean"], table
    quarantine = spark.read.format("delta").load(str(tmp_path / "bronze" / "quarantine" / "orders"))
    assert [r["quarantine_reason"] for r in quarantine.collect()] == ["invalid_order_status"]
    assert stats["orders"] == {"clean": 1, "quarantined": 1}
    assert stats["product_category_translation"] == {"clean": 1, "quarantined": 0}

    history = spark.sql(f"DESCRIBE HISTORY delta.`{tmp_path / 'silver' / 'orders'}`")
    assert history.agg(F.max("version")).first()[0] == 1


@needs_file_writes
def test_failed_delta_overwrite_keeps_previous_version(spark, tmp_path):
    # Why Silver is Delta: a plain Parquet overwrite deletes the old files
    # first, so a crash leaves an empty table. Delta only commits on success.
    path = str(tmp_path / "orders")
    spark.range(3).write.format("delta").mode("overwrite").save(path)

    failing = spark.range(5).withColumn(
        "id", F.when(F.col("id") == 4, F.raise_error(F.lit("boom"))).otherwise(F.col("id"))
    )
    with pytest.raises(Exception, match="boom"):
        failing.write.format("delta").mode("overwrite").save(path)

    assert spark.read.format("delta").load(path).count() == 3
