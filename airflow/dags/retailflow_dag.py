import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow"
DBT_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    # Empty list when ALERT_EMAIL is unset, so DAG parsing never breaks.
    "email": [os.environ["ALERT_EMAIL"]] if os.environ.get("ALERT_EMAIL") else [],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # A hung Spark or dbt job fails after 1h instead of blocking the DAG forever.
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="retailflow_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-end Medallion Lakehouse ETL with PySpark and dbt",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # One run at a time: parallel runs would fight over the DuckDB write lock
    # and over the Silver Delta overwrite.
    max_active_runs=1,
    tags=["retailflow", "azure", "lakehouse"],
) as dag:

    ingest_raw_task = BashOperator(
        task_id="ingest_raw_to_bronze",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m ingestion.upload_to_adls all bronze data/raw"
        ),
    )

    spark_clean_task = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=f"cd {PROJECT_DIR} && python -m spark_jobs.bronze_to_silver",
    )

    # `dbt build` runs each model's tests right after building it, and a
    # failing test skips every model downstream of it.
    dbt_build_task = BashOperator(
        task_id="dbt_build_gold",
        bash_command=f"cd {DBT_DIR} && dbt build --profiles-dir . --no-partial-parse",
        # Metabase keeps its DuckDB connection open ~10 min after a dashboard
        # query (measured), which blocks this write. 2 retries x 10 min outlasts it.
        # ponytail: retry-based; upgrade to build-new-file-then-swap if dashboards
        # are queried constantly during the run.
        retry_delay=timedelta(minutes=10),
    )

    ingest_raw_task >> spark_clean_task >> dbt_build_task
