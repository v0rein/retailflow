import pytest

EXPECTED_TASK_IDS = {
    "ingest_raw_to_bronze",
    "spark_bronze_to_silver",
    "dbt_build_gold",
}


def test_dag_loads_without_errors():
    # ``airflow/`` in the repo is a namespace package, so import the real
    # submodule to detect whether Apache Airflow is actually installed.
    pytest.importorskip("airflow.models")
    from airflow.models import DagBag

    dag_bag = DagBag(dag_folder="airflow/dags", include_examples=False)
    assert dag_bag.import_errors == {}, f"Import errors: {dag_bag.import_errors}"
    assert "retailflow_lakehouse_pipeline" in dag_bag.dags

    dag = dag_bag.get_dag("retailflow_lakehouse_pipeline")
    assert {task.task_id for task in dag.tasks} == EXPECTED_TASK_IDS
    # Concurrent runs would fight over the DuckDB lock and the Silver overwrite.
    assert dag.max_active_runs == 1

    spark_task = dag.get_task("spark_bronze_to_silver")
    assert spark_task.upstream_task_ids == {"ingest_raw_to_bronze"}
    assert spark_task.downstream_task_ids == {"dbt_build_gold"}
    assert dag.get_task("dbt_build_gold").downstream_task_ids == set()

    for task in dag.tasks:
        assert task.retries == 2, task.task_id
        assert task.execution_timeout is not None, task.task_id
