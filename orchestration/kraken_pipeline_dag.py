"""
Kraken lakehouse orchestration DAG.

Orchestrates the cloud-side of the pipeline:
  - run dbt on Databricks
  - run dbt tests
  - verify mart table has rows
  - alert on failure

Silver/Gold PySpark jobs run on the host manually because they need
Windows-native PySpark.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/airflow/project")
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/transform/kraken_analytics"

default_args = {
    "owner": "mongezi",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="kraken_lakehouse_pipeline",
    description="Kraken -> dbt on Databricks -> data quality checks",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["kraken", "lakehouse", "dbt"],
) as dag:

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --no-partial-parse",
    )

    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --no-partial-parse",
    )

    def _check_mart_row_count(**context) -> None:
        import requests
        from airflow.models import Variable

        token = os.environ["DBT_DATABRICKS_TOKEN"]
        host = os.environ["DATABRICKS_HOST"].rstrip("/")
        http_path_id = Variable.get("databricks_http_path").split("/")[-1]
        catalog = Variable.get("databricks_catalog")
        schema = Variable.get("databricks_schema")

        sql = f"SELECT COUNT(*) AS c FROM {catalog}.{schema}.mart_symbol_summary"

        submit = requests.post(
            f"{host}/api/2.0/sql/statements",
            headers={"Authorization": f"Bearer {token}"},
            json={"warehouse_id": http_path_id, "statement": sql, "wait_timeout": "30s"},
            timeout=60,
        )
        submit.raise_for_status()
        result = submit.json()

        state = result.get("status", {}).get("state")
        if state != "SUCCEEDED":
            raise RuntimeError(f"Databricks query failed: {result}")

        row_count = int(result["result"]["data_array"][0][0])
        print(f"mart_symbol_summary row count = {row_count}")
        if row_count < 1:
            raise ValueError("mart_symbol_summary is empty")

    check_mart = PythonOperator(
        task_id="check_mart_row_count",
        python_callable=_check_mart_row_count,
    )

    def _notify_failure(**context) -> None:
        ti = context["task_instance"]
        print(f"ALERT (stub): task {ti.task_id} failed in dag {ti.dag_id}")

    notify_failure = PythonOperator(
        task_id="notify_failure",
        python_callable=_notify_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    run_dbt >> test_dbt >> check_mart
    [run_dbt, test_dbt, check_mart] >> notify_failure
