from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ETL.stage import run_stage_etl

default_args = {
    "owner": "marcelo",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="staging_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["staging", "analytics", "etl"],
) as dag:

    run_stage = PythonOperator(
        task_id="run_stage_etl",
        python_callable=run_stage_etl,
    )

    run_stage