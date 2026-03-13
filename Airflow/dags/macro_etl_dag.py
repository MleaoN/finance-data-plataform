from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ETL.macro import run_macro_etl

default_args = {
    "owner": "marcelo",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="macro_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    tags=["macro", "world_bank", "etl"],
) as dag:

    run_macro = PythonOperator(
        task_id="run_macro_etl",
        python_callable=run_macro_etl,
    )

    run_macro