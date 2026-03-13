from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ETL.stocks import run_market_etl

default_args = {
    "owner": "marcelo",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="market_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["finance", "etl", "market"],
) as dag:

    run_market = PythonOperator(
        task_id="run_market_etl",
        python_callable=run_market_etl,
    )

    run_market