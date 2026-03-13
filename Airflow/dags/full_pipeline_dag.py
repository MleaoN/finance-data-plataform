from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "marcelo",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="full_pipeline_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["pipeline", "orchestration", "etl"],
) as dag:

    trigger_macro = TriggerDagRunOperator(
        task_id="trigger_macro_etl",
        trigger_dag_id="macro_etl_pipeline",
    )

    trigger_market = TriggerDagRunOperator(
        task_id="trigger_market_etl",
        trigger_dag_id="market_etl_pipeline",
    )

    trigger_staging = TriggerDagRunOperator(
        task_id="trigger_staging_etl",
        trigger_dag_id="staging_etl_pipeline",
    )

    trigger_macro >> trigger_market >> trigger_staging