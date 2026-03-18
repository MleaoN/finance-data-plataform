from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="init_schema",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # run manually or triggered by other DAGs
    catchup=False,
    tags=["infrastructure", "schema"],
) as dag:

    init_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="etl_db",   # Create this in Airflow UI
        sql="sql/schema.sql",        # Path relative to /opt/airflow
    )