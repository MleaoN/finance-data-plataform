from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="spark_jobs_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # -------------------------
    # MOUNTS
    # -------------------------

    # Mount for Spark job scripts
    spark_code_mount = Mount(
        source="C:/DS_courses/projects/Finance_Docker/spark",
        target="/opt/app",
        type="bind"
    )

    # Mount for data lake (stage + curated)
    spark_data_mount = Mount(
        source="C:/DS_courses/projects/Finance_Docker/data",
        target="/opt/app/data",
        type="bind"
    )

    shared_mounts = [spark_code_mount, spark_data_mount]

    # -------------------------
    # SPARK JOB: MARKET
    # -------------------------
    spark_market = DockerOperator(
        task_id="spark_market",
        image="spark-spark",
        command="/opt/entrypoint.sh spark-submit /opt/app/run_job.py market",
        docker_url="unix://var/run/docker.sock",
        network_mode="spark_net",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=shared_mounts,
    )

    # -------------------------
    # SPARK JOB: MACRO
    # -------------------------
    spark_macro = DockerOperator(
        task_id="spark_macro",
        image="spark-spark",
        command="/opt/entrypoint.sh spark-submit /opt/app/run_job.py macro",
        docker_url="unix://var/run/docker.sock",
        network_mode="spark_net",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=shared_mounts,
    )

    # Task order
    spark_market >> spark_macro