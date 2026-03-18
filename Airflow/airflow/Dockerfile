FROM apache/airflow:2.9.0-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends procps

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt