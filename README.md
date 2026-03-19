# 📘 Finance Data Platform — Airflow-Orchestrated ETL (Work in Progress)

A containerized data engineering platform for ingesting, staging, and transforming financial and macroeconomic datasets using Apache Airflow and Apache Spark, fully orchestrated through Docker.
Phase 1 delivers a stable, production‑style foundation for a modern data platform, including:
- A complete Airflow orchestration layer
- A modular Python ETL ingestion pipeline
- A Spark transformation layer running inside Docker
- A structured data lake with staged and curated zones
- Clean project organization ready for expansion into a full lakehouse


---

# 🚀 What’s New in Phase 1

## ✔ Fully Containerized Airflow Environment
A complete Airflow stack running inside Docker:
• 	Webserver
• 	Scheduler
• 	PostgreSQL metadata DB
• 	Custom Airflow image with project‑specific dependencies
• 	DAGs for macro ETL, market ETL, staging, and full‑pipeline orchestration
This provides reproducible, dependency‑aware execution across all workflows.

---

## ✔ Modular ETL Architecture

ETL modules are organized inside the `ETL/` directory:

* **macro.py** — World Bank macroeconomic ingestion
* **stocks.py** — Market data ingestion
* **stage.py** — Staging and normalization logic
* **etl_utils.py** — Shared utilities (DB connections, validation, logging)

This modular structure makes it easier to extend the pipeline and reuse components across workflows.

---

## ✔ Spark Transformation Layer (NEW)
Phase 1 introduces a Dockerized Spark runtime that performs heavy transformations:
- Spark jobs run inside a dedicated container (spark-spark)
- Airflow triggers Spark jobs via DockerOperator
- Data is read from /data/stage/ and written to /data/curated/
- Clean job routing via run_job.py
- Modular Spark jobs in spark_jobs/
This is the foundation for Phase 2’s lakehouse and Delta Lake integration.

---

## ✔ Full Pipeline DAG
The unified DAG (full_pipeline_dag.py) orchestrates:
- Schema initialization
- Macro ETL
- Market ETL
- Staging
- Spark transformations
This ensures deterministic, dependency‑aware execution of the entire platform.



---

# 📂 Project Structure

```# 📂 Project Structure

FINANCE_DATA_PLATFORM/
│
├── airflow/                      # Airflow orchestration layer
│   ├── dags/                     # DAG definitions
│   │   ├── full_pipeline_dag.py
│   │   ├── macro_etl_dag.py
│   │   ├── market_etl_dag.py
│   │   └── staging_etl_dag.py
│   │
│   ├── plugins/                  # (optional) custom operators/hooks
│   │
│   ├── .env_Spark                # Airflow enviroment setup│(ignored)
│   ├── Dockerfile                # Custom Airflow image
│   ├── requirements.txt
│   └── docker-compose.yaml       # Airflow stack (webserver, scheduler, db)
│
├── etl/                          # Python ETL ingestion layer
│   ├── __init__.py
│   ├── macro.py                  # World Bank ingestion
│   ├── stocks.py                 # Market data ingestion
│   ├── stage.py                  # Data normalization
│   ├── init_db.py                # DB schema initialization
│   ├── db_utils.py               # DB helpers
│   └── etl_utils.py              # Shared utilities
│
├── spark/                        # Spark processing layer
│   ├── config/
│   │   └── config.py             # Spark configs / constants
│   │
│   ├── jobs/                     # Transformation jobs (rename from spark_jobs)
│   │   ├── transform_macro.py
│   │   ├── transform_market.py
│   │   └── test_spark.py
│   │
│   ├── run_job.py                # Entry point for Spark jobs
│   ├── Dockerfile                # Spark container image
│   ├── docker-compose.yaml       # Spark service definition
│   └── .env.spark
│
├── data/                         # Data lake
│   ├── raw/                      # Immutable source data
│   ├── stage/                    # Cleaned / normalized
│   ├── curated/                  # Business-ready datasets
│   └── normalized/               # (optional) intermediate layer
│
├── app/                          # (optional future layer)
│   └── app.py                    # Dashboard / API entrypoint
│
├── .env                          # Global environment variables (ignored)
├── .env.example
├── .gitignore
├── docker-compose.yaml           # (optional) unified orchestration
├── requirements.txt
└── README.md
```

---

# 🛠 Tech Stack

Current technologies used in the platform:

* **Apache Airflow 2.9**
* **Python 3.11**
* **Docker & Docker Compose**
* **PostgreSQL**
* **SQLAlchemy**
* **Pandas**
* **World Bank API (`wbgapi`)**
* **Market Data APIs**

---

# ▶️ Running the Airflow Platform

Navigate to the Airflow directory:

```bash
cd Airflow
```

Build the containers:

```bash
docker compose build
```

Start the platform:

```bash
docker compose up -d
```

---

## Access the Airflow UI

Open:

```
http://localhost:8080
```

The default admin user is created automatically during initialization.

---

# 🔐 Environment Variables

Sensitive configuration is stored in a `.env` file that is **not committed to Git**.

Example configuration:

```
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```

Make sure to create your own `.env` file before running the platform.

---

# 🧭 Roadmap

This platform is actively evolving. Planned components include:

* [ ] **Delta Lake** (ACID transactions, schema evolution, time travel)
* [ ] **Data lake zones** (raw → stage → curated → gold)
* [ ] **Data quality checks** (Great Expectations or Spark validators)
* [ ] **Dashboard layer** (Dash / Streamlit / Superset)
* [ ] **CI/CD pipeline** for automated deployment
* [ ] **Monitoring and logging**
* [ ] **Automated data quality checks** (Great Expectations)

---

# 🤝 Contributing

This project is primarily a **personal learning and portfolio project**, but suggestions and improvements are welcome.

Feel free to open issues or submit pull requests.

---

# 📄 License

MIT License.
