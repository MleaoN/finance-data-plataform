# 📘 Finance Data Platform — Airflow-Orchestrated ETL (Work in Progress)

A **containerized data platform** for collecting, transforming, and orchestrating financial and macroeconomic datasets.

This version introduces:

* A full **Airflow orchestration layer**
* A reorganized **modular ETL architecture**
* A **Dockerized environment** for reproducible development
* A scalable foundation for future **data engineering and analytics components**

---

# 🚀 What’s New in This Version

## ✔ Airflow Orchestration Layer

A fully containerized **Apache Airflow environment** running:

* Webserver
* Scheduler
* PostgreSQL metadata database
* Custom Airflow image with project-specific Python dependencies
* DAGs for macro, market, staging, and full-pipeline orchestration

---

## ✔ Modular ETL Architecture

ETL modules are organized inside the `ETL/` directory:

* **macro.py** — World Bank macroeconomic ingestion
* **stocks.py** — Market data ingestion
* **stage.py** — Staging and normalization logic
* **etl_utils.py** — Shared utilities (DB connections, validation, logging)

This modular structure makes it easier to extend the pipeline and reuse components across workflows.

---

## ✔ Full Pipeline DAG

A unified Airflow DAG (`full_pipeline_dag.py`) orchestrates:

1. Extract
2. Transform
3. Stage
4. Load

This ensures **reproducible, dependency-aware execution** of the entire pipeline.

---

# 📂 Project Structure

```
FINANCE_DATA_PLATFORM/
│
├── Airflow/
│   │
│   ├── airflow/                     # Custom Airflow image
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── dags/                        # Airflow DAG definitions
│   │   ├── full_pipeline_dag.py
│   │   ├── macro_etl_dag.py
│   │   ├── market_etl_dag.py
│   │   └── staging_etl_dag.py
│   │
│   ├── docker-compose.yml           # Airflow services (webserver, scheduler, postgres)
│   └── .env                         # Airflow environment variables (not committed)
│
├── ETL/                             # Core ETL modules used by Airflow
│   ├── __init__.py
│   ├── macro.py                     # World Bank macroeconomic ingestion
│   ├── stocks.py                    # Market data ingestion
│   ├── stage.py                     # Staging & normalization logic
│   └── etl_utils.py                 # Shared helpers (DB connection, logging, validation)
│
├── db/
│   └── schema.sql                   # Database schema definitions
│
├── .devcontainer/                   # VS Code Dev Container configuration
│   ├── devcontainer.json
│   └── Dockerfile
│
├── .env.example                     # Environment variable template
├── .gitignore
├── requirements.txt                 # Local development dependencies
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

* [ ] **Apache Spark cluster** for distributed transformations
* [ ] **Data lake architecture** (raw / delta / curated zones)
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
