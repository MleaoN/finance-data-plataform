📘 Finance Data Platform — Airflow‑Orchestrated ETL (Work in Progress)
A containerized data platform for collecting, transforming, and orchestrating financial and macroeconomic datasets.
This new version introduces a full Airflow orchestration layer, a reorganized ETL module structure, and a scalable foundation for future analytics and data engineering components.

🚀 What’s New in This Version
✔ Airflow Orchestration Layer
A fully containerized Airflow environment running:
- Webserver
- Scheduler
- Postgres metadata database
- Custom Airflow image with project‑specific Python dependencies
- DAGs for macro, market, staging, and full‑pipeline orchestration
✔ Modular ETL Architecture
Located in the ETL/ directory:
- macro.py — World Bank macroeconomic ingestion
- stocks.py — market data ingestion
- stage.py — staging and normalization logic
- etl_utils.py — shared helpers (DB connections, validation, logging)
✔ Full Pipeline DAG
A unified DAG (full_pipeline_dag.py) orchestrates:
- Extract
- Transform
- Stage
- Load
This ensures reproducible, dependency‑aware execution.

📂 Updated Project Structure
FINANCE_DATA_PLATFORM/
│
├── Airflow/
│   ├── airflow/                 # Custom Airflow image
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── dags/                    # Airflow DAGs
│   │   ├── full_pipeline_dag.py
│   │   ├── macro_etl_dag.py
│   │   ├── market_etl_dag.py
│   │   └── staging_etl_dag.py
│   │
│   ├── ETL/                     # Python ETL modules used by DAGs
│   │   ├── macro.py
│   │   ├── stocks.py
│   │   ├── stage.py
│   │   └── etl_utils.py
│   │
│   └── docker-compose.yml       # Airflow + Postgres services
│
├── README.md
└── .gitignore



🛠 Tech Stack (Current)
- Airflow 2.9
- Python 3.11
- Docker Compose
- PostgreSQL
- SQLAlchemy
- Pandas
- World Bank API (wbgapi)
- Market Data APIs

▶️ Running the Airflow Platform
From inside the Airflow/ directory:
docker compose build
docker compose up -d


Then access the Airflow UI:
http://localhost:8080


Default admin user is created automatically during initialization.

🔐 Environment Variables
Airflow uses a .env file (not committed to Git) for database and API configuration.
Example:
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow



🧭 Roadmap
This platform is actively evolving. Upcoming components include:
- [ ] Spark cluster for distributed transformations
- [ ] Data lake zones (raw, delta, curated)
- [ ] Dashboard layer (Streamlit / Superset)
- [ ] CI/CD pipeline
- [ ] Monitoring & logging
- [ ] Automated data quality checks (Great Expectations)

🤝 Contributing
This is a personal learning and portfolio project.
Suggestions, issues, and improvements are welcome.

📄 License
MIT License.
