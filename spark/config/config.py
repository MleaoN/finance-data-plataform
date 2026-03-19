import os
from dotenv import load_dotenv

# Load .env file from the spark directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env_Spark")

load_dotenv(ENV_PATH)

class Config:
    DATA_DIR = os.getenv("DATA_DIR", "/opt/app/data")
    ETL_DIR = os.getenv("ETL_DIR", "/opt/app/ETL")

    #future DB configs
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_NAME = os.getenv("DB_NAME")