import os
import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")

# Validate required variables
missing = [k for k, v in {
    "DB_NAME": DB_NAME,
    "DB_USER": DB_USER,
    "DB_PASSWORD": DB_PASSWORD
}.items() if not v]

if missing:
    raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

# ---------------------------------------------------------
# Connect to Postgres
# ---------------------------------------------------------
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    print("Connected to Postgres!")
    conn.close()

except Exception as e:
    print("Failed to connect to Postgres:", e)