import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def load_schema(schema_path: str):
    """Read the SQL schema file."""
    with open(schema_path, "r", encoding="utf-8") as file:
        return file.read()

def initialize_database():
    """Create tables in Postgres using schema.sql."""
    load_dotenv()

    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL not found in environment variables")

    engine = create_engine(db_url)

    schema_path = os.path.join(os.path.dirname(__file__), "..", "db", "schema.sql")
    schema_sql = load_schema(schema_path)

    print("📦 Initializing database schema...")

    with engine.connect() as conn:
        conn.execute(text(schema_sql))
        conn.commit()

    print("✅ Database initialized successfully")

if __name__ == "__main__":
    initialize_database()