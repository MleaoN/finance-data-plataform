import os
import json
import pandas as pd
import re
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ---------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------
load_dotenv()

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise ValueError("Environment variable DB_URL is missing. Add it to your .env file.")

engine = create_engine(DB_URL)


# ---------------------------------------------------------
# JSON Normalization Helper
# ---------------------------------------------------------
def load_raw_json(value):
    """
    Safely load JSON from the database, handling:
    - double-encoded JSON
    - JSON strings inside JSON
    - lists containing dicts
    - malformed JSON
    """
    try:
        obj = json.loads(value)

        # Case: JSON is still a string → decode again
        if isinstance(obj, str):
            obj = json.loads(obj)

        # Case: JSON is a list → take first element
        if isinstance(obj, list) and len(obj) > 0:
            obj = obj[0]

        # Only return dicts
        return obj if isinstance(obj, dict) else {}

    except Exception:
        return {}


# ---------------------------------------------------------
# Stage macro values
# ---------------------------------------------------------
def stage_macro_values():
    print("Staging macro values...")

    df = pd.read_sql(
        "SELECT country_code, indicator_code, raw_json FROM wb_macro_raw",
        engine
    )

    records = []

    for _, row in df.iterrows():
        raw = load_raw_json(row["raw_json"])

        # Iterate over keys like YR2000, YR2001, etc.
        for key, val in raw.items():
            if re.match(r"YR\d{4}", key):
                year = int(key.replace("YR", ""))

                if val not in (None, "", "null"):
                    try:
                        value = float(val)
                    except Exception:
                        continue  # skip non-numeric values

                    records.append({
                        "country_code": row["country_code"],
                        "indicator_code": row["indicator_code"],
                        "year": year,
                        "value": value,
                    })

    out = pd.DataFrame(records)

    out.to_sql(
        "fact_macro_value",
        engine,
        if_exists="replace",
        index=False,
    )

    print(f"Inserted {len(out)} macro value records.")


# ---------------------------------------------------------
# Stage market values
# ---------------------------------------------------------
def stage_market_values():
    print("Staging market values...")

    df = pd.read_sql(
        """
        SELECT label, date, close, volume
        FROM market_raw
        WHERE close IS NOT NULL
        """,
        engine
    )

    df.to_sql(
        "fact_market_value",
        engine,
        if_exists="replace",
        index=False,
    )

    print(f"Inserted {len(df)} market value records.")


# ---------------------------------------------------------
# Run both staging steps
# ---------------------------------------------------------
def run_stage():
    stage_macro_values()
    stage_market_values()
    print("Staging complete.")


if __name__ == "__main__":
    run_stage()