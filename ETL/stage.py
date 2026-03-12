import os
import pandas as pd
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
# Stage macro data (from normalized tables)
# ---------------------------------------------------------
def stage_macro_timeseries():
    print("Staging macro timeseries...")

    df = pd.read_sql("""
        SELECT 
            c.iso_code AS country,
            i.code AS indicator,
            m.year,
            m.value
        FROM macro_data m
        JOIN countries c ON m.country_id = c.country_id
        JOIN indicators i ON m.indicator_id = i.indicator_id
        ORDER BY c.iso_code, i.code, m.year
    """, engine)

    df.to_sql(
        "stage_macro_timeseries",
        engine,
        if_exists="replace",
        index=False
    )

    print(f"Inserted {len(df)} staged macro records.")

# ---------------------------------------------------------
# Stage market data (from normalized tables)
# ---------------------------------------------------------
def stage_market_daily():
    print("Staging market daily data...")

    df = pd.read_sql("""
        SELECT 
            t.symbol,
            t.name AS label,
            s.date,
            s.open,
            s.high,
            s.low,
            s.close,
            s.volume
        FROM stock_prices s
        JOIN tickers t ON s.ticker_id = t.ticker_id
        ORDER BY t.symbol, s.date
    """, engine)

    df.to_sql(
        "stage_market_daily",
        engine,
        if_exists="replace",
        index=False
    )

    print(f"Inserted {len(df)} staged market records.")

# ---------------------------------------------------------
# Run staging
# ---------------------------------------------------------
def run_stage():
    stage_macro_timeseries()
    stage_market_daily()
    print("Staging complete.")

if __name__ == "__main__":
    run_stage()