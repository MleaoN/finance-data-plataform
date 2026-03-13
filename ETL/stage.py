import pandas as pd
from ETL.etl_utils import engine, log_step

# ---------------------------------------------------------
# Stage macro data
# ---------------------------------------------------------

def stage_macro_timeseries():
    log_step("Staging macro timeseries...")

    query = """
        SELECT 
            c.iso_code AS country,
            i.code AS indicator,
            m.year,
            m.value
        FROM macro_data m
        JOIN countries c ON m.country_id = c.country_id
        JOIN indicators i ON m.indicator_id = i.indicator_id
        ORDER BY c.iso_code, i.code, m.year
    """

    df = pd.read_sql(query, engine)

    df.to_sql(
        "stage_macro_timeseries",
        engine,
        if_exists="replace",
        index=False
    )

    log_step(f"Inserted {len(df)} staged macro records.")

# ---------------------------------------------------------
# Stage market data
# ---------------------------------------------------------

def stage_market_daily():
    log_step("Staging market daily data...")

    query = """
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
    """

    df = pd.read_sql(query, engine)

    df.to_sql(
        "stage_market_daily",
        engine,
        if_exists="replace",
        index=False
    )

    log_step(f"Inserted {len(df)} staged market records.")

# ---------------------------------------------------------
# Airflow entrypoint
# ---------------------------------------------------------

def run_stage_etl():
    log_step("Starting STAGING ETL pipeline...")

    stage_macro_timeseries()
    stage_market_daily()

    log_step("STAGING ETL pipeline completed.")