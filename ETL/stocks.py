import os
import json
import pandas as pd
import yfinance as yf
from typing import Dict
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
# Ticker groups
# ---------------------------------------------------------

FX_TICKERS = {
    "EURUSD=X": "eur_usd",
    "JPY=X": "usd_jpy",
    "BRL=X": "usd_brl",
    "CNY=X": "usd_cny",
}

INDEX_TICKERS = {
    "^GSPC": "sp500",
    "^BVSP": "bovespa",
    "^FTSE": "ftse_100",
    "^N225": "nikkei_225",
}

COMMODITY_TICKERS = {
    "CL=F": "wti_crude",
    "GC=F": "gold",
    "HG=F": "copper",
}

YIELD_TICKERS = {
    "^TNX": "us10y",
    "^FVX": "us5y",
    "^IRX": "us13w",
}

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def clean_json_value(x):
    return None if pd.isna(x) else x

# ---------------------------------------------------------
# Fetch Yahoo Finance history
# ---------------------------------------------------------

def fetch_yfinance_history(
    tickers: Dict[str, str],
    start_date: str,
    end_date: str,
    interval: str = "1d",
) -> pd.DataFrame:

    df = yf.download(
        tickers=list(tickers.keys()),
        start=start_date,
        end=end_date,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        threads=True,
    )

    records = []

    for symbol, label in tickers.items():
        if symbol not in df.columns.levels[0]:
            continue

        sub = df[symbol].reset_index()

        for _, row in sub.iterrows():
            raw = {
                "open": clean_json_value(row.get("Open")),
                "high": clean_json_value(row.get("High")),
                "low": clean_json_value(row.get("Low")),
                "close": clean_json_value(row.get("Close")),
                "adj_close": clean_json_value(row.get("Adj Close")),
                "volume": clean_json_value(row.get("Volume")),
            }

            records.append({
                "symbol": symbol,
                "label": label,
                "date": row["Date"],
                **raw,
                "raw_json": json.dumps(raw),
            })

    return pd.DataFrame(records)

# ---------------------------------------------------------
# Load into Postgres
# ---------------------------------------------------------

def load_market_raw(df: pd.DataFrame) -> None:
    df = df.copy()

    df.to_sql(
        "market_raw",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

if __name__ == "__main__":
    df_fx = fetch_yfinance_history(FX_TICKERS, "2000-01-01", "2024-12-31")
    df_idx = fetch_yfinance_history(INDEX_TICKERS, "2000-01-01", "2024-12-31")
    df_cmd = fetch_yfinance_history(COMMODITY_TICKERS, "2000-01-01", "2024-12-31")
    df_yld = fetch_yfinance_history(YIELD_TICKERS, "2000-01-01", "2024-12-31")

    df_all = pd.concat([df_fx, df_idx, df_cmd, df_yld], ignore_index=True)

    load_market_raw(df_all)