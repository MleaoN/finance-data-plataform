import os
import json
import pandas as pd
import yfinance as yf
from typing import Dict
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from ETL.db_utils import get_or_create_ticker

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
# Fetch Yahoo Finance history (vectorized)
# ---------------------------------------------------------

def fetch_yfinance_history(tickers: Dict[str, str], start_date: str, end_date: str) -> pd.DataFrame:
    df = yf.download(
        tickers=list(tickers.keys()),
        start=start_date,
        end=end_date,
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
    )

    records = []

    for symbol, label in tickers.items():
        if symbol not in df.columns.levels[0]:
            continue

        sub = df[symbol].reset_index()

        sub["symbol"] = symbol
        sub["label"] = label

        sub.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }, inplace=True)

        records.append(sub[[
            "symbol", "label", "date",
            "open", "high", "low", "close", "adj_close", "volume"
        ]])

    return pd.concat(records, ignore_index=True)

# ---------------------------------------------------------
# Load raw market data (fast)
# ---------------------------------------------------------

def load_market_raw(df: pd.DataFrame):
    df.to_sql(
        "market_raw",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )

# ---------------------------------------------------------
# Clean + validate market data (vectorized)
# ---------------------------------------------------------

def clean_market_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Remove rows with missing date or close
    df = df.dropna(subset=["date", "close", "volume"])

    # Remove rows where all OHLC are NaN
    df = df[~(
        df["open"].isna() &
        df["high"].isna() &
        df["low"].isna() &
        df["close"].isna()
    )]

    # Convert volume to int safely
    df["volume"] = df["volume"].astype("Int64")

    return df

# ---------------------------------------------------------
# Batch insert normalized market data (super fast)
# ---------------------------------------------------------

def load_market_to_db(df: pd.DataFrame):
    df = clean_market_df(df)

    # Preload tickers once
    ticker_map = {}
    for symbol, label in df[["symbol", "label"]].drop_duplicates().values:
        ticker_map[symbol] = get_or_create_ticker(symbol=symbol, name=label)

    # Prepare rows for batch insert
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "ticker_id": ticker_map[row["symbol"]],
            "date": row["date"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": int(row["volume"]) if pd.notna(row["volume"]) else None,
        })

    query = text("""
        INSERT INTO stock_prices (
            ticker_id, date, open, high, low, close, volume
        )
        VALUES (
            :ticker_id, :date, :open, :high, :low, :close, :volume
        )
        ON CONFLICT (ticker_id, date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
    """)

    # Batch insert
    with engine.begin() as conn:
        conn.execute(query, rows)

    print(f"✅ Inserted {len(rows)} normalized market rows")

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
    load_market_to_db(df_all)