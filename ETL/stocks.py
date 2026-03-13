import pandas as pd
import yfinance as yf
from sqlalchemy import text

from ETL.db_utils import get_or_create_ticker
from ETL.etl_utils import (
    engine,
    FX_TICKERS,
    INDEX_TICKERS,
    COMMODITY_TICKERS,
    YIELD_TICKERS,
    log_step,
)

# ---------------------------------------------------------
# Extract
# ---------------------------------------------------------

def extract_group(tickers: dict, start_date: str, end_date: str) -> pd.DataFrame:
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


def extract_market(start_date: str, end_date: str) -> pd.DataFrame:
    log_step("Fetching market data from Yahoo Finance...")

    df_fx = extract_group(FX_TICKERS, start_date, end_date)
    df_idx = extract_group(INDEX_TICKERS, start_date, end_date)
    df_cmd = extract_group(COMMODITY_TICKERS, start_date, end_date)
    df_yld = extract_group(YIELD_TICKERS, start_date, end_date)

    df_all = pd.concat([df_fx, df_idx, df_cmd, df_yld], ignore_index=True)

    log_step(f"Fetched {len(df_all)} market rows.")
    return df_all

# ---------------------------------------------------------
# Load raw
# ---------------------------------------------------------

def load_market_raw(df: pd.DataFrame):
    log_step("Loading raw market data into market_raw...")
    df.to_sql(
        "market_raw",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    log_step("Raw market data loaded.")

# ---------------------------------------------------------
# Load normalized
# ---------------------------------------------------------

def clean_market_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.dropna(subset=["date", "close", "volume"])
    df = df[~(
        df["open"].isna() &
        df["high"].isna() &
        df["low"].isna() &
        df["close"].isna()
    )]
    df["volume"] = df["volume"].astype("Int64")
    return df


def load_market_normalized(df: pd.DataFrame):
    df = clean_market_df(df)

    log_step("Loading normalized market data...")

    ticker_map = {}
    for symbol, label in df[["symbol", "label"]].drop_duplicates().values:
        ticker_map[symbol] = get_or_create_ticker(symbol=symbol, name=label)

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

    with engine.begin() as conn:
        conn.execute(query, rows)

    log_step(f"Inserted {len(rows)} normalized market rows.")

# ---------------------------------------------------------
# Airflow entrypoint
# ---------------------------------------------------------

def run_market_etl(start_date="2000-01-01", end_date="2024-12-31"):
    log_step("Starting MARKET ETL pipeline...")

    df = extract_market(start_date, end_date)
    load_market_raw(df)
    load_market_normalized(df)

    log_step("MARKET ETL pipeline completed.")