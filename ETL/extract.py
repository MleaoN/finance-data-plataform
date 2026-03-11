import os
import json
import pandas as pd
import wbgapi as wb
import yfinance as yf
from typing import List, Dict
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
# Constants
# ---------------------------------------------------------

WORLD_BANK_INDICATORS: Dict[str, str] = {
    "NY.GDP.MKTP.KD.ZG": "gdp_growth",
    "FP.CPI.TOTL.ZG": "inflation",
    "SL.UEM.TOTL.ZS": "unemployment",
    "GC.DOD.TOTL.GD.ZS": "debt_gdp",
    "BN.CAB.XOKA.GD.ZS": "current_account_gdp",
}

COUNTRIES = ["BRA", "USA", "CAN", "MEX", "CHN", "IND"]

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

MIN_START = {
    "EURUSD=X": "2003-01-01",
    "JPY=X": "2000-01-01",
    "BRL=X": "2000-01-01",
    "CNY=X": "2000-01-01",
    "^GSPC": "2000-01-01",
    "^BVSP": "2000-01-01",
    "^FTSE": "2000-01-01",
    "^N225": "2000-01-01",
    "CL=F": "2000-01-01",
    "GC=F": "2000-01-01",
    "HG=F": "2000-01-01",
    "^TNX": "2000-01-01",
    "^FVX": "2000-01-01",
    "^IRX": "2000-01-01",
}

# ---------------------------------------------------------
# World Bank Fetch
# ---------------------------------------------------------

def fetch_indicator(indicator_code: str, countries: List[str], start_year: int, end_year: int) -> pd.DataFrame:
    df = wb.data.DataFrame(
        indicator_code,
        countries,
        time=range(start_year, end_year + 1)
    )

    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "country_code", "country_name", "indicator_code",
            "indicator_name", "year", "value", "raw_json"
        ])

    df = df.reset_index()

    rename_map = {}
    if "economy" in df.columns:
        rename_map["economy"] = "country_code"
    if "Time" in df.columns:
        rename_map["Time"] = "year"
    if indicator_code in df.columns:
        rename_map[indicator_code] = "value"

    df = df.rename(columns=rename_map)

    for col in ["country_code", "year", "value"]:
        if col not in df.columns:
            df[col] = None

    df["indicator_code"] = indicator_code
    df["indicator_name"] = wb.series.get(indicator_code)["value"]

    def safe_country_name(code):
        try:
            return wb.economy.get(code)["value"]
        except:
            return None

    df["country_name"] = df["country_code"].apply(safe_country_name)
    df["raw_json"] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)

    return df[[
        "country_code", "country_name", "indicator_code",
        "indicator_name", "year", "value", "raw_json"
    ]]


def fetch_worldbank_macro_bundle(countries: List[str], start_year: int, end_year: int) -> pd.DataFrame:
    frames = [
        fetch_indicator(code, countries, start_year, end_year)
        for code in WORLD_BANK_INDICATORS
    ]
    return pd.concat(frames, ignore_index=True)


def load_wb_macro_raw(df: pd.DataFrame) -> None:
    df = df.copy()
    df["raw_json"] = df["raw_json"].apply(json.dumps)

    df.to_sql(
        "wb_macro_raw",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )

# ---------------------------------------------------------
# Yahoo Finance Fetch
# ---------------------------------------------------------

def clean_json_value(x):
    return None if pd.isna(x) else x


def fetch_yfinance_history(tickers, start_date, end_date, interval="1d"):
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
        sub = sub[sub["Date"] >= pd.to_datetime(MIN_START.get(symbol, start_date))]
        sub = sub.dropna(subset=["Open", "High", "Low", "Close"], how="all")

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


def load_market_raw(df):
    df.to_sql(
        "market_raw",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )

# ---------------------------------------------------------
# Main Execution
# ---------------------------------------------------------

if __name__ == "__main__":
    # World Bank
    df = fetch_worldbank_macro_bundle(COUNTRIES, 2000, 2024)
    load_wb_macro_raw(df)

    # Market Data
    df_fx = fetch_yfinance_history(FX_TICKERS, "2000-01-01", "2024-12-31")
    df_idx = fetch_yfinance_history(INDEX_TICKERS, "2000-01-01", "2024-12-31")
    df_cmd = fetch_yfinance_history(COMMODITY_TICKERS, "2000-01-01", "2024-12-31")
    df_yld = fetch_yfinance_history(YIELD_TICKERS, "2000-01-01", "2024-12-31")

    df_all = pd.concat([df_fx, df_idx, df_cmd, df_yld], ignore_index=True)
    load_market_raw(df_all)