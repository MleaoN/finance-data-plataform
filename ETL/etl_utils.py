import os
import json
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
# Shared constants
# ---------------------------------------------------------

WORLD_BANK_INDICATORS = {
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

# ---------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------

def clean_json_value(x):
    return None if pd.isna(x) else x

def log_step(message: str):
    print(f"🔹 {message}")

def safe_json(row: dict) -> str:
    try:
        return json.dumps(row)
    except Exception:
        return "{}"