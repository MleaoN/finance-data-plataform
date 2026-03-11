import os
import json
import pandas as pd
import wbgapi as wb
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

# ---------------------------------------------------------
# Fetch and normalize one indicator
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
        except Exception:
            return None

    df["country_name"] = df["country_code"].apply(safe_country_name)
    df["raw_json"] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)

    return df[[
        "country_code", "country_name", "indicator_code",
        "indicator_name", "year", "value", "raw_json"
    ]]

# ---------------------------------------------------------
# Fetch all indicators
# ---------------------------------------------------------

def fetch_worldbank_macro_bundle(countries: List[str], start_year: int, end_year: int) -> pd.DataFrame:
    frames = [
        fetch_indicator(code, countries, start_year, end_year)
        for code in WORLD_BANK_INDICATORS
    ]
    return pd.concat(frames, ignore_index=True)

# ---------------------------------------------------------
# Load into Postgres
# ---------------------------------------------------------

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
# Main
# ---------------------------------------------------------

if __name__ == "__main__":
    df = fetch_worldbank_macro_bundle(
        countries=COUNTRIES,
        start_year=2000,
        end_year=2024,
    )
    load_wb_macro_raw(df)