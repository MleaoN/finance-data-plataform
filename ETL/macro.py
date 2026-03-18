import pandas as pd
import wbgapi as wb
from pathlib import Path

from ETL.db_utils import (
    get_or_create_country,
    get_or_create_indicator,
    insert_macro_record,
)

from ETL.etl_utils import (
    engine,
    WORLD_BANK_INDICATORS,
    COUNTRIES,
    log_step,
    safe_json,
    DATA_ROOT,
    write_parquet,
)


# ---------------------------------------------------------
# Optimized Extract
# ---------------------------------------------------------

def extract_macro(start_year: int, end_year: int) -> pd.DataFrame:
    log_step("Fetching World Bank macroeconomic indicators (optimized)...")

    frames = []

    # Preload metadata ONCE
    indicator_meta = wb.series.list()
    indicator_lookup = {item['id']: item['value'] for item in indicator_meta}

    country_meta = wb.economy.list()
    country_lookup = {item['id']: item['value'] for item in country_meta}

    for indicator_code in WORLD_BANK_INDICATORS:
        log_step(f"Fetching {indicator_code}...")

        df = wb.data.DataFrame(
            indicator_code,
            COUNTRIES,
            time=range(start_year, end_year + 1)
        )

        if df is None or df.empty:
            continue

        df = df.reset_index()

        # Rename economy → country_code
        if "economy" in df.columns:
            df = df.rename(columns={"economy": "country_code"})

        # Identify year columns
        year_cols = [c for c in df.columns if c.startswith("YR")]

        # Vectorized melt
        df_long = df.melt(
            id_vars=["country_code"],
            value_vars=year_cols,
            var_name="year",
            value_name="value"
        )

        df_long["year"] = df_long["year"].str.replace("YR", "").astype(int)

        # Vectorized metadata lookup
        df_long["indicator_code"] = indicator_code
        df_long["indicator_name"] = indicator_lookup.get(indicator_code, None)
        df_long["country_name"] = df_long["country_code"].map(country_lookup)

        # Raw JSON (vectorized)
        df_long["raw_json"] = df_long.apply(lambda r: safe_json(r.to_dict()), axis=1)

        frames.append(df_long)

    df_final = pd.concat(frames, ignore_index=True)
    log_step(f"Fetched {len(df_final)} macro records (optimized).")

    return df_final[[
        "country_code", "country_name", "indicator_code",
        "indicator_name", "year", "value", "raw_json"
    ]]


# ---------------------------------------------------------
# Load raw
# ---------------------------------------------------------

def load_macro_raw(df: pd.DataFrame) -> None:
    log_step("Loading raw macro data into wb_macro_raw...")
    df.to_sql(
        "wb_macro_raw",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    log_step("Raw macro data loaded.")


# ---------------------------------------------------------
# Load normalized
# ---------------------------------------------------------

def load_macro_normalized(df: pd.DataFrame) -> None:
    log_step("Loading normalized macro data...")

    count = 0

    for _, row in df.iterrows():
        if pd.isna(row["year"]) or pd.isna(row["value"]):
            continue

        country_id = get_or_create_country(
            iso_code=row["country_code"],
            name=row["country_name"]
        )

        indicator_id = get_or_create_indicator(
            code=row["indicator_code"],
            name=row["indicator_name"]
        )

        insert_macro_record(
            country_id=country_id,
            indicator_id=indicator_id,
            year=int(row["year"]),
            value=row["value"]
        )
        count += 1

    log_step(f"Inserted {count} normalized macro records.")


# ---------------------------------------------------------
# Airflow entrypoint
# ---------------------------------------------------------

def run_macro_etl(start_year=2000, end_year=2024):
    log_step("Starting MACRO ETL pipeline (optimized)...")

    df = extract_macro(start_year, end_year)

    write_parquet(df, "raw/macro.parquet")

    load_macro_raw(df)
    load_macro_normalized(df)

    log_step("MACRO ETL pipeline completed.")