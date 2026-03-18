-- ============================
-- DIMENSIONS
-- ============================

CREATE TABLE countries (
    country_id SERIAL PRIMARY KEY,
    iso_code VARCHAR(3) UNIQUE NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE indicators (
    indicator_id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name TEXT NOT NULL,
    source TEXT
);

CREATE TABLE tickers (
    ticker_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) UNIQUE NOT NULL,
    name TEXT,
    exchange TEXT
);

-- ============================
-- FACT TABLES
-- ============================

CREATE TABLE macro_data (
    id BIGSERIAL PRIMARY KEY,
    country_id INT REFERENCES countries(country_id),
    indicator_id INT REFERENCES indicators(indicator_id),
    year INT NOT NULL,
    value NUMERIC,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(country_id, indicator_id, year)
);

CREATE TABLE stock_prices (
    id BIGSERIAL PRIMARY KEY,
    ticker_id INT REFERENCES tickers(ticker_id),
    date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(ticker_id, date)
);

-- ============================
-- ETL AUDIT TABLE
-- ============================

CREATE TABLE etl_runs (
    run_id BIGSERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMP DEFAULT NOW(),
    finished_at TIMESTAMP
);