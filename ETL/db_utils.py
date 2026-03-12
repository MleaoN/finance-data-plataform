import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("DB_URL"))



def get_or_create_country(iso_code: str, name: str):
    query = text("""
        INSERT INTO countries (iso_code, name)
        VALUES (:iso, :name)
        ON CONFLICT (iso_code) DO UPDATE SET name = EXCLUDED.name
        RETURNING country_id;
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"iso": iso_code, "name": name})
        conn.commit()
        return result.scalar()

def get_or_create_indicator(code: str, name: str, source: str = "World Bank"):
    query = text("""
        INSERT INTO indicators (code, name, source)
        VALUES (:code, :name, :source)
        ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
        RETURNING indicator_id;
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"code": code, "name": name, "source": source})
        conn.commit()
        return result.scalar()

def insert_macro_record(country_id: int, indicator_id: int, year: int, value):
    query = text("""
        INSERT INTO macro_data (country_id, indicator_id, year, value)
        VALUES (:country_id, :indicator_id, :year, :value)
        ON CONFLICT (country_id, indicator_id, year)
        DO UPDATE SET value = EXCLUDED.value;
    """)
    with engine.connect() as conn:
        conn.execute(query, {
            "country_id": country_id,
            "indicator_id": indicator_id,
            "year": year,
            "value": value
        })
        conn.commit()

def get_or_create_ticker(symbol: str, name: str = None, exchange: str = None):
    query = text("""
        INSERT INTO tickers (symbol, name, exchange)
        VALUES (:symbol, :name, :exchange)
        ON CONFLICT (symbol) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, tickers.name),
            exchange = COALESCE(EXCLUDED.exchange, tickers.exchange)
        RETURNING ticker_id;
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {
            "symbol": symbol,
            "name": name,
            "exchange": exchange
        })
        conn.commit()
        return result.scalar()


def insert_stock_price(ticker_id: int, date, open_, high, low, close, volume):
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
    with engine.connect() as conn:
        conn.execute(query, {
            "ticker_id": ticker_id,
            "date": date,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume
        })
        conn.commit()