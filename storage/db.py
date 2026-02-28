import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_URL
from utils.logger import get_logger

logger = get_logger("storage.db")

engine = create_engine(DB_URL)


def init_db():
    """Create tables if they don't exist."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                ticker TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                fetched_at TIMESTAMP,
                UNIQUE(date, ticker)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS processed_stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                ticker TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                ma_7 REAL,
                ma_30 REAL,
                daily_pct_change REAL,
                volatility_7d REAL,
                above_ma30 INTEGER,
                UNIQUE(date, ticker)
            )
        """))
        conn.commit()
    logger.info("Database initialized.")


def save_raw(df: pd.DataFrame):
    """Insert raw stock data — skip duplicates."""
    if df.empty:
        return
    _upsert(df[["date", "ticker", "open", "high", "low", "close", "volume", "fetched_at"]], "raw_stocks")


def save_processed(df: pd.DataFrame):
    """Insert processed stock data — skip duplicates."""
    if df.empty:
        return
    cols = ["date", "ticker", "open", "high", "low", "close", "volume",
            "ma_7", "ma_30", "daily_pct_change", "volatility_7d", "above_ma30"]
    _upsert(df[cols], "processed_stocks")

def _upsert(df: pd.DataFrame, table: str):
    inserted = 0
    with engine.connect() as conn:
        for _, row in df.iterrows():
            # Convert row to plain Python types — SQLite can't handle pandas Timestamps
            row_dict = {}
            for k, v in row.items():
                if hasattr(v, 'tzinfo') and v.tzinfo is not None:
                    # Strip timezone from datetime
                    row_dict[k] = v.tz_localize(None).isoformat() if hasattr(v, 'tz_localize') else v.replace(tzinfo=None).isoformat()
                elif hasattr(v, 'isoformat'):
                    row_dict[k] = v.isoformat()
                else:
                    row_dict[k] = v

            cols = ", ".join(row_dict.keys())
            placeholders = ", ".join([f":{c}" for c in row_dict.keys()])
            stmt = text(f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({placeholders})")
            result = conn.execute(stmt, row_dict)
            inserted += result.rowcount
        conn.commit()
    logger.info(f"Saved {inserted} new rows to '{table}' (skipped {len(df) - inserted} duplicates)")

def load_processed(ticker: str = None) -> pd.DataFrame:
    """Load processed data, optionally filtered by ticker."""
    query = "SELECT * FROM processed_stocks"
    if ticker:
        query += f" WHERE ticker = '{ticker}'"
    query += " ORDER BY ticker, date"
    return pd.read_sql(query, engine)
