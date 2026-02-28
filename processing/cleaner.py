import pandas as pd
from utils.logger import get_logger

logger = get_logger("processing.cleaner")


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw stock data:
    - Remove duplicates
    - Fix data types
    - Handle missing values
    - Normalize date column
    """
    if df.empty:
        logger.warning("Received empty DataFrame — nothing to clean.")
        return df

    original_len = len(df)
    logger.info(f"Starting cleaning — {original_len} rows")

    # 1. Drop duplicate rows
    df = df.drop_duplicates(subset=["date", "ticker"]).copy()
    logger.info(f"After dedup: {len(df)} rows (removed {original_len - len(df)})")

    # 2. Normalize date column — strip timezone info, keep date only
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.normalize()

    # 3. Ensure numeric columns are correct type
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 4. Drop rows where close price is missing (core metric)
    before = len(df)
    df = df.dropna(subset=["close"])
    logger.info(f"Dropped {before - len(df)} rows with missing close price")

    # 5. Fill other minor missing values forward within each ticker
    df = df.sort_values(["ticker", "date"])
    df[numeric_cols] = df.groupby("ticker")[numeric_cols].ffill()

    # 6. Round price columns to 4 decimal places
    price_cols = ["open", "high", "low", "close"]
    df[price_cols] = df[price_cols].round(4)

    logger.info(f"Cleaning complete — {len(df)} rows remaining")
    return df.reset_index(drop=True)
