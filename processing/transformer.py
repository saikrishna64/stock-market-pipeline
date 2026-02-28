import pandas as pd
from utils.logger import get_logger

logger = get_logger("processing.transformer")


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add analytical features to clean stock data:
    - 7-day and 30-day moving averages
    - Daily % change
    - Volatility (rolling std dev)
    - Above/below MA signal
    """
    if df.empty:
        logger.warning("Empty DataFrame — skipping transformation.")
        return df

    logger.info(f"Transforming {len(df)} rows...")

    df = df.sort_values(["ticker", "date"]).copy()

    # Compute features per ticker group
    def add_features(group):
        group = group.copy()

        group["ma_7"] = group["close"].rolling(window=7, min_periods=1).mean().round(4)
        group["ma_30"] = group["close"].rolling(window=30, min_periods=1).mean().round(4)
        group["daily_pct_change"] = group["close"].pct_change().mul(100).round(4)
        group["volatility_7d"] = group["daily_pct_change"].rolling(window=7, min_periods=1).std().round(4)
        group["above_ma30"] = (group["close"] > group["ma_30"]).astype(int)

        return group

    df = df.groupby("ticker", group_keys=False).apply(add_features, include_groups=True)

    logger.info("Transformation complete.")
    return df.reset_index(drop=True)
