import pandas as pd
from utils.logger import get_logger

logger = get_logger("processing.validator")


def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate cleaned stock data.
    Returns (valid_df, rejected_df) — bad records are separated, not silently dropped.
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to validator.")
        return df, pd.DataFrame()

    rejection_reasons = []

    def flag(mask, reason):
        for idx in df[mask].index:
            rejection_reasons.append({"index": idx, "reason": reason})

    # Rule 1: Negative or zero prices
    flag(df["close"] <= 0, "close price is zero or negative")
    flag(df["open"] <= 0, "open price is zero or negative")
    flag(df["high"] <= 0, "high price is zero or negative")
    flag(df["low"] <= 0, "low price is zero or negative")

    # Rule 2: High must be >= low
    flag(df["high"] < df["low"], "high < low — invalid price range")

    # Rule 3: Close must be within high/low range
    flag(df["close"] > df["high"], "close > high — impossible")
    flag(df["close"] < df["low"], "close < low — impossible")

    # Rule 4: Volume must be positive
    flag(df["volume"] <= 0, "volume is zero or negative")

    # Rule 5: Future dates not allowed
    flag(df["date"] > pd.Timestamp.today().normalize(), "date is in the future")
    # Build rejected set
    bad_indices = set(r["index"] for r in rejection_reasons)
    rejected_df = df.loc[list(bad_indices)].copy()
    if not rejected_df.empty:
        reason_map = {}
        for r in rejection_reasons:
            reason_map.setdefault(r["index"], []).append(r["reason"])
        rejected_df["rejection_reason"] = rejected_df.index.map(
            lambda i: "; ".join(reason_map.get(i, []))
        )

    valid_df = df.drop(index=list(bad_indices)).reset_index(drop=True)

    logger.info(f"Validation complete — {len(valid_df)} valid, {len(rejected_df)} rejected")
    if not rejected_df.empty:
        logger.warning(f"Rejected records:\n{rejected_df[['ticker', 'date', 'rejection_reason']]}")

    return valid_df, rejected_df
