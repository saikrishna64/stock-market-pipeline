import yfinance as yf
import pandas as pd
from datetime import datetime
from config import STOCKS, HISTORICAL_PERIOD, INTERVAL
from utils.logger import get_logger

logger = get_logger("ingestion.fetcher")


def fetch_stock(ticker: str, period: str = HISTORICAL_PERIOD, interval: str = INTERVAL) -> pd.DataFrame | None:
    """
    Fetch OHLCV data for a single stock ticker.
    Returns a DataFrame or None if fetch fails.
    """
    try:
        logger.info(f"Fetching data for {ticker}...")
        stock = yf.Ticker(ticker)
        df = stock.history(period=period, interval=interval)

        if df.empty:
            logger.warning(f"No data returned for {ticker}")
            return None

        # Flatten and clean up column names
        df = df.reset_index()
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        # Add metadata
        df["ticker"] = ticker
        df["fetched_at"] = datetime.utcnow()

        # Keep only the columns we need
        df = df[["date", "ticker", "open", "high", "low", "close", "volume", "fetched_at"]]

        logger.info(f"Fetched {len(df)} rows for {ticker}")
        return df

    except Exception as e:
        logger.error(f"Failed to fetch {ticker}: {e}")
        return None


def fetch_all_stocks(tickers: list = STOCKS) -> pd.DataFrame:
    """
    Fetch data for all configured tickers and combine into one DataFrame.
    """
    logger.info(f"Starting fetch for {len(tickers)} stocks: {tickers}")
    frames = []

    for ticker in tickers:
        df = fetch_stock(ticker)
        if df is not None:
            frames.append(df)

    if not frames:
        logger.error("No data fetched for any ticker.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"Total rows fetched: {len(combined)}")
    return combined


if __name__ == "__main__":
    df = fetch_all_stocks()
    print(df.head(10))
    print(f"\nShape: {df.shape}")
    print(f"\nTickers: {df['ticker'].unique()}")
