import os
from dotenv import load_dotenv

load_dotenv()

# Stocks to track — NSE listed (yfinance format adds .NS suffix)
STOCKS = ["RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "WIPRO.NS"]

# Database
DB_URL = os.getenv("DATABASE_URL", "sqlite:///stock_pipeline.db")  # fallback to SQLite for local dev

# Logging
LOG_DIR = "logs"
LOG_FILE = "logs/pipeline.log"

# Schedule — runs daily at 4:05 PM EST (after market close)
SCHEDULE_HOUR = 16
SCHEDULE_MINUTE = 5

# Data
HISTORICAL_PERIOD = "6mo"   # how far back to fetch on first run
INTERVAL = "1d"             # daily candles
