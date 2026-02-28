# 📈 Stock Market Data Pipeline

An automated end-to-end data pipeline that fetches live stock market data, cleans and validates it, engineers analytical features, stores it in a database, and serves a live interactive dashboard.

## 🏗️ Architecture

```
yfinance API
     ↓
ingestion/fetcher.py     ← pulls OHLCV data for 5 stocks
     ↓
processing/cleaner.py    ← deduplication, type fixing, null handling
processing/validator.py  ← flags bad records (negative prices, bad ranges)
processing/transformer.py← adds MA, % change, volatility features
     ↓
storage/db.py            ← incremental upsert to SQLite / PostgreSQL
     ↓
dashboard/app.py         ← Streamlit + Plotly interactive dashboard
     ↓
main.py (APScheduler)    ← runs entire pipeline daily at 4:05 PM EST
```

## 🛠️ Tech Stack

- **Python** — pandas, SQLAlchemy, APScheduler
- **Data** — yfinance API
- **Database** — SQLite (local) / PostgreSQL (production)
- **Dashboard** — Streamlit + Plotly
- **Testing** — pytest (15+ tests)
- **Deployment** — Docker + Railway

## 🚀 Getting Started

```bash
# 1. Clone and set up environment
git clone https://github.com/yourusername/stock_pipeline.git
cd stock_pipeline
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env

# 3. Run pipeline once to load data
python main.py --now

# 4. Launch dashboard
streamlit run dashboard/app.py

# 5. Run tests
pytest tests/
```

## 📊 Dashboard Features

- Live price chart with 7-day and 30-day moving averages
- Volume bar chart
- Volatility comparison across all tracked stocks
- Daily % change table with color coding
- Date range and ticker filters

## 🔄 Automation

The pipeline runs automatically every weekday at 4:05 PM EST (after NYSE close) using APScheduler. All runs are logged to `logs/pipeline.log`.

## 📁 Project Structure

```
stock_pipeline/
├── ingestion/        ← data fetching
├── processing/       ← cleaning, validation, feature engineering
├── storage/          ← database layer
├── dashboard/        ← Streamlit app
├── utils/            ← logging
├── tests/            ← pytest test suite
├── logs/             ← run logs + rejected records
├── config.py         ← central configuration
├── main.py           ← pipeline orchestrator + scheduler
└── requirements.txt
```

## 📈 Tracked Stocks

NSE/BSE Stocks