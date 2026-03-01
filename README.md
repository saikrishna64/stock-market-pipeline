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
main.py (APScheduler)    ← runs entire pipeline daily at 4:05 PM IST
```

## 🛠️ Tech Stack

- **Python** — pandas, SQLAlchemy, APScheduler
- **Data** — yfinance API
- **Database** — SQLite (local) / PostgreSQL (production)
- **Dashboard** — Streamlit + Plotly
- **Testing** — pytest (15+ tests)
- **Deployment** — Docker + Railway

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- Git

### Installation

**1. Clone the repository**
```bash
git clone https://github.com/saikrishna64/stock-market-pipeline.git
cd stock-market-pipeline
```

**2. Create virtual environment**
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Run the pipeline** (fetches 6 months of NSE stock data)
```bash
python main.py --now
```

**5. Launch the dashboard**
```bash
streamlit run dashboard/app.py
```

Open your browser at `http://localhost:8501` 🎉

### Running Tests
```bash
pytest tests/ -v
```

### Scheduling Automatic Daily Runs
```bash
python main.py
```
This starts the scheduler — pipeline runs automatically every day at 4:05 PM IST after NSE market close.

## 📊 Dashboard Features

- Live price chart with 7-day and 30-day moving averages
- Volume bar chart
- Volatility comparison across all tracked stocks
- Daily % change table with color coding
- Date range and ticker filters

## 🔄 Automation

The pipeline runs automatically every weekday at 4:05 PM EST (after NSE close) using APScheduler. All runs are logged to `logs/pipeline.log`.

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
