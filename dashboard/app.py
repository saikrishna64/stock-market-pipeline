import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import requests
import io
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from streamlit_searchbox import st_searchbox
from storage.db import load_processed, init_db

# ── Page Config ─────────────────────────────────────────────
st.set_page_config(
    page_title="Stock Pipeline Dashboard",
    page_icon="📈",
    layout="wide"
)

# ── Fetch Full NSE Company List ──────────────────────────────
@st.cache_data(ttl=86400)  # Cache for 24 hours
def get_nse_companies():
    try:
        url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        df_nse = pd.read_csv(io.StringIO(response.text))
        company_map = {
            row["NAME OF COMPANY"].strip(): row["SYMBOL"].strip() + ".NS"
            for _, row in df_nse.iterrows()
        }
        return company_map
    except Exception as e:
        return {}

# ── Load Pipeline Data ───────────────────────────────────────
init_db()

@st.cache_data(ttl=300)
def get_data():
    return load_processed()

# ── Title ────────────────────────────────────────────────────
st.title("📈 Stock Market Data Pipeline")
st.caption("Automated daily pipeline — NSE Stocks | Real-time data updated daily")

df = get_data()

if df.empty:
    st.warning("No data yet. Run `python main.py --now` to load data.")
    st.stop()

df["date"] = pd.to_datetime(df["date"])

# ── Sidebar ──────────────────────────────────────────────────
st.sidebar.header("Filters")
nse_companies = get_nse_companies()
st.sidebar.markdown("**Add a new stock**")

search_input = st.sidebar.text_input(
    "Search company name",
    placeholder="e.g. Tata, HDFC, Bajaj, Infosys..."
).strip()

selected_company = None
new_ticker = None

if search_input:
    if nse_companies:
        matches = {name: ticker for name, ticker in nse_companies.items()
                   if search_input.lower() in name.lower() or search_input.lower() in ticker.lower()}
        if matches:
            top_matches = dict(list(matches.items())[:10])
            selected_company = st.sidebar.selectbox(
                f"Found {len(matches)} match(es)",
                options=list(top_matches.keys())
            )
            new_ticker = top_matches[selected_company]
            st.sidebar.caption(f"Ticker: `{new_ticker}`")
        else:
            st.sidebar.warning("No match found.")
    else:
        # NSE list failed — fallback to manual ticker entry
        st.sidebar.caption("Type exact NSE ticker:")
        manual = search_input.upper()
        new_ticker = manual if manual.endswith(".NS") else manual + ".NS"
        selected_company = search_input
        st.sidebar.caption(f"Ticker: `{new_ticker}`")

if selected_company and new_ticker and st.sidebar.button(f"Fetch & Add {selected_company}"):
    with st.spinner(f"Fetching {selected_company}..."):
        try:
            from ingestion.fetcher import fetch_stock
            from processing.cleaner import clean
            from processing.validator import validate
            from processing.transformer import transform
            from storage.db import save_raw, save_processed

            raw = fetch_stock(new_ticker)
            if raw is not None and not raw.empty:
                clean_df = clean(raw)
                valid_df, _ = validate(clean_df)
                processed_df = transform(valid_df)
                save_raw(raw)
                save_processed(processed_df)
                st.sidebar.success(f"✅ {selected_company} added!")
                st.cache_data.clear()
                st.rerun()
            else:
                st.sidebar.error(f"❌ No data found.")
        except Exception as e:
            st.sidebar.error(f"Error: {e}")

st.sidebar.divider()
# ── Stock Selector ───────────────────────────────────────────
tickers = sorted(df["ticker"].unique())
display_names = [t.replace(".NS", "").replace(".BO", "") for t in tickers]
ticker_map = dict(zip(display_names, tickers))

selected_display = st.sidebar.selectbox("Select Stock to View", display_names)
selected_ticker = ticker_map[selected_display]

min_date = df["date"].min().date()
max_date = df["date"].max().date()
date_range = st.sidebar.date_input("Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date)

# ── Filter Data ──────────────────────────────────────────────
filtered = df[df["ticker"] == selected_ticker].copy()
if len(date_range) == 2:
    filtered = filtered[(filtered["date"].dt.date >= date_range[0]) & (filtered["date"].dt.date <= date_range[1])]

# ── KPI Cards ────────────────────────────────────────────────
latest = filtered.iloc[-1] if not filtered.empty else None
col1, col2, col3, col4 = st.columns(4)

if latest is not None:
    col1.metric("Latest Close", f"₹{latest['close']:.2f}", f"{latest['daily_pct_change']:+.2f}%")
    col2.metric("7-Day MA", f"₹{latest['ma_7']:.2f}")
    col3.metric("30-Day MA", f"₹{latest['ma_30']:.2f}")
    col4.metric("7-Day Volatility", f"{latest['volatility_7d']:.2f}%")

st.divider()

# ── Price Chart with MAs ─────────────────────────────────────
st.subheader(f"{selected_display} — Price & Moving Averages")
fig = go.Figure()
fig.add_trace(go.Scatter(x=filtered["date"], y=filtered["close"], name="Close", line=dict(color="#00b4d8", width=2)))
fig.add_trace(go.Scatter(x=filtered["date"], y=filtered["ma_7"], name="7-Day MA", line=dict(color="#f77f00", width=1.5, dash="dot")))
fig.add_trace(go.Scatter(x=filtered["date"], y=filtered["ma_30"], name="30-Day MA", line=dict(color="#d62828", width=1.5, dash="dash")))
fig.update_layout(height=400, template="plotly_dark", legend=dict(orientation="h"))
st.plotly_chart(fig, use_container_width=True)

# ── Volume Chart ─────────────────────────────────────────────
st.subheader("Volume")
fig_vol = go.Figure()
fig_vol.add_trace(go.Bar(x=filtered["date"], y=filtered["volume"], marker_color="#48cae4"))
fig_vol.update_layout(height=200, template="plotly_dark")
st.plotly_chart(fig_vol, use_container_width=True)

st.divider()

# ── Volatility Comparison ────────────────────────────────────
st.subheader("Volatility Comparison — All Stocks")
latest_all = df.groupby("ticker").last().reset_index()
latest_all["display_name"] = latest_all["ticker"].str.replace(".NS", "").str.replace(".BO", "")
fig_v = px.bar(latest_all, x="display_name", y="volatility_7d", color="display_name",
               title="7-Day Volatility (%)", template="plotly_dark")
st.plotly_chart(fig_v, use_container_width=True)

# ── Daily % Change Table ─────────────────────────────────────
st.subheader("Recent Daily % Changes")
recent = df.sort_values("date").groupby("ticker").tail(7)[["date", "ticker", "close", "daily_pct_change"]].copy()
recent["ticker"] = recent["ticker"].str.replace(".NS", "").str.replace(".BO", "")
recent["date"] = recent["date"].dt.date
recent = recent.sort_values(["date", "ticker"], ascending=[False, True])
recent["close"] = recent["close"].apply(lambda x: f"₹{x:,.2f}")
st.dataframe(
    recent.style.background_gradient(subset=["daily_pct_change"], cmap="RdYlGn"),
    use_container_width=True
)