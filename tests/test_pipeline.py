import pytest
import pandas as pd
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from processing.cleaner import clean
from processing.validator import validate
from processing.transformer import transform


# ── Fixtures ────────────────────────────────────────────────

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        "ticker": ["AAPL", "AAPL", "AAPL"],
        "open":   [180.0, 182.0, 183.0],
        "high":   [185.0, 186.0, 187.0],
        "low":    [179.0, 181.0, 182.0],
        "close":  [184.0, 185.0, 186.0],
        "volume": [1000000, 1200000, 900000],
        "fetched_at": pd.Timestamp.utcnow()
    })


# ── Cleaner Tests ────────────────────────────────────────────

def test_clean_removes_duplicates(sample_df):
    df_with_dups = pd.concat([sample_df, sample_df], ignore_index=True)
    result = clean(df_with_dups)
    assert len(result) == len(sample_df)


def test_clean_handles_missing_close(sample_df):
    sample_df.loc[1, "close"] = None
    result = clean(sample_df)
    assert result["close"].isna().sum() == 0


def test_clean_numeric_types(sample_df):
    sample_df["close"] = sample_df["close"].astype(str)
    result = clean(sample_df)
    assert result["close"].dtype in [float, "float64"]


def test_clean_empty_dataframe():
    result = clean(pd.DataFrame())
    assert result.empty


def test_clean_date_normalization(sample_df):
    sample_df["date"] = pd.to_datetime(["2024-01-01 09:30:00+00:00",
                                         "2024-01-02 09:30:00+00:00",
                                         "2024-01-03 09:30:00+00:00"])
    result = clean(sample_df)
    assert result["date"].dt.tz is None


# ── Validator Tests ──────────────────────────────────────────

def test_validate_passes_clean_data(sample_df):
    valid, rejected = validate(sample_df)
    assert len(valid) == len(sample_df)
    assert rejected.empty


def test_validate_rejects_negative_close(sample_df):
    sample_df.loc[0, "close"] = -1.0
    valid, rejected = validate(sample_df)
    assert len(rejected) == 1
    assert len(valid) == len(sample_df) - 1


def test_validate_rejects_high_less_than_low(sample_df):
    sample_df.loc[1, "high"] = 170.0  # below low of 181
    valid, rejected = validate(sample_df)
    assert len(rejected) >= 1


def test_validate_rejects_zero_volume(sample_df):
    sample_df.loc[2, "volume"] = 0
    valid, rejected = validate(sample_df)
    assert len(rejected) >= 1


def test_validate_empty_dataframe():
    valid, rejected = validate(pd.DataFrame())
    assert valid.empty


# ── Transformer Tests ────────────────────────────────────────

def test_transform_adds_ma_columns(sample_df):
    result = transform(sample_df)
    assert "ma_7" in result.columns
    assert "ma_30" in result.columns


def test_transform_adds_pct_change(sample_df):
    result = transform(sample_df)
    assert "daily_pct_change" in result.columns


def test_transform_adds_volatility(sample_df):
    result = transform(sample_df)
    assert "volatility_7d" in result.columns


def test_transform_above_ma30_is_binary(sample_df):
    result = transform(sample_df)
    assert set(result["above_ma30"].unique()).issubset({0, 1})
