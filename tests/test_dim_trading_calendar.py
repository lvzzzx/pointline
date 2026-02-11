"""Tests for dim_trading_calendar table module."""

import datetime as dt

import polars as pl
import pytest

from pointline.tables.dim_trading_calendar import (
    DIM_TRADING_CALENDAR_SCHEMA,
    bootstrap_crypto,
    canonical_columns,
    normalize_schema,
    trading_days,
)


def test_schema_columns():
    cols = canonical_columns()
    assert cols == (
        "exchange",
        "date",
        "is_trading_day",
        "session_type",
        "open_time_us",
        "close_time_us",
    )


def test_bootstrap_crypto_all_trading_days():
    start = dt.date(2024, 5, 1)
    end = dt.date(2024, 5, 7)
    df = bootstrap_crypto("binance-futures", start, end)

    assert df.height == 7
    assert df["is_trading_day"].to_list() == [True] * 7
    assert df["session_type"].to_list() == ["regular"] * 7
    assert df["exchange"].unique().to_list() == ["binance-futures"]

    # Dates should cover the full range
    dates = df["date"].to_list()
    assert dates[0] == start
    assert dates[-1] == end


def test_bootstrap_crypto_single_day():
    d = dt.date(2024, 1, 1)
    df = bootstrap_crypto("deribit", d, d)
    assert df.height == 1
    assert df["date"][0] == d


def test_normalize_schema_casts():
    df = pl.DataFrame(
        {
            "exchange": ["szse"],
            "date": [dt.date(2024, 5, 1)],
            "is_trading_day": [True],
            "session_type": ["regular"],
            "open_time_us": [1714521600000000],
            "close_time_us": [1714546800000000],
        }
    )
    result = normalize_schema(df)
    for col, dtype in DIM_TRADING_CALENDAR_SCHEMA.items():
        assert result[col].dtype == dtype


def test_normalize_schema_missing_column():
    df = pl.DataFrame({"exchange": ["a"], "date": [dt.date(2024, 1, 1)]})
    with pytest.raises(ValueError, match="missing required columns"):
        normalize_schema(df)


def test_trading_days_filter():
    cal = pl.DataFrame(
        {
            "exchange": ["szse"] * 5 + ["binance"] * 2,
            "date": [
                dt.date(2024, 5, 1),  # Wed - trading
                dt.date(2024, 5, 2),  # Thu - trading
                dt.date(2024, 5, 3),  # Fri - trading
                dt.date(2024, 5, 4),  # Sat - weekend
                dt.date(2024, 5, 5),  # Sun - weekend
                dt.date(2024, 5, 1),
                dt.date(2024, 5, 2),
            ],
            "is_trading_day": [True, True, True, False, False, True, True],
            "session_type": [
                "regular",
                "regular",
                "regular",
                "weekend",
                "weekend",
                "regular",
                "regular",
            ],
            "open_time_us": [None] * 7,
            "close_time_us": [None] * 7,
        },
        schema=DIM_TRADING_CALENDAR_SCHEMA,
    )

    result = trading_days(cal, "szse", dt.date(2024, 5, 1), dt.date(2024, 5, 5))
    assert len(result) == 3
    assert result == [dt.date(2024, 5, 1), dt.date(2024, 5, 2), dt.date(2024, 5, 3)]

    # Different exchange
    result2 = trading_days(cal, "binance", dt.date(2024, 5, 1), dt.date(2024, 5, 5))
    assert len(result2) == 2
