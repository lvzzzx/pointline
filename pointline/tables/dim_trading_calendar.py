"""dim_trading_calendar table: trading day/session metadata per exchange.

Non-24/7 markets (equities, futures) have holidays, early closes, and trading
hours. Cross-asset queries require knowing "was this a trading day?"

For 24/7 crypto exchanges the calendar is trivially "every day is a trading
day", so rows are generated automatically when needed.

Unpartitioned Silver dimension table.
"""

from __future__ import annotations

import datetime as dt

import polars as pl

DIM_TRADING_CALENDAR_SCHEMA: dict[str, pl.DataType] = {
    "exchange": pl.Utf8,  # e.g., "szse", "nyse"
    "date": pl.Date,
    "is_trading_day": pl.Boolean,
    "session_type": pl.Utf8,  # "regular", "early_close", "holiday", "weekend"
    "open_time_us": pl.Int64,  # Market open in UTC microseconds (nullable)
    "close_time_us": pl.Int64,  # Market close in UTC microseconds (nullable)
}


def normalize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to canonical schema and select only schema columns."""
    missing = [col for col in DIM_TRADING_CALENDAR_SCHEMA if col not in df.columns]
    if missing:
        raise ValueError(f"dim_trading_calendar missing required columns: {missing}")

    casts = [pl.col(col).cast(dtype) for col, dtype in DIM_TRADING_CALENDAR_SCHEMA.items()]
    return df.with_columns(casts).select(list(DIM_TRADING_CALENDAR_SCHEMA.keys()))


def canonical_columns() -> tuple[str, ...]:
    return tuple(DIM_TRADING_CALENDAR_SCHEMA.keys())


def _is_weekend(d: dt.date) -> bool:
    return d.weekday() >= 5  # Saturday=5, Sunday=6


def bootstrap_crypto(
    exchange: str,
    start: dt.date,
    end: dt.date,
) -> pl.DataFrame:
    """Generate calendar for a 24/7 crypto exchange (every day is a trading day).

    Args:
        exchange: Exchange name (e.g., "binance-futures")
        start: Start date (inclusive)
        end: End date (inclusive)

    Returns:
        DataFrame in canonical schema.
    """
    dates = pl.date_range(start, end, eager=True).alias("date")
    n = len(dates)
    df = pl.DataFrame(
        {
            "exchange": [exchange] * n,
            "date": dates,
            "is_trading_day": [True] * n,
            "session_type": ["regular"] * n,
            "open_time_us": [None] * n,  # 24/7, no specific open/close
            "close_time_us": [None] * n,
        },
        schema=DIM_TRADING_CALENDAR_SCHEMA,
    )
    return df


def trading_days(
    calendar_df: pl.DataFrame,
    exchange: str,
    start: dt.date,
    end: dt.date,
) -> list[dt.date]:
    """Return list of trading days for an exchange in a date range.

    Args:
        calendar_df: Full calendar DataFrame
        exchange: Exchange name
        start: Start date (inclusive)
        end: End date (inclusive)

    Returns:
        Sorted list of trading dates.
    """
    result = calendar_df.filter(
        (pl.col("exchange") == exchange)
        & (pl.col("date") >= start)
        & (pl.col("date") <= end)
        & (pl.col("is_trading_day") == True)  # noqa: E712
    ).sort("date")

    return result["date"].to_list()
