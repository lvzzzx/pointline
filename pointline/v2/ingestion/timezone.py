"""Exchange-timezone-aware trading date derivation helpers."""

from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import polars as pl

from pointline.v2.ingestion.exchange import get_exchange_timezone


def derive_trading_date(ts_event_us: int, exchange: str) -> date:
    ts_seconds = ts_event_us / 1_000_000
    utc_dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
    exchange_tz = ZoneInfo(get_exchange_timezone(exchange))
    return utc_dt.astimezone(exchange_tz).date()


def derive_trading_date_frame(
    df: pl.DataFrame,
    *,
    exchange_col: str = "exchange",
    ts_col: str = "ts_event_us",
    trading_date_col: str = "trading_date",
) -> pl.DataFrame:
    required = {exchange_col, ts_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for trading date derivation: {sorted(missing)}")

    if df.is_empty():
        if trading_date_col in df.columns:
            return df
        return df.with_columns(pl.lit(None, dtype=pl.Date).alias(trading_date_col))

    result = df
    if trading_date_col not in result.columns:
        result = result.with_columns(pl.lit(None, dtype=pl.Date).alias(trading_date_col))

    for exchange in sorted(result.get_column(exchange_col).unique().to_list()):
        tz = get_exchange_timezone(exchange)
        derived = (
            pl.from_epoch(pl.col(ts_col), time_unit="us")
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone(tz)
            .dt.date()
        )
        result = result.with_columns(
            pl.when(pl.col(exchange_col) == exchange)
            .then(derived)
            .otherwise(pl.col(trading_date_col))
            .alias(trading_date_col)
        )

    return result
