"""Schema and utilities for stock_basic_cn in Polars.

This table stores China-only stock reference metadata sourced from Tushare's
stock_basic endpoint. It is treated as a full snapshot (overwrite on sync).
"""

from __future__ import annotations

import polars as pl

SCHEMA: dict[str, pl.DataType] = {
    "ts_code": pl.Utf8,
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "area": pl.Utf8,
    "industry": pl.Utf8,
    "fullname": pl.Utf8,
    "enname": pl.Utf8,
    "cnspell": pl.Utf8,
    "market": pl.Utf8,
    "exchange": pl.Utf8,
    "curr_type": pl.Utf8,
    "list_status": pl.Utf8,
    "list_date": pl.Date,
    "delist_date": pl.Date,
    "is_hs": pl.Utf8,
    "act_name": pl.Utf8,
    "act_ent_type": pl.Utf8,
    "exchange_id": pl.Int16,
    "exchange_symbol": pl.Utf8,
    "as_of_date": pl.Date,
    "ingest_ts_us": pl.Int64,
}


def normalize_stock_basic_cn_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical stock_basic_cn schema.

    Args:
        df: DataFrame with stock_basic_cn columns

    Returns:
        DataFrame with normalized schema

    Raises:
        ValueError: If required columns are missing
    """
    missing = [col for col in SCHEMA if col not in df.columns]
    if missing:
        raise ValueError(f"stock_basic_cn missing required columns: {missing}")

    return df.with_columns([
        pl.col(col).cast(dtype, strict=False) for col, dtype in SCHEMA.items()
    ])


def required_stock_basic_cn_columns() -> tuple[str, ...]:
    """Columns required for a stock_basic_cn DataFrame."""
    return tuple(SCHEMA.keys())
