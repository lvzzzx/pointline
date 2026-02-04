"""Derivative ticker domain logic for parsing, validation, and transformation.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.
"""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

# Import parser from new location for backward compatibility
from pointline.tables._base import (
    exchange_id_validation_expr,
    generic_resolve_symbol_ids,
    generic_validate,
    required_columns_validation_expr,
    timestamp_validation_expr,
)
from pointline.validation_utils import with_expected_exchange_id

# Required metadata fields for ingestion
REQUIRED_METADATA_FIELDS: set[str] = set()

# Schema definition matching docs/schemas.md Section 2.6
#
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - These are automatically converted to signed types (Int16 and Int32) when written
# - Use Int16 instead of UInt16 for exchange_id
# - Use Int32 instead of UInt32 for file_id
# - Use Int64 for symbol_id to match dim_symbol
DERIVATIVE_TICKER_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "exchange_id": pl.Int16,
    "symbol_id": pl.Int64,
    "ts_local_us": pl.Int64,
    "ts_exch_us": pl.Int64,
    "mark_px": pl.Float64,
    "index_px": pl.Float64,
    "last_px": pl.Float64,
    "funding_rate": pl.Float64,
    "predicted_funding_rate": pl.Float64,
    "funding_ts_us": pl.Int64,
    "open_interest": pl.Float64,
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}


def normalize_derivative_ticker_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical derivative_ticker schema and select only schema columns."""
    optional_columns = {
        "funding_rate",
        "predicted_funding_rate",
        "funding_ts_us",
        "open_interest",
        "last_px",
        "index_px",
        "mark_px",
    }

    missing_required = [
        col
        for col in DERIVATIVE_TICKER_SCHEMA
        if col not in df.columns and col not in optional_columns
    ]
    if missing_required:
        raise ValueError(f"derivative_ticker missing required columns: {missing_required}")

    casts = []
    for col, dtype in DERIVATIVE_TICKER_SCHEMA.items():
        if col in df.columns:
            casts.append(pl.col(col).cast(dtype))
        elif col in optional_columns:
            casts.append(pl.lit(None, dtype=dtype).alias(col))
        else:
            raise ValueError(f"Required non-nullable column {col} is missing")

    return df.with_columns(casts).select(list(DERIVATIVE_TICKER_SCHEMA.keys()))


def validate_derivative_ticker(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to derivative ticker data."""
    if df.is_empty():
        return df

    required = [
        "ts_local_us",
        "ts_exch_us",
        "exchange",
        "exchange_id",
        "symbol_id",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_derivative_ticker: missing required columns: {missing}")

    df_with_expected = with_expected_exchange_id(df)

    combined_filter = (
        timestamp_validation_expr("ts_local_us")
        & timestamp_validation_expr("ts_exch_us")
        & required_columns_validation_expr(["exchange", "exchange_id", "symbol_id"])
        & exchange_id_validation_expr()
        & pl.when(pl.col("mark_px").is_not_null()).then(pl.col("mark_px") > 0).otherwise(True)
        & pl.when(pl.col("index_px").is_not_null()).then(pl.col("index_px") > 0).otherwise(True)
        & pl.when(pl.col("last_px").is_not_null()).then(pl.col("last_px") > 0).otherwise(True)
        & pl.when(pl.col("open_interest").is_not_null())
        .then(pl.col("open_interest") >= 0)
        .otherwise(True)
        & pl.when(pl.col("funding_ts_us").is_not_null())
        .then(pl.col("funding_ts_us") > 0)
        .otherwise(True)
    )

    rules = [
        ("ts_local_us", pl.col("ts_local_us").is_null() | (pl.col("ts_local_us") <= 0)),
        ("ts_exch_us", pl.col("ts_exch_us").is_null() | (pl.col("ts_exch_us") <= 0)),
        ("exchange", pl.col("exchange").is_null()),
        ("exchange_id", pl.col("exchange_id").is_null()),
        ("symbol_id", pl.col("symbol_id").is_null()),
        (
            "exchange_id_mismatch",
            pl.col("expected_exchange_id").is_null()
            | (pl.col("exchange_id") != pl.col("expected_exchange_id")),
        ),
        ("mark_px", pl.col("mark_px").is_not_null() & (pl.col("mark_px") <= 0)),
        ("index_px", pl.col("index_px").is_not_null() & (pl.col("index_px") <= 0)),
        ("last_px", pl.col("last_px").is_not_null() & (pl.col("last_px") <= 0)),
        (
            "open_interest",
            pl.col("open_interest").is_not_null() & (pl.col("open_interest") < 0),
        ),
        (
            "funding_ts_us",
            pl.col("funding_ts_us").is_not_null() & (pl.col("funding_ts_us") <= 0),
        ),
    ]

    valid = generic_validate(df_with_expected, combined_filter, rules, "derivative_ticker")
    return valid.select(df.columns)


def encode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
) -> pl.DataFrame:
    """No-op for derivative_ticker (prices already in Float64 format).

    Unlike trades/quotes which use fixed-point integers, derivative_ticker stores
    prices directly as Float64 (mark_px, index_px, last_px).
    """
    return df


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int | None,
    exchange_symbol: str | None,
    *,
    ts_col: str = "ts_local_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for derivative_ticker data using as-of join with dim_symbol.

    This is a wrapper around the generic symbol resolution function.

    Args:
        data: DataFrame with timestamp column
        dim_symbol: dim_symbol table in canonical schema
        exchange_id: Exchange ID to use for all rows
        exchange_symbol: Exchange symbol to use for all rows
        ts_col: Timestamp column name (default: ts_local_us)

    Returns:
        DataFrame with symbol_id column added
    """
    return generic_resolve_symbol_ids(data, dim_symbol, exchange_id, exchange_symbol, ts_col=ts_col)


def required_derivative_ticker_columns() -> Sequence[str]:
    """Columns required for a derivative_ticker DataFrame after normalization."""
    return tuple(DERIVATIVE_TICKER_SCHEMA.keys())
