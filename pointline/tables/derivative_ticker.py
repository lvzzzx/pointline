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
    "mark_px_int": pl.Int64,
    "index_px_int": pl.Int64,
    "last_px_int": pl.Int64,
    "funding_rate_int": pl.Int64,
    "predicted_funding_rate_int": pl.Int64,
    "funding_ts_us": pl.Int64,
    "oi_int": pl.Int64,
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}


def normalize_derivative_ticker_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical derivative_ticker schema and select only schema columns."""
    optional_columns = {
        "funding_rate_int",
        "predicted_funding_rate_int",
        "funding_ts_us",
        "oi_int",
        "last_px_int",
        "index_px_int",
        "mark_px_int",
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
        & pl.when(pl.col("mark_px_int").is_not_null())
        .then(pl.col("mark_px_int") > 0)
        .otherwise(True)
        & pl.when(pl.col("index_px_int").is_not_null())
        .then(pl.col("index_px_int") > 0)
        .otherwise(True)
        & pl.when(pl.col("last_px_int").is_not_null())
        .then(pl.col("last_px_int") > 0)
        .otherwise(True)
        & pl.when(pl.col("oi_int").is_not_null()).then(pl.col("oi_int") >= 0).otherwise(True)
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
        ("mark_px_int", pl.col("mark_px_int").is_not_null() & (pl.col("mark_px_int") <= 0)),
        ("index_px_int", pl.col("index_px_int").is_not_null() & (pl.col("index_px_int") <= 0)),
        ("last_px_int", pl.col("last_px_int").is_not_null() & (pl.col("last_px_int") <= 0)),
        (
            "oi_int",
            pl.col("oi_int").is_not_null() & (pl.col("oi_int") < 0),
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
    exchange: str,
) -> pl.DataFrame:
    """Encode derivative ticker float fields to fixed-point integers.

    Prices → profile.price scalar
    Open interest → profile.amount scalar
    Funding rates → profile.rate scalar
    """
    from pointline.encoding import get_profile

    profile = get_profile(exchange)

    return df.with_columns(
        [
            pl.when(pl.col("mark_px").is_not_null())
            .then((pl.col("mark_px") / profile.price).round().cast(pl.Int64))
            .otherwise(None)
            .alias("mark_px_int"),
            pl.when(pl.col("index_px").is_not_null())
            .then((pl.col("index_px") / profile.price).round().cast(pl.Int64))
            .otherwise(None)
            .alias("index_px_int"),
            pl.when(pl.col("last_px").is_not_null())
            .then((pl.col("last_px") / profile.price).round().cast(pl.Int64))
            .otherwise(None)
            .alias("last_px_int"),
            pl.when(pl.col("funding_rate").is_not_null())
            .then((pl.col("funding_rate") / profile.rate).round().cast(pl.Int64))
            .otherwise(None)
            .alias("funding_rate_int"),
            pl.when(pl.col("predicted_funding_rate").is_not_null())
            .then((pl.col("predicted_funding_rate") / profile.rate).round().cast(pl.Int64))
            .otherwise(None)
            .alias("predicted_funding_rate_int"),
            pl.when(pl.col("open_interest").is_not_null())
            .then((pl.col("open_interest") / profile.amount).round().cast(pl.Int64))
            .otherwise(None)
            .alias("oi_int"),
        ]
    )


def decode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame | None = None,
    *,
    keep_ints: bool = False,
    exchange: str | None = None,
) -> pl.DataFrame:
    """Decode derivative ticker fixed-point integers to floats."""
    profile = _resolve_profile(df, exchange)

    result = df.with_columns(
        [
            pl.when(pl.col("mark_px_int").is_not_null())
            .then((pl.col("mark_px_int") * profile.price).cast(pl.Float64))
            .otherwise(None)
            .alias("mark_px"),
            pl.when(pl.col("index_px_int").is_not_null())
            .then((pl.col("index_px_int") * profile.price).cast(pl.Float64))
            .otherwise(None)
            .alias("index_px"),
            pl.when(pl.col("last_px_int").is_not_null())
            .then((pl.col("last_px_int") * profile.price).cast(pl.Float64))
            .otherwise(None)
            .alias("last_px"),
            pl.when(pl.col("funding_rate_int").is_not_null())
            .then((pl.col("funding_rate_int") * profile.rate).cast(pl.Float64))
            .otherwise(None)
            .alias("funding_rate"),
            pl.when(pl.col("predicted_funding_rate_int").is_not_null())
            .then((pl.col("predicted_funding_rate_int") * profile.rate).cast(pl.Float64))
            .otherwise(None)
            .alias("predicted_funding_rate"),
            pl.when(pl.col("oi_int").is_not_null())
            .then((pl.col("oi_int") * profile.amount).cast(pl.Float64))
            .otherwise(None)
            .alias("open_interest"),
        ]
    )

    if not keep_ints:
        int_cols = [
            "mark_px_int",
            "index_px_int",
            "last_px_int",
            "funding_rate_int",
            "predicted_funding_rate_int",
            "oi_int",
        ]
        result = result.drop([c for c in int_cols if c in result.columns])
    return result


def _resolve_profile(df: pl.DataFrame, exchange: str | None = None):
    """Resolve ScalarProfile from exchange parameter or DataFrame 'exchange' column."""
    from pointline.encoding import get_profile

    if exchange is not None:
        return get_profile(exchange)
    if "exchange" in df.columns:
        exchanges = df["exchange"].unique().to_list()
        if len(exchanges) != 1:
            raise ValueError(
                f"decode: DataFrame has {len(exchanges)} exchanges; pass exchange= explicitly"
            )
        return get_profile(exchanges[0])
    raise ValueError("No 'exchange' column and no exchange= argument")


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


# ---------------------------------------------------------------------------
# Schema registry registration
# ---------------------------------------------------------------------------
from pointline.schema_registry import register_schema as _register_schema  # noqa: E402

_register_schema(
    "derivative_ticker", DERIVATIVE_TICKER_SCHEMA, partition_by=["exchange", "date"], has_date=True
)
