"""Trades domain logic for parsing, validation, and transformation.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl
    from pointline.tables.trades import parse_tardis_trades_csv, normalize_trades_schema

    raw_df = pl.read_csv("trades.csv")
    parsed = parse_tardis_trades_csv(raw_df)
    normalized = normalize_trades_schema(parsed)
"""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

# Import parser from new location for backward compatibility
from pointline.tables._base import (
    generic_validate,
    required_columns_validation_expr,
    timestamp_validation_expr,
)

# Required metadata fields for ingestion
REQUIRED_METADATA_FIELDS: set[str] = set()

# Schema definition matching design.md Section 5.3
#
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - These are automatically converted to signed types (Int16 and Int32) when written
# - Use Int32 instead of UInt32 for file_id, flags
# - UInt8 is supported and maps to TINYINT (use for side, asset_type)
#
# This schema is the single source of truth - all code should use these types directly.
TRADES_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,  # Exchange name (string) for partitioning and human readability
    "symbol": pl.Utf8,  # Exchange symbol string (e.g., "BTCUSDT")
    "ts_local_us": pl.Int64,
    "ts_exch_us": pl.Int64,
    "trade_id": pl.Utf8,
    "side": pl.UInt8,  # UInt8 is supported (maps to TINYINT)
    "px_int": pl.Int64,
    "qty_int": pl.Int64,
    "flags": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
    # Multi-asset fields (Phase 2) — nullable, unused by crypto
    "conditions": pl.Int32,  # Sale condition bitfield (nullable, equities)
    "venue_id": pl.Int16,  # Reporting venue (nullable, equities)
    "sequence_number": pl.Int64,  # SIP/exchange sequence (nullable, equities)
    # Lineage
    "file_id": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
    "file_line_number": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
}

# Side encoding constants
SIDE_BUY = 0
SIDE_SELL = 1
SIDE_UNKNOWN = 2


def normalize_trades_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical trades schema and select only schema columns.

    Ensures all required columns exist and have correct types.
    Optional columns (trade_id, flags) are filled with None if missing.
    Drops any extra columns (e.g., original float columns, dim_symbol metadata).
    """
    # Optional columns that can be missing (filled with null)
    optional_columns = {"trade_id", "flags", "conditions", "venue_id", "sequence_number"}

    # Check for missing required (non-optional) columns
    missing_required = [
        col for col in TRADES_SCHEMA if col not in df.columns and col not in optional_columns
    ]
    if missing_required:
        raise ValueError(f"trades missing required columns: {missing_required}")

    # Cast columns to schema types
    casts = []
    for col, dtype in TRADES_SCHEMA.items():
        if col in df.columns:
            casts.append(pl.col(col).cast(dtype))
        else:
            # Fill missing optional columns with None
            if col in optional_columns:
                casts.append(pl.lit(None, dtype=dtype).alias(col))
            else:
                raise ValueError(f"Required non-nullable column {col} is missing")

    # Cast and select only schema columns (drops extra columns)
    return df.with_columns(casts).select(list(TRADES_SCHEMA.keys()))


def validate_trades(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to trades data.

    Validates:
    - Non-negative px_int and qty_int
    - Valid timestamp ranges (reasonable values) for local and exchange times
    - Valid side codes (0-2)
    - Non-null required fields
    - Exchange column exists and is non-null
    Returns filtered DataFrame (invalid rows removed) or raises on critical errors.
    """
    if df.is_empty():
        return df

    # Check required columns
    required = [
        "px_int",
        "qty_int",
        "ts_local_us",
        "ts_exch_us",
        "side",
        "exchange",
        "symbol",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_trades: missing required columns: {missing}")

    # Build filter expression
    combined_filter = (
        (pl.col("px_int") > 0)
        & (pl.col("qty_int") > 0)
        & timestamp_validation_expr("ts_local_us")
        & timestamp_validation_expr("ts_exch_us")
        & (pl.col("side").is_in([0, 1, 2]))
        & required_columns_validation_expr(["exchange", "symbol"])
    )

    # Define validation rules for diagnostics
    rules = [
        ("px_int", pl.col("px_int").is_null() | (pl.col("px_int") <= 0)),
        ("qty_int", pl.col("qty_int").is_null() | (pl.col("qty_int") <= 0)),
        (
            "ts_local_us",
            pl.col("ts_local_us").is_null()
            | (pl.col("ts_local_us") <= 0)
            | (pl.col("ts_local_us") >= 2**63),
        ),
        (
            "ts_exch_us",
            pl.col("ts_exch_us").is_null()
            | (pl.col("ts_exch_us") <= 0)
            | (pl.col("ts_exch_us") >= 2**63),
        ),
        ("side", ~pl.col("side").is_in([0, 1, 2]) | pl.col("side").is_null()),
        ("exchange", pl.col("exchange").is_null()),
        ("symbol", pl.col("symbol").is_null()),
    ]

    # Use generic validation
    valid = generic_validate(df, combined_filter, rules, "trades")
    return valid.select(df.columns)


def encode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange: str,
    *,
    price_col: str = "price_px",
    qty_col: str = "qty",
) -> pl.DataFrame:
    """Encode price and quantity as fixed-point integers using asset-class scalar profile.

    Requires:
    - df must have price_col and qty_col float columns

    Computes:
    - px_int = round(price / profile.price)
    - qty_int = round(qty / profile.amount)

    Returns DataFrame with px_int and qty_int columns added.
    """
    from pointline.encoding import encode_amount, encode_price, get_profile

    profile = get_profile(exchange)
    return df.with_columns(
        [
            encode_price(price_col, profile).alias("px_int"),
            encode_amount(qty_col, profile).alias("qty_int"),
        ]
    )


def decode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame | None = None,
    *,
    price_col: str = "price_px",
    qty_col: str = "qty",
    keep_ints: bool = False,
    exchange: str | None = None,
) -> pl.DataFrame:
    """Decode fixed-point integers into float price/qty columns using asset-class profile.

    Requires:
    - df must have 'px_int' and 'qty_int' columns
    - df must have 'exchange' column OR exchange must be provided

    Returns DataFrame with price_col and qty_col added (Float64).
    By default, drops the *_int columns.
    """
    required_cols = ["px_int", "qty_int"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    profile = _resolve_profile(df, exchange)

    result = df.with_columns(
        [
            pl.when(pl.col("px_int").is_not_null())
            .then((pl.col("px_int") * profile.price).cast(pl.Float64))
            .otherwise(None)
            .alias(price_col),
            pl.when(pl.col("qty_int").is_not_null())
            .then((pl.col("qty_int") * profile.amount).cast(pl.Float64))
            .otherwise(None)
            .alias(qty_col),
        ]
    )

    if not keep_ints:
        result = result.drop(required_cols)
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
                f"decode_fixed_point: DataFrame has {len(exchanges)} exchanges; "
                "pass exchange= explicitly for multi-exchange DataFrames"
            )
        return get_profile(exchanges[0])
    raise ValueError("decode_fixed_point: no 'exchange' column and no exchange= argument")


def required_trades_columns() -> Sequence[str]:
    """Columns required for a trades DataFrame after normalization."""
    return tuple(TRADES_SCHEMA.keys())


# ---------------------------------------------------------------------------
# Schema registry registration
# ---------------------------------------------------------------------------
from pointline.schema_registry import register_schema as _register_schema  # noqa: E402

_register_schema("trades", TRADES_SCHEMA, partition_by=["exchange", "date"], has_date=True)
