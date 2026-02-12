"""Quotes domain logic for parsing, validation, and transformation.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl
    from pointline.tables.quotes import parse_tardis_quotes_csv, normalize_quotes_schema

    raw_df = pl.read_csv("quotes.csv")
    parsed = parse_tardis_quotes_csv(raw_df)
    normalized = normalize_quotes_schema(parsed)
"""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

# Import parser from new location for backward compatibility
from pointline.tables._base import (
    generic_resolve_symbol_ids,
    generic_validate,
)
from pointline.validation_utils import with_expected_exchange_id

# Required metadata fields for ingestion
REQUIRED_METADATA_FIELDS: set[str] = set()

# Schema definition matching design.md Section 5.4
#
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - These are automatically converted to signed types (Int16 and Int32) when written
# - Use Int16 instead of UInt16 for exchange_id
# - Use Int32 instead of UInt32 for symbol_id, file_id
# - UInt8 is supported and maps to TINYINT
#
# This schema is the single source of truth - all code should use these types directly.
QUOTES_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,  # Exchange name (string) for partitioning and human readability
    "exchange_id": pl.Int16,  # Delta Lake stores as Int16 (not UInt16) - for joins and compression
    "symbol_id": pl.Int64,  # Match dim_symbol's symbol_id type
    "ts_local_us": pl.Int64,
    "ts_exch_us": pl.Int64,
    "bid_px_int": pl.Int64,
    "bid_sz_int": pl.Int64,
    "ask_px_int": pl.Int64,
    "ask_sz_int": pl.Int64,
    # Multi-asset fields (Phase 2) — nullable, unused by crypto
    "conditions": pl.Int32,  # Quote condition bitfield (nullable, equities)
    "venue_id": pl.Int16,  # Reporting venue (nullable, equities)
    # Lineage
    "file_id": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
    "file_line_number": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
}


def normalize_quotes_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical quotes schema and select only schema columns.

    Ensures all required columns exist and have correct types.
    Drops any extra columns (e.g., original float columns, dim_symbol metadata).
    """
    # Optional columns that can be missing (filled with null)
    optional_columns = {"conditions", "venue_id"}

    # Check for missing required (non-optional) columns
    missing_required = [
        col for col in QUOTES_SCHEMA if col not in df.columns and col not in optional_columns
    ]
    if missing_required:
        raise ValueError(f"quotes missing required columns: {missing_required}")

    # Cast columns to schema types
    casts = []
    for col, dtype in QUOTES_SCHEMA.items():
        if col in df.columns:
            casts.append(pl.col(col).cast(dtype))
        elif col in optional_columns:
            casts.append(pl.lit(None, dtype=dtype).alias(col))
        else:
            raise ValueError(f"Required non-nullable column {col} is missing")

    # Cast and select only schema columns (drops extra columns)
    return df.with_columns(casts).select(list(QUOTES_SCHEMA.keys()))


def validate_quotes(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to quotes data.

    Validates:
    - Non-negative prices and sizes (when present)
    - Valid timestamp ranges (reasonable values) for local and exchange times
    - Crossed book check: bid_px_int < ask_px_int when both are present
    - At least one of bid or ask must be present (filter rows with both missing)
    - exchange_id matches normalized exchange

    Returns filtered DataFrame (invalid rows removed) or raises on critical errors.
    """
    if df.is_empty():
        return df

    # Check required columns
    required = [
        "bid_px_int",
        "bid_sz_int",
        "ask_px_int",
        "ask_sz_int",
        "ts_local_us",
        "ts_exch_us",
        "exchange",
        "exchange_id",
        "symbol_id",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_quotes: missing required columns: {missing}")

    df_with_expected = with_expected_exchange_id(df)
    combined_filter, rules = _quote_validation_rules(df_with_expected)

    valid = generic_validate(df_with_expected, combined_filter, rules, "quotes")
    return valid.select(df.columns)


def _quote_validation_rules(df: pl.DataFrame) -> tuple[pl.Expr, list[tuple[str, pl.Expr]]]:
    has_bid = (pl.col("bid_px_int").is_not_null()) & (pl.col("bid_sz_int").is_not_null())
    has_ask = (pl.col("ask_px_int").is_not_null()) & (pl.col("ask_sz_int").is_not_null())

    filters = [
        (pl.col("ts_local_us") > 0)
        & (pl.col("ts_local_us") < 2**63)
        & (pl.col("ts_exch_us") > 0)
        & (pl.col("ts_exch_us") < 2**63)
        & (pl.col("exchange").is_not_null())
        & (pl.col("exchange_id").is_not_null())
        & (pl.col("symbol_id").is_not_null())
        & (pl.col("exchange_id") == pl.col("expected_exchange_id")),
        has_bid | has_ask,
        pl.when(has_bid)
        .then((pl.col("bid_px_int") > 0) & (pl.col("bid_sz_int") > 0))
        .otherwise(True),
        pl.when(has_ask)
        .then((pl.col("ask_px_int") > 0) & (pl.col("ask_sz_int") > 0))
        .otherwise(True),
        pl.when(has_bid & has_ask)
        .then(pl.col("bid_px_int") < pl.col("ask_px_int"))
        .otherwise(True),
    ]

    combined_filter = filters[0]
    for f in filters[1:]:
        combined_filter = combined_filter & f

    rules = [
        ("no_bid_or_ask", ~(has_bid | has_ask)),
        ("bid_vals", has_bid & ((pl.col("bid_px_int") <= 0) | (pl.col("bid_sz_int") <= 0))),
        ("ask_vals", has_ask & ((pl.col("ask_px_int") <= 0) | (pl.col("ask_sz_int") <= 0))),
        ("crossed", has_bid & has_ask & (pl.col("bid_px_int") >= pl.col("ask_px_int"))),
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
        ("exchange", pl.col("exchange").is_null()),
        ("exchange_id", pl.col("exchange_id").is_null()),
        ("symbol_id", pl.col("symbol_id").is_null()),
        (
            "exchange_id_mismatch",
            pl.col("expected_exchange_id").is_null()
            | (pl.col("exchange_id") != pl.col("expected_exchange_id")),
        ),
    ]

    return combined_filter, rules


def encode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange: str,
) -> pl.DataFrame:
    """Encode bid/ask prices and sizes as fixed-point integers using asset-class scalar profile.

    Requires:
    - df must have 'bid_px', 'bid_sz', 'ask_px', 'ask_sz' float columns

    Computes:
    - bid_px_int = round(bid_px / profile.price)
    - bid_sz_int = round(bid_sz / profile.amount)
    - ask_px_int = round(ask_px / profile.price)
    - ask_sz_int = round(ask_sz / profile.amount)

    The universal scalar (1e-9 for crypto, 1e-4 for cn-equity) ensures all exchange
    prices are exactly representable, so symmetric round() is used everywhere.

    Returns DataFrame with bid_px_int, bid_sz_int, ask_px_int, ask_sz_int columns added.
    """
    from pointline.encoding import encode_nullable_price, get_profile

    required_cols = ["bid_px", "bid_sz", "ask_px", "ask_sz"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"encode_fixed_point: df missing columns: {missing}")

    profile = get_profile(exchange)

    # Encode nullable price helper handles the when/then/otherwise(None) pattern.
    # For amounts, build the nullable expression inline (same pattern).
    def _encode_nullable_amount(col: str) -> pl.Expr:
        return (
            pl.when(pl.col(col).is_not_null())
            .then((pl.col(col) / profile.amount).round().cast(pl.Int64))
            .otherwise(None)
        )

    return df.with_columns(
        [
            encode_nullable_price("bid_px", profile).alias("bid_px_int"),
            _encode_nullable_amount("bid_sz").alias("bid_sz_int"),
            encode_nullable_price("ask_px", profile).alias("ask_px_int"),
            _encode_nullable_amount("ask_sz").alias("ask_sz_int"),
        ]
    )


def decode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame | None = None,
    *,
    keep_ints: bool = False,
    exchange: str | None = None,
) -> pl.DataFrame:
    """Decode fixed-point integers into float bid/ask columns using asset-class profile.

    Requires:
    - df must have 'bid_px_int', 'bid_sz_int', 'ask_px_int', 'ask_sz_int' columns
    - df must have 'exchange' column OR exchange must be provided

    Returns DataFrame with bid_px, bid_sz, ask_px, ask_sz added (Float64).
    By default, drops the *_int columns.
    """
    from pointline.encoding import decode_nullable_amount, decode_nullable_price

    required_cols = ["bid_px_int", "bid_sz_int", "ask_px_int", "ask_sz_int"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    profile = _resolve_profile(df, exchange)

    result = df.with_columns(
        [
            decode_nullable_price("bid_px_int", profile).alias("bid_px"),
            decode_nullable_amount("bid_sz_int", profile).alias("bid_sz"),
            decode_nullable_price("ask_px_int", profile).alias("ask_px"),
            decode_nullable_amount("ask_sz_int", profile).alias("ask_sz"),
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


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int | None,
    exchange_symbol: str | None,
    *,
    ts_col: str = "ts_local_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for quotes data using as-of join with dim_symbol.

    This is a wrapper around the generic symbol resolution function.

    Args:
        data: DataFrame with ts_local_us (or ts_col) column
        dim_symbol: dim_symbol table in canonical schema
        exchange_id: Exchange ID to use for all rows
        exchange_symbol: Exchange symbol to use for all rows
        ts_col: Timestamp column name (default: ts_local_us)

    Returns:
        DataFrame with symbol_id column added
    """
    return generic_resolve_symbol_ids(data, dim_symbol, exchange_id, exchange_symbol, ts_col=ts_col)


def required_quotes_columns() -> Sequence[str]:
    """Columns required for a quotes DataFrame after normalization."""
    return tuple(QUOTES_SCHEMA.keys())


# ---------------------------------------------------------------------------
# Schema registry registration
# ---------------------------------------------------------------------------
from pointline.schema_registry import register_schema as _register_schema  # noqa: E402

_register_schema("quotes", QUOTES_SCHEMA, partition_by=["exchange", "date"], has_date=True)
