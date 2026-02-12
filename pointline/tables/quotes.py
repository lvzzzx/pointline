"""Quotes domain logic for parsing, validation, and transformation.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl
    from pointline.tables.quotes import QUOTES_DOMAIN

    raw_df = pl.read_csv("quotes.csv")
    parsed = parse_tardis_quotes_csv(raw_df)
    normalized = QUOTES_DOMAIN.normalize_schema(parsed)
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

# Import parser from new location for backward compatibility
from pointline.tables._base import (
    generic_validate,
)
from pointline.tables.domain_contract import EventTableDomain, TableSpec
from pointline.tables.domain_registry import register_domain

# Required metadata fields for ingestion
REQUIRED_METADATA_FIELDS: set[str] = set()

# Schema definition matching design.md Section 5.4
#
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - These are automatically converted to signed types (Int16 and Int32) when written
# - Use Int32 instead of UInt32 for symbol_id, file_id
# - UInt8 is supported and maps to TINYINT
#
# This schema is the single source of truth - all code should use these types directly.
QUOTES_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,  # Exchange name (string) for partitioning and human readability
    "symbol": pl.Utf8,
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


def _normalize_schema(df: pl.DataFrame) -> pl.DataFrame:
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


def _validate(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to quotes data.

    Validates:
    - Non-negative prices and sizes (when present)
    - Valid timestamp ranges (reasonable values) for local and exchange times
    - Crossed book check: bid_px_int < ask_px_int when both are present
    - At least one of bid or ask must be present (filter rows with both missing)

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
        "symbol",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_quotes: missing required columns: {missing}")

    combined_filter, rules = _quote_validation_rules(df)

    valid = generic_validate(df, combined_filter, rules, "quotes")
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
        & (pl.col("symbol").is_not_null()),
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
        ("symbol", pl.col("symbol").is_null()),
    ]

    return combined_filter, rules


def _canonicalize_vendor_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Quotes have no table-specific enum remapping at canonicalization stage."""
    return df


def _encode_storage(
    df: pl.DataFrame,
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
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    required_cols = ["bid_px", "bid_sz", "ask_px", "ask_sz"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"encode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars(df)

    def _encode_nullable_price(col: str) -> pl.Expr:
        return (
            pl.when(pl.col(col).is_not_null())
            .then((pl.col(col) / pl.col(PROFILE_PRICE_COL)).round().cast(pl.Int64))
            .otherwise(None)
        )

    def _encode_nullable_amount(col: str) -> pl.Expr:
        return (
            pl.when(pl.col(col).is_not_null())
            .then((pl.col(col) / pl.col(PROFILE_AMOUNT_COL)).round().cast(pl.Int64))
            .otherwise(None)
        )

    result = working.with_columns(
        [
            _encode_nullable_price("bid_px").alias("bid_px_int"),
            _encode_nullable_amount("bid_sz").alias("bid_sz_int"),
            _encode_nullable_price("ask_px").alias("ask_px_int"),
            _encode_nullable_amount("ask_sz").alias("ask_sz_int"),
        ]
    )
    return result.drop([col for col in PROFILE_SCALAR_COLS if col in result.columns])


def _decode_storage(
    df: pl.DataFrame,
    *,
    keep_ints: bool = False,
) -> pl.DataFrame:
    """Decode fixed-point integers into float bid/ask columns.

    Requires:
    - df must have 'bid_px_int', 'bid_sz_int', 'ask_px_int', 'ask_sz_int' columns
    - df must have non-null 'exchange' values

    Returns DataFrame with bid_px, bid_sz, ask_px, ask_sz added (Float64).
    By default, drops the *_int columns.
    """
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    required_cols = ["bid_px_int", "bid_sz_int", "ask_px_int", "ask_sz_int"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars(df)

    result = working.with_columns(
        [
            (pl.col("bid_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("bid_px"),
            (pl.col("bid_sz_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64).alias("bid_sz"),
            (pl.col("ask_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("ask_px"),
            (pl.col("ask_sz_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64).alias("ask_sz"),
        ]
    )

    if not keep_ints:
        result = result.drop(required_cols)
    return result.drop([col for col in PROFILE_SCALAR_COLS if col in result.columns])


def _decode_storage_lazy(
    lf: pl.LazyFrame,
    *,
    keep_ints: bool = False,
) -> pl.LazyFrame:
    """Decode fixed-point integers lazily into float bid/ask columns."""
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars_lazy,
    )

    schema = lf.collect_schema()
    required_cols = ["bid_px_int", "bid_sz_int", "ask_px_int", "ask_sz_int"]
    missing = [c for c in required_cols if c not in schema]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars_lazy(lf)
    result = working.with_columns(
        [
            (pl.col("bid_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("bid_px"),
            (pl.col("bid_sz_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64).alias("bid_sz"),
            (pl.col("ask_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("ask_px"),
            (pl.col("ask_sz_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64).alias("ask_sz"),
        ]
    )
    if not keep_ints:
        result = result.drop(required_cols)
    return result.drop(list(PROFILE_SCALAR_COLS))


def _required_decode_columns() -> tuple[str, ...]:
    """Columns needed to decode storage fields for quotes."""
    return ("exchange", "bid_px_int", "bid_sz_int", "ask_px_int", "ask_sz_int")


@dataclass(frozen=True)
class QuotesDomain(EventTableDomain):
    spec: TableSpec = TableSpec(
        table_name="quotes",
        table_kind="event",
        schema=QUOTES_SCHEMA,
        partition_by=("exchange", "date"),
        has_date=True,
        layer="silver",
        allowed_exchanges=None,
        ts_column="ts_local_us",
    )

    def canonicalize_vendor_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        return _canonicalize_vendor_frame(df)

    def encode_storage(self, df: pl.DataFrame) -> pl.DataFrame:
        return _encode_storage(df)

    def normalize_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        return _normalize_schema(df)

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        return _validate(df)

    def required_decode_columns(self) -> tuple[str, ...]:
        return _required_decode_columns()

    def decode_storage(self, df: pl.DataFrame, *, keep_ints: bool = False) -> pl.DataFrame:
        return _decode_storage(df, keep_ints=keep_ints)

    def decode_storage_lazy(self, lf: pl.LazyFrame, *, keep_ints: bool = False) -> pl.LazyFrame:
        return _decode_storage_lazy(lf, keep_ints=keep_ints)


QUOTES_DOMAIN = QuotesDomain()


register_domain(QUOTES_DOMAIN)
