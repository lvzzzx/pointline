"""Derivative ticker domain logic for parsing, validation, and transformation.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import polars as pl

# Import parser from new location for backward compatibility
from pointline.tables._base import (
    generic_validate,
    required_columns_validation_expr,
    timestamp_validation_expr,
)
from pointline.tables.domain_contract import TableDomain, TableSpec
from pointline.tables.domain_registry import register_domain

# Required metadata fields for ingestion
REQUIRED_METADATA_FIELDS: set[str] = set()

# Schema definition matching docs/schemas.md Section 2.6
#
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - These are automatically converted to signed types (Int16 and Int32) when written
# - Use Int32 instead of UInt32 for file_id
# - Use Int64 for symbol_id to match dim_symbol
DERIVATIVE_TICKER_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "symbol": pl.Utf8,
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
        "symbol",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_derivative_ticker: missing required columns: {missing}")

    combined_filter = (
        timestamp_validation_expr("ts_local_us")
        & timestamp_validation_expr("ts_exch_us")
        & required_columns_validation_expr(["exchange", "symbol"])
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
        ("symbol", pl.col("symbol").is_null()),
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

    valid = generic_validate(df, combined_filter, rules, "derivative_ticker")
    return valid.select(df.columns)


def canonicalize_derivative_ticker_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Derivative ticker has no enum remapping at canonicalization stage."""
    return df


def encode_fixed_point(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """Encode derivative ticker float fields to fixed-point integers.

    Prices → profile.price scalar
    Open interest → profile.amount scalar
    Funding rates → profile.rate scalar
    """
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_RATE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    working = with_profile_scalars(df)

    result = working.with_columns(
        [
            pl.when(pl.col("mark_px").is_not_null())
            .then((pl.col("mark_px") / pl.col(PROFILE_PRICE_COL)).round().cast(pl.Int64))
            .otherwise(None)
            .alias("mark_px_int"),
            pl.when(pl.col("index_px").is_not_null())
            .then((pl.col("index_px") / pl.col(PROFILE_PRICE_COL)).round().cast(pl.Int64))
            .otherwise(None)
            .alias("index_px_int"),
            pl.when(pl.col("last_px").is_not_null())
            .then((pl.col("last_px") / pl.col(PROFILE_PRICE_COL)).round().cast(pl.Int64))
            .otherwise(None)
            .alias("last_px_int"),
            pl.when(pl.col("funding_rate").is_not_null())
            .then((pl.col("funding_rate") / pl.col(PROFILE_RATE_COL)).round().cast(pl.Int64))
            .otherwise(None)
            .alias("funding_rate_int"),
            pl.when(pl.col("predicted_funding_rate").is_not_null())
            .then(
                (pl.col("predicted_funding_rate") / pl.col(PROFILE_RATE_COL)).round().cast(pl.Int64)
            )
            .otherwise(None)
            .alias("predicted_funding_rate_int"),
            pl.when(pl.col("open_interest").is_not_null())
            .then((pl.col("open_interest") / pl.col(PROFILE_AMOUNT_COL)).round().cast(pl.Int64))
            .otherwise(None)
            .alias("oi_int"),
        ]
    )
    return result.drop([col for col in PROFILE_SCALAR_COLS if col in result.columns])


def decode_fixed_point(
    df: pl.DataFrame,
    *,
    keep_ints: bool = False,
) -> pl.DataFrame:
    """Decode derivative ticker fixed-point integers to floats."""
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_RATE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    int_cols = [
        "mark_px_int",
        "index_px_int",
        "last_px_int",
        "funding_rate_int",
        "predicted_funding_rate_int",
        "oi_int",
    ]
    missing = [c for c in int_cols if c not in df.columns]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars(df)

    result = working.with_columns(
        [
            pl.when(pl.col("mark_px_int").is_not_null())
            .then((pl.col("mark_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("mark_px"),
            pl.when(pl.col("index_px_int").is_not_null())
            .then((pl.col("index_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("index_px"),
            pl.when(pl.col("last_px_int").is_not_null())
            .then((pl.col("last_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("last_px"),
            pl.when(pl.col("funding_rate_int").is_not_null())
            .then((pl.col("funding_rate_int") * pl.col(PROFILE_RATE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("funding_rate"),
            pl.when(pl.col("predicted_funding_rate_int").is_not_null())
            .then(
                (pl.col("predicted_funding_rate_int") * pl.col(PROFILE_RATE_COL)).cast(pl.Float64)
            )
            .otherwise(None)
            .alias("predicted_funding_rate"),
            pl.when(pl.col("oi_int").is_not_null())
            .then((pl.col("oi_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("open_interest"),
        ]
    )

    if not keep_ints:
        result = result.drop([c for c in int_cols if c in result.columns])
    return result.drop([col for col in PROFILE_SCALAR_COLS if col in result.columns])


def decode_fixed_point_lazy(
    lf: pl.LazyFrame,
    *,
    keep_ints: bool = False,
) -> pl.LazyFrame:
    """Decode derivative ticker fixed-point integers lazily to floats."""
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_RATE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars_lazy,
    )

    schema = lf.collect_schema()
    int_cols = [
        "mark_px_int",
        "index_px_int",
        "last_px_int",
        "funding_rate_int",
        "predicted_funding_rate_int",
        "oi_int",
    ]
    missing = [c for c in int_cols if c not in schema]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars_lazy(lf)
    result = working.with_columns(
        [
            pl.when(pl.col("mark_px_int").is_not_null())
            .then((pl.col("mark_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("mark_px"),
            pl.when(pl.col("index_px_int").is_not_null())
            .then((pl.col("index_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("index_px"),
            pl.when(pl.col("last_px_int").is_not_null())
            .then((pl.col("last_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("last_px"),
            pl.when(pl.col("funding_rate_int").is_not_null())
            .then((pl.col("funding_rate_int") * pl.col(PROFILE_RATE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("funding_rate"),
            pl.when(pl.col("predicted_funding_rate_int").is_not_null())
            .then(
                (pl.col("predicted_funding_rate_int") * pl.col(PROFILE_RATE_COL)).cast(pl.Float64)
            )
            .otherwise(None)
            .alias("predicted_funding_rate"),
            pl.when(pl.col("oi_int").is_not_null())
            .then((pl.col("oi_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("open_interest"),
        ]
    )
    if not keep_ints:
        result = result.drop(int_cols)
    return result.drop(list(PROFILE_SCALAR_COLS))


def required_derivative_ticker_columns() -> Sequence[str]:
    """Columns required for a derivative_ticker DataFrame after normalization."""
    return tuple(DERIVATIVE_TICKER_SCHEMA.keys())


def required_decode_columns() -> tuple[str, ...]:
    """Columns needed to decode storage fields for derivative ticker."""
    return (
        "exchange",
        "mark_px_int",
        "index_px_int",
        "last_px_int",
        "funding_rate_int",
        "predicted_funding_rate_int",
        "oi_int",
    )


@dataclass(frozen=True)
class DerivativeTickerDomain(TableDomain):
    spec: TableSpec = TableSpec(
        table_name="derivative_ticker",
        schema=DERIVATIVE_TICKER_SCHEMA,
        partition_by=("exchange", "date"),
        has_date=True,
        layer="silver",
        allowed_exchanges=None,
        ts_column="ts_local_us",
    )

    def canonicalize_vendor_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        return canonicalize_derivative_ticker_frame(df)

    def encode_storage(self, df: pl.DataFrame) -> pl.DataFrame:
        return encode_fixed_point(df)

    def normalize_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        return normalize_derivative_ticker_schema(df)

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        return validate_derivative_ticker(df)

    def required_decode_columns(self) -> tuple[str, ...]:
        return required_decode_columns()

    def decode_storage(self, df: pl.DataFrame, *, keep_ints: bool = False) -> pl.DataFrame:
        return decode_fixed_point(df, keep_ints=keep_ints)

    def decode_storage_lazy(self, lf: pl.LazyFrame, *, keep_ints: bool = False) -> pl.LazyFrame:
        return decode_fixed_point_lazy(lf, keep_ints=keep_ints)


# ---------------------------------------------------------------------------
# Schema registry registration
# ---------------------------------------------------------------------------
from pointline.schema_registry import register_schema as _register_schema  # noqa: E402

_register_schema(
    "derivative_ticker", DERIVATIVE_TICKER_SCHEMA, partition_by=["exchange", "date"], has_date=True
)
register_domain(DerivativeTickerDomain())
