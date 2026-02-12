"""Options chain domain logic for parsing, validation, and transformation."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from pointline.tables._base import (
    generic_validate,
    required_columns_validation_expr,
    timestamp_validation_expr,
)

# Required metadata fields for ingestion
REQUIRED_METADATA_FIELDS: set[str] = set()

OPTION_TYPE_CALL = 0
OPTION_TYPE_PUT = 1

OPTIONS_CHAIN_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "underlying_symbol": pl.Utf8,
    "symbol": pl.Utf8,
    "underlying_index": pl.Utf8,
    "ts_local_us": pl.Int64,
    "ts_exch_us": pl.Int64,
    "option_type": pl.UInt8,
    "strike_int": pl.Int64,
    "expiry_ts_us": pl.Int64,
    "bid_px_int": pl.Int64,
    "ask_px_int": pl.Int64,
    "bid_sz_int": pl.Int64,
    "ask_sz_int": pl.Int64,
    "mark_px_int": pl.Int64,
    "underlying_px_int": pl.Int64,
    "iv": pl.Float64,
    "mark_iv": pl.Float64,
    "delta": pl.Float64,
    "gamma": pl.Float64,
    "vega": pl.Float64,
    "theta": pl.Float64,
    "rho": pl.Float64,
    "open_interest": pl.Float64,
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}


def normalize_options_chain_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to canonical options_chain schema and select schema columns only."""
    optional_columns = {
        "underlying_symbol",
        "underlying_index",
        "bid_px_int",
        "ask_px_int",
        "bid_sz_int",
        "ask_sz_int",
        "mark_px_int",
        "underlying_px_int",
        "iv",
        "mark_iv",
        "delta",
        "gamma",
        "vega",
        "theta",
        "rho",
        "open_interest",
    }

    if "option_type" not in df.columns and "option_type_raw" in df.columns:
        option_type_raw = (
            pl.col("option_type_raw").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
        )
        df = df.with_columns(
            pl.when(option_type_raw.is_in(["call", "c", "0"]))
            .then(pl.lit(OPTION_TYPE_CALL, dtype=pl.UInt8))
            .when(option_type_raw.is_in(["put", "p", "1"]))
            .then(pl.lit(OPTION_TYPE_PUT, dtype=pl.UInt8))
            .otherwise(pl.lit(None, dtype=pl.UInt8))
            .alias("option_type")
        )

    missing_required = [
        col for col in OPTIONS_CHAIN_SCHEMA if col not in df.columns and col not in optional_columns
    ]
    if missing_required:
        raise ValueError(f"options_chain missing required columns: {missing_required}")

    casts: list[pl.Expr] = []
    for col, dtype in OPTIONS_CHAIN_SCHEMA.items():
        if col in df.columns:
            casts.append(pl.col(col).cast(dtype))
        elif col in optional_columns:
            casts.append(pl.lit(None, dtype=dtype).alias(col))
        else:
            raise ValueError(f"Required non-nullable column {col} is missing")

    return df.with_columns(casts).select(list(OPTIONS_CHAIN_SCHEMA.keys()))


def encode_fixed_point(df: pl.DataFrame, dim_symbol: pl.DataFrame, exchange: str) -> pl.DataFrame:
    """Encode options_chain numeric fields into fixed-point integers using asset-class profile."""
    from pointline.encoding import get_profile

    profile = get_profile(exchange)

    def _enc_px(float_col: str, out_col: str) -> pl.Expr:
        return (
            pl.when(pl.col(float_col).is_not_null())
            .then((pl.col(float_col) / profile.price).round(0).cast(pl.Int64))
            .otherwise(None)
            .alias(out_col)
        )

    def _enc_amt(float_col: str, out_col: str) -> pl.Expr:
        return (
            pl.when(pl.col(float_col).is_not_null())
            .then((pl.col(float_col) / profile.amount).round(0).cast(pl.Int64))
            .otherwise(None)
            .alias(out_col)
        )

    return df.with_columns(
        [
            _enc_px("strike_px", "strike_int"),
            _enc_px("bid_px", "bid_px_int"),
            _enc_px("ask_px", "ask_px_int"),
            _enc_px("mark_px", "mark_px_int"),
            _enc_px("underlying_px", "underlying_px_int"),
            _enc_amt("bid_sz", "bid_sz_int"),
            _enc_amt("ask_sz", "ask_sz_int"),
        ]
    )


def validate_options_chain(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to options_chain rows."""
    if df.is_empty():
        return df

    required = [
        "exchange",
        "symbol",
        "ts_local_us",
        "ts_exch_us",
        "expiry_ts_us",
        "option_type",
        "strike_int",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_options_chain: missing required columns: {missing}")

    non_negative_cols = [
        "strike_int",
        "bid_px_int",
        "ask_px_int",
        "bid_sz_int",
        "ask_sz_int",
        "mark_px_int",
        "underlying_px_int",
        "open_interest",
    ]

    non_negative_expr = pl.lit(True)
    for col in non_negative_cols:
        if col in df.columns:
            non_negative_expr = non_negative_expr & pl.when(pl.col(col).is_not_null()).then(
                pl.col(col) >= 0
            ).otherwise(True)

    combined_filter = (
        timestamp_validation_expr("ts_local_us")
        & timestamp_validation_expr("ts_exch_us")
        & timestamp_validation_expr("expiry_ts_us")
        & required_columns_validation_expr(required)
        & pl.col("option_type").is_in([OPTION_TYPE_CALL, OPTION_TYPE_PUT])
        & non_negative_expr
    )

    rules = [
        ("exchange", pl.col("exchange").is_null()),
        ("symbol", pl.col("symbol").is_null()),
        ("ts_local_us", pl.col("ts_local_us").is_null() | (pl.col("ts_local_us") <= 0)),
        ("ts_exch_us", pl.col("ts_exch_us").is_null() | (pl.col("ts_exch_us") <= 0)),
        ("expiry_ts_us", pl.col("expiry_ts_us").is_null() | (pl.col("expiry_ts_us") <= 0)),
        ("option_type", pl.col("option_type").is_null() | ~pl.col("option_type").is_in([0, 1])),
        ("strike_int", pl.col("strike_int").is_null() | (pl.col("strike_int") < 0)),
        ("bid_px_int", pl.col("bid_px_int").is_not_null() & (pl.col("bid_px_int") < 0)),
        ("ask_px_int", pl.col("ask_px_int").is_not_null() & (pl.col("ask_px_int") < 0)),
        ("bid_sz_int", pl.col("bid_sz_int").is_not_null() & (pl.col("bid_sz_int") < 0)),
        ("ask_sz_int", pl.col("ask_sz_int").is_not_null() & (pl.col("ask_sz_int") < 0)),
        (
            "open_interest",
            pl.col("open_interest").is_not_null() & (pl.col("open_interest") < 0),
        ),
    ]

    valid = generic_validate(df, combined_filter, rules, "options_chain")
    return valid.select(df.columns)


def required_options_chain_columns() -> Sequence[str]:
    """Columns required for an options_chain DataFrame after normalization."""
    return tuple(OPTIONS_CHAIN_SCHEMA.keys())


# ---------------------------------------------------------------------------
# Schema registry registration
# ---------------------------------------------------------------------------
from pointline.schema_registry import register_schema as _register_schema  # noqa: E402

_register_schema(
    "options_chain", OPTIONS_CHAIN_SCHEMA, partition_by=["exchange", "date"], has_date=True
)
