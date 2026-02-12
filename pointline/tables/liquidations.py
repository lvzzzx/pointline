"""Liquidations domain logic for parsing, validation, and transformation."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

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

SIDE_BUY = 0
SIDE_SELL = 1

LIQUIDATIONS_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "exchange_id": pl.Int16,
    "symbol_id": pl.Int64,
    "ts_local_us": pl.Int64,
    "ts_exch_us": pl.Int64,
    "liq_id": pl.Utf8,
    "side": pl.UInt8,
    "px_int": pl.Int64,
    "qty_int": pl.Int64,
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}


def normalize_liquidations_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to canonical liquidations schema and select schema columns only."""
    optional_columns = {"liq_id"}

    if "side" not in df.columns and "side_raw" in df.columns:
        side_raw = pl.col("side_raw").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
        df = df.with_columns(
            pl.when(side_raw.is_in(["buy", "b", "0"]))
            .then(pl.lit(SIDE_BUY, dtype=pl.UInt8))
            .when(side_raw.is_in(["sell", "s", "1"]))
            .then(pl.lit(SIDE_SELL, dtype=pl.UInt8))
            .otherwise(pl.lit(None, dtype=pl.UInt8))
            .alias("side")
        )

    missing_required = [
        col for col in LIQUIDATIONS_SCHEMA if col not in df.columns and col not in optional_columns
    ]
    if missing_required:
        raise ValueError(f"liquidations missing required columns: {missing_required}")

    casts: list[pl.Expr] = []
    for col, dtype in LIQUIDATIONS_SCHEMA.items():
        if col in df.columns:
            casts.append(pl.col(col).cast(dtype))
        elif col in optional_columns:
            casts.append(pl.lit(None, dtype=dtype).alias(col))
        else:
            raise ValueError(f"Required non-nullable column {col} is missing")

    return df.with_columns(casts).select(list(LIQUIDATIONS_SCHEMA.keys()))


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int | None,
    exchange_symbol: str | None,
    *,
    ts_col: str = "ts_local_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for liquidations rows using as-of dim_symbol join."""
    return generic_resolve_symbol_ids(data, dim_symbol, exchange_id, exchange_symbol, ts_col=ts_col)


def encode_fixed_point(df: pl.DataFrame, dim_symbol: pl.DataFrame, exchange: str) -> pl.DataFrame:
    """Encode liquidation price and quantity fields to fixed-point integers."""
    from pointline.encoding import get_profile

    profile = get_profile(exchange)
    return df.with_columns(
        [
            pl.when(pl.col("price_px").is_not_null())
            .then((pl.col("price_px") / profile.price).round(0).cast(pl.Int64))
            .otherwise(None)
            .alias("px_int"),
            pl.when(pl.col("qty").is_not_null())
            .then((pl.col("qty") / profile.amount).round(0).cast(pl.Int64))
            .otherwise(None)
            .alias("qty_int"),
        ]
    )


def validate_liquidations(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to liquidation rows."""
    if df.is_empty():
        return df

    required = [
        "exchange",
        "exchange_id",
        "symbol_id",
        "ts_local_us",
        "ts_exch_us",
        "side",
        "px_int",
        "qty_int",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_liquidations: missing required columns: {missing}")

    df_with_expected = with_expected_exchange_id(df)

    combined_filter = (
        timestamp_validation_expr("ts_local_us")
        & timestamp_validation_expr("ts_exch_us")
        & required_columns_validation_expr(required)
        & pl.col("side").is_in([SIDE_BUY, SIDE_SELL])
        & (pl.col("px_int") > 0)
        & (pl.col("qty_int") > 0)
        & exchange_id_validation_expr()
    )

    rules = [
        ("exchange", pl.col("exchange").is_null()),
        ("exchange_id", pl.col("exchange_id").is_null()),
        ("symbol_id", pl.col("symbol_id").is_null()),
        ("ts_local_us", pl.col("ts_local_us").is_null() | (pl.col("ts_local_us") <= 0)),
        ("ts_exch_us", pl.col("ts_exch_us").is_null() | (pl.col("ts_exch_us") <= 0)),
        ("side", pl.col("side").is_null() | ~pl.col("side").is_in([SIDE_BUY, SIDE_SELL])),
        ("px_int", pl.col("px_int").is_null() | (pl.col("px_int") <= 0)),
        ("qty_int", pl.col("qty_int").is_null() | (pl.col("qty_int") <= 0)),
        (
            "exchange_id_mismatch",
            pl.col("expected_exchange_id").is_null()
            | (pl.col("exchange_id") != pl.col("expected_exchange_id")),
        ),
    ]

    valid = generic_validate(df_with_expected, combined_filter, rules, "liquidations")
    return valid.select(df.columns)


def required_liquidations_columns() -> Sequence[str]:
    """Columns required for a liquidations DataFrame after normalization."""
    return tuple(LIQUIDATIONS_SCHEMA.keys())


# ---------------------------------------------------------------------------
# Schema registry registration
# ---------------------------------------------------------------------------
from pointline.schema_registry import register_schema as _register_schema  # noqa: E402

_register_schema(
    "liquidations", LIQUIDATIONS_SCHEMA, partition_by=["exchange", "date"], has_date=True
)
