"""Liquidations domain logic for parsing, validation, and transformation."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import polars as pl

from pointline.tables._base import (
    generic_validate,
    required_columns_validation_expr,
    timestamp_validation_expr,
)
from pointline.tables.domain_contract import TableDomain, TableSpec
from pointline.tables.domain_registry import register_domain

# Required metadata fields for ingestion
REQUIRED_METADATA_FIELDS: set[str] = set()

SIDE_BUY = 0
SIDE_SELL = 1

LIQUIDATIONS_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "symbol": pl.Utf8,
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


def canonicalize_liquidations_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Apply canonical enum semantics for liquidation vendor-neutral frames."""
    if "side" in df.columns or "side_raw" not in df.columns:
        return df

    side_raw = pl.col("side_raw").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
    return df.with_columns(
        pl.when(side_raw.is_in(["buy", "b", "0"]))
        .then(pl.lit(SIDE_BUY, dtype=pl.UInt8))
        .when(side_raw.is_in(["sell", "s", "1"]))
        .then(pl.lit(SIDE_SELL, dtype=pl.UInt8))
        .otherwise(pl.lit(None, dtype=pl.UInt8))
        .alias("side")
    )


def encode_fixed_point(df: pl.DataFrame) -> pl.DataFrame:
    """Encode liquidation price and quantity fields to fixed-point integers."""
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    working = with_profile_scalars(df)

    result = working.with_columns(
        [
            pl.when(pl.col("price_px").is_not_null())
            .then((pl.col("price_px") / pl.col(PROFILE_PRICE_COL)).round(0).cast(pl.Int64))
            .otherwise(None)
            .alias("px_int"),
            pl.when(pl.col("qty").is_not_null())
            .then((pl.col("qty") / pl.col(PROFILE_AMOUNT_COL)).round(0).cast(pl.Int64))
            .otherwise(None)
            .alias("qty_int"),
        ]
    )
    return result.drop([col for col in PROFILE_SCALAR_COLS if col in result.columns])


def validate_liquidations(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to liquidation rows."""
    if df.is_empty():
        return df

    required = [
        "exchange",
        "symbol",
        "ts_local_us",
        "ts_exch_us",
        "side",
        "px_int",
        "qty_int",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_liquidations: missing required columns: {missing}")

    combined_filter = (
        timestamp_validation_expr("ts_local_us")
        & timestamp_validation_expr("ts_exch_us")
        & required_columns_validation_expr(required)
        & pl.col("side").is_in([SIDE_BUY, SIDE_SELL])
        & (pl.col("px_int") > 0)
        & (pl.col("qty_int") > 0)
    )

    rules = [
        ("exchange", pl.col("exchange").is_null()),
        ("symbol", pl.col("symbol").is_null()),
        ("ts_local_us", pl.col("ts_local_us").is_null() | (pl.col("ts_local_us") <= 0)),
        ("ts_exch_us", pl.col("ts_exch_us").is_null() | (pl.col("ts_exch_us") <= 0)),
        ("side", pl.col("side").is_null() | ~pl.col("side").is_in([SIDE_BUY, SIDE_SELL])),
        ("px_int", pl.col("px_int").is_null() | (pl.col("px_int") <= 0)),
        ("qty_int", pl.col("qty_int").is_null() | (pl.col("qty_int") <= 0)),
    ]

    valid = generic_validate(df, combined_filter, rules, "liquidations")
    return valid.select(df.columns)


def decode_fixed_point(
    df: pl.DataFrame,
    *,
    keep_ints: bool = False,
) -> pl.DataFrame:
    """Decode liquidations fixed-point integers to float fields."""
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    required_cols = ["px_int", "qty_int"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars(df)
    result = working.with_columns(
        [
            pl.when(pl.col("px_int").is_not_null())
            .then((pl.col("px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("price_px"),
            pl.when(pl.col("qty_int").is_not_null())
            .then((pl.col("qty_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("qty"),
        ]
    )
    if not keep_ints:
        result = result.drop(required_cols)
    return result.drop([col for col in PROFILE_SCALAR_COLS if col in result.columns])


def decode_fixed_point_lazy(
    lf: pl.LazyFrame,
    *,
    keep_ints: bool = False,
) -> pl.LazyFrame:
    """Decode liquidations fixed-point integers lazily to float fields."""
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars_lazy,
    )

    schema = lf.collect_schema()
    required_cols = ["px_int", "qty_int"]
    missing = [c for c in required_cols if c not in schema]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars_lazy(lf)
    result = working.with_columns(
        [
            pl.when(pl.col("px_int").is_not_null())
            .then((pl.col("px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("price_px"),
            pl.when(pl.col("qty_int").is_not_null())
            .then((pl.col("qty_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("qty"),
        ]
    )
    if not keep_ints:
        result = result.drop(required_cols)
    return result.drop(list(PROFILE_SCALAR_COLS))


def required_liquidations_columns() -> Sequence[str]:
    """Columns required for a liquidations DataFrame after normalization."""
    return tuple(LIQUIDATIONS_SCHEMA.keys())


def required_decode_columns() -> tuple[str, ...]:
    """Columns needed to decode storage fields for liquidations."""
    return ("exchange", "px_int", "qty_int")


@dataclass(frozen=True)
class LiquidationsDomain(TableDomain):
    spec: TableSpec = TableSpec(
        table_name="liquidations",
        schema=LIQUIDATIONS_SCHEMA,
        partition_by=("exchange", "date"),
        has_date=True,
        layer="silver",
        allowed_exchanges=None,
        ts_column="ts_local_us",
    )

    def canonicalize_vendor_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        return canonicalize_liquidations_frame(df)

    def encode_storage(self, df: pl.DataFrame) -> pl.DataFrame:
        return encode_fixed_point(df)

    def normalize_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        return normalize_liquidations_schema(df)

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        return validate_liquidations(df)

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
    "liquidations", LIQUIDATIONS_SCHEMA, partition_by=["exchange", "date"], has_date=True
)
register_domain(LiquidationsDomain())
