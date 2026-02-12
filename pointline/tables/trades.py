"""Trades domain logic for schema, semantics, validation, and storage mapping.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl
    from pointline.tables.trades import TRADES_DOMAIN

    normalized = TRADES_DOMAIN.normalize_schema(pl.DataFrame(...))
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

# Shared validation helpers
from pointline.tables._base import (
    generic_validate,
    required_columns_validation_expr,
    timestamp_validation_expr,
)
from pointline.tables.domain_contract import EventTableDomain, TableSpec
from pointline.tables.domain_registry import register_domain

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


@dataclass(frozen=True)
class TradesDomain(EventTableDomain):
    spec: TableSpec = TableSpec(
        table_name="trades",
        table_kind="event",
        schema=TRADES_SCHEMA,
        partition_by=("exchange", "date"),
        has_date=True,
        layer="silver",
        allowed_exchanges=None,
        ts_column="ts_local_us",
    )

    def canonicalize_vendor_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        if "side" in df.columns:
            return df
        if "side_raw" not in df.columns:
            return df

        side_raw = pl.col("side_raw").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
        return df.with_columns(
            pl.when(side_raw.is_in(["buy", "b", "0"]))
            .then(pl.lit(SIDE_BUY, dtype=pl.UInt8))
            .when(side_raw.is_in(["sell", "s", "1"]))
            .then(pl.lit(SIDE_SELL, dtype=pl.UInt8))
            .when(side_raw.is_in(["2", "unknown", "u"]))
            .then(pl.lit(SIDE_UNKNOWN, dtype=pl.UInt8))
            .otherwise(pl.lit(SIDE_UNKNOWN, dtype=pl.UInt8))
            .alias("side")
        )

    def encode_storage(self, df: pl.DataFrame) -> pl.DataFrame:
        from pointline.encoding import (
            PROFILE_AMOUNT_COL,
            PROFILE_PRICE_COL,
            PROFILE_SCALAR_COLS,
            with_profile_scalars,
        )

        price_col = "price_px"
        qty_col = "qty"
        if price_col not in df.columns or qty_col not in df.columns:
            missing = [c for c in [price_col, qty_col] if c not in df.columns]
            raise ValueError(f"encode_fixed_point: df missing columns: {missing}")

        working = with_profile_scalars(df)
        result = working.with_columns(
            [
                (pl.col(price_col) / pl.col(PROFILE_PRICE_COL))
                .round()
                .cast(pl.Int64)
                .alias("px_int"),
                (pl.col(qty_col) / pl.col(PROFILE_AMOUNT_COL))
                .round()
                .cast(pl.Int64)
                .alias("qty_int"),
            ]
        )
        return result.drop([col for col in PROFILE_SCALAR_COLS if col in result.columns])

    def normalize_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        optional_columns = {"trade_id", "flags", "conditions", "venue_id", "sequence_number"}
        missing_required = [
            col for col in TRADES_SCHEMA if col not in df.columns and col not in optional_columns
        ]
        if missing_required:
            raise ValueError(f"trades missing required columns: {missing_required}")

        casts = []
        for col, dtype in TRADES_SCHEMA.items():
            if col in df.columns:
                casts.append(pl.col(col).cast(dtype))
            elif col in optional_columns:
                casts.append(pl.lit(None, dtype=dtype).alias(col))
            else:
                raise ValueError(f"Required non-nullable column {col} is missing")

        return df.with_columns(casts).select(list(TRADES_SCHEMA.keys()))

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

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

        combined_filter = (
            (pl.col("px_int") > 0)
            & (pl.col("qty_int") > 0)
            & timestamp_validation_expr("ts_local_us")
            & timestamp_validation_expr("ts_exch_us")
            & (pl.col("side").is_in([0, 1, 2]))
            & required_columns_validation_expr(["exchange", "symbol"])
        )
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
        valid = generic_validate(df, combined_filter, rules, "trades")
        return valid.select(df.columns)

    def required_decode_columns(self) -> tuple[str, ...]:
        return ("exchange", "px_int", "qty_int")

    def decode_storage(self, df: pl.DataFrame, *, keep_ints: bool = False) -> pl.DataFrame:
        from pointline.encoding import (
            PROFILE_AMOUNT_COL,
            PROFILE_PRICE_COL,
            PROFILE_SCALAR_COLS,
            with_profile_scalars,
        )

        price_col = "price_px"
        qty_col = "qty"
        required_cols = ["px_int", "qty_int"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

        working = with_profile_scalars(df)
        result = working.with_columns(
            [
                (pl.col("px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias(price_col),
                (pl.col("qty_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64).alias(qty_col),
            ]
        )
        if not keep_ints:
            result = result.drop(required_cols)
        return result.drop([col for col in PROFILE_SCALAR_COLS if col in result.columns])

    def decode_storage_lazy(self, lf: pl.LazyFrame, *, keep_ints: bool = False) -> pl.LazyFrame:
        from pointline.encoding import (
            PROFILE_AMOUNT_COL,
            PROFILE_PRICE_COL,
            PROFILE_SCALAR_COLS,
            with_profile_scalars_lazy,
        )

        price_col = "price_px"
        qty_col = "qty"
        schema = lf.collect_schema()
        required_cols = ["px_int", "qty_int"]
        missing = [c for c in required_cols if c not in schema]
        if missing:
            raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

        working = with_profile_scalars_lazy(lf)
        result = working.with_columns(
            [
                (pl.col("px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias(price_col),
                (pl.col("qty_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64).alias(qty_col),
            ]
        )
        if not keep_ints:
            result = result.drop(required_cols)
        return result.drop(list(PROFILE_SCALAR_COLS))


TRADES_DOMAIN = TradesDomain()


register_domain(TRADES_DOMAIN)
