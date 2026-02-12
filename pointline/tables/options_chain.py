"""Options chain domain logic for parsing, validation, and transformation."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from pointline.tables._base import (
    generic_validate,
    required_columns_validation_expr,
    timestamp_validation_expr,
)
from pointline.tables.domain_contract import EventTableDomain, TableSpec
from pointline.tables.domain_registry import register_domain

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


def _normalize_schema(df: pl.DataFrame) -> pl.DataFrame:
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


def _canonicalize_vendor_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Apply canonical enum semantics for options-chain vendor-neutral frames."""
    if "option_type" in df.columns or "option_type_raw" not in df.columns:
        return df

    option_type_raw = pl.col("option_type_raw").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
    return df.with_columns(
        pl.when(option_type_raw.is_in(["call", "c", "0"]))
        .then(pl.lit(OPTION_TYPE_CALL, dtype=pl.UInt8))
        .when(option_type_raw.is_in(["put", "p", "1"]))
        .then(pl.lit(OPTION_TYPE_PUT, dtype=pl.UInt8))
        .otherwise(pl.lit(None, dtype=pl.UInt8))
        .alias("option_type")
    )


def _encode_storage(df: pl.DataFrame) -> pl.DataFrame:
    """Encode options_chain numeric fields into fixed-point integers using asset-class profile."""
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    working = with_profile_scalars(df)

    def _enc_px(float_col: str, out_col: str) -> pl.Expr:
        return (
            pl.when(pl.col(float_col).is_not_null())
            .then((pl.col(float_col) / pl.col(PROFILE_PRICE_COL)).round(0).cast(pl.Int64))
            .otherwise(None)
            .alias(out_col)
        )

    def _enc_amt(float_col: str, out_col: str) -> pl.Expr:
        return (
            pl.when(pl.col(float_col).is_not_null())
            .then((pl.col(float_col) / pl.col(PROFILE_AMOUNT_COL)).round(0).cast(pl.Int64))
            .otherwise(None)
            .alias(out_col)
        )

    result = working.with_columns(
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
    return result.drop([col for col in PROFILE_SCALAR_COLS if col in result.columns])


def _validate(df: pl.DataFrame) -> pl.DataFrame:
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


def _decode_storage(
    df: pl.DataFrame,
    *,
    keep_ints: bool = False,
) -> pl.DataFrame:
    """Decode options-chain fixed-point integers to float fields."""
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    int_cols = [
        "strike_int",
        "bid_px_int",
        "ask_px_int",
        "mark_px_int",
        "underlying_px_int",
        "bid_sz_int",
        "ask_sz_int",
    ]
    missing = [c for c in int_cols if c not in df.columns]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars(df)
    result = working.with_columns(
        [
            pl.when(pl.col("strike_int").is_not_null())
            .then((pl.col("strike_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("strike_px"),
            pl.when(pl.col("bid_px_int").is_not_null())
            .then((pl.col("bid_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("bid_px"),
            pl.when(pl.col("ask_px_int").is_not_null())
            .then((pl.col("ask_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("ask_px"),
            pl.when(pl.col("mark_px_int").is_not_null())
            .then((pl.col("mark_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("mark_px"),
            pl.when(pl.col("underlying_px_int").is_not_null())
            .then((pl.col("underlying_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("underlying_px"),
            pl.when(pl.col("bid_sz_int").is_not_null())
            .then((pl.col("bid_sz_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("bid_sz"),
            pl.when(pl.col("ask_sz_int").is_not_null())
            .then((pl.col("ask_sz_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("ask_sz"),
        ]
    )
    if not keep_ints:
        result = result.drop(int_cols)
    return result.drop([col for col in PROFILE_SCALAR_COLS if col in result.columns])


def _decode_storage_lazy(
    lf: pl.LazyFrame,
    *,
    keep_ints: bool = False,
) -> pl.LazyFrame:
    """Decode options-chain fixed-point integers lazily to float fields."""
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars_lazy,
    )

    schema = lf.collect_schema()
    int_cols = [
        "strike_int",
        "bid_px_int",
        "ask_px_int",
        "mark_px_int",
        "underlying_px_int",
        "bid_sz_int",
        "ask_sz_int",
    ]
    missing = [c for c in int_cols if c not in schema]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars_lazy(lf)
    result = working.with_columns(
        [
            pl.when(pl.col("strike_int").is_not_null())
            .then((pl.col("strike_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("strike_px"),
            pl.when(pl.col("bid_px_int").is_not_null())
            .then((pl.col("bid_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("bid_px"),
            pl.when(pl.col("ask_px_int").is_not_null())
            .then((pl.col("ask_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("ask_px"),
            pl.when(pl.col("mark_px_int").is_not_null())
            .then((pl.col("mark_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("mark_px"),
            pl.when(pl.col("underlying_px_int").is_not_null())
            .then((pl.col("underlying_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("underlying_px"),
            pl.when(pl.col("bid_sz_int").is_not_null())
            .then((pl.col("bid_sz_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("bid_sz"),
            pl.when(pl.col("ask_sz_int").is_not_null())
            .then((pl.col("ask_sz_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Float64))
            .otherwise(None)
            .alias("ask_sz"),
        ]
    )
    if not keep_ints:
        result = result.drop(int_cols)
    return result.drop(list(PROFILE_SCALAR_COLS))


def _required_decode_columns() -> tuple[str, ...]:
    """Columns needed to decode storage fields for options chain."""
    return (
        "exchange",
        "strike_int",
        "bid_px_int",
        "ask_px_int",
        "mark_px_int",
        "underlying_px_int",
        "bid_sz_int",
        "ask_sz_int",
    )


@dataclass(frozen=True)
class OptionsChainDomain(EventTableDomain):
    spec: TableSpec = TableSpec(
        table_name="options_chain",
        table_kind="event",
        schema=OPTIONS_CHAIN_SCHEMA,
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


OPTIONS_CHAIN_DOMAIN = OptionsChainDomain()


register_domain(OPTIONS_CHAIN_DOMAIN)
