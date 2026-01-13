"""Derivative ticker domain logic for parsing, validation, and transformation.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.
"""

from __future__ import annotations

from typing import Sequence

import polars as pl

from pointline.validation_utils import with_expected_exchange_id

# Schema definition matching docs/schemas.md Section 2.6
#
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - These are automatically converted to signed types (Int16 and Int32) when written
# - Use Int16 instead of UInt16 for exchange_id
# - Use Int32 instead of UInt32 for ingest_seq, file_id
# - Use Int64 for symbol_id to match dim_symbol
DERIVATIVE_TICKER_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "exchange_id": pl.Int16,
    "symbol_id": pl.Int64,
    "ts_local_us": pl.Int64,
    "ts_exch_us": pl.Int64,
    "ingest_seq": pl.Int32,
    "mark_px": pl.Float64,
    "index_px": pl.Float64,
    "last_px": pl.Float64,
    "funding_rate": pl.Float64,
    "predicted_funding_rate": pl.Float64,
    "funding_ts_us": pl.Int64,
    "open_interest": pl.Float64,
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}


def parse_tardis_derivative_ticker_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Tardis derivative_ticker CSV format into normalized columns.

    Tardis schema is standardized with exact column names:
    - exchange, symbol, timestamp, local_timestamp, funding_timestamp
    - funding_rate, predicted_funding_rate, open_interest
    - last_price, index_price, mark_price

    Returns DataFrame with columns:
    - ts_local_us (i64)
    - ts_exch_us (i64)
    - funding_ts_us (i64, nullable)
    - funding_rate (f64, nullable)
    - predicted_funding_rate (f64, nullable)
    - open_interest (f64, nullable)
    - last_px (f64, nullable)
    - index_px (f64, nullable)
    - mark_px (f64, nullable)
    """
    required_cols = [
        "exchange",
        "symbol",
        "timestamp",
        "local_timestamp",
        "funding_timestamp",
        "funding_rate",
        "predicted_funding_rate",
        "open_interest",
        "last_price",
        "index_price",
        "mark_price",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"parse_tardis_derivative_ticker_csv: missing required columns: {missing}"
        )

    result = df.clone().with_columns(
        [
            pl.col("local_timestamp").cast(pl.Int64).alias("ts_local_us"),
            pl.col("timestamp").cast(pl.Int64).alias("ts_exch_us"),
            pl.col("funding_timestamp").cast(pl.Int64, strict=False).alias("funding_ts_us"),
            pl.col("funding_rate").cast(pl.Float64, strict=False),
            pl.col("predicted_funding_rate").cast(pl.Float64, strict=False),
            pl.col("open_interest").cast(pl.Float64, strict=False),
            pl.col("last_price").cast(pl.Float64, strict=False).alias("last_px"),
            pl.col("index_price").cast(pl.Float64, strict=False).alias("index_px"),
            pl.col("mark_price").cast(pl.Float64, strict=False).alias("mark_px"),
        ]
    )

    select_cols = [
        "ts_local_us",
        "ts_exch_us",
        "funding_ts_us",
        "funding_rate",
        "predicted_funding_rate",
        "open_interest",
        "last_px",
        "index_px",
        "mark_px",
    ]
    if "file_line_number" in result.columns:
        select_cols = ["file_line_number"] + select_cols
    return result.select(select_cols)


def normalize_derivative_ticker_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical derivative_ticker schema and select only schema columns."""
    optional_columns = {
        "funding_rate",
        "predicted_funding_rate",
        "funding_ts_us",
        "open_interest",
        "last_px",
        "index_px",
        "mark_px",
    }

    missing_required = [
        col
        for col in DERIVATIVE_TICKER_SCHEMA
        if col not in df.columns and col not in optional_columns
    ]
    if missing_required:
        raise ValueError(
            f"derivative_ticker missing required columns: {missing_required}"
        )

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
    valid = df_with_expected.filter(
        (pl.col("ts_local_us") > 0)
        & (pl.col("ts_local_us") < 2**63)
        & (pl.col("ts_exch_us") > 0)
        & (pl.col("ts_exch_us") < 2**63)
        & (pl.col("exchange").is_not_null())
        & (pl.col("exchange_id").is_not_null())
        & (pl.col("symbol_id").is_not_null())
        & (pl.col("exchange_id") == pl.col("expected_exchange_id"))
        & pl.when(pl.col("mark_px").is_not_null())
        .then(pl.col("mark_px") > 0)
        .otherwise(True)
        & pl.when(pl.col("index_px").is_not_null())
        .then(pl.col("index_px") > 0)
        .otherwise(True)
        & pl.when(pl.col("last_px").is_not_null())
        .then(pl.col("last_px") > 0)
        .otherwise(True)
        & pl.when(pl.col("open_interest").is_not_null())
        .then(pl.col("open_interest") >= 0)
        .otherwise(True)
        & pl.when(pl.col("funding_ts_us").is_not_null())
        .then(pl.col("funding_ts_us") > 0)
        .otherwise(True)
    ).select(df.columns)

    if valid.height < df.height:
        import warnings

        line_col = "file_line_number" if "file_line_number" in df.columns else "__row_nr"
        df_with_line = df_with_expected
        if line_col == "__row_nr":
            df_with_line = (
                df_with_expected.with_row_index("__row_nr")
                if hasattr(df_with_expected, "with_row_index")
                else df_with_expected.with_row_count("__row_nr")
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
            ("mark_px", pl.col("mark_px").is_not_null() & (pl.col("mark_px") <= 0)),
            ("index_px", pl.col("index_px").is_not_null() & (pl.col("index_px") <= 0)),
            ("last_px", pl.col("last_px").is_not_null() & (pl.col("last_px") <= 0)),
            (
                "open_interest",
                pl.col("open_interest").is_not_null() & (pl.col("open_interest") < 0),
            ),
            (
                "funding_ts_us",
                pl.col("funding_ts_us").is_not_null() & (pl.col("funding_ts_us") <= 0),
            ),
        ]

        counts = df_with_line.select(
            [rule.sum().alias(name) for name, rule in rules]
        ).row(0)
        breakdown = []
        for (name, rule), count in zip(rules, counts):
            if count:
                sample = (
                    df_with_line.filter(rule)
                    .select(line_col)
                    .head(5)
                    .to_series()
                    .to_list()
                )
                breakdown.append(f"{name}={count} lines={sample}")

        detail = "; ".join(breakdown) if breakdown else "no rule breakdown available"
        warnings.warn(
            f"validate_derivative_ticker: filtered {df.height - valid.height} invalid rows; "
            f"{detail}",
            UserWarning,
        )

    return valid


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int,
    exchange_symbol: str,
    *,
    ts_col: str = "ts_local_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for derivative_ticker data using as-of join with dim_symbol."""
    from pointline.dim_symbol import resolve_symbol_ids as _resolve_symbol_ids

    result = data.clone()
    if "exchange_id" not in result.columns:
        result = result.with_columns(pl.lit(exchange_id, dtype=pl.Int16).alias("exchange_id"))
    else:
        result = result.with_columns(pl.col("exchange_id").cast(pl.Int16))
    if "exchange_symbol" not in result.columns:
        result = result.with_columns(pl.lit(exchange_symbol).alias("exchange_symbol"))

    return _resolve_symbol_ids(result, dim_symbol, ts_col=ts_col)


def required_derivative_ticker_columns() -> Sequence[str]:
    """Columns required for a derivative_ticker DataFrame after normalization."""
    return tuple(DERIVATIVE_TICKER_SCHEMA.keys())
