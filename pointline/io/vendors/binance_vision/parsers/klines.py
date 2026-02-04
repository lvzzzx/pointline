"""Binance klines parser.

This module contains the vendor-specific parsing logic for Binance kline CSV files.
"""

import polars as pl

from pointline.io.vendors.registry import register_parser

# Raw kline column names from Binance
RAW_KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]


def _ensure_raw_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure DataFrame has correct column names."""
    if len(df.columns) < len(RAW_KLINE_COLUMNS):
        raise ValueError(
            f"parse_binance_klines_csv: expected at least {len(RAW_KLINE_COLUMNS)} columns, "
            f"got {len(df.columns)}"
        )

    if df.columns[: len(RAW_KLINE_COLUMNS)] == RAW_KLINE_COLUMNS:
        return df.select(RAW_KLINE_COLUMNS)

    # Select only the first 12 columns to avoid conflicts with extra columns
    df_subset = df.select(df.columns[: len(RAW_KLINE_COLUMNS)])

    # Rename to standard column names
    rename_map = {df_subset.columns[i]: RAW_KLINE_COLUMNS[i] for i in range(len(RAW_KLINE_COLUMNS))}
    return df_subset.rename(rename_map)


def _filter_header_row(df: pl.DataFrame) -> pl.DataFrame:
    """Filter out header row if present.

    Some Binance CSV files have headers, some don't. Detect and remove header rows.
    A row is considered a header if 'open_time' column contains the string 'open_time'.
    """
    if df.is_empty():
        return df

    # Check if first row is a header by looking for string 'open_time' in the open_time column
    # Cast to string for comparison since the column might be read as various types
    first_row_open_time = df.select(pl.col("open_time").cast(pl.Utf8).first()).item()

    # If the first value is literally the column name, it's a header row
    if first_row_open_time and first_row_open_time.lower() == "open_time":
        return df.slice(1, df.height - 1)

    return df


@register_parser(vendor="binance_vision", data_type="klines")
def parse_binance_klines_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Binance kline CSV rows into typed columns.

    Binance kline CSV columns (may or may not have header):
    open_time, open, high, low, close, volume, close_time, quote_volume,
    trade_count, taker_buy_base_volume, taker_buy_quote_volume, ignore

    Handles both cases:
    - CSVs without headers (raw data rows only)
    - CSVs with headers (filters out the header row automatically)

    Args:
        df: Raw DataFrame from Binance CSV file

    Returns:
        DataFrame with normalized columns:
        - ts_bucket_start_us (i64)
        - ts_bucket_end_us (i64)
        - open_px (f64)
        - high_px (f64)
        - low_px (f64)
        - close_px (f64)
        - volume (f64)
        - quote_volume (f64)
        - trade_count (i64)
        - taker_buy_base_volume (f64)
        - taker_buy_quote_volume (f64)

    Raises:
        ValueError: If expected columns are missing
    """
    if df.is_empty():
        return df

    result = _ensure_raw_columns(df)
    result = _filter_header_row(result)

    def to_us_expr(col_name: str) -> pl.Expr:
        col = pl.col(col_name).cast(pl.Int64, strict=False)
        return pl.when(col >= 1_000_000_000_000_000).then(col).otherwise(col * 1_000)

    select_cols = [
        to_us_expr("open_time").alias("ts_bucket_start_us"),
        to_us_expr("close_time").alias("ts_bucket_end_us"),
        pl.col("open").cast(pl.Float64, strict=False).alias("open_px"),
        pl.col("high").cast(pl.Float64, strict=False).alias("high_px"),
        pl.col("low").cast(pl.Float64, strict=False).alias("low_px"),
        pl.col("close").cast(pl.Float64, strict=False).alias("close_px"),
        pl.col("volume").cast(pl.Float64, strict=False).alias("volume"),
        pl.col("quote_volume").cast(pl.Float64, strict=False).alias("quote_volume"),
        pl.col("trade_count").cast(pl.Int64, strict=False).alias("trade_count"),
        pl.col("taker_buy_base_volume")
        .cast(pl.Float64, strict=False)
        .alias("taker_buy_base_volume"),
        pl.col("taker_buy_quote_volume")
        .cast(pl.Float64, strict=False)
        .alias("taker_buy_quote_volume"),
    ]

    if "file_line_number" in result.columns:
        select_cols = [pl.col("file_line_number")] + select_cols

    parsed = result.select(select_cols)
    return parsed.filter(
        pl.col("ts_bucket_start_us").is_not_null() & pl.col("ts_bucket_end_us").is_not_null()
    )
