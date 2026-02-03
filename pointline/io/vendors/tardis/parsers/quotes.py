"""Tardis quotes parser.

This module contains the vendor-specific parsing logic for Tardis quotes CSV files.
"""

import polars as pl

from pointline.io.vendors.registry import register_parser


@register_parser(vendor="tardis", data_type="quotes")
def parse_tardis_quotes_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Tardis quotes CSV format into normalized columns.

    Tardis provides timestamps as microseconds since epoch (integers).
    Tardis schema is standardized with exact column names:
    - exchange, symbol, timestamp, local_timestamp
    - bid_price, bid_amount, ask_price, ask_amount

    Both timestamp and local_timestamp are always present (Tardis handles fallback internally).
    Bid/ask fields may be empty when there are no bids or asks.

    Args:
        df: Raw DataFrame from Tardis CSV file

    Returns:
        DataFrame with normalized columns:
        - ts_local_us (i64): local timestamp in microseconds since epoch
        - ts_exch_us (i64): exchange timestamp in microseconds since epoch
        - bid_px (f64): best bid price (nullable)
        - bid_sz (f64): best bid size (nullable)
        - ask_px (f64): best ask price (nullable)
        - ask_sz (f64): best ask size (nullable)

    Raises:
        ValueError: If required columns are missing
    """
    # Check for required columns
    required_cols = ["exchange", "symbol", "timestamp", "local_timestamp"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"parse_tardis_quotes_csv: missing required columns: {missing}")

    result = df.clone()

    # Parse timestamps (both always present per Tardis spec)
    result = result.with_columns(
        [
            pl.col("local_timestamp").cast(pl.Int64).alias("ts_local_us"),
            pl.col("timestamp").cast(pl.Int64).alias("ts_exch_us"),
        ]
    )

    # Parse bid/ask fields (may be empty)
    # Tardis uses exact column names: bid_price, bid_amount, ask_price, ask_amount
    bid_ask_cols = ["bid_price", "bid_amount", "ask_price", "ask_amount"]
    missing_bid_ask = [c for c in bid_ask_cols if c not in df.columns]
    if missing_bid_ask:
        raise ValueError(f"parse_tardis_quotes_csv: missing bid/ask columns: {missing_bid_ask}")

    # Cast to float64, handling empty strings as null, and rename to _px/_sz
    result = result.with_columns(
        [
            pl.col("bid_price").cast(pl.Float64, strict=False).alias("bid_px"),
            pl.col("bid_amount").cast(pl.Float64, strict=False).alias("bid_sz"),
            pl.col("ask_price").cast(pl.Float64, strict=False).alias("ask_px"),
            pl.col("ask_amount").cast(pl.Float64, strict=False).alias("ask_sz"),
        ]
    )

    # Select only the columns we need (preserve file_line_number if provided)
    select_cols = [
        "ts_local_us",
        "ts_exch_us",
        "bid_px",
        "bid_sz",
        "ask_px",
        "ask_sz",
    ]
    if "file_line_number" in result.columns:
        select_cols = ["file_line_number"] + select_cols
    return result.select(select_cols)
