"""Tardis book snapshots parser.

This module contains the vendor-specific parsing logic for Tardis book snapshots CSV files.
"""

import polars as pl

from pointline.io.parsers.registry import register_parser


@register_parser(vendor="tardis", data_type="book_snapshot_25")
def parse_tardis_book_snapshots_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Tardis book snapshots CSV format into normalized columns.

    Tardis provides timestamps as microseconds since epoch (integers).
    Tardis schema is standardized with exact column names:
    - exchange, symbol, timestamp, local_timestamp
    - asks[0..24].price, asks[0..24].amount
    - bids[0..24].price, bids[0..24].amount

    Both timestamp and local_timestamp are always present (Tardis handles fallback internally).
    Missing levels may be empty strings or null.

    Args:
        df: Raw DataFrame from Tardis CSV file

    Returns:
        DataFrame with normalized columns:
        - ts_local_us (i64): local timestamp in microseconds since epoch
        - ts_exch_us (i64): exchange timestamp in microseconds since epoch
        - asks[0..24].price (f64): ask prices (nullable)
        - asks[0..24].amount (f64): ask sizes (nullable)
        - bids[0..24].price (f64): bid prices (nullable)
        - bids[0..24].amount (f64): bid sizes (nullable)

    Raises:
        ValueError: If required columns are missing
    """
    # Check for required columns
    required_cols = ["exchange", "symbol", "timestamp", "local_timestamp"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"parse_tardis_book_snapshots_csv: missing required columns: {missing}")

    result = df.clone()

    # Parse timestamps (both always present per Tardis spec)
    result = result.with_columns(
        [
            pl.col("local_timestamp").cast(pl.Int64).alias("ts_local_us"),
            pl.col("timestamp").cast(pl.Int64).alias("ts_exch_us"),
        ]
    )

    # Find array columns: asks[0..24].price, asks[0..24].amount, bids[0..24].price,
    # bids[0..24].amount
    # Tardis uses exact naming: asks[0].price, asks[1].price, ..., asks[24].price
    asks_price_cols = [f"asks[{i}].price" for i in range(25)]
    asks_amount_cols = [f"asks[{i}].amount" for i in range(25)]
    bids_price_cols = [f"bids[{i}].price" for i in range(25)]
    bids_amount_cols = [f"bids[{i}].amount" for i in range(25)]

    # Check which columns exist (some may be missing if fewer than 25 levels)
    existing_asks_price = [c for c in asks_price_cols if c in df.columns]
    existing_asks_amount = [c for c in asks_amount_cols if c in df.columns]
    existing_bids_price = [c for c in bids_price_cols if c in df.columns]
    existing_bids_amount = [c for c in bids_amount_cols if c in df.columns]

    if not existing_asks_price and not existing_bids_price:
        raise ValueError(
            "parse_tardis_book_snapshots_csv: no asks or bids price columns found. "
            "Expected asks[0].price, asks[1].price, ... or bids[0].price, bids[1].price, ..."
        )

    # Cast existing level columns to float64, handling empty strings as null.
    level_cols = (
        existing_asks_price + existing_asks_amount + existing_bids_price + existing_bids_amount
    )
    if level_cols:
        result = result.with_columns(
            [pl.col(col).cast(pl.Float64, strict=False) for col in level_cols]
        )

    # Select only the columns we need (preserve file_line_number if present).
    keep_cols = ["ts_local_us", "ts_exch_us", *level_cols]
    if "file_line_number" in result.columns:
        keep_cols.append("file_line_number")
    return result.select(keep_cols)
