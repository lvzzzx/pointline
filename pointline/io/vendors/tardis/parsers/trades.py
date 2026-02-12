"""Tardis trades parser.

This module contains the vendor-specific parsing logic for Tardis trades CSV files.
"""

import polars as pl

from pointline.io.vendors.registry import register_parser


@register_parser(vendor="tardis", data_type="trades")
def parse_tardis_trades_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Tardis trades CSV format into normalized columns.

    Tardis provides timestamps as microseconds since epoch (integers).

    Handles common Tardis column name variations:
    - Timestamps: local_timestamp, timestamp, localTimestamp, etc.
    - Trade ID: trade_id, tradeId, id
    - Side: side, takerSide, taker_side
    - Price: price, tradePrice, trade_price
    - Quantity: amount, quantity, size, qty

    Args:
        df: Raw DataFrame from Tardis CSV file

    Returns:
        DataFrame with normalized columns:
        - ts_local_us (i64): local timestamp in microseconds since epoch
        - ts_exch_us (i64): exchange timestamp in microseconds since epoch (nullable)
        - trade_id (str): trade identifier (nullable)
        - side_raw (str): vendor-side value normalized to lowercase string
        - price_px (f64): trade price
        - qty (f64): trade quantity

    Raises:
        ValueError: If required columns (timestamp, price, quantity) are missing
    """
    result = df.clone()

    # Find timestamp columns (flexible matching)
    ts_local_col = None
    ts_exch_col = None

    for col in df.columns:
        col_lower = col.lower()
        if "timestamp" not in col_lower:
            continue
        if "local" in col_lower:
            ts_local_col = col
            continue
        if "exch" in col_lower:
            ts_exch_col = col
            continue
        if ts_exch_col is None:
            ts_exch_col = col

    # Parse local timestamp (required)
    # Tardis provides timestamps as microseconds since epoch (integers)
    if ts_local_col:
        result = result.with_columns(pl.col(ts_local_col).cast(pl.Int64).alias("ts_local_us"))
    else:
        raise ValueError("Could not find local_timestamp column in CSV")

    # Parse exchange timestamp (optional)
    # Tardis provides timestamps as microseconds since epoch (integers)
    if ts_exch_col:
        result = result.with_columns(pl.col(ts_exch_col).cast(pl.Int64).alias("ts_exch_us"))
    else:
        result = result.with_columns(pl.lit(None, dtype=pl.Int64).alias("ts_exch_us"))

    # Find trade_id column
    trade_id_col = None
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ("trade_id", "tradeid", "id", "trade_id_str"):
            trade_id_col = col
            break

    if trade_id_col:
        result = result.with_columns(pl.col(trade_id_col).cast(pl.Utf8).alias("trade_id"))
    else:
        result = result.with_columns(pl.lit(None, dtype=pl.Utf8).alias("trade_id"))

    # Find side column
    side_col = None
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ("side", "takerside", "taker_side", "takerSide"):
            side_col = col
            break

    if side_col:
        result = result.with_columns(
            pl.col(side_col).cast(pl.Utf8).str.to_lowercase().str.strip_chars().alias("side_raw")
        )
    else:
        result = result.with_columns(pl.lit("unknown", dtype=pl.Utf8).alias("side_raw"))

    # Find price column
    price_col = None
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ("price", "price_px", "tradeprice", "trade_price", "tradePrice"):
            price_col = col
            break

    if price_col:
        result = result.with_columns(pl.col(price_col).cast(pl.Float64).alias("price_px"))
    else:
        raise ValueError("Could not find price column in CSV")

    # Find quantity column
    qty_col = None
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ("amount", "quantity", "size", "qty", "volume"):
            qty_col = col
            break

    if qty_col:
        result = result.with_columns(pl.col(qty_col).cast(pl.Float64).alias("qty"))
    else:
        raise ValueError("Could not find quantity/amount column in CSV")

    # Select only the columns we need (preserve file_line_number if provided)
    select_cols = [
        "ts_local_us",
        "ts_exch_us",
        "trade_id",
        "side_raw",
        "price_px",
        "qty",
    ]
    if "file_line_number" in result.columns:
        select_cols = ["file_line_number"] + select_cols
    return result.select(select_cols)
