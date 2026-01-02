"""Trades domain logic for parsing, validation, and transformation.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl
    from pointline.trades import parse_tardis_trades_csv, normalize_trades_schema

    raw_df = pl.read_csv("trades.csv")
    parsed = parse_tardis_trades_csv(raw_df)
    normalized = normalize_trades_schema(parsed)
"""

from __future__ import annotations

from typing import Sequence

import polars as pl

# Schema definition matching design.md Section 5.3
# 
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - These are automatically converted to signed types (Int16 and Int32) when written
# - Use Int16 instead of UInt16 for exchange_id
# - Use Int32 instead of UInt32 for symbol_id, ingest_seq, file_id, flags
# - UInt8 is supported and maps to TINYINT (use for side, asset_type)
#
# This schema is the single source of truth - all code should use these types directly.
TRADES_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,  # Exchange name (string) for partitioning and human readability
    "exchange_id": pl.Int16,  # Delta Lake stores as Int16 (not UInt16) - for joins and compression
    "symbol_id": pl.Int64,  # Match dim_symbol's symbol_id type
    "ts_local_us": pl.Int64,
    "ts_exch_us": pl.Int64,
    "ingest_seq": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
    "trade_id": pl.Utf8,
    "side": pl.UInt8,  # UInt8 is supported (maps to TINYINT)
    "price_int": pl.Int64,
    "qty_int": pl.Int64,
    "flags": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
    "file_id": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
    "file_line_number": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
}

# Side encoding constants
SIDE_BUY = 0
SIDE_SELL = 1
SIDE_UNKNOWN = 2




def _map_side_to_code(side: str | int | None) -> int:
    """Map side string to u8 code: 0=buy, 1=sell, 2=unknown."""
    if side is None:
        return SIDE_UNKNOWN
    
    if isinstance(side, int):
        if side in (0, 1, 2):
            return side
        return SIDE_UNKNOWN
    
    side_lower = str(side).lower().strip()
    if side_lower in ("buy", "b", "0"):
        return SIDE_BUY
    elif side_lower in ("sell", "s", "1"):
        return SIDE_SELL
    else:
        return SIDE_UNKNOWN


def parse_tardis_trades_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Tardis trades CSV format into normalized columns.
    
    Tardis provides timestamps as microseconds since epoch (integers).
    
    Handles common Tardis column name variations:
    - Timestamps: local_timestamp, timestamp, localTimestamp, etc.
    - Trade ID: trade_id, tradeId, id
    - Side: side, takerSide, taker_side
    - Price: price, tradePrice, trade_price
    - Quantity: amount, quantity, size, qty
    
    Returns DataFrame with columns:
    - ts_local_us (i64): local timestamp in microseconds since epoch
    - ts_exch_us (i64): exchange timestamp in microseconds since epoch (nullable)
    - trade_id (str): trade identifier (nullable)
    - side (u8): 0=buy, 1=sell, 2=unknown
    - price (f64): trade price
    - qty (f64): trade quantity
    """
    result = df.clone()
    
    # Find timestamp columns (flexible matching)
    ts_local_col = None
    ts_exch_col = None
    
    for col in df.columns:
        col_lower = col.lower()
        if "local" in col_lower and "timestamp" in col_lower:
            ts_local_col = col
        elif "timestamp" in col_lower and "local" not in col_lower and ts_local_col is None:
            # If no local timestamp found, use first timestamp column
            if ts_exch_col is None:
                ts_exch_col = col
        elif "timestamp" in col_lower and "exch" in col_lower:
            ts_exch_col = col
    
    # Parse local timestamp (required)
    # Tardis provides timestamps as microseconds since epoch (integers)
    if ts_local_col:
        result = result.with_columns(
            pl.col(ts_local_col).cast(pl.Int64).alias("ts_local_us")
        )
    else:
        raise ValueError("Could not find local_timestamp column in CSV")
    
    # Parse exchange timestamp (optional)
    # Tardis provides timestamps as microseconds since epoch (integers)
    if ts_exch_col:
        result = result.with_columns(
            pl.col(ts_exch_col).cast(pl.Int64).alias("ts_exch_us")
        )
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
            pl.col(side_col)
            .map_elements(_map_side_to_code, return_dtype=pl.UInt8)
            .fill_null(SIDE_UNKNOWN)
            .alias("side")
        )
    else:
        result = result.with_columns(pl.lit(SIDE_UNKNOWN, dtype=pl.UInt8).alias("side"))
    
    # Find price column
    price_col = None
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ("price", "tradeprice", "trade_price", "tradePrice"):
            price_col = col
            break
    
    if price_col:
        result = result.with_columns(pl.col(price_col).cast(pl.Float64).alias("price"))
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
    
    # Select only the columns we need
    return result.select([
        "ts_local_us",
        "ts_exch_us",
        "trade_id",
        "side",
        "price",
        "qty",
    ])


def normalize_trades_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical trades schema and select only schema columns.
    
    Ensures all required columns exist and have correct types.
    Optional columns (trade_id, flags) are filled with None if missing.
    Drops any extra columns (e.g., original float columns, dim_symbol metadata).
    """
    # Optional columns that can be missing
    optional_columns = {"trade_id", "flags"}
    
    # Check for missing required (non-optional) columns
    missing_required = [
        col for col in TRADES_SCHEMA
        if col not in df.columns and col not in optional_columns
    ]
    if missing_required:
        raise ValueError(f"trades missing required columns: {missing_required}")
    
    # Cast columns to schema types
    casts = []
    for col, dtype in TRADES_SCHEMA.items():
        if col in df.columns:
            casts.append(pl.col(col).cast(dtype))
        else:
            # Fill missing optional columns with None
            if col in optional_columns:
                casts.append(pl.lit(None, dtype=dtype).alias(col))
            else:
                raise ValueError(f"Required non-nullable column {col} is missing")
    
    # Cast and select only schema columns (drops extra columns)
    return df.with_columns(casts).select(list(TRADES_SCHEMA.keys()))


def validate_trades(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to trades data.
    
    Validates:
    - Non-negative price_int and qty_int
    - Valid timestamp ranges (reasonable values)
    - Valid side codes (0-2)
    - Non-null required fields
    - Exchange column exists and is non-null
    - Exchange_id matches exchange via EXCHANGE_MAP
    
    Returns filtered DataFrame (invalid rows removed) or raises on critical errors.
    """
    if df.is_empty():
        return df
    
    # Check required columns
    required = ["price_int", "qty_int", "ts_local_us", "side", "exchange", "exchange_id", "symbol_id"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_trades: missing required columns: {missing}")
    
    # Filter invalid rows
    valid = df.filter(
        (pl.col("price_int") > 0) &
        (pl.col("qty_int") > 0) &
        (pl.col("ts_local_us") > 0) &
        (pl.col("ts_local_us") < 2**63) &
        (pl.col("side").is_in([0, 1, 2])) &
        (pl.col("exchange").is_not_null()) &
        (pl.col("exchange_id").is_not_null()) &
        (pl.col("symbol_id").is_not_null())
    )
    
    # Warn if rows were filtered
    if valid.height < df.height:
        import warnings
        warnings.warn(
            f"validate_trades: filtered {df.height - valid.height} invalid rows",
            UserWarning
        )
    
    return valid


def encode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    *,
    price_col: str = "price",
    qty_col: str = "qty",
) -> pl.DataFrame:
    """Encode price and quantity as fixed-point integers using dim_symbol metadata.
    
    Requires:
    - df must have 'symbol_id' column (from resolve_symbol_ids)
    - dim_symbol must have 'symbol_id', 'price_increment', 'amount_increment' columns
    
    Computes:
    - price_int = round(price / price_increment)
    - qty_int = round(qty / amount_increment)
    
    Returns DataFrame with price_int and qty_int columns added.
    """
    if "symbol_id" not in df.columns:
        raise ValueError("encode_fixed_point: df must have 'symbol_id' column")
    
    required_dims = ["symbol_id", "price_increment", "amount_increment"]
    missing = [c for c in required_dims if c not in dim_symbol.columns]
    if missing:
        raise ValueError(f"encode_fixed_point: dim_symbol missing columns: {missing}")
    
    # Join to get increments
    joined = df.join(
        dim_symbol.select(["symbol_id", "price_increment", "amount_increment"]),
        on="symbol_id",
        how="left",
    )
    
    # Check for missing symbol_ids
    missing_ids = joined.filter(pl.col("price_increment").is_null())
    if not missing_ids.is_empty():
        missing_symbols = missing_ids.select("symbol_id").unique()
        raise ValueError(
            f"encode_fixed_point: {missing_symbols.height} symbol_ids not found in dim_symbol"
        )
    
    # Encode to fixed-point
    result = joined.with_columns([
        (pl.col(price_col) / pl.col("price_increment")).round().cast(pl.Int64).alias("price_int"),
        (pl.col(qty_col) / pl.col("amount_increment")).round().cast(pl.Int64).alias("qty_int"),
    ])
    
    # Drop intermediate columns
    return result.drop(["price_increment", "amount_increment"])


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int,
    exchange_symbol: str,
    *,
    ts_col: str = "ts_local_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for trades data using as-of join with dim_symbol.
    
    This is a wrapper around the dim_symbol.resolve_symbol_ids function,
    but adds exchange_id and exchange_symbol columns first if needed.
    
    Args:
        data: DataFrame with ts_local_us (or ts_col) column
        dim_symbol: dim_symbol table in canonical schema
        exchange_id: Exchange ID to use for all rows
        exchange_symbol: Exchange symbol to use for all rows
        ts_col: Timestamp column name (default: ts_local_us)
    
    Returns:
        DataFrame with symbol_id column added
    """
    from pointline.dim_symbol import resolve_symbol_ids as _resolve_symbol_ids
    
    # Add exchange_id and exchange_symbol if not present
    result = data.clone()
    if "exchange_id" not in result.columns:
        # Cast to match dim_symbol's exchange_id type (Int16, not UInt16)
        result = result.with_columns(pl.lit(exchange_id, dtype=pl.Int16).alias("exchange_id"))
    else:
        # Ensure existing exchange_id matches dim_symbol type
        result = result.with_columns(pl.col("exchange_id").cast(pl.Int16))
    if "exchange_symbol" not in result.columns:
        result = result.with_columns(pl.lit(exchange_symbol).alias("exchange_symbol"))
    
    # Use the dim_symbol function
    return _resolve_symbol_ids(result, dim_symbol, ts_col=ts_col)


def required_trades_columns() -> Sequence[str]:
    """Columns required for a trades DataFrame after normalization."""
    return tuple(TRADES_SCHEMA.keys())
