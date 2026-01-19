"""L2 updates domain logic for parsing, validation, and transformation.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl
    from pointline.tables.l2_updates import parse_tardis_l2_updates_csv, normalize_l2_updates_schema

    raw_df = pl.read_csv("l2_updates.csv")
    parsed = parse_tardis_l2_updates_csv(raw_df)
    normalized = normalize_l2_updates_schema(parsed)
"""

from __future__ import annotations

from typing import Sequence

import polars as pl

from pointline.validation_utils import with_expected_exchange_id

# Schema definition matching docs/schemas.md and design.md
#
# Delta Lake Integer Type Limitations:
# - Use Int16 instead of UInt16 for exchange_id
# - Use Int32 instead of UInt32 for file_id, file_line_number
# - Use Int64 for symbol_id to match dim_symbol
# - UInt8 is supported and maps to TINYINT (use for side)
#
# This schema is the single source of truth.
L2_UPDATES_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "exchange_id": pl.Int16,
    "symbol_id": pl.Int64,
    "ts_local_us": pl.Int64,
    "ts_exch_us": pl.Int64,
    "side": pl.UInt8,       # 0=bid, 1=ask
    "price_int": pl.Int64,  # Fixed-point
    "size_int": pl.Int64,   # Fixed-point (absolute size)
    "is_snapshot": pl.Boolean,
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}


def parse_tardis_l2_updates_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Tardis incremental_book_L2 CSV format into normalized columns.
    
    Tardis columns:
    - exchange, symbol, timestamp, local_timestamp, is_snapshot, side, price, amount
    
    Returns DataFrame with columns:
    - ts_local_us (i64)
    - ts_exch_us (i64)
    - is_snapshot (bool)
    - side (u8): 0=bid, 1=ask
    - price (f64): temp column for encoding
    - amount (f64): temp column for encoding
    """
    # Check for required columns
    required_cols = ["exchange", "symbol", "timestamp", "local_timestamp", "is_snapshot", "side", "price", "amount"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"parse_tardis_l2_updates_csv: missing required columns: {missing}")
    
    result = df.clone()
    
    # Parse timestamps
    result = result.with_columns([
        pl.col("local_timestamp").cast(pl.Int64).alias("ts_local_us"),
        pl.col("timestamp").cast(pl.Int64).alias("ts_exch_us"),
    ])
    
    # Parse is_snapshot (Tardis uses "true"/"false" strings or booleans)
    # The sample showed "true", so likely string in CSV, but polars read_csv might infer bool
    # We force cast to match expectation.
    if result.schema["is_snapshot"] == pl.Utf8:
        result = result.with_columns(
            (pl.col("is_snapshot") == "true").alias("is_snapshot")
        )
    else:
        result = result.with_columns(pl.col("is_snapshot").cast(pl.Boolean))
        
    # Parse side: "bid" -> 0, "ask" -> 1
    # Tardis uses "bid", "ask"
    result = result.with_columns(
        pl.when(pl.col("side") == "bid").then(0)
        .when(pl.col("side") == "ask").then(1)
        .otherwise(None) # Should not happen
        .cast(pl.UInt8)
        .alias("side")
    )
    
    # Ensure price/amount are floats
    result = result.with_columns([
        pl.col("price").cast(pl.Float64),
        pl.col("amount").cast(pl.Float64),
    ])
    
    return result


def normalize_l2_updates_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical l2_updates schema and select only schema columns."""
    missing_required = [
        col for col in L2_UPDATES_SCHEMA
        if col not in df.columns
    ]
    if missing_required:
        raise ValueError(f"l2_updates missing required columns: {missing_required}")
    
    casts = []
    for col, dtype in L2_UPDATES_SCHEMA.items():
        if col in df.columns:
            casts.append(pl.col(col).cast(dtype))
            
    return df.with_columns(casts).select(list(L2_UPDATES_SCHEMA.keys()))


def validate_l2_updates(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to l2_updates data.
    
    Validates:
    - Required columns exist
    - side is 0 or 1
    - price_int > 0
    - size_int >= 0 (0 means delete level)
    - Valid timestamp ranges (reasonable values) for local and exchange times
    - exchange_id matches normalized exchange
    """
    if df.is_empty():
        return df
        
    required = [
        "price_int",
        "size_int",
        "side",
        "ts_local_us",
        "ts_exch_us",
        "exchange",
        "exchange_id",
        "symbol_id",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_l2_updates: missing required columns: {missing}")
        
    df_with_expected = with_expected_exchange_id(df)
    filters = [
        (pl.col("ts_local_us") > 0) & (pl.col("ts_local_us") < 2**63),
        (pl.col("ts_exch_us") > 0) & (pl.col("ts_exch_us") < 2**63),
        (pl.col("side").is_in([0, 1])),
        (pl.col("price_int") > 0),
        (pl.col("size_int") >= 0),
        (pl.col("exchange").is_not_null()),
        (pl.col("exchange_id").is_not_null()),
        (pl.col("symbol_id").is_not_null()),
        (pl.col("exchange_id") == pl.col("expected_exchange_id")),
    ]
    
    combined_filter = filters[0]
    for f in filters[1:]:
        combined_filter = combined_filter & f
        
    valid = df_with_expected.filter(combined_filter).select(df.columns)
    
    if valid.height < df.height:
        import warnings
        warnings.warn(
            f"validate_l2_updates: filtered {df.height - valid.height} invalid rows",
            UserWarning
        )
        
    return valid


def encode_l2_updates_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
) -> pl.DataFrame:
    """Encode price and amount as fixed-point integers using dim_symbol metadata.
    
    Requires:
    - df with symbol_id, price (float), amount (float)
    - dim_symbol with symbol_id, price_increment, amount_increment
    
    Computes:
    - price_int = round(price / price_increment)
    - size_int = round(amount / amount_increment)
    """
    if "symbol_id" not in df.columns:
        raise ValueError("encode_l2_updates_fixed_point: df must have 'symbol_id' column")
        
    # Check if price/amount cols exist (from parse step)
    if "price" not in df.columns or "amount" not in df.columns:
         raise ValueError("encode_l2_updates_fixed_point: df must have 'price' and 'amount' float columns")
    
    joined = df.join(
        dim_symbol.select(["symbol_id", "price_increment", "amount_increment"]),
        on="symbol_id",
        how="left",
    )
    
    # Check for missing symbol_ids
    if joined.filter(pl.col("price_increment").is_null()).height > 0:
         missing = joined.filter(pl.col("price_increment").is_null()).select("symbol_id").unique()
         raise ValueError(f"encode_l2_updates_fixed_point: symbols not found in dim_symbol: {missing.to_dict(as_series=False)}")

    # Handle single-symbol batch assumption for efficiency/correctness check if desired,
    # but join works generally. However, strict type checking is good.
    
    result = joined.with_columns([
        (pl.col("price") / pl.col("price_increment")).round().cast(pl.Int64).alias("price_int"),
        (pl.col("amount") / pl.col("amount_increment")).round().cast(pl.Int64).alias("size_int"),
    ])
    
    return result.drop(["price_increment", "amount_increment", "price", "amount"])


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int,
    exchange_symbol: str,
    *,
    ts_col: str = "ts_local_us",
    tie_breaker_cols: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Resolve symbol_ids for l2_updates data using as-of join with dim_symbol."""
    from pointline.dim_symbol import resolve_symbol_ids as _resolve_symbol_ids
    
    result = data.clone()
    if "exchange_id" not in result.columns:
        result = result.with_columns(pl.lit(exchange_id, dtype=pl.Int16).alias("exchange_id"))
    else:
        result = result.with_columns(pl.col("exchange_id").cast(pl.Int16))
        
    if "exchange_symbol" not in result.columns:
        result = result.with_columns(pl.lit(exchange_symbol).alias("exchange_symbol"))
        
    return _resolve_symbol_ids(result, dim_symbol, ts_col=ts_col, tie_breaker_cols=tie_breaker_cols)


def required_l2_updates_columns() -> Sequence[str]:
    return tuple(L2_UPDATES_SCHEMA.keys())
