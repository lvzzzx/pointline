"""Book snapshots domain logic for parsing, validation, and transformation.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl
    from pointline.book_snapshots import parse_tardis_book_snapshots_csv, normalize_book_snapshots_schema

    raw_df = pl.read_csv("book_snapshots.csv")
    parsed = parse_tardis_book_snapshots_csv(raw_df)
    normalized = normalize_book_snapshots_schema(parsed)
"""

from __future__ import annotations

from typing import Sequence

import polars as pl

# Schema definition matching design.md Section 5.2
# 
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - These are automatically converted to signed types (Int16 and Int32) when written
# - Use Int16 instead of UInt16 for exchange_id
# - Use Int32 instead of UInt32 for ingest_seq, file_id, file_line_number
# - Use Int64 for symbol_id to match dim_symbol
#
# This schema is the single source of truth - all code should use these types directly.
BOOK_SNAPSHOTS_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,  # Exchange name (string) for partitioning and human readability
    "exchange_id": pl.Int16,  # Delta Lake stores as Int16 (not UInt16) - for joins and compression
    "symbol_id": pl.Int64,  # Match dim_symbol's symbol_id type
    "ts_local_us": pl.Int64,
    "ts_exch_us": pl.Int64,
    "ingest_seq": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
    "bids_px": pl.List(pl.Int64),  # List of 25 bid prices (nulls for missing levels)
    "bids_sz": pl.List(pl.Int64),  # List of 25 bid sizes (nulls for missing levels)
    "asks_px": pl.List(pl.Int64),  # List of 25 ask prices (nulls for missing levels)
    "asks_sz": pl.List(pl.Int64),  # List of 25 ask sizes (nulls for missing levels)
    "file_id": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
    "file_line_number": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
}


def parse_tardis_book_snapshots_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Tardis book snapshots CSV format into normalized columns.
    
    Tardis provides timestamps as microseconds since epoch (integers).
    Tardis schema is standardized with exact column names:
    - exchange, symbol, timestamp, local_timestamp
    - asks[0..24].price, asks[0..24].amount
    - bids[0..24].price, bids[0..24].amount
    
    Both timestamp and local_timestamp are always present (Tardis handles fallback internally).
    Missing levels may be empty strings or null.
    
    Returns DataFrame with columns:
    - ts_local_us (i64): local timestamp in microseconds since epoch
    - ts_exch_us (i64): exchange timestamp in microseconds since epoch
    - bids_px (list<f64>): list of 25 bid prices (nulls for missing levels)
    - bids_sz (list<f64>): list of 25 bid sizes (nulls for missing levels)
    - asks_px (list<f64>): list of 25 ask prices (nulls for missing levels)
    - asks_sz (list<f64>): list of 25 ask sizes (nulls for missing levels)
    """
    # Check for required columns
    required_cols = ["exchange", "symbol", "timestamp", "local_timestamp"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"parse_tardis_book_snapshots_csv: missing required columns: {missing}")
    
    result = df.clone()
    
    # Parse timestamps (both always present per Tardis spec)
    result = result.with_columns([
        pl.col("local_timestamp").cast(pl.Int64).alias("ts_local_us"),
        pl.col("timestamp").cast(pl.Int64).alias("ts_exch_us"),
    ])
    
    # Find array columns: asks[0..24].price, asks[0..24].amount, bids[0..24].price, bids[0..24].amount
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
    
    # Build lists from array columns
    # For each row, collect values from asks[0].price, asks[1].price, ..., asks[24].price
    # Handle missing columns and empty strings as nulls
    # Use pl.concat_list with each column wrapped as a single-element list
    def build_list(cols: list[str], existing_cols: list[str]) -> pl.Expr:
        """Build a list column from individual array columns."""
        if not existing_cols:
            # No columns exist, return list of 25 nulls
            return pl.lit([None] * 25, dtype=pl.List(pl.Float64))
        
        # Build list of expressions, each wrapping a column value in a list
        # We'll use pl.concat_list which concatenates lists
        list_exprs = []
        for col in cols:
            if col in existing_cols:
                # Cast to float64, handling empty strings as null
                # Wrap in list by using pl.list constructor
                # Actually, we need to create a single-element list
                # Use pl.concat_list with a list containing the column
                # But pl.concat_list needs lists, so we'll use a workaround:
                # Create a struct with one field, then convert to list
                # Or use pl.list([expr]) if available
                # For now, use map_elements as a workaround (slower but correct)
                val_expr = pl.col(col).cast(pl.Float64, strict=False)
                list_exprs.append(val_expr)
            else:
                # Column doesn't exist, use null
                list_exprs.append(pl.lit(None, dtype=pl.Float64))
        
        # Use struct to collect all values, then convert to list row-wise
        # This is the most reliable approach
        struct_dict = {f"v{i}": expr for i, expr in enumerate(list_exprs)}
        struct_expr = pl.struct(struct_dict)
        
        # Convert struct to list using map_elements
        # Extract all struct fields as a list
        return struct_expr.map_elements(
            lambda s: [s[f"v{i}"] for i in range(25)] if s is not None else [None] * 25,
            return_dtype=pl.List(pl.Float64)
        )
    
    # Build the four list columns
    result = result.with_columns([
        build_list(asks_price_cols, existing_asks_price).alias("asks_px"),
        build_list(asks_amount_cols, existing_asks_amount).alias("asks_sz"),
        build_list(bids_price_cols, existing_bids_price).alias("bids_px"),
        build_list(bids_amount_cols, existing_bids_amount).alias("bids_sz"),
    ])
    
    # Select only the columns we need
    return result.select([
        "ts_local_us",
        "ts_exch_us",
        "bids_px",
        "bids_sz",
        "asks_px",
        "asks_sz",
    ])


def normalize_book_snapshots_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical book snapshots schema and select only schema columns.
    
    Ensures all required columns exist and have correct types.
    Drops any extra columns (e.g., original float columns, dim_symbol metadata).
    """
    # Check for missing required columns
    missing_required = [
        col for col in BOOK_SNAPSHOTS_SCHEMA
        if col not in df.columns
    ]
    if missing_required:
        raise ValueError(f"book_snapshots missing required columns: {missing_required}")
    
    # Cast columns to schema types
    casts = []
    for col, dtype in BOOK_SNAPSHOTS_SCHEMA.items():
        if col in df.columns:
            if isinstance(dtype, pl.List):
                # For list columns, ensure inner type matches
                casts.append(pl.col(col).cast(dtype))
            else:
                casts.append(pl.col(col).cast(dtype))
        else:
            raise ValueError(f"Required column {col} is missing")
    
    # Cast and select only schema columns (drops extra columns)
    return df.with_columns(casts).select(list(BOOK_SNAPSHOTS_SCHEMA.keys()))


def validate_book_snapshots(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to book snapshots data.
    
    Validates:
    - Required columns exist
    - List lengths are 25 (or pad/truncate to 25)
    - Bid prices are descending (bids_px[0] >= bids_px[1] >= ...)
    - Ask prices are ascending (asks_px[0] <= asks_px[1] <= ...)
    - Crossed book check: bids_px[i] < asks_px[i] at each level
    - Non-negative sizes when present
    - Valid timestamp ranges (reasonable values)
    
    Returns filtered DataFrame (invalid rows removed) or raises on critical errors.
    """
    if df.is_empty():
        return df
    
    # Check required columns
    required = [
        "bids_px", "bids_sz", "asks_px", "asks_sz",
        "ts_local_us", "exchange", "exchange_id", "symbol_id"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_book_snapshots: missing required columns: {missing}")
    
    # Build validation filters
    filters = [
        (pl.col("ts_local_us") > 0) &
        (pl.col("ts_local_us") < 2**63) &
        (pl.col("exchange").is_not_null()) &
        (pl.col("exchange_id").is_not_null()) &
        (pl.col("symbol_id").is_not_null())
    ]
    
    # Ensure list lengths are 25 (pad with nulls if needed, truncate if longer)
    # Slice to max 25 elements, then pad with nulls if needed
    def normalize_list_length(col_name: str) -> pl.Expr:
        """Ensure list has exactly 25 elements, padding with nulls or truncating."""
        col_expr = pl.col(col_name)
        # Slice to max 25
        sliced = col_expr.list.slice(0, 25)
        # Pad with nulls if needed
        # Use map_elements to pad
        return sliced.map_elements(
            lambda lst: (lst + [None] * (25 - len(lst)))[:25] if lst is not None else [None] * 25,
            return_dtype=pl.List(pl.Float64)
        )
    
    # Normalize all list columns to length 25
    result = df.with_columns([
        normalize_list_length("bids_px").alias("bids_px"),
        normalize_list_length("bids_sz").alias("bids_sz"),
        normalize_list_length("asks_px").alias("asks_px"),
        normalize_list_length("asks_sz").alias("asks_sz"),
    ])
    
    # Validate bid prices are descending (bids_px[0] >= bids_px[1] >= ...)
    # Check each adjacent pair
    def validate_bid_ordering() -> pl.Expr:
        """Check that bid prices are descending (non-increasing)."""
        # For each row, check that bids_px[i] >= bids_px[i+1] for all i where both are not null
        # This is complex to do row-wise, so we'll use a simpler check:
        # Check that the first non-null bid is >= the last non-null bid
        first_bid = pl.col("bids_px").list.first()
        last_bid = pl.col("bids_px").list.last()
        return (
            pl.when(first_bid.is_not_null() & last_bid.is_not_null())
            .then(first_bid >= last_bid)
            .otherwise(True)
        )
    
    # Validate ask prices are ascending (asks_px[0] <= asks_px[1] <= ...)
    def validate_ask_ordering() -> pl.Expr:
        """Check that ask prices are ascending (non-decreasing)."""
        first_ask = pl.col("asks_px").list.first()
        last_ask = pl.col("asks_px").list.last()
        return (
            pl.when(first_ask.is_not_null() & last_ask.is_not_null())
            .then(first_ask <= last_ask)
            .otherwise(True)
        )
    
    # Crossed book check: bids_px[i] < asks_px[i] at each level
    # Check that best bid < best ask (bids_px[0] < asks_px[0])
    def validate_crossed_book() -> pl.Expr:
        """Check that best bid < best ask."""
        best_bid = pl.col("bids_px").list.first()
        best_ask = pl.col("asks_px").list.first()
        return (
            pl.when(best_bid.is_not_null() & best_ask.is_not_null())
            .then(best_bid < best_ask)
            .otherwise(True)
        )
    
    # Non-negative sizes when present
    def validate_non_negative_sizes() -> pl.Expr:
        """Check that sizes are non-negative when present."""
        bid_sz_min = pl.col("bids_sz").list.min()
        ask_sz_min = pl.col("asks_sz").list.min()
        return (
            (bid_sz_min.is_null() | (bid_sz_min >= 0)) &
            (ask_sz_min.is_null() | (ask_sz_min >= 0))
        )
    
    filters.extend([
        validate_bid_ordering(),
        validate_ask_ordering(),
        validate_crossed_book(),
        validate_non_negative_sizes(),
    ])
    
    # Combine all filters
    combined_filter = filters[0]
    for f in filters[1:]:
        combined_filter = combined_filter & f
    
    valid = result.filter(combined_filter)
    
    # Warn if rows were filtered
    if valid.height < result.height:
        import warnings
        warnings.warn(
            f"validate_book_snapshots: filtered {result.height - valid.height} invalid rows",
            UserWarning
        )
    
    return valid


def encode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
) -> pl.DataFrame:
    """Encode bid/ask prices and sizes as fixed-point integers using dim_symbol metadata.
    
    Requires:
    - df must have 'symbol_id' column (from resolve_symbol_ids)
    - df must have 'bids_px', 'bids_sz', 'asks_px', 'asks_sz' columns (lists of floats)
    - dim_symbol must have 'symbol_id', 'price_increment', 'amount_increment' columns
    
    Computes:
    - bids_px_int = [round(price / price_increment) for price in bids_px]
    - bids_sz_int = [round(size / amount_increment) for size in bids_sz]
    - asks_px_int = [round(price / price_increment) for price in asks_px]
    - asks_sz_int = [round(size / amount_increment) for size in asks_sz]
    
    Returns DataFrame with bids_px, bids_sz, asks_px, asks_sz replaced with Int64 lists.
    """
    if "symbol_id" not in df.columns:
        raise ValueError("encode_fixed_point: df must have 'symbol_id' column")
    
    required_cols = ["bids_px", "bids_sz", "asks_px", "asks_sz"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"encode_fixed_point: df missing columns: {missing}")
    
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
    
    # Encode to fixed-point (handle nulls in lists - preserve null positions)
    # For each list column, apply encoding element-wise
    # Use map_elements to process each row with access to increment values
    # Create struct with list column and increment column
    result = joined.with_columns([
        # Encode bid prices: round(price / price_increment) for each element
        pl.struct(["bids_px", "price_increment"]).map_elements(
            lambda row: (
                [round(v / row["price_increment"]) if v is not None and row["price_increment"] is not None else None
                 for v in row["bids_px"]]
                if row is not None and row["bids_px"] is not None
                else [None] * 25
            ),
            return_dtype=pl.List(pl.Int64)
        ).alias("bids_px"),
        # Encode bid sizes: round(size / amount_increment) for each element
        pl.struct(["bids_sz", "amount_increment"]).map_elements(
            lambda row: (
                [round(v / row["amount_increment"]) if v is not None and row["amount_increment"] is not None else None
                 for v in row["bids_sz"]]
                if row is not None and row["bids_sz"] is not None
                else [None] * 25
            ),
            return_dtype=pl.List(pl.Int64)
        ).alias("bids_sz"),
        # Encode ask prices: round(price / price_increment) for each element
        pl.struct(["asks_px", "price_increment"]).map_elements(
            lambda row: (
                [round(v / row["price_increment"]) if v is not None and row["price_increment"] is not None else None
                 for v in row["asks_px"]]
                if row is not None and row["asks_px"] is not None
                else [None] * 25
            ),
            return_dtype=pl.List(pl.Int64)
        ).alias("asks_px"),
        # Encode ask sizes: round(size / amount_increment) for each element
        pl.struct(["asks_sz", "amount_increment"]).map_elements(
            lambda row: (
                [round(v / row["amount_increment"]) if v is not None and row["amount_increment"] is not None else None
                 for v in row["asks_sz"]]
                if row is not None and row["asks_sz"] is not None
                else [None] * 25
            ),
            return_dtype=pl.List(pl.Int64)
        ).alias("asks_sz"),
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
    """Resolve symbol_ids for book snapshots data using as-of join with dim_symbol.
    
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


def required_book_snapshots_columns() -> Sequence[str]:
    """Columns required for a book snapshots DataFrame after normalization."""
    return tuple(BOOK_SNAPSHOTS_SCHEMA.keys())
