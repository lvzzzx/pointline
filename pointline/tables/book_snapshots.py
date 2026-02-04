"""Book snapshots domain logic for parsing, validation, and transformation.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl
    from pointline.tables.book_snapshots import (
        encode_fixed_point,
        normalize_book_snapshots_schema,
        parse_tardis_book_snapshots_csv,
        resolve_symbol_ids,
    )

    raw_df = pl.read_csv("book_snapshots.csv")
    parsed = parse_tardis_book_snapshots_csv(raw_df)
    resolved = resolve_symbol_ids(
        parsed,
        dim_symbol,
        exchange_id=1,
        exchange_symbol="BTCUSDT",
    )
    encoded = encode_fixed_point(resolved, dim_symbol)
    normalized = normalize_book_snapshots_schema(encoded)
"""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

# Import parser from new location for backward compatibility
from pointline.tables._base import generic_resolve_symbol_ids
from pointline.validation_utils import DataQualityWarning, with_expected_exchange_id

# Required metadata fields for ingestion
REQUIRED_METADATA_FIELDS = {"exchange", "symbol", "date"}

# Schema definition matching design.md Section 5.2
#
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - These are automatically converted to signed types (Int16 and Int32) when written
# - Use Int16 instead of UInt16 for exchange_id
# - Use Int32 instead of UInt32 for file_id, file_line_number
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
    "bids_px_int": pl.List(pl.Int64),  # List of 25 bid prices (nulls for missing levels)
    "bids_sz_int": pl.List(pl.Int64),  # List of 25 bid sizes (nulls for missing levels)
    "asks_px_int": pl.List(pl.Int64),  # List of 25 ask prices (nulls for missing levels)
    "asks_sz_int": pl.List(pl.Int64),  # List of 25 ask sizes (nulls for missing levels)
    "file_id": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
    "file_line_number": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
}


def normalize_book_snapshots_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical book snapshots schema and select only schema columns.

    Ensures all required columns exist and have correct types.
    Drops any extra columns (e.g., original float columns, dim_symbol metadata).
    """
    # Check for missing required columns
    missing_required = [col for col in BOOK_SNAPSHOTS_SCHEMA if col not in df.columns]
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
    - Bid prices are descending (bids_px_int[0] >= bids_px_int[1] >= ...)
    - Ask prices are ascending (asks_px_int[0] <= asks_px_int[1] <= ...)
    - Crossed book check: best bid < best ask
    - Non-negative sizes when present
    - Valid timestamp ranges (reasonable values)
    - exchange_id matches normalized exchange

    Returns filtered DataFrame (invalid rows removed) or raises on critical errors.
    """
    if df.is_empty():
        return df

    # Check required columns
    required = [
        "bids_px_int",
        "bids_sz_int",
        "asks_px_int",
        "asks_sz_int",
        "ts_local_us",
        "exchange",
        "exchange_id",
        "symbol_id",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_book_snapshots: missing required columns: {missing}")

    df_with_expected = with_expected_exchange_id(df)
    # Build validation filters
    filters = [
        (pl.col("ts_local_us") > 0)
        & (pl.col("ts_local_us") < 2**63)
        & (pl.col("exchange").is_not_null())
        & (pl.col("exchange_id").is_not_null())
        & (pl.col("symbol_id").is_not_null())
        & (pl.col("exchange_id") == pl.col("expected_exchange_id"))
    ]

    # Ensure list lengths are 25 (pad with nulls if needed, truncate if longer)
    # Slice to max 25 elements, then pad with nulls if needed
    # Preserve the existing dtype (Int64 after encoding, Float64 before encoding)
    def normalize_list_length(col_name: str) -> pl.Expr:
        """Ensure list has exactly 25 elements, padding with nulls or truncating."""
        col_expr = pl.col(col_name)
        # Prefer actual column dtype; fall back to schema or Int64.
        col_dtype = df.schema.get(col_name)
        if isinstance(col_dtype, pl.List):
            inner_dtype = col_dtype.inner
        else:
            expected_dtype = BOOK_SNAPSHOTS_SCHEMA.get(col_name, pl.List(pl.Int64))
            inner_dtype = expected_dtype.inner if isinstance(expected_dtype, pl.List) else pl.Int64

        # Slice to max 25, then pad with nulls (vectorized).
        sliced = col_expr.list.slice(0, 25)
        null_pad = pl.lit(None, dtype=inner_dtype).repeat_by(pl.lit(25))
        padded = pl.concat_list([sliced, null_pad]).list.slice(0, 25)
        return pl.when(col_expr.is_null()).then(null_pad).otherwise(padded)

    # Normalize all list columns to length 25
    result = df_with_expected.with_columns(
        [
            normalize_list_length("bids_px_int").alias("bids_px_int"),
            normalize_list_length("bids_sz_int").alias("bids_sz_int"),
            normalize_list_length("asks_px_int").alias("asks_px_int"),
            normalize_list_length("asks_sz_int").alias("asks_sz_int"),
        ]
    )

    # Validate bid prices are descending (bids_px_int[0] >= bids_px_int[1] >= ...)
    # Vectorized monotonicity check using list.diff on non-null values.
    def validate_bid_ordering() -> pl.Expr:
        """Check that bid prices are descending (non-increasing) across all adjacent levels."""
        bids = pl.col("bids_px_int").list.drop_nulls()
        diffs = bids.list.diff()
        max_diff = diffs.list.max()
        return pl.when(max_diff.is_null()).then(True).otherwise(max_diff <= 0)

    # Validate ask prices are ascending (asks_px_int[0] <= asks_px_int[1] <= ...)
    # Vectorized monotonicity check using list.diff on non-null values.
    def validate_ask_ordering() -> pl.Expr:
        """Check that ask prices are ascending (non-decreasing) across all adjacent levels."""
        asks = pl.col("asks_px_int").list.drop_nulls()
        diffs = asks.list.diff()
        min_diff = diffs.list.min()
        return pl.when(min_diff.is_null()).then(True).otherwise(min_diff >= 0)

    # Crossed book check: bids_px_int[i] < asks_px_int[i] at each level
    # Check that best bid < best ask (bids_px_int[0] < asks_px_int[0])
    def validate_crossed_book() -> pl.Expr:
        """Check that best bid < best ask."""
        best_bid = pl.col("bids_px_int").list.first()
        best_ask = pl.col("asks_px_int").list.first()
        return (
            pl.when(best_bid.is_not_null() & best_ask.is_not_null())
            .then(best_bid < best_ask)
            .otherwise(True)
        )

    # Non-negative sizes when present
    def validate_non_negative_sizes() -> pl.Expr:
        """Check that sizes are non-negative when present."""
        bid_sz_min = pl.col("bids_sz_int").list.min()
        ask_sz_min = pl.col("asks_sz_int").list.min()
        return (bid_sz_min.is_null() | (bid_sz_min >= 0)) & (
            ask_sz_min.is_null() | (ask_sz_min >= 0)
        )

    filters.extend(
        [
            validate_bid_ordering(),
            validate_ask_ordering(),
            validate_crossed_book(),
            validate_non_negative_sizes(),
        ]
    )

    # Combine all filters
    combined_filter = filters[0]
    for f in filters[1:]:
        combined_filter = combined_filter & f

    valid = result.filter(combined_filter).select(df.columns)

    # Warn if rows were filtered
    if valid.height < result.height:
        import warnings

        warnings.warn(
            f"validate_book_snapshots: filtered {result.height - valid.height} invalid rows",
            DataQualityWarning,
            stacklevel=2,
        )

    return valid


def encode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
) -> pl.DataFrame:
    """Encode bid/ask prices and sizes as fixed-point integers using dim_symbol metadata.

    Requires:
    - df must have 'symbol_id' column (from resolve_symbol_ids)
    - df must have raw level columns: asks[0..24].price/amount, bids[0..24].price/amount
    - dim_symbol must have 'symbol_id', 'price_increment', 'amount_increment' columns

    Computes:
    - bids_px_int = [floor(price / price_increment) for price in bids_px]
    - bids_sz_int = [round(size / amount_increment) for size in bids_sz]
    - asks_px_int = [ceil(price / price_increment) for price in asks_px]
    - asks_sz_int = [round(size / amount_increment) for size in asks_sz]

    Returns DataFrame with bids_px_int, bids_sz_int, asks_px_int, asks_sz_int as Int64 lists.
    Supports multiple symbol_id values by encoding per symbol and restoring row order.
    """
    if "symbol_id" not in df.columns:
        raise ValueError("encode_fixed_point: df must have 'symbol_id' column")

    asks_price_cols = [f"asks[{i}].price" for i in range(25)]
    asks_amount_cols = [f"asks[{i}].amount" for i in range(25)]
    bids_price_cols = [f"bids[{i}].price" for i in range(25)]
    bids_amount_cols = [f"bids[{i}].amount" for i in range(25)]
    raw_cols = asks_price_cols + asks_amount_cols + bids_price_cols + bids_amount_cols
    has_raw_cols = any(col in df.columns for col in raw_cols)

    if not has_raw_cols:
        raise ValueError("encode_fixed_point: df must contain raw level columns")

    required_dims = ["symbol_id", "price_increment", "amount_increment"]
    missing = [c for c in required_dims if c not in dim_symbol.columns]
    if missing:
        raise ValueError(f"encode_fixed_point: dim_symbol missing columns: {missing}")

    # Join to get increments
    df_with_index = (
        df.with_row_index("__row_nr")
        if hasattr(df, "with_row_index")
        else df.with_row_count("__row_nr")
    )
    joined = df_with_index.join(
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

    def encode_list(
        cols: list[str],
        existing_cols: list[str],
        increment: float,
        *,
        mode: str,
    ) -> pl.Expr:
        list_exprs: list[pl.Expr] = []
        inc = pl.lit(increment)
        for col in cols:
            if col in existing_cols:
                scaled = pl.col(col) / inc
                if mode == "floor":
                    scaled = scaled.floor()
                elif mode == "ceil":
                    scaled = scaled.ceil()
                else:
                    scaled = scaled.round()

                list_exprs.append(
                    pl.when(pl.col(col).is_not_null()).then(scaled.cast(pl.Int64)).otherwise(None)
                )
            else:
                list_exprs.append(pl.lit(None, dtype=pl.Int64))
        return pl.concat_list(list_exprs)

    if joined.is_empty():
        drop_cols = [c for c in raw_cols if c in joined.columns]
        return joined.drop(drop_cols + ["price_increment", "amount_increment", "__row_nr"])

    existing_asks_price = [c for c in asks_price_cols if c in joined.columns]
    existing_asks_amount = [c for c in asks_amount_cols if c in joined.columns]
    existing_bids_price = [c for c in bids_price_cols if c in joined.columns]
    existing_bids_amount = [c for c in bids_amount_cols if c in joined.columns]

    def _encode_group(group: pl.DataFrame) -> pl.DataFrame:
        if group.is_empty():
            return group
        price_inc = group["price_increment"][0]
        amount_inc = group["amount_increment"][0]
        return group.with_columns(
            [
                encode_list(
                    asks_price_cols,
                    existing_asks_price,
                    price_inc,
                    mode="ceil",
                ).alias("asks_px_int"),
                encode_list(
                    asks_amount_cols,
                    existing_asks_amount,
                    amount_inc,
                    mode="round",
                ).alias("asks_sz_int"),
                encode_list(
                    bids_price_cols,
                    existing_bids_price,
                    price_inc,
                    mode="floor",
                ).alias("bids_px_int"),
                encode_list(
                    bids_amount_cols,
                    existing_bids_amount,
                    amount_inc,
                    mode="round",
                ).alias("bids_sz_int"),
            ]
        )

    unique_symbols = joined.select("symbol_id").unique(maintain_order=True)
    symbol_ids = unique_symbols["symbol_id"].to_list()
    if len(symbol_ids) == 1:
        result = _encode_group(joined)
    else:
        groups = [
            _encode_group(joined.filter(pl.col("symbol_id") == symbol_id))
            for symbol_id in symbol_ids
        ]
        result = pl.concat(groups, how="vertical").sort("__row_nr")

    drop_cols = [c for c in raw_cols if c in result.columns]
    return result.drop(drop_cols + ["price_increment", "amount_increment", "__row_nr"])


def decode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
) -> pl.DataFrame:
    """Decode fixed-point list columns into float lists using dim_symbol metadata.

    Requires:
    - df must have 'symbol_id' column
    - df must have 'bids_px_int', 'bids_sz_int', 'asks_px_int', 'asks_sz_int' columns (lists of ints)
    - dim_symbol must have 'symbol_id', 'price_increment', 'amount_increment' columns

    Returns DataFrame with bids_px, bids_sz, asks_px, asks_sz replaced with Float64 lists.
    Supports multiple symbol_id values by decoding per row using each symbol's increments.
    """
    if "symbol_id" not in df.columns:
        raise ValueError("decode_fixed_point: df must have 'symbol_id' column")

    list_cols = ["bids_px_int", "bids_sz_int", "asks_px_int", "asks_sz_int"]
    missing = [c for c in list_cols if c not in df.columns]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    required_dims = ["symbol_id", "price_increment", "amount_increment"]
    missing_dims = [c for c in required_dims if c not in dim_symbol.columns]
    if missing_dims:
        raise ValueError(f"decode_fixed_point: dim_symbol missing columns: {missing_dims}")

    df_with_index = (
        df.with_row_index("__row_nr")
        if hasattr(df, "with_row_index")
        else df.with_row_count("__row_nr")
    )
    joined = df_with_index.join(
        dim_symbol.select(["symbol_id", "price_increment", "amount_increment"]),
        on="symbol_id",
        how="left",
    )

    missing_ids = joined.filter(pl.col("price_increment").is_null())
    if not missing_ids.is_empty():
        missing_symbols = missing_ids.select("symbol_id").unique()
        raise ValueError(
            f"decode_fixed_point: {missing_symbols.height} symbol_ids not found in dim_symbol"
        )

    if joined.is_empty():
        return joined.drop(["price_increment", "amount_increment", "__row_nr"])

    def _decode_group(group: pl.DataFrame) -> pl.DataFrame:
        if group.is_empty():
            return group
        price_inc = group["price_increment"][0]
        amount_inc = group["amount_increment"][0]
        return group.with_columns(
            [
                pl.col("bids_px_int")
                .list.eval(
                    pl.when(pl.element().is_not_null())
                    .then((pl.element() * pl.lit(price_inc)).cast(pl.Float64))
                    .otherwise(None)
                )
                .alias("bids_px"),
                pl.col("bids_sz_int")
                .list.eval(
                    pl.when(pl.element().is_not_null())
                    .then((pl.element() * pl.lit(amount_inc)).cast(pl.Float64))
                    .otherwise(None)
                )
                .alias("bids_sz"),
                pl.col("asks_px_int")
                .list.eval(
                    pl.when(pl.element().is_not_null())
                    .then((pl.element() * pl.lit(price_inc)).cast(pl.Float64))
                    .otherwise(None)
                )
                .alias("asks_px"),
                pl.col("asks_sz_int")
                .list.eval(
                    pl.when(pl.element().is_not_null())
                    .then((pl.element() * pl.lit(amount_inc)).cast(pl.Float64))
                    .otherwise(None)
                )
                .alias("asks_sz"),
            ]
        )

    unique_symbols = joined.select("symbol_id").unique(maintain_order=True)
    symbol_ids = unique_symbols["symbol_id"].to_list()
    if len(symbol_ids) == 1:
        result = _decode_group(joined)
    else:
        groups = [
            _decode_group(joined.filter(pl.col("symbol_id") == symbol_id))
            for symbol_id in symbol_ids
        ]
        result = pl.concat(groups, how="vertical").sort("__row_nr")

    return result.drop(["price_increment", "amount_increment", "__row_nr"])


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int,
    exchange_symbol: str,
    *,
    ts_col: str = "ts_local_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for book snapshots data using as-of join with dim_symbol.

    This is a wrapper around the generic symbol resolution function.

    Args:
        data: DataFrame with ts_local_us (or ts_col) column
        dim_symbol: dim_symbol table in canonical schema
        exchange_id: Exchange ID to use for all rows
        exchange_symbol: Exchange symbol to use for all rows
        ts_col: Timestamp column name (default: ts_local_us)

    Returns:
        DataFrame with symbol_id column added
    """
    return generic_resolve_symbol_ids(data, dim_symbol, exchange_id, exchange_symbol, ts_col=ts_col)


def required_book_snapshots_columns() -> Sequence[str]:
    """Columns required for a book snapshots DataFrame after normalization."""
    return tuple(BOOK_SNAPSHOTS_SCHEMA.keys())
