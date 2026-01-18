"""Quotes domain logic for parsing, validation, and transformation.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl
    from pointline.tables.quotes import parse_tardis_quotes_csv, normalize_quotes_schema

    raw_df = pl.read_csv("quotes.csv")
    parsed = parse_tardis_quotes_csv(raw_df)
    normalized = normalize_quotes_schema(parsed)
"""

from __future__ import annotations

from decimal import Decimal
from typing import Iterable, Sequence

import polars as pl

from pointline.validation_utils import with_expected_exchange_id

# Schema definition matching design.md Section 5.4
# 
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - These are automatically converted to signed types (Int16 and Int32) when written
# - Use Int16 instead of UInt16 for exchange_id
# - Use Int32 instead of UInt32 for symbol_id, ingest_seq, file_id
# - UInt8 is supported and maps to TINYINT
#
# This schema is the single source of truth - all code should use these types directly.
QUOTES_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,  # Exchange name (string) for partitioning and human readability
    "exchange_id": pl.Int16,  # Delta Lake stores as Int16 (not UInt16) - for joins and compression
    "symbol_id": pl.Int64,  # Match dim_symbol's symbol_id type
    "ts_local_us": pl.Int64,
    "ts_exch_us": pl.Int64,
    "ingest_seq": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
    "bid_px_int": pl.Int64,
    "bid_sz_int": pl.Int64,
    "ask_px_int": pl.Int64,
    "ask_sz_int": pl.Int64,
    "file_id": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
    "file_line_number": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
}


def parse_tardis_quotes_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Tardis quotes CSV format into normalized columns.
    
    Tardis provides timestamps as microseconds since epoch (integers).
    Tardis schema is standardized with exact column names:
    - exchange, symbol, timestamp, local_timestamp
    - bid_price, bid_amount, ask_price, ask_amount
    
    Both timestamp and local_timestamp are always present (Tardis handles fallback internally).
    Bid/ask fields may be empty when there are no bids or asks.
    
    Returns DataFrame with columns:
    - ts_local_us (i64): local timestamp in microseconds since epoch
    - ts_exch_us (i64): exchange timestamp in microseconds since epoch
    - bid_price (f64): best bid price (nullable)
    - bid_amount (f64): best bid amount (nullable)
    - ask_price (f64): best ask price (nullable)
    - ask_amount (f64): best ask amount (nullable)
    """
    # Check for required columns
    required_cols = ["exchange", "symbol", "timestamp", "local_timestamp"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"parse_tardis_quotes_csv: missing required columns: {missing}")
    
    result = df.clone()
    
    # Parse timestamps (both always present per Tardis spec)
    result = result.with_columns([
        pl.col("local_timestamp").cast(pl.Int64).alias("ts_local_us"),
        pl.col("timestamp").cast(pl.Int64).alias("ts_exch_us"),
    ])
    
    # Parse bid/ask fields (may be empty)
    # Tardis uses exact column names: bid_price, bid_amount, ask_price, ask_amount
    bid_ask_cols = ["bid_price", "bid_amount", "ask_price", "ask_amount"]
    missing_bid_ask = [c for c in bid_ask_cols if c not in df.columns]
    if missing_bid_ask:
        raise ValueError(f"parse_tardis_quotes_csv: missing bid/ask columns: {missing_bid_ask}")
    
    # Cast to float64, handling empty strings as null
    result = result.with_columns([
        pl.col("bid_price").cast(pl.Float64, strict=False),
        pl.col("bid_amount").cast(pl.Float64, strict=False),
        pl.col("ask_price").cast(pl.Float64, strict=False),
        pl.col("ask_amount").cast(pl.Float64, strict=False),
    ])
    
    # Select only the columns we need (preserve file_line_number if provided)
    select_cols = [
        "ts_local_us",
        "ts_exch_us",
        "bid_price",
        "bid_amount",
        "ask_price",
        "ask_amount",
    ]
    if "file_line_number" in result.columns:
        select_cols = ["file_line_number"] + select_cols
    return result.select(select_cols)


def normalize_quotes_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical quotes schema and select only schema columns.
    
    Ensures all required columns exist and have correct types.
    Drops any extra columns (e.g., original float columns, dim_symbol metadata).
    """
    # Check for missing required columns
    missing_required = [
        col for col in QUOTES_SCHEMA
        if col not in df.columns
    ]
    if missing_required:
        raise ValueError(f"quotes missing required columns: {missing_required}")
    
    # Cast columns to schema types
    casts = []
    for col, dtype in QUOTES_SCHEMA.items():
        if col in df.columns:
            casts.append(pl.col(col).cast(dtype))
        else:
            raise ValueError(f"Required column {col} is missing")
    
    # Cast and select only schema columns (drops extra columns)
    return df.with_columns(casts).select(list(QUOTES_SCHEMA.keys()))


def validate_quotes(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to quotes data.
    
    Validates:
    - Non-negative prices and sizes (when present)
    - Valid timestamp ranges (reasonable values) for local and exchange times
    - Crossed book check: bid_px_int < ask_px_int when both are present
    - At least one of bid or ask must be present (filter rows with both missing)
    - exchange_id matches normalized exchange
    
    Returns filtered DataFrame (invalid rows removed) or raises on critical errors.
    """
    if df.is_empty():
        return df
    
    # Check required columns
    required = [
        "bid_px_int", "bid_sz_int", "ask_px_int", "ask_sz_int",
        "ts_local_us", "ts_exch_us", "exchange", "exchange_id", "symbol_id"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_quotes: missing required columns: {missing}")
    
    df_with_expected = with_expected_exchange_id(df)
    combined_filter, rules = _quote_validation_rules(df_with_expected)
    valid = df_with_expected.filter(combined_filter).select(df.columns)
    
    # Warn if rows were filtered
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
            f"validate_quotes: filtered {df.height - valid.height} invalid rows; {detail}",
            UserWarning,
        )
    
    return valid


def _quote_validation_rules(df: pl.DataFrame) -> tuple[pl.Expr, list[tuple[str, pl.Expr]]]:
    has_bid = (pl.col("bid_px_int").is_not_null()) & (pl.col("bid_sz_int").is_not_null())
    has_ask = (pl.col("ask_px_int").is_not_null()) & (pl.col("ask_sz_int").is_not_null())

    filters = [
        (pl.col("ts_local_us") > 0)
        & (pl.col("ts_local_us") < 2**63)
        & (pl.col("ts_exch_us") > 0)
        & (pl.col("ts_exch_us") < 2**63)
        & (pl.col("exchange").is_not_null())
        & (pl.col("exchange_id").is_not_null())
        & (pl.col("symbol_id").is_not_null())
        & (pl.col("exchange_id") == pl.col("expected_exchange_id")),
        has_bid | has_ask,
        pl.when(has_bid)
        .then((pl.col("bid_px_int") > 0) & (pl.col("bid_sz_int") > 0))
        .otherwise(True),
        pl.when(has_ask)
        .then((pl.col("ask_px_int") > 0) & (pl.col("ask_sz_int") > 0))
        .otherwise(True),
        pl.when(has_bid & has_ask)
        .then(pl.col("bid_px_int") < pl.col("ask_px_int"))
        .otherwise(True),
    ]

    combined_filter = filters[0]
    for f in filters[1:]:
        combined_filter = combined_filter & f

    rules = [
        ("no_bid_or_ask", ~(has_bid | has_ask)),
        ("bid_vals", has_bid & ((pl.col("bid_px_int") <= 0) | (pl.col("bid_sz_int") <= 0))),
        ("ask_vals", has_ask & ((pl.col("ask_px_int") <= 0) | (pl.col("ask_sz_int") <= 0))),
        ("crossed", has_bid & has_ask & (pl.col("bid_px_int") >= pl.col("ask_px_int"))),
        (
            "ts_local_us",
            pl.col("ts_local_us").is_null()
            | (pl.col("ts_local_us") <= 0)
            | (pl.col("ts_local_us") >= 2**63),
        ),
        (
            "ts_exch_us",
            pl.col("ts_exch_us").is_null()
            | (pl.col("ts_exch_us") <= 0)
            | (pl.col("ts_exch_us") >= 2**63),
        ),
        ("exchange", pl.col("exchange").is_null()),
        ("exchange_id", pl.col("exchange_id").is_null()),
        ("symbol_id", pl.col("symbol_id").is_null()),
        (
            "exchange_id_mismatch",
            pl.col("expected_exchange_id").is_null()
            | (pl.col("exchange_id") != pl.col("expected_exchange_id")),
        ),
    ]

    return combined_filter, rules


def encode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
) -> pl.DataFrame:
    """Encode bid/ask prices and sizes as fixed-point integers using dim_symbol metadata.
    
    Requires:
    - df must have 'symbol_id' column (from resolve_symbol_ids)
    - df must have 'bid_price', 'bid_amount', 'ask_price', 'ask_amount' columns
    - dim_symbol must have 'symbol_id', 'price_increment', 'amount_increment' columns
    
    Computes:
    - bid_px_int = floor(bid_price / price_increment)
    - bid_sz_int = round(bid_amount / amount_increment)
    - ask_px_int = ceil(ask_price / price_increment)
    - ask_sz_int = round(ask_amount / amount_increment)
    
    Returns DataFrame with bid_px_int, bid_sz_int, ask_px_int, ask_sz_int columns added.
    """
    if "symbol_id" not in df.columns:
        raise ValueError("encode_fixed_point: df must have 'symbol_id' column")
    
    required_cols = ["bid_price", "bid_amount", "ask_price", "ask_amount"]
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
    
    # Encode to fixed-point (handle nulls - preserve null for empty bid/ask)
    result = joined.with_columns([
        pl.when(pl.col("bid_price").is_not_null())
        .then((pl.col("bid_price") / pl.col("price_increment")).floor().cast(pl.Int64))
        .otherwise(None)
        .alias("bid_px_int"),
        pl.when(pl.col("bid_amount").is_not_null())
        .then((pl.col("bid_amount") / pl.col("amount_increment")).round().cast(pl.Int64))
        .otherwise(None)
        .alias("bid_sz_int"),
        pl.when(pl.col("ask_price").is_not_null())
        .then((pl.col("ask_price") / pl.col("price_increment")).ceil().cast(pl.Int64))
        .otherwise(None)
        .alias("ask_px_int"),
        pl.when(pl.col("ask_amount").is_not_null())
        .then((pl.col("ask_amount") / pl.col("amount_increment")).round().cast(pl.Int64))
        .otherwise(None)
        .alias("ask_sz_int"),
    ])
    
    # Drop intermediate columns
    return result.drop(["price_increment", "amount_increment"])


def decode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    *,
    keep_ints: bool = False,
) -> pl.DataFrame:
    """Decode fixed-point integers into float bid/ask columns using dim_symbol metadata.

    Requires:
    - df must have 'symbol_id' column
    - df must have 'bid_px_int', 'bid_sz_int', 'ask_px_int', 'ask_sz_int' columns
    - dim_symbol must have 'symbol_id', 'price_increment', 'amount_increment' columns

    Returns DataFrame with bid_price, bid_amount, ask_price, ask_amount added (Float64).
    By default, drops the *_int columns.
    """
    if "symbol_id" not in df.columns:
        raise ValueError("decode_fixed_point: df must have 'symbol_id' column")

    required_cols = ["bid_px_int", "bid_sz_int", "ask_px_int", "ask_sz_int"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    required_dims = ["symbol_id", "price_increment", "amount_increment"]
    missing_dims = [c for c in required_dims if c not in dim_symbol.columns]
    if missing_dims:
        raise ValueError(f"decode_fixed_point: dim_symbol missing columns: {missing_dims}")

    joined = df.join(
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

    price_decimals = _max_decimal_places(dim_symbol["price_increment"].to_list())
    amount_decimals = _max_decimal_places(dim_symbol["amount_increment"].to_list())

    result = joined.with_columns(
        [
            pl.when(pl.col("bid_px_int").is_not_null())
            .then(
                (pl.col("bid_px_int") * pl.col("price_increment"))
                .round(price_decimals)
                .cast(pl.Float64)
            )
            .otherwise(None)
            .alias("bid_price"),
            pl.when(pl.col("bid_sz_int").is_not_null())
            .then(
                (pl.col("bid_sz_int") * pl.col("amount_increment"))
                .round(amount_decimals)
                .cast(pl.Float64)
            )
            .otherwise(None)
            .alias("bid_amount"),
            pl.when(pl.col("ask_px_int").is_not_null())
            .then(
                (pl.col("ask_px_int") * pl.col("price_increment"))
                .round(price_decimals)
                .cast(pl.Float64)
            )
            .otherwise(None)
            .alias("ask_price"),
            pl.when(pl.col("ask_sz_int").is_not_null())
            .then(
                (pl.col("ask_sz_int") * pl.col("amount_increment"))
                .round(amount_decimals)
                .cast(pl.Float64)
            )
            .otherwise(None)
            .alias("ask_amount"),
        ]
    )

    drop_cols = ["price_increment", "amount_increment"]
    if not keep_ints:
        drop_cols += required_cols
    return result.drop(drop_cols)


def _max_decimal_places(values: Iterable[float | None]) -> int:
    max_places = 0
    for value in values:
        if value is None:
            continue
        try:
            exponent = Decimal(str(value)).normalize().as_tuple().exponent
        except (ArithmeticError, ValueError):
            continue
        places = -exponent if exponent < 0 else 0
        if places > max_places:
            max_places = places
    return max_places


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int,
    exchange_symbol: str,
    *,
    ts_col: str = "ts_local_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for quotes data using as-of join with dim_symbol.
    
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


def required_quotes_columns() -> Sequence[str]:
    """Columns required for a quotes DataFrame after normalization."""
    return tuple(QUOTES_SCHEMA.keys())
