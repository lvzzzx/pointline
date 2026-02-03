"""SZSE Level 3 order placements domain logic for parsing, validation, and transformation.

This module handles Quant360 SZSE order stream data, which represents new limit and market
orders entering the matching engine. This is fundamentally different from L2 aggregated
data - each row represents an individual order with a unique order ID (appl_seq_num).
"""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

# Import parsers from new location for backward compatibility
from pointline.tables._base import (
    exchange_id_validation_expr,
    generic_resolve_symbol_ids,
    generic_validate,
    required_columns_validation_expr,
    timestamp_validation_expr,
)
from pointline.validation_utils import with_expected_exchange_id

# Schema definition for szse_l3_orders Silver table
#
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - Use Int16 for exchange_id
# - Use Int32 for file_id, file_line_number, channel_no
# - Use Int64 for symbol_id, appl_seq_num, px_int, order_qty_int
# - UInt8 is supported (used for side, ord_type)
SZSE_L3_ORDERS_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "exchange_id": pl.Int16,
    "symbol_id": pl.Int64,
    "ts_local_us": pl.Int64,  # Arrival time in UTC (converted from Asia/Shanghai TransactTime)
    "appl_seq_num": pl.Int64,  # Order ID (unique per day per symbol)
    "side": pl.UInt8,  # 0=buy, 1=sell
    "ord_type": pl.UInt8,  # 0=market, 1=limit
    "px_int": pl.Int64,  # Fixed-point encoded (price / price_increment)
    "order_qty_int": pl.Int64,  # Lot-based encoding (qty / 100 shares)
    "channel_no": pl.Int32,  # Exchange channel ID
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}

# Side codes
SIDE_BUY = 0
SIDE_SELL = 1

# Order type codes
ORD_TYPE_MARKET = 0
ORD_TYPE_LIMIT = 1


def normalize_szse_l3_orders_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical szse_l3_orders schema and select only schema columns."""
    missing_required = [col for col in SZSE_L3_ORDERS_SCHEMA if col not in df.columns]
    if missing_required:
        raise ValueError(f"szse_l3_orders missing required columns: {missing_required}")

    casts = [pl.col(col).cast(dtype) for col, dtype in SZSE_L3_ORDERS_SCHEMA.items()]
    return df.with_columns(casts).select(list(SZSE_L3_ORDERS_SCHEMA.keys()))


def validate_szse_l3_orders(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to SZSE L3 order data.

    Validates:
    - Non-negative px_int and order_qty_int
    - Valid timestamp ranges
    - Valid side codes (0-1)
    - Valid order type codes (0-1)
    - Non-null required fields
    - exchange_id matches normalized exchange

    Returns filtered DataFrame (invalid rows removed) or raises on critical errors.
    """
    if df.is_empty():
        return df

    required = [
        "px_int",
        "order_qty_int",
        "ts_local_us",
        "appl_seq_num",
        "side",
        "ord_type",
        "exchange",
        "exchange_id",
        "symbol_id",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_szse_l3_orders: missing required columns: {missing}")

    df_with_expected = with_expected_exchange_id(df)

    combined_filter = (
        (pl.col("px_int") >= 0)  # Market orders can have price=0
        & (pl.col("order_qty_int") > 0)
        & timestamp_validation_expr("ts_local_us")
        & (pl.col("appl_seq_num") > 0)
        & (pl.col("side").is_in([SIDE_BUY, SIDE_SELL]))
        & (pl.col("ord_type").is_in([ORD_TYPE_MARKET, ORD_TYPE_LIMIT]))
        & required_columns_validation_expr(["exchange", "exchange_id", "symbol_id"])
        & exchange_id_validation_expr()
    )

    rules = [
        ("px_int", (pl.col("px_int").is_null()) | (pl.col("px_int") < 0)),
        ("order_qty_int", (pl.col("order_qty_int").is_null()) | (pl.col("order_qty_int") <= 0)),
        (
            "ts_local_us",
            pl.col("ts_local_us").is_null()
            | (pl.col("ts_local_us") <= 0)
            | (pl.col("ts_local_us") >= 2**63),
        ),
        ("appl_seq_num", (pl.col("appl_seq_num").is_null()) | (pl.col("appl_seq_num") <= 0)),
        ("side", ~pl.col("side").is_in([SIDE_BUY, SIDE_SELL]) | pl.col("side").is_null()),
        (
            "ord_type",
            ~pl.col("ord_type").is_in([ORD_TYPE_MARKET, ORD_TYPE_LIMIT])
            | pl.col("ord_type").is_null(),
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

    valid = generic_validate(df_with_expected, combined_filter, rules, "szse_l3_orders")
    return valid.select(df.columns)


def encode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
) -> pl.DataFrame:
    """Encode price and quantity as fixed-point integers using dim_symbol metadata.

    Requires:
    - df must have 'symbol_id' column (from resolve_symbol_ids)
    - df must have 'price_px', 'order_qty' columns (floats/ints)
    - dim_symbol must have 'symbol_id', 'price_increment', 'amount_increment' columns

    Computes:
    - px_int = round(price_px / price_increment)
    - order_qty_int = round(order_qty / amount_increment)

    Lot-based quantity encoding (amount_increment = 100 shares):
    - 100 shares → order_qty_int = 1
    - 500 shares → order_qty_int = 5
    - 1000 shares → order_qty_int = 10

    Returns DataFrame with px_int, order_qty_int columns added.
    """
    if "symbol_id" not in df.columns:
        raise ValueError("encode_fixed_point: df must have 'symbol_id' column")

    required_cols = ["price_px", "order_qty"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"encode_fixed_point: df missing columns: {missing}")

    required_dims = ["symbol_id", "price_increment", "amount_increment"]
    missing_dims = [c for c in required_dims if c not in dim_symbol.columns]
    if missing_dims:
        raise ValueError(f"encode_fixed_point: dim_symbol missing columns: {missing_dims}")

    joined = df.join(
        dim_symbol.select(["symbol_id", "price_increment", "amount_increment"]),
        on="symbol_id",
        how="left",
    )

    missing_ids = joined.filter(pl.col("price_increment").is_null())
    if not missing_ids.is_empty():
        missing_symbols = missing_ids.select("symbol_id").unique()
        raise ValueError(
            f"encode_fixed_point: {missing_symbols.height} symbol_ids not found in dim_symbol"
        )

    result = joined.with_columns(
        [
            (pl.col("price_px") / pl.col("price_increment")).round().cast(pl.Int64).alias("px_int"),
            (pl.col("order_qty") / pl.col("amount_increment"))
            .round()
            .cast(pl.Int64)
            .alias("order_qty_int"),
        ]
    )

    drop_cols = ["price_increment", "amount_increment", "price_px", "order_qty"]
    return result.drop(drop_cols)


def decode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    *,
    keep_ints: bool = False,
) -> pl.DataFrame:
    """Decode fixed-point integers into float price and int quantity using dim_symbol metadata.

    Requires:
    - df must have 'symbol_id' column
    - df must have 'px_int', 'order_qty_int' columns
    - dim_symbol must have 'symbol_id', 'price_increment', 'amount_increment' columns

    Returns DataFrame with price_px (Float64) and order_qty (Int64) columns added.
    By default, drops the *_int columns.
    """
    if "symbol_id" not in df.columns:
        raise ValueError("decode_fixed_point: df must have 'symbol_id' column")

    required_cols = ["px_int", "order_qty_int"]
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

    result = joined.with_columns(
        [
            (pl.col("px_int") * pl.col("price_increment")).cast(pl.Float64).alias("price_px"),
            (pl.col("order_qty_int") * pl.col("amount_increment"))
            .cast(pl.Int64)
            .alias("order_qty"),
        ]
    )

    drop_cols = ["price_increment", "amount_increment"]
    if not keep_ints:
        drop_cols += required_cols
    return result.drop(drop_cols)


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int,
    exchange_symbol: str,
    *,
    ts_col: str = "ts_local_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for SZSE L3 order data using as-of join with dim_symbol.

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


def required_szse_l3_orders_columns() -> Sequence[str]:
    """Columns required for a szse_l3_orders DataFrame after normalization."""
    return tuple(SZSE_L3_ORDERS_SCHEMA.keys())
