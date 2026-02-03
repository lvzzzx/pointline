"""Quant360 L3 orders parser.

This module contains the vendor-specific parsing logic for Quant360 SZSE L3 order CSV files.
"""

import polars as pl

from pointline.io.vendors.quant360.parsers.utils import parse_quant360_timestamp
from pointline.io.vendors.registry import register_parser

# Side codes (duplicated to avoid circular imports)
SIDE_BUY = 0
SIDE_SELL = 1

# Order type codes
ORD_TYPE_MARKET = 0
ORD_TYPE_LIMIT = 1


@register_parser(vendor="quant360", data_type="l3_orders")
def parse_quant360_orders_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Quant360 order CSV format into normalized columns.

    Expected Quant360 columns:
    - OrderQty, OrdType, TransactTime, ExpirationDays, Side, ApplSeqNum
    - Contactor, SendingTime, Price, ChannelNo, ExpirationType, ContactInfo, ConfirmID

    Args:
        df: Raw DataFrame from Quant360 CSV file

    Returns:
        DataFrame with normalized columns:
        - ts_local_us (i64) - from TransactTime, converted to UTC microseconds
        - appl_seq_num (i64) - Order ID
        - side (u8) - 0=buy, 1=sell (remapped from Quant360's 1/2)
        - ord_type (u8) - 0=market, 1=limit (remapped from Quant360's 1/2)
        - price_px (f64) - Price in CNY (will be encoded to px_int later)
        - order_qty (i64) - Quantity in shares (will be encoded to order_qty_int later)
        - channel_no (i32) - Exchange channel ID

    Raises:
        ValueError: If required columns are missing
    """
    required_cols = [
        "ApplSeqNum",
        "Side",
        "OrdType",
        "Price",
        "OrderQty",
        "TransactTime",
        "ChannelNo",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"parse_quant360_orders_csv: missing required columns: {missing}")

    # Parse timestamps (vectorized using map_batches for efficiency)
    def parse_ts_batch(s: pl.Series) -> pl.Series:
        return s.map_elements(parse_quant360_timestamp, return_dtype=pl.Int64)

    result = df.clone().with_columns(
        [
            # Timestamps
            parse_ts_batch(pl.col("TransactTime")).alias("ts_local_us"),
            # Order ID
            pl.col("ApplSeqNum").cast(pl.Int64).alias("appl_seq_num"),
            # Side: Quant360 uses 1=buy, 2=sell → remap to 0=buy, 1=sell
            pl.when(pl.col("Side") == 1)
            .then(pl.lit(SIDE_BUY, dtype=pl.UInt8))
            .when(pl.col("Side") == 2)
            .then(pl.lit(SIDE_SELL, dtype=pl.UInt8))
            .otherwise(pl.lit(255, dtype=pl.UInt8))  # Invalid marker
            .alias("side"),
            # Order type: Quant360 uses 1=market, 2=limit → remap to 0=market, 1=limit
            pl.when(pl.col("OrdType") == 1)
            .then(pl.lit(ORD_TYPE_MARKET, dtype=pl.UInt8))
            .when(pl.col("OrdType") == 2)
            .then(pl.lit(ORD_TYPE_LIMIT, dtype=pl.UInt8))
            .otherwise(pl.lit(255, dtype=pl.UInt8))  # Invalid marker
            .alias("ord_type"),
            # Price and quantity (keep as float/int for now, will encode later)
            pl.col("Price").cast(pl.Float64).alias("price_px"),
            pl.col("OrderQty").cast(pl.Int64).alias("order_qty"),
            # Channel number
            pl.col("ChannelNo").cast(pl.Int32).alias("channel_no"),
        ]
    )

    select_cols = [
        "ts_local_us",
        "appl_seq_num",
        "side",
        "ord_type",
        "price_px",
        "order_qty",
        "channel_no",
    ]
    if "file_line_number" in result.columns:
        select_cols = ["file_line_number"] + select_cols

    return result.select(select_cols)
