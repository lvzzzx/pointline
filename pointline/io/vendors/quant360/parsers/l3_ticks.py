"""Quant360 L3 ticks parser.

This module contains the vendor-specific parsing logic for Quant360 SZSE L3 tick CSV files.
"""

import polars as pl

from pointline.io.vendors.quant360.parsers.utils import parse_quant360_timestamp
from pointline.io.vendors.registry import register_parser


@register_parser(vendor="quant360", data_type="l3_ticks")
def parse_quant360_ticks_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Quant360 tick CSV format into normalized columns.

    Expected Quant360 columns:
    - ApplSeqNum, BidApplSeqNum, OfferApplSeqNum, Price, Qty, TransactTime
    - SendingTime, ChannelNo, Amt, ExecType

    Args:
        df: Raw DataFrame from Quant360 CSV file

    Returns:
        DataFrame with normalized columns:
        - ts_local_us (i64) - from TransactTime, converted to UTC microseconds
        - appl_seq_num (i64) - Tick ID
        - bid_appl_seq_num (i64) - Buy order ID (0 if N/A)
        - offer_appl_seq_num (i64) - Sell order ID (0 if N/A)
        - exec_type_raw (str) - vendor exec type code/value
        - price_px (f64) - Price in CNY (will be encoded to px_int later)
        - qty (i64) - Quantity in shares (will be encoded to qty_int later)
        - channel_no (i32) - Exchange channel ID

    Raises:
        ValueError: If required columns are missing
    """
    required_cols = [
        "ApplSeqNum",
        "BidApplSeqNum",
        "OfferApplSeqNum",
        "Price",
        "Qty",
        "TransactTime",
        "ChannelNo",
        "ExecType",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"parse_quant360_ticks_csv: missing required columns: {missing}")

    # Parse timestamps (vectorized using map_batches for efficiency)
    def parse_ts_batch(s: pl.Series) -> pl.Series:
        return s.map_elements(parse_quant360_timestamp, return_dtype=pl.Int64)

    result = df.clone().with_columns(
        [
            # Timestamps
            parse_ts_batch(pl.col("TransactTime")).alias("ts_local_us"),
            # Tick ID
            pl.col("ApplSeqNum").cast(pl.Int64).alias("appl_seq_num"),
            # Order IDs (0 if not applicable)
            pl.col("BidApplSeqNum").cast(pl.Int64).alias("bid_appl_seq_num"),
            pl.col("OfferApplSeqNum").cast(pl.Int64).alias("offer_appl_seq_num"),
            pl.col("ExecType").cast(pl.Utf8).str.to_lowercase().alias("exec_type_raw"),
            # Price and quantity (keep as float/int for now, will encode later)
            pl.col("Price").cast(pl.Float64).alias("price_px"),
            pl.col("Qty").cast(pl.Int64).alias("qty"),
            # Channel number
            pl.col("ChannelNo").cast(pl.Int32).alias("channel_no"),
        ]
    )

    select_cols = [
        "ts_local_us",
        "appl_seq_num",
        "bid_appl_seq_num",
        "offer_appl_seq_num",
        "exec_type_raw",
        "price_px",
        "qty",
        "channel_no",
    ]
    if "file_line_number" in result.columns:
        select_cols = ["file_line_number"] + select_cols

    return result.select(select_cols)
