"""Tardis derivative ticker parser.

This module contains the vendor-specific parsing logic for Tardis derivative_ticker CSV files.
"""

import polars as pl

from pointline.io.vendors.registry import register_parser


@register_parser(vendor="tardis", data_type="derivative_ticker")
def parse_tardis_derivative_ticker_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Tardis derivative_ticker CSV format into normalized columns.

    Tardis schema is standardized with exact column names:
    - exchange, symbol, timestamp, local_timestamp, funding_timestamp
    - funding_rate, predicted_funding_rate, open_interest
    - last_price, index_price, mark_price

    Args:
        df: Raw DataFrame from Tardis CSV file

    Returns:
        DataFrame with normalized columns:
        - ts_local_us (i64)
        - ts_exch_us (i64)
        - funding_ts_us (i64, nullable)
        - funding_rate (f64, nullable)
        - predicted_funding_rate (f64, nullable)
        - open_interest (f64, nullable)
        - last_px (f64, nullable)
        - index_px (f64, nullable)
        - mark_px (f64, nullable)

    Raises:
        ValueError: If required columns are missing
    """
    required_cols = [
        "exchange",
        "symbol",
        "timestamp",
        "local_timestamp",
        "funding_timestamp",
        "funding_rate",
        "predicted_funding_rate",
        "open_interest",
        "last_price",
        "index_price",
        "mark_price",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"parse_tardis_derivative_ticker_csv: missing required columns: {missing}")

    result = df.clone().with_columns(
        [
            pl.col("local_timestamp").cast(pl.Int64).alias("ts_local_us"),
            pl.col("timestamp").cast(pl.Int64).alias("ts_exch_us"),
            pl.col("funding_timestamp").cast(pl.Int64, strict=False).alias("funding_ts_us"),
            pl.col("funding_rate").cast(pl.Float64, strict=False),
            pl.col("predicted_funding_rate").cast(pl.Float64, strict=False),
            pl.col("open_interest").cast(pl.Float64, strict=False),
            pl.col("last_price").cast(pl.Float64, strict=False).alias("last_px"),
            pl.col("index_price").cast(pl.Float64, strict=False).alias("index_px"),
            pl.col("mark_price").cast(pl.Float64, strict=False).alias("mark_px"),
        ]
    )

    select_cols = [
        "ts_local_us",
        "ts_exch_us",
        "funding_ts_us",
        "funding_rate",
        "predicted_funding_rate",
        "open_interest",
        "last_px",
        "index_px",
        "mark_px",
    ]
    if "file_line_number" in result.columns:
        select_cols = ["file_line_number"] + select_cols
    return result.select(select_cols)
