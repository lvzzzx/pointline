"""Tardis liquidations parser."""

from __future__ import annotations

import polars as pl

from pointline.io.vendors.registry import register_parser


@register_parser(vendor="tardis", data_type="liquidations")
def parse_tardis_liquidations_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Tardis liquidations CSV format into normalized columns."""
    required_cols = [
        "symbol",
        "timestamp",
        "local_timestamp",
        "side",
        "price",
        "amount",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"parse_tardis_liquidations_csv: missing required columns: {missing}")

    liq_id_expr = (
        pl.when(pl.col("id").cast(pl.Utf8, strict=False).str.strip_chars().str.len_chars() > 0)
        .then(pl.col("id").cast(pl.Utf8, strict=False).str.strip_chars())
        .otherwise(None)
        .alias("liq_id")
        if "id" in df.columns
        else pl.lit(None, dtype=pl.Utf8).alias("liq_id")
    )

    result = df.clone().with_columns(
        [
            pl.col("symbol").cast(pl.Utf8).alias("exchange_symbol"),
            pl.col("local_timestamp").cast(pl.Int64).alias("ts_local_us"),
            pl.col("timestamp").cast(pl.Int64).alias("ts_exch_us"),
            liq_id_expr,
            pl.col("side").cast(pl.Utf8).alias("side_raw"),
            pl.col("price").cast(pl.Float64, strict=False).alias("price_px"),
            pl.col("amount").cast(pl.Float64, strict=False).alias("qty"),
        ]
    )

    select_cols = [
        "exchange_symbol",
        "ts_local_us",
        "ts_exch_us",
        "liq_id",
        "side_raw",
        "price_px",
        "qty",
    ]
    if "file_line_number" in result.columns:
        select_cols = ["file_line_number", *select_cols]
    return result.select(select_cols)
