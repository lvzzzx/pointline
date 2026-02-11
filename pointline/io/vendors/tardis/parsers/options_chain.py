"""Tardis options_chain parser."""

from __future__ import annotations

import polars as pl

from pointline.io.vendors.registry import register_parser


@register_parser(vendor="tardis", data_type="options_chain")
def parse_tardis_options_chain_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Tardis options_chain CSV format into normalized columns."""
    required_cols = [
        "symbol",
        "underlying_index",
        "timestamp",
        "local_timestamp",
        "expiration_timestamp",
        "strike_price",
        "option_type",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"parse_tardis_options_chain_csv: missing required columns: {missing}")

    def _first_existing(candidates: list[str]) -> str | None:
        return next((col for col in candidates if col in df.columns), None)

    bid_px_col = _first_existing(["bid_price", "best_bid_price"])
    ask_px_col = _first_existing(["ask_price", "best_ask_price"])
    bid_sz_col = _first_existing(["bid_amount", "best_bid_amount", "bid_size"])
    ask_sz_col = _first_existing(["ask_amount", "best_ask_amount", "ask_size"])
    iv_col = _first_existing(["iv", "implied_volatility"])
    mark_iv_col = _first_existing(["mark_iv", "mark_implied_volatility"])
    mark_px_col = _first_existing(["mark_price", "mark_px"])
    underlying_px_col = _first_existing(["underlying_price", "underlying_px"])
    oi_col = _first_existing(["open_interest", "oi"])

    exprs: list[pl.Expr] = [
        pl.col("symbol").cast(pl.Utf8).alias("exchange_symbol"),
        pl.col("underlying_index").cast(pl.Utf8),
        pl.col("local_timestamp").cast(pl.Int64).alias("ts_local_us"),
        pl.col("timestamp").cast(pl.Int64).alias("ts_exch_us"),
        pl.col("expiration_timestamp").cast(pl.Int64).alias("expiry_ts_us"),
        pl.col("option_type").cast(pl.Utf8).alias("option_type_raw"),
        pl.col("strike_price").cast(pl.Float64, strict=False).alias("strike_px"),
        pl.col("delta").cast(pl.Float64, strict=False)
        if "delta" in df.columns
        else pl.lit(None, dtype=pl.Float64).alias("delta"),
        pl.col("gamma").cast(pl.Float64, strict=False)
        if "gamma" in df.columns
        else pl.lit(None, dtype=pl.Float64).alias("gamma"),
        pl.col("vega").cast(pl.Float64, strict=False)
        if "vega" in df.columns
        else pl.lit(None, dtype=pl.Float64).alias("vega"),
        pl.col("theta").cast(pl.Float64, strict=False)
        if "theta" in df.columns
        else pl.lit(None, dtype=pl.Float64).alias("theta"),
        pl.col("rho").cast(pl.Float64, strict=False)
        if "rho" in df.columns
        else pl.lit(None, dtype=pl.Float64).alias("rho"),
    ]

    exprs.append(
        (pl.col(bid_px_col).cast(pl.Float64, strict=False).alias("bid_px"))
        if bid_px_col
        else pl.lit(None, dtype=pl.Float64).alias("bid_px")
    )
    exprs.append(
        (pl.col(ask_px_col).cast(pl.Float64, strict=False).alias("ask_px"))
        if ask_px_col
        else pl.lit(None, dtype=pl.Float64).alias("ask_px")
    )
    exprs.append(
        (pl.col(bid_sz_col).cast(pl.Float64, strict=False).alias("bid_sz"))
        if bid_sz_col
        else pl.lit(None, dtype=pl.Float64).alias("bid_sz")
    )
    exprs.append(
        (pl.col(ask_sz_col).cast(pl.Float64, strict=False).alias("ask_sz"))
        if ask_sz_col
        else pl.lit(None, dtype=pl.Float64).alias("ask_sz")
    )
    exprs.append(
        (pl.col(iv_col).cast(pl.Float64, strict=False).alias("iv"))
        if iv_col
        else pl.lit(None, dtype=pl.Float64).alias("iv")
    )
    exprs.append(
        (pl.col(mark_iv_col).cast(pl.Float64, strict=False).alias("mark_iv"))
        if mark_iv_col
        else pl.lit(None, dtype=pl.Float64).alias("mark_iv")
    )
    exprs.append(
        (pl.col(mark_px_col).cast(pl.Float64, strict=False).alias("mark_px"))
        if mark_px_col
        else pl.lit(None, dtype=pl.Float64).alias("mark_px")
    )
    exprs.append(
        (pl.col(underlying_px_col).cast(pl.Float64, strict=False).alias("underlying_px"))
        if underlying_px_col
        else pl.lit(None, dtype=pl.Float64).alias("underlying_px")
    )
    exprs.append(
        (pl.col(oi_col).cast(pl.Float64, strict=False).alias("open_interest"))
        if oi_col
        else pl.lit(None, dtype=pl.Float64).alias("open_interest")
    )

    result = df.clone().with_columns(exprs)

    select_cols = [
        "exchange_symbol",
        "underlying_index",
        "ts_local_us",
        "ts_exch_us",
        "expiry_ts_us",
        "option_type_raw",
        "strike_px",
        "bid_px",
        "ask_px",
        "bid_sz",
        "ask_sz",
        "mark_px",
        "underlying_px",
        "iv",
        "mark_iv",
        "delta",
        "gamma",
        "vega",
        "theta",
        "rho",
        "open_interest",
    ]
    if "file_line_number" in result.columns:
        select_cols = ["file_line_number", *select_cols]
    return result.select(select_cols)
