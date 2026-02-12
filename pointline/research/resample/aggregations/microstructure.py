"""Microstructure aggregations for high-frequency research.

This module implements advanced microstructure aggregations:
- Microprice (mid-price weighted by opposite-side size)
- Spread distribution
- Order Flow Imbalance (OFI)

CORRECTED: Uses actual table column names from current schemas.
"""

import polars as pl

from pointline.research.resample.config import AggregationSpec
from pointline.research.resample.registry import AggregationRegistry


@AggregationRegistry.register_compute_features(
    name="microprice_close",
    semantic_type="book_top",
    mode_allowlist=["HFT", "MFT"],
    required_columns=["bids_px_int", "asks_px_int", "bids_sz_int", "asks_sz_int"],
)
def microprice_close(lf: pl.LazyFrame, spec: AggregationSpec) -> pl.LazyFrame:
    """Compute microprice at tick level, take last per bucket.

    Microprice = (bid_px * ask_sz + ask_px * bid_sz) / (bid_sz + ask_sz)

    This is a more accurate mid-price that accounts for liquidity imbalance.

    CORRECTED: Uses actual book_snapshot_25 column names.
    Note: bids_px_int is array[0] for best bid.

    Args:
        lf: LazyFrame with book snapshot data
        spec: Aggregation specification

    Returns:
        LazyFrame with _microprice_close_feature column

    Example:
        Best bid: 50000, size 100
        Best ask: 50005, size 80
        Microprice = (50000 * 80 + 50005 * 100) / (100 + 80) = 50002.78
    """
    denom = pl.col("bids_sz_int").list.get(0) + pl.col("asks_sz_int").list.get(0)
    return lf.with_columns(
        [
            pl.when(denom > 0)
            .then(
                (
                    pl.col("bids_px_int").list.get(0) * pl.col("asks_sz_int").list.get(0)
                    + pl.col("asks_px_int").list.get(0) * pl.col("bids_sz_int").list.get(0)
                )
                / denom
            )
            .otherwise(None)
            .alias("_microprice_close_feature")
        ]
    )


@AggregationRegistry.register_compute_features(
    name="spread_distribution",
    semantic_type="quote_top",
    mode_allowlist=["HFT", "MFT"],
    required_columns=["ask_px_int", "bid_px_int"],
)
def spread_distribution(lf: pl.LazyFrame, spec: AggregationSpec) -> pl.LazyFrame:
    """Compute spread at tick level for distribution stats.

    Returns mean, std, min, max of spread in basis points.
    Pattern B from bar-aggregation.md.

    CORRECTED: Uses actual quotes table column names.

    Args:
        lf: LazyFrame with quotes data
        spec: Aggregation specification

    Returns:
        LazyFrame with _spread_distribution_feature column (spread in bps)

    Example:
        bid=50000, ask=50005 → spread = 5 / 50000 * 10000 = 1 bps
    """
    return lf.with_columns(
        [
            ((pl.col("ask_px_int") - pl.col("bid_px_int")) / pl.col("bid_px_int") * 10000).alias(
                "_spread_distribution_feature"
            )
        ]
    )


@AggregationRegistry.register_compute_features(
    name="ofi_cont",
    semantic_type="book_depth",
    mode_allowlist=["HFT"],
    required_columns=[
        "exchange_id",
        "symbol",
        "ts_local_us",
        "bids_px_int",
        "asks_px_int",
        "bids_sz_int",
        "asks_sz_int",
    ],
)
def ofi_cont(lf: pl.LazyFrame, spec: AggregationSpec) -> pl.LazyFrame:
    """Classical contingent OFI (top-of-book) at each tick.

    Uses the Cont et al. style piecewise top-of-book update:
    - Bid contribution depends on bid price move (up/equal/down).
    - Ask contribution depends on ask price move (down/equal/up).
    - OFI = bid_contribution + ask_contribution.

    Positive OFI indicates net buying pressure, negative indicates selling pressure.

    Args:
        lf: LazyFrame with book snapshot data
        spec: Aggregation specification

    Returns:
        LazyFrame with _ofi_cont_feature column
    """
    schema_names = set(lf.collect_schema().names())
    sort_cols = [
        col
        for col in ["exchange_id", "symbol", "ts_local_us", "file_id", "file_line_number"]
        if col in schema_names
    ]
    sorted_lf = lf.sort(sort_cols) if sort_cols else lf

    partition_cols = [col for col in ["exchange_id", "symbol"] if col in schema_names]

    bid_px = pl.col("bids_px_int").list.get(0)
    ask_px = pl.col("asks_px_int").list.get(0)
    bid_sz = pl.col("bids_sz_int").list.get(0)
    ask_sz = pl.col("asks_sz_int").list.get(0)

    if partition_cols:
        bid_px_prev = bid_px.shift(1).over(partition_cols)
        ask_px_prev = ask_px.shift(1).over(partition_cols)
        bid_sz_prev = bid_sz.shift(1).over(partition_cols)
        ask_sz_prev = ask_sz.shift(1).over(partition_cols)
    else:
        bid_px_prev = bid_px.shift(1)
        ask_px_prev = ask_px.shift(1)
        bid_sz_prev = bid_sz.shift(1)
        ask_sz_prev = ask_sz.shift(1)

    bid_contrib = (
        pl.when(bid_px_prev.is_null())
        .then(0)
        .when(bid_px > bid_px_prev)
        .then(bid_sz)
        .when(bid_px == bid_px_prev)
        .then(bid_sz - bid_sz_prev)
        .otherwise(-bid_sz_prev)
    )

    ask_contrib = (
        pl.when(ask_px_prev.is_null())
        .then(0)
        .when(ask_px < ask_px_prev)
        .then(-ask_sz)
        .when(ask_px == ask_px_prev)
        .then(ask_sz_prev - ask_sz)
        .otherwise(ask_sz_prev)
    )

    return sorted_lf.with_columns(
        [(bid_contrib + ask_contrib).fill_null(0).alias("_ofi_cont_feature")]
    )
