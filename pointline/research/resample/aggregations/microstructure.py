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
    name="ofi_sum",
    semantic_type="book_depth",
    mode_allowlist=["HFT"],
    required_columns=["bids_sz_int", "asks_sz_int"],
)
def ofi_sum(lf: pl.LazyFrame, spec: AggregationSpec) -> pl.LazyFrame:
    """Order flow imbalance (OFI) at each tick.

    OFI = ΔBid_volume - ΔAsk_volume

    Measures buying vs selling pressure from order book changes.
    Positive OFI = more buying pressure (bid volume increasing)
    Negative OFI = more selling pressure (ask volume increasing)

    CORRECTED: Uses book_snapshot_25 array columns.

    Args:
        lf: LazyFrame with book snapshot data
        spec: Aggregation specification

    Returns:
        LazyFrame with _ofi_sum_feature column

    Example:
        t0: bid_vol=100, ask_vol=80
        t1: bid_vol=150, ask_vol=90
        OFI = (150-100) - (90-80) = 50 - 10 = 40 (buying pressure)
    """
    sorted_lf = lf.sort(["exchange_id", "symbol_id", "ts_local_us"])
    return sorted_lf.with_columns(
        [
            (
                pl.col("bids_sz_int")
                .list.get(0)
                .diff()
                .over(["exchange_id", "symbol_id"])
                .fill_null(0)
                - pl.col("asks_sz_int")
                .list.get(0)
                .diff()
                .over(["exchange_id", "symbol_id"])
                .fill_null(0)
            ).alias("_ofi_sum_feature")
        ]
    )
