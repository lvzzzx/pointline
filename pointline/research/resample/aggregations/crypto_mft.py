"""Custom aggregations for crypto middle-frequency trading (MFT).

These aggregations are designed for volume bar resampling and focus on
microstructure signals: order flow, liquidity, and volatility.

Author: Quant Research
Date: 2026-02-09
"""

from __future__ import annotations

import polars as pl

from pointline.research.resample.registry import AggregationRegistry

__all__ = [
    "flow_imbalance",
    "spread_bps",
    "book_imbalance_top5",
    "realized_volatility",
    "avg_trade_size",
    "median_trade_size",
    "max_trade_size",
    "aggressive_ratio",
    "vw_return",
]


@AggregationRegistry.register_aggregate_raw(
    name="flow_imbalance",
    semantic_type="trade_flow",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["qty_int", "side"],
)
def flow_imbalance(source_col: str) -> pl.Expr:
    """Order flow imbalance: (buy_volume - sell_volume) / total_volume.

    Measures buying vs selling pressure within the bar.

    Range: [-1, 1]
        -1 = all sells (bearish)
         0 = balanced (neutral)
        +1 = all buys (bullish)

    Academic: Easley et al. (2012) - VPIN (Volume-Synchronized Probability of
    Informed Trading)

    Args:
        source_col: Volume column name (typically "qty_int")

    Returns:
        Polars expression computing flow imbalance
    """
    # side: 0 = buy, 1 = sell
    buy_vol = pl.when(pl.col("side") == 0).then(pl.col(source_col)).otherwise(0).sum()
    sell_vol = pl.when(pl.col("side") == 1).then(pl.col(source_col)).otherwise(0).sum()
    total_vol = buy_vol + sell_vol

    return pl.when(total_vol > 0).then((buy_vol - sell_vol) / total_vol).otherwise(0.0)


@AggregationRegistry.register_aggregate_raw(
    name="spread_bps",
    semantic_type="quote_top",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["bid_px_int", "ask_px_int"],
)
def spread_bps(source_col: str) -> pl.Expr:
    """Bid-ask spread in basis points (bps).

    Measures liquidity: tighter spread = more liquid.

    Range: [0, ∞)
        0 = no spread (theoretical)
        10 = 0.1% spread (typical for BTC)
        100 = 1% spread (illiquid)

    Note: Uses last known quote in bar (as-of join backward).
    Works directly on integer columns — the encoding scalar cancels in the ratio.

    Args:
        source_col: Bid price column (typically "bid_px_int")

    Returns:
        Polars expression computing spread in bps
    """
    # Take last quote in bar (most recent before bar close)
    # Scalar cancels: (ask - bid) / mid is the same in int domain as float domain
    bid_px = pl.col("bid_px_int").last().cast(pl.Float64)
    ask_px = pl.col("ask_px_int").last().cast(pl.Float64)
    mid_px = (bid_px + ask_px) / 2

    return pl.when(mid_px > 0).then(((ask_px - bid_px) / mid_px) * 10000).otherwise(0.0)


@AggregationRegistry.register_aggregate_raw(
    name="book_imbalance_top5",
    semantic_type="book_state",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["bids_qty_int", "asks_qty_int"],
)
def book_imbalance_top5(source_col: str) -> pl.Expr:
    """Book imbalance over top 5 levels.

    Measures order book pressure: more bids = bullish, more asks = bearish.

    Range: [-1, 1]
        -1 = all depth on ask side (bearish)
         0 = balanced book
        +1 = all depth on bid side (bullish)

    Academic: Cao et al. (2009) - Order imbalance predicts short-term returns

    Args:
        source_col: Bids quantity column (typically "bids_qty_int")

    Returns:
        Polars expression computing book imbalance
    """
    # bids_qty_int and asks_qty_int are List[Int64] with 25 levels
    # Sum top 5 levels (indices 0-4)

    # Take last snapshot in bar
    bids_qty = pl.col("bids_qty_int").last()
    asks_qty = pl.col("asks_qty_int").last()

    # Sum top 5 levels
    bid_depth = bids_qty.list.slice(0, 5).list.sum()
    ask_depth = asks_qty.list.slice(0, 5).list.sum()
    total_depth = bid_depth + ask_depth

    return pl.when(total_depth > 0).then((bid_depth - ask_depth) / total_depth).otherwise(0.0)


@AggregationRegistry.register_aggregate_raw(
    name="realized_volatility",
    semantic_type="volatility",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["px_int"],
)
def realized_volatility(source_col: str) -> pl.Expr:
    """Realized volatility: standard deviation of log returns within bar.

    Measures intra-bar price variability (risk).

    Range: [0, ∞)
        0.0001 = 1 bps volatility (very quiet)
        0.001 = 10 bps volatility (normal)
        0.01 = 100 bps volatility (high volatility event)

    Works directly on integer columns — log returns are scale-invariant
    (the encoding scalar cancels: log(a*x) - log(a*y) = log(x) - log(y)).

    Args:
        source_col: Price column (typically "px_int")

    Returns:
        Polars expression computing realized volatility
    """
    price = pl.col(source_col).cast(pl.Float64)

    # Log returns (scalar cancels in differences)
    log_price = price.log()
    log_ret = log_price.diff()

    # Standard deviation of log returns
    return log_ret.std().fill_null(0.0)


@AggregationRegistry.register_aggregate_raw(
    name="avg_trade_size",
    semantic_type="size",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["qty_int"],
)
def avg_trade_size(source_col: str) -> pl.Expr:
    """Average trade size (in encoded integer units).

    Large trades = institutional flow (whales)
    Small trades = retail flow

    Values are in the encoded integer domain. Multiply by profile.amount
    to get float quantities.

    Args:
        source_col: Quantity column (typically "qty_int")

    Returns:
        Polars expression computing average trade size
    """
    return pl.col(source_col).cast(pl.Float64).mean()


@AggregationRegistry.register_aggregate_raw(
    name="median_trade_size",
    semantic_type="size",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["qty_int"],
)
def median_trade_size(source_col: str) -> pl.Expr:
    """Median trade size (in encoded integer units).

    Args:
        source_col: Quantity column (typically "qty_int")

    Returns:
        Polars expression computing median trade size
    """
    return pl.col(source_col).cast(pl.Float64).median()


@AggregationRegistry.register_aggregate_raw(
    name="max_trade_size",
    semantic_type="size",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["qty_int"],
)
def max_trade_size(source_col: str) -> pl.Expr:
    """Maximum trade size in bar (in encoded integer units).

    Args:
        source_col: Quantity column (typically "qty_int")

    Returns:
        Polars expression computing max trade size
    """
    return pl.col(source_col).cast(pl.Float64).max()


@AggregationRegistry.register_aggregate_raw(
    name="aggressive_ratio",
    semantic_type="trade_flow",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["qty_int", "side"],
)
def aggressive_ratio(source_col: str) -> pl.Expr:
    """Ratio of aggressive orders (market orders) to total volume.

    Aggressive orders = takers (remove liquidity)
    Passive orders = makers (provide liquidity)

    High aggressive ratio = urgency, information flow

    Range: [0, 1]
        0 = all passive (limit orders being filled)
        1 = all aggressive (market orders)

    Note: This is a simplified version. For accurate aggressor detection,
    join trades with quotes and check if trade price matches ask (buy aggressor)
    or bid (sell aggressor).

    Args:
        source_col: Quantity column (typically "qty_int")

    Returns:
        Polars expression computing aggressive ratio (placeholder: returns 1.0)
    """
    # Placeholder: Assume all trades are aggressive (market orders)
    # In production, enhance with aggressor detection logic
    return pl.lit(1.0)


@AggregationRegistry.register_aggregate_raw(
    name="vw_return",
    semantic_type="return",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["px_int", "qty_int"],
)
def vw_return(source_col: str) -> pl.Expr:
    """Volume-weighted return within bar.

    Each trade contributes proportionally to its volume.
    More robust than simple close/open return.

    Works directly on integer columns — log returns are scale-invariant
    (the encoding scalar cancels in the ratio).

    Args:
        source_col: Price column (typically "px_int")

    Returns:
        Polars expression computing volume-weighted return
    """
    price = pl.col(source_col).cast(pl.Float64)
    qty = pl.col("qty_int").cast(pl.Float64)

    # First price (bar open) — scalar cancels in ratio
    first_price = price.first()

    # Volume-weighted average price change
    # vw_return = sum(qty * log(price / first_price)) / sum(qty)
    log_ret = (price / first_price).log()
    vw_log_ret = (log_ret * qty).sum() / qty.sum()

    return vw_log_ret.fill_null(0.0)
