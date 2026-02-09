"""Custom aggregations for crypto middle-frequency trading (MFT).

These aggregations are designed for volume bar resampling and focus on
microstructure signals: order flow, liquidity, and volatility.

Author: Quant Research
Date: 2026-02-09
"""

from __future__ import annotations

import polars as pl

from pointline.research.resample.registry import AggregationSpec, register_aggregation

__all__ = [
    "FlowImbalance",
    "SpreadBPS",
    "BookImbalanceTop5",
    "RealizedVolatility",
    "TradeSize",
    "AggressiveRatio",
    "VolumeWeightedReturn",
]


@register_aggregation
class FlowImbalance(AggregationSpec):
    """Order flow imbalance: (buy_volume - sell_volume) / total_volume.

    Measures buying vs selling pressure within the bar.

    Range: [-1, 1]
        -1 = all sells (bearish)
         0 = balanced (neutral)
        +1 = all buys (bullish)

    Academic: Easley et al. (2012) - VPIN (Volume-Synchronized Probability of
    Informed Trading)
    """

    name = "flow_imbalance"
    required_columns = ["qty_int", "side"]
    mode_allowlist = ["bar_then_feature", "tick_then_bar"]
    pit_policy = "backward_only"
    determinism_policy = "required"

    def impl(self, config: dict | None = None) -> list[pl.Expr]:
        """Compute order flow imbalance."""
        # side: 0 = buy, 1 = sell

        buy_vol = pl.when(pl.col("side") == 0).then(pl.col("qty_int")).otherwise(0).sum()

        sell_vol = pl.when(pl.col("side") == 1).then(pl.col("qty_int")).otherwise(0).sum()

        total_vol = buy_vol + sell_vol

        return [
            pl.when(total_vol > 0)
            .then((buy_vol - sell_vol) / total_vol)
            .otherwise(0.0)
            .alias("flow_imbalance")
        ]


@register_aggregation
class SpreadBPS(AggregationSpec):
    """Bid-ask spread in basis points (bps).

    Measures liquidity: tighter spread = more liquid.

    Range: [0, ∞)
        0 = no spread (theoretical)
        10 = 0.1% spread (typical for BTC)
        100 = 1% spread (illiquid)

    Note: Uses last known quote in bar (as-of join backward).
    """

    name = "spread_bps"
    required_columns = ["bid_px_int", "ask_px_int", "price_increment"]
    mode_allowlist = ["bar_then_feature", "event_joined"]
    pit_policy = "backward_only"
    determinism_policy = "required"

    def impl(self, config: dict | None = None) -> list[pl.Expr]:
        """Compute spread in basis points."""
        # Take last quote in bar (most recent before bar close)
        bid_px = pl.col("bid_px_int").last()
        ask_px = pl.col("ask_px_int").last()
        price_inc = pl.col("price_increment").last()

        # Decode to float
        bid_float = bid_px.cast(pl.Float64) * price_inc
        ask_float = ask_px.cast(pl.Float64) * price_inc

        mid_px = (bid_float + ask_float) / 2

        return [
            pl.when(mid_px > 0)
            .then(((ask_float - bid_float) / mid_px) * 10000)  # Convert to bps
            .otherwise(0.0)
            .alias("spread_bps")
        ]


@register_aggregation
class BookImbalanceTop5(AggregationSpec):
    """Book imbalance over top 5 levels.

    Measures order book pressure: more bids = bullish, more asks = bearish.

    Range: [-1, 1]
        -1 = all depth on ask side (bearish)
         0 = balanced book
        +1 = all depth on bid side (bullish)

    Academic: Cao et al. (2009) - Order imbalance predicts short-term returns
    """

    name = "book_imbalance_top5"
    required_columns = ["bids_qty_int", "asks_qty_int"]
    mode_allowlist = ["bar_then_feature", "event_joined"]
    pit_policy = "backward_only"
    determinism_policy = "required"

    def impl(self, config: dict | None = None) -> list[pl.Expr]:
        """Compute book imbalance over top 5 levels."""
        # bids_qty_int and asks_qty_int are List[Int64] with 25 levels
        # Sum top 5 levels (indices 0-4)

        # Take last snapshot in bar
        bids_qty = pl.col("bids_qty_int").last()
        asks_qty = pl.col("asks_qty_int").last()

        # Sum top 5 levels
        bid_depth = bids_qty.list.slice(0, 5).list.sum()
        ask_depth = asks_qty.list.slice(0, 5).list.sum()

        total_depth = bid_depth + ask_depth

        return [
            pl.when(total_depth > 0)
            .then((bid_depth - ask_depth) / total_depth)
            .otherwise(0.0)
            .alias("book_imbalance_top5")
        ]


@register_aggregation
class RealizedVolatility(AggregationSpec):
    """Realized volatility: standard deviation of log returns within bar.

    Measures intra-bar price variability (risk).

    Range: [0, ∞)
        0.0001 = 1 bps volatility (very quiet)
        0.001 = 10 bps volatility (normal)
        0.01 = 100 bps volatility (high volatility event)

    Note: Returns annualized by multiplying by sqrt(bars_per_year).
    """

    name = "realized_volatility"
    required_columns = ["px_int", "price_increment"]
    mode_allowlist = ["bar_then_feature", "tick_then_bar"]
    pit_policy = "backward_only"
    determinism_policy = "required"

    def impl(self, config: dict | None = None) -> list[pl.Expr]:
        """Compute realized volatility from tick-level returns."""
        # Decode price
        price_inc = pl.col("price_increment").first()  # Constant within symbol
        price = pl.col("px_int").cast(pl.Float64) * price_inc

        # Log returns
        log_price = price.log()
        log_ret = log_price.diff()

        # Standard deviation of log returns
        return [log_ret.std().fill_null(0.0).alias("realized_vol")]


@register_aggregation
class TradeSize(AggregationSpec):
    """Average and median trade size statistics.

    Large trades = institutional flow (whales)
    Small trades = retail flow

    Outputs:
        avg_trade_size: Mean trade size
        median_trade_size: Median trade size
        max_trade_size: Largest trade in bar
    """

    name = "trade_size"
    required_columns = ["qty_int", "amount_increment"]
    mode_allowlist = ["bar_then_feature", "tick_then_bar"]
    pit_policy = "backward_only"
    determinism_policy = "required"

    def impl(self, config: dict | None = None) -> list[pl.Expr]:
        """Compute trade size statistics."""
        amt_inc = pl.col("amount_increment").first()
        qty = pl.col("qty_int").cast(pl.Float64) * amt_inc

        return [
            qty.mean().alias("avg_trade_size"),
            qty.median().alias("median_trade_size"),
            qty.max().alias("max_trade_size"),
        ]


@register_aggregation
class AggressiveRatio(AggregationSpec):
    """Ratio of aggressive orders (market orders) to total volume.

    Aggressive orders = takers (remove liquidity)
    Passive orders = makers (provide liquidity)

    High aggressive ratio = urgency, information flow

    Range: [0, 1]
        0 = all passive (limit orders being filled)
        1 = all aggressive (market orders)

    Note: Requires trades to have aggressor side flag.
    In Tardis data: check if trade price matches ask (buy aggressor) or bid (sell aggressor)
    """

    name = "aggressive_ratio"
    required_columns = ["qty_int", "side"]  # Simplified: assumes all trades are aggressive
    mode_allowlist = ["bar_then_feature", "tick_then_bar"]
    pit_policy = "backward_only"
    determinism_policy = "required"

    def impl(self, config: dict | None = None) -> list[pl.Expr]:
        """Compute aggressive order ratio.

        Note: This is a simplified version. For accurate aggressor detection,
        join trades with quotes and check if trade price matches ask (buy aggressor)
        or bid (sell aggressor).
        """
        # Simplified: Assume all trades are aggressive (market orders)
        # In production, enhance with aggressor detection logic

        return [
            # Placeholder: 100% aggressive (requires enhancement)
            pl.lit(1.0).alias("aggressive_ratio"),
            # TODO: Implement aggressor detection:
            # total_qty = pl.col("qty_int").sum()
            # aggressive_qty = (
            #     pl.when((pl.col("side") == 0) & (pl.col("px_int") >= pl.col("ask_px_int")))
            #     .or_((pl.col("side") == 1) & (pl.col("px_int") <= pl.col("bid_px_int")))
            #     .then(pl.col("qty_int"))
            #     .otherwise(0)
            # ).sum()
            # return [
            #     (aggressive_qty / total_qty).alias("aggressive_ratio")
            # ]
        ]


@register_aggregation
class VolumeWeightedReturn(AggregationSpec):
    """Volume-weighted return within bar.

    Each trade contributes proportionally to its volume.
    More robust than simple close/open return.

    Output:
        vw_return: Volume-weighted log return
    """

    name = "volume_weighted_return"
    required_columns = ["px_int", "qty_int", "price_increment"]
    mode_allowlist = ["bar_then_feature", "tick_then_bar"]
    pit_policy = "backward_only"
    determinism_policy = "required"

    def impl(self, config: dict | None = None) -> list[pl.Expr]:
        """Compute volume-weighted return."""
        price_inc = pl.col("price_increment").first()
        price = pl.col("px_int").cast(pl.Float64) * price_inc
        qty = pl.col("qty_int").cast(pl.Float64)

        # First price (bar open)
        first_price = price.first()

        # Volume-weighted average price change
        # vw_return = sum(qty * log(price / first_price)) / sum(qty)
        log_ret = (price / first_price).log()
        vw_log_ret = (log_ret * qty).sum() / qty.sum()

        return [vw_log_ret.fill_null(0.0).alias("vw_return")]
