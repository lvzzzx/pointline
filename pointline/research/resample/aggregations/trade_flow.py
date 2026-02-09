"""Trade flow aggregations for order flow analysis.

This module implements trade flow imbalance metrics:
- Signed trade imbalance (buy volume vs sell volume)

CORRECTED: Uses actual trades table column names and side values.
"""

import polars as pl

from pointline.research.resample.registry import AggregationRegistry


@AggregationRegistry.register_aggregate_raw(
    name="signed_trade_imbalance",
    semantic_type="trade_flow",
    mode_allowlist=["HFT", "MFT"],
    required_columns=["qty_int", "side"],
)
def signed_trade_imbalance(source_col: str) -> pl.Expr:
    """Signed trade volume imbalance.

    Pattern A: Aggregate signed volumes, then compute imbalance.

    Imbalance = (Buy_volume - Sell_volume) / (Buy_volume + Sell_volume)

    Returns value in [-1, 1]:
    - +1 = all buy volume
    - -1 = all sell volume
    - 0 = balanced

    CORRECTED: Uses actual side values (0=buy, 1=sell from trades table).

    Args:
        source_col: Column name for quantity (typically "qty_int")

    Returns:
        Polars expression computing trade imbalance

    Example:
        Buy trades: 100 + 200 = 300
        Sell trades: 150 + 50 = 200
        Imbalance = (300 - 200) / (300 + 200) = 0.2 (20% buy pressure)
    """
    buy_vol = (
        pl.when(pl.col("side") == 0)  # 0 = buy
        .then(pl.col(source_col))
        .otherwise(0)
        .sum()
    )
    sell_vol = (
        pl.when(pl.col("side") == 1)  # 1 = sell
        .then(pl.col(source_col))
        .otherwise(0)
        .sum()
    )
    return (buy_vol - sell_vol) / (buy_vol + sell_vol)
