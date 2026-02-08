"""Derivative aggregations for futures/perpetuals research.

This module implements derivative-specific metrics:
- Funding rate aggregation
- Open interest change

CORRECTED: Uses actual derivative_ticker schema (float columns, not int).
"""

import polars as pl

from pointline.research.resample.registry import AggregationRegistry


@AggregationRegistry.register_aggregate_raw(
    name="funding_rate_mean",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["funding_rate"],
)
def funding_rate_mean(source_col: str) -> pl.Expr:
    """Mean funding rate over bar.

    Funding rate is the periodic payment between longs and shorts
    in perpetual futures contracts.

    CORRECTED: Uses actual derivative_ticker column name (funding_rate).
    Note: This is float, not fixed-point int.

    Args:
        source_col: Column name (typically "funding_rate")

    Returns:
        Polars expression computing mean funding rate

    Example:
        Funding rate snapshots: [0.0001, 0.0002, 0.00015]
        Mean = 0.00015 (1.5 bps per funding period)
    """
    return pl.col(source_col).mean()


@AggregationRegistry.register_aggregate_raw(
    name="oi_change",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["open_interest"],
)
def oi_change(source_col: str) -> pl.Expr:
    """Open interest change over bar.

    OI change indicates:
    - Positive: New positions being opened (increasing leverage)
    - Negative: Positions being closed (decreasing leverage)

    CORRECTED: Uses actual column name (open_interest, not open_interest_int).
    This is float in derivative_ticker table.

    Args:
        source_col: Column name (typically "open_interest")

    Returns:
        Polars expression computing OI change (last - first)

    Example:
        OI at bar start: 1,000,000
        OI at bar end: 1,008,000
        OI change: +8,000 contracts (new longs/shorts opened)
    """
    return pl.col(source_col).last() - pl.col(source_col).first()


@AggregationRegistry.register_aggregate_raw(
    name="oi_last",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["open_interest"],
)
def oi_last(source_col: str) -> pl.Expr:
    """Last open interest value in bar.

    Useful for getting the final OI snapshot in each bar.

    Args:
        source_col: Column name (typically "open_interest")

    Returns:
        Polars expression computing last OI value
    """
    return pl.col(source_col).last()
