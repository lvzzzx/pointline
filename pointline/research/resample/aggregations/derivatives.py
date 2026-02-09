"""Derivative aggregations for futures/perpetuals research.

This module implements derivative-specific metrics:
- Funding rate state and delta features
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
    name="funding_close",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["funding_rate"],
)
def funding_close(source_col: str) -> pl.Expr:
    """Funding snapshot at bar close (as-of last).

    This is typically a more meaningful baseline than intra-bar mean for
    state-style funding feeds.
    """
    return pl.col(source_col).last()


@AggregationRegistry.register_aggregate_raw(
    name="funding_step",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["funding_rate"],
)
def funding_step(source_col: str) -> pl.Expr:
    """Funding step inside bar: close - open."""
    return pl.col(source_col).last() - pl.col(source_col).first()


@AggregationRegistry.register_aggregate_raw(
    name="funding_carry_8h_per_hour",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["funding_rate"],
)
def funding_carry_8h_per_hour(source_col: str) -> pl.Expr:
    """Funding carry normalized to per-hour under an 8h settlement convention."""
    return pl.col(source_col).last() / pl.lit(8.0)


@AggregationRegistry.register_aggregate_raw(
    name="funding_surprise",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["funding_rate", "predicted_funding_rate"],
)
def funding_surprise(source_col: str) -> pl.Expr:
    """Funding surprise at close: funding_close - predicted_funding_rate_close."""
    return pl.col(source_col).last() - pl.col("predicted_funding_rate").last()


@AggregationRegistry.register_aggregate_raw(
    name="funding_pressure",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["funding_rate", "open_interest"],
)
def funding_pressure(source_col: str) -> pl.Expr:
    """Funding step scaled by OI turnover pressure.

    Formula:
        (funding_close - funding_open) * ((oi_close - oi_open) / max(abs(oi_close), eps))
    """
    eps = pl.lit(1e-12)
    funding_delta = pl.col(source_col).last() - pl.col(source_col).first()
    oi_delta = pl.col("open_interest").last() - pl.col("open_interest").first()
    oi_close_abs = pl.col("open_interest").last().abs()
    denom = pl.max_horizontal(oi_close_abs, eps)
    return funding_delta * (oi_delta / denom)


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


@AggregationRegistry.register_aggregate_raw(
    name="oi_open",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["open_interest"],
)
def oi_open(source_col: str) -> pl.Expr:
    """First open interest value in bar."""
    return pl.col(source_col).first()


@AggregationRegistry.register_aggregate_raw(
    name="oi_high",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["open_interest"],
)
def oi_high(source_col: str) -> pl.Expr:
    """Highest open interest value in bar."""
    return pl.col(source_col).max()


@AggregationRegistry.register_aggregate_raw(
    name="oi_low",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["open_interest"],
)
def oi_low(source_col: str) -> pl.Expr:
    """Lowest open interest value in bar."""
    return pl.col(source_col).min()


@AggregationRegistry.register_aggregate_raw(
    name="oi_range",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["open_interest"],
)
def oi_range(source_col: str) -> pl.Expr:
    """Open interest range inside bar: max - min."""
    return pl.col(source_col).max() - pl.col(source_col).min()


@AggregationRegistry.register_aggregate_raw(
    name="oi_pct_change",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["open_interest"],
)
def oi_pct_change(source_col: str) -> pl.Expr:
    """Normalized OI change vs bar open: (close - open) / max(abs(open), eps)."""
    eps = pl.lit(1e-12)
    oi_open_val = pl.col(source_col).first()
    denom = pl.max_horizontal(oi_open_val.abs(), eps)
    return (pl.col(source_col).last() - oi_open_val) / denom


@AggregationRegistry.register_aggregate_raw(
    name="oi_pressure",
    semantic_type="state_variable",
    mode_allowlist=["MFT", "LFT"],
    required_columns=["open_interest"],
)
def oi_pressure(source_col: str) -> pl.Expr:
    """Normalized OI pressure vs bar close: (close - open) / max(abs(close), eps)."""
    eps = pl.lit(1e-12)
    oi_close_val = pl.col(source_col).last()
    denom = pl.max_horizontal(oi_close_val.abs(), eps)
    return (oi_close_val - pl.col(source_col).first()) / denom
