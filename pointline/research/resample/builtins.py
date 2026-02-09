"""Built-in aggregations for resample operations.

This module registers standard aggregations using Pattern A (aggregate_then_feature).
These are simple aggregations that operate on raw column values.
"""

import polars as pl

from .registry import AggregationRegistry

# CORRECTED: Use register_aggregate_raw for Pattern A


@AggregationRegistry.register_aggregate_raw(
    name="sum",
    semantic_type="size",
    mode_allowlist=["HFT", "MFT", "LFT"],
)
def agg_sum(source_col: str) -> pl.Expr:
    """Sum aggregation (Pattern A).

    Suitable for: volume, notional, event counts
    Not suitable for: prices (use mean or last instead)
    """
    return pl.col(source_col).sum()


@AggregationRegistry.register_aggregate_raw(
    name="mean",
    semantic_type="size",
    mode_allowlist=["HFT", "MFT", "LFT"],
)
def agg_mean(source_col: str) -> pl.Expr:
    """Mean aggregation (Pattern A).

    Suitable for: prices, volumes, spreads
    """
    return pl.col(source_col).mean()


@AggregationRegistry.register_aggregate_raw(
    name="std",
    semantic_type="size",
    mode_allowlist=["HFT", "MFT", "LFT"],
)
def agg_std(source_col: str) -> pl.Expr:
    """Standard deviation aggregation (Pattern A).

    Suitable for: volatility measures, spread distributions
    """
    return pl.col(source_col).std()


@AggregationRegistry.register_aggregate_raw(
    name="min",
    semantic_type="size",
    mode_allowlist=["HFT", "MFT", "LFT"],
)
def agg_min(source_col: str) -> pl.Expr:
    """Minimum aggregation (Pattern A).

    Suitable for: low prices, min spreads
    """
    return pl.col(source_col).min()


@AggregationRegistry.register_aggregate_raw(
    name="max",
    semantic_type="size",
    mode_allowlist=["HFT", "MFT", "LFT"],
)
def agg_max(source_col: str) -> pl.Expr:
    """Maximum aggregation (Pattern A).

    Suitable for: high prices, max spreads
    """
    return pl.col(source_col).max()


@AggregationRegistry.register_aggregate_raw(
    name="last",
    semantic_type="state_variable",
    mode_allowlist=["HFT", "MFT", "LFT"],
)
def agg_last(source_col: str) -> pl.Expr:
    """Last value aggregation (Pattern A).

    Suitable for: closing prices, final states, snapshots
    """
    return pl.col(source_col).last()


@AggregationRegistry.register_aggregate_raw(
    name="first",
    semantic_type="state_variable",
    mode_allowlist=["HFT", "MFT", "LFT"],
)
def agg_first(source_col: str) -> pl.Expr:
    """First value aggregation (Pattern A).

    Suitable for: opening prices, initial states
    """
    return pl.col(source_col).first()


@AggregationRegistry.register_aggregate_raw(
    name="count",
    semantic_type="event_id",
    mode_allowlist=["HFT", "MFT", "LFT"],
)
def agg_count(source_col: str) -> pl.Expr:
    """Count aggregation (Pattern A).

    Counts non-null values in the column.
    Suitable for: event counts, trade counts
    """
    return pl.col(source_col).count()


@AggregationRegistry.register_aggregate_raw(
    name="nunique",
    semantic_type="event_id",
    mode_allowlist=["HFT", "MFT", "LFT"],
)
def agg_nunique(source_col: str) -> pl.Expr:
    """Count unique values aggregation (Pattern A).

    Suitable for: counting distinct event types, unique participants
    """
    return pl.col(source_col).n_unique()
