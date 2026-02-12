"""Starter custom rollup methods for Pattern B."""

from __future__ import annotations

import polars as pl

from .registry import FeatureRollupRegistry


@FeatureRollupRegistry.register_feature_rollup(
    name="weighted_close",
    required_params={"weight_column": "column_name"},
    required_columns=[],
    mode_allowlist=["HFT", "MFT", "LFT"],
    semantic_allowlist=["any"],
    pit_policy={"feature_direction": "backward_only"},
    determinism_policy={"required_sort": ["exchange_id", "symbol", "ts_local_us"]},
)
def weighted_close(feature_col: str, params: dict[str, object]) -> pl.Expr:
    """Weighted close over ticks within a bar.

    Formula: sum(feature * weight_column) / sum(weight_column)
    """
    weight_col = str(params["weight_column"])
    weights = pl.col(weight_col)
    denom = weights.sum()
    numer = (pl.col(feature_col) * weights).sum()
    return pl.when(denom.abs() > 0).then(numer / denom).otherwise(None)


@FeatureRollupRegistry.register_feature_rollup(
    name="trimmed_mean_10pct",
    required_params={},
    required_columns=[],
    mode_allowlist=["HFT", "MFT", "LFT"],
    semantic_allowlist=["any"],
    pit_policy={"feature_direction": "backward_only"},
    determinism_policy={"required_sort": ["exchange_id", "symbol", "ts_local_us"]},
)
def trimmed_mean_10pct(feature_col: str, params: dict[str, object]) -> pl.Expr:
    """Mean after symmetric 10% trimming using deterministic quantile interpolation."""
    col = pl.col(feature_col)
    q10 = col.quantile(0.10, interpolation="nearest")
    q90 = col.quantile(0.90, interpolation="nearest")
    return col.filter((col >= q10) & (col <= q90)).mean()


@FeatureRollupRegistry.register_feature_rollup(
    name="tail_ratio_p95_p50",
    required_params={},
    required_columns=[],
    mode_allowlist=["HFT", "MFT", "LFT"],
    semantic_allowlist=["any"],
    pit_policy={"feature_direction": "backward_only"},
    determinism_policy={"required_sort": ["exchange_id", "symbol", "ts_local_us"]},
    optional_params={"epsilon": "number"},
    default_params={"epsilon": 1e-12},
)
def tail_ratio_p95_p50(feature_col: str, params: dict[str, object]) -> pl.Expr:
    """Tail heaviness proxy: q95 / max(abs(q50), epsilon)."""
    col = pl.col(feature_col)
    q95 = col.quantile(0.95, interpolation="nearest")
    q50 = col.quantile(0.50, interpolation="nearest")
    epsilon = max(float(params.get("epsilon", 1e-12)), 0.0)
    denom = pl.max_horizontal(q50.abs(), pl.lit(epsilon))
    return q95 / denom


@FeatureRollupRegistry.register_feature_rollup(
    name="ofi_imbalance",
    required_params={},
    required_columns=[],
    mode_allowlist=["HFT", "MFT", "LFT"],
    semantic_allowlist=["any"],
    pit_policy={"feature_direction": "backward_only"},
    determinism_policy={"required_sort": ["exchange_id", "symbol", "ts_local_us"]},
    optional_params={"epsilon": "number"},
    default_params={"epsilon": 1e-12},
)
def ofi_imbalance(feature_col: str, params: dict[str, object]) -> pl.Expr:
    """Normalized OFI rollup: sum(OFI) / max(sum(abs(OFI)), epsilon)."""
    col = pl.col(feature_col)
    epsilon = max(float(params.get("epsilon", 1e-12)), 0.0)
    numer = col.sum()
    denom = col.abs().sum()
    return pl.when(denom > epsilon).then(numer / denom).otherwise(None)
