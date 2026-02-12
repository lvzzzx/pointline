"""OI capacity context plugin."""

from __future__ import annotations

import polars as pl

from pointline.research.context.config import ContextSpec
from pointline.research.context.registry import ContextRegistry


@ContextRegistry.register_context(
    name="oi_capacity",
    required_columns=["exchange_id", "symbol", "ts_local_us"],
    mode_allowlist=["MFT", "LFT"],
    pit_policy={"feature_direction": "backward_only"},
    determinism_policy={
        "required_sort": ["exchange_id", "symbol", "ts_local_us"],
        "partition_by": ["exchange_id", "symbol"],
    },
    required_params={
        "oi_col": "column_name",
        "base_notional": "number",
    },
    optional_params={
        "price_col": "column_name",
        "lookback_bars": "integer",
        "min_ratio": "number",
        "clip_min": "number",
        "clip_max": "number",
        "epsilon": "number",
    },
    default_params={
        "lookback_bars": 96,
        "min_ratio": 0.6,
        "clip_min": 0.5,
        "clip_max": 1.5,
        "epsilon": 1e-12,
    },
)
def oi_capacity(frame: pl.LazyFrame, spec: ContextSpec) -> pl.LazyFrame:
    """Build OI-based tradeability and sizing context features.

    Outputs:
      - <name>_oi_notional            (if price_col provided)
      - <name>_oi_level_ratio
      - <name>_capacity_ok
      - <name>_capacity_mult
      - <name>_max_trade_notional
    """
    oi_col = str(spec.params["oi_col"])
    base_notional = float(spec.params["base_notional"])
    lookback = max(int(spec.params.get("lookback_bars", 96)), 1)
    min_ratio = float(spec.params.get("min_ratio", 0.6))
    clip_min = float(spec.params.get("clip_min", 0.5))
    clip_max = float(spec.params.get("clip_max", 1.5))
    epsilon = max(float(spec.params.get("epsilon", 1e-12)), 0.0)
    if clip_max < clip_min:
        raise ValueError("oi_capacity requires clip_max >= clip_min")

    schema_names = set(frame.collect_schema().names())
    sort_cols = [
        col
        for col in ["exchange_id", "symbol", "ts_local_us", "file_id", "file_line_number"]
        if col in schema_names
    ]
    out = frame.sort(sort_cols) if sort_cols else frame

    partition_by = [col for col in ["exchange_id", "symbol"] if col in schema_names]

    oi_expr = pl.col(oi_col)
    if partition_by:
        rolling_mean = oi_expr.rolling_mean(window_size=lookback, min_samples=1).over(partition_by)
    else:
        rolling_mean = oi_expr.rolling_mean(window_size=lookback, min_samples=1)

    ratio = pl.when(rolling_mean.abs() > epsilon).then(oi_expr / rolling_mean).otherwise(None)
    capacity_mult = ratio.clip(clip_min, clip_max).fill_null(clip_min)

    prefix = spec.name
    cols: list[pl.Expr] = [
        ratio.alias(f"{prefix}_oi_level_ratio"),
        (ratio.fill_null(0.0) >= min_ratio).alias(f"{prefix}_capacity_ok"),
        capacity_mult.alias(f"{prefix}_capacity_mult"),
        (pl.lit(base_notional) * capacity_mult).alias(f"{prefix}_max_trade_notional"),
    ]

    price_col = spec.params.get("price_col")
    if isinstance(price_col, str) and price_col:
        cols.insert(0, (oi_expr * pl.col(price_col)).alias(f"{prefix}_oi_notional"))

    return out.with_columns(cols)
