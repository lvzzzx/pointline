"""Configuration schemas for resample and aggregate operations."""

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class AggregationSpec:
    """Specification for a single aggregation.

    Attributes:
        name: Output column name
        source_column: Input column to aggregate
        agg: Aggregation name (e.g., "sum", "mean", "microprice_close")
        semantic_type: Optional semantic type for validation (e.g., "price", "size")
        feature_rollups: Optional rollups for feature_then_aggregate stage
            Example: ["sum", "last", "close"].
            If omitted, Pattern B defaults to ["mean", "std", "min", "max"].
        feature_rollup_params: Optional params per rollup name.
            Example: {"weighted_close": {"weight_column": "qty_int"}}.
    """

    name: str
    source_column: str
    agg: str
    semantic_type: str | None = None
    feature_rollups: list[str] | None = None
    feature_rollup_params: dict[str, dict[str, Any]] | None = None


@dataclass(frozen=True)
class AggregateConfig:
    """Configuration for aggregation operations.

    Attributes:
        by: Grouping columns (typically ["exchange_id", "symbol", "bucket_ts"])
        aggregations: List of aggregation specifications
        mode: Pipeline execution mode
        research_mode: Research frequency mode (HFT/MFT/LFT) for registry validation
        registry_profile: Optional registry profile to use
    """

    by: list[str]
    aggregations: list[AggregationSpec]
    mode: Literal["event_joined", "tick_then_bar", "bar_then_feature"]
    research_mode: Literal["HFT", "MFT", "LFT"]
    registry_profile: str = "default"


@dataclass(frozen=True)
class ResampleConfig:
    """Configuration for resampling operations.

    Attributes:
        time_col: Timestamp column name
        by: Partitioning columns (typically ["exchange_id", "symbol"])
        every: Resampling interval (e.g., "1m", "5m", "1h")
        period: Optional window period (defaults to every)
        closed: Interval closure ("left" or "right")
        label: Interval label position ("left" or "right")
        mode: Pipeline execution mode
        fill_policy: How to handle missing values ("none", "forward", "backward")
        deterministic: Whether to enforce deterministic ordering
    """

    time_col: str
    by: list[str]
    every: str
    period: str | None = None
    closed: str = "left"
    label: str = "left"
    mode: Literal["event_joined", "tick_then_bar", "bar_then_feature"] = "bar_then_feature"
    fill_policy: str = "none"
    deterministic: bool = True
