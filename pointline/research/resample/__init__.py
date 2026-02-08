"""Resample and aggregate operations for research workflows.

This module provides:
- Typed aggregation registry with semantic validation
- Built-in aggregations (sum, mean, std, etc.)
- Custom aggregation support (microprice, OFI, etc.)
- Configuration schemas for resample/aggregate operations
- Bucket assignment with window-map semantics
- Aggregation execution engine
"""

# Import builtins/custom aggregations and rollups to register
from . import aggregations, builtins, rollups  # noqa: F401
from .aggregate import aggregate
from .bucket_assignment import assign_to_buckets
from .config import AggregateConfig, AggregationSpec, ResampleConfig
from .registry import (
    SEMANTIC_POLICIES,
    AggregateRawCallable,
    AggregationMetadata,
    AggregationRegistry,
    ComputeFeaturesCallable,
)
from .rollups import (
    BUILTIN_FEATURE_ROLLUPS,
    DEFAULT_FEATURE_ROLLUPS,
    FeatureRollupCallable,
    FeatureRollupMetadata,
    FeatureRollupRegistry,
)

__all__ = [
    # Registry
    "AggregationRegistry",
    "AggregationMetadata",
    "AggregateRawCallable",
    "ComputeFeaturesCallable",
    "FeatureRollupRegistry",
    "FeatureRollupMetadata",
    "FeatureRollupCallable",
    "DEFAULT_FEATURE_ROLLUPS",
    "BUILTIN_FEATURE_ROLLUPS",
    "SEMANTIC_POLICIES",
    # Config
    "AggregateConfig",
    "AggregationSpec",
    "ResampleConfig",
    # Operations
    "assign_to_buckets",
    "aggregate",
]
