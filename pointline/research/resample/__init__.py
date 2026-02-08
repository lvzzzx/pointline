"""Resample and aggregate operations for research workflows.

This module provides:
- Typed aggregation registry with semantic validation
- Built-in aggregations (sum, mean, std, etc.)
- Custom aggregation support (microprice, OFI, etc.)
- Configuration schemas for resample/aggregate operations
- Bucket assignment with window-map semantics
- Aggregation execution engine
"""

# Import builtins and custom aggregations to register
from . import aggregations, builtins  # noqa: F401
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

__all__ = [
    # Registry
    "AggregationRegistry",
    "AggregationMetadata",
    "AggregateRawCallable",
    "ComputeFeaturesCallable",
    "SEMANTIC_POLICIES",
    # Config
    "AggregateConfig",
    "AggregationSpec",
    "ResampleConfig",
    # Operations
    "assign_to_buckets",
    "aggregate",
]
