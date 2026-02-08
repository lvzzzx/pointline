"""User-defined rollups for Pattern B tick-feature aggregation."""

from . import custom  # noqa: F401
from .builtins import (
    BUILTIN_FEATURE_ROLLUPS,
    DEFAULT_FEATURE_ROLLUPS,
    build_builtin_feature_rollup_expr,
    is_builtin_feature_rollup,
    normalize_feature_rollup_names,
)
from .registry import FeatureRollupCallable, FeatureRollupMetadata, FeatureRollupRegistry

__all__ = [
    "FeatureRollupRegistry",
    "FeatureRollupMetadata",
    "FeatureRollupCallable",
    "DEFAULT_FEATURE_ROLLUPS",
    "BUILTIN_FEATURE_ROLLUPS",
    "normalize_feature_rollup_names",
    "is_builtin_feature_rollup",
    "build_builtin_feature_rollup_expr",
]
