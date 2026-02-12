"""Typed aggregation registry for resample operations.

This module provides a registry system for custom aggregations with:
- Typed callables per stage (Pattern A vs Pattern B)
- Semantic type validation
- Research mode allowlists (HFT/MFT/LFT)
- PIT policy enforcement
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import polars as pl

if TYPE_CHECKING:
    from .config import AggregationSpec

# CORRECTED: Separate typed callables per stage
# Pattern A: Aggregate raw values first, then compute features
AggregateRawCallable = Callable[[str], pl.Expr]

# Pattern B: Compute features on ticks, then aggregate
# Forward reference for AggregationSpec to avoid circular import
ComputeFeaturesCallable = Callable[[pl.LazyFrame, "AggregationSpec"], pl.LazyFrame]


@dataclass(frozen=True)
class AggregationMetadata:
    """Registry entry with correctly typed callables.

    CORRECTED: Uses separate callable types per stage.
    Exactly ONE of aggregate_raw or compute_features is non-None.

    Attributes:
        name: Aggregation identifier
        stage: Processing stage (aggregate_then_feature or feature_then_aggregate)
        semantic_type: Data type category (price, size, notional, etc.)
        mode_allowlist: Research modes where this aggregation is allowed (HFT/MFT/LFT)
        required_columns: Columns that must exist in the input data
        pit_policy: Point-in-time correctness policy
        determinism: Deterministic ordering requirements
        aggregate_raw: Pattern A callable (aggregate raw values)
        compute_features: Pattern B callable (compute features on ticks)
    """

    name: str
    stage: Literal["feature_then_aggregate", "aggregate_then_feature"]
    semantic_type: str
    mode_allowlist: list[str]
    required_columns: list[str]
    pit_policy: dict[str, str]
    determinism: dict[str, list[str]]
    impl_ref: str = ""
    version: str = "2.0"

    # CORRECTED: Separate callables, not single impl
    aggregate_raw: AggregateRawCallable | None = None  # Pattern A only
    compute_features: ComputeFeaturesCallable | None = None  # Pattern B only

    def __post_init__(self):
        """Validate exactly one callable is set based on stage."""
        if self.stage == "aggregate_then_feature":
            if self.aggregate_raw is None:
                raise ValueError(f"{self.name}: aggregate_then_feature requires aggregate_raw")
            if self.compute_features is not None:
                raise ValueError(
                    f"{self.name}: aggregate_then_feature cannot have compute_features"
                )
        else:  # feature_then_aggregate
            if self.compute_features is None:
                raise ValueError(f"{self.name}: feature_then_aggregate requires compute_features")
            if self.aggregate_raw is not None:
                raise ValueError(f"{self.name}: feature_then_aggregate cannot have aggregate_raw")

    @property
    def determinism_policy(self) -> dict[str, list[str]]:
        """Alias for contract naming consistency."""
        return self.determinism


class AggregationRegistry:
    """Global registry for aggregations.

    This registry manages both built-in and custom aggregations with:
    - Type-safe callable registration
    - Mode-based validation
    - Semantic type policies
    - Registry profiles for different research workflows
    """

    _registry: dict[str, AggregationMetadata] = {}
    _profiles: dict[str, set[str]] = {
        "hft_default": {"sum", "mean", "last", "count", "microprice_close", "ofi_cont"},
        "mft_default": {"sum", "mean", "std", "last", "count", "spread_distribution"},
        "lft_default": {"sum", "mean", "last", "count"},
    }

    @classmethod
    def register_aggregate_raw(
        cls,
        name: str,
        *,
        semantic_type: str,
        mode_allowlist: list[str],
        required_columns: list[str] | None = None,
        pit_policy: dict | None = None,
    ):
        """Decorator for Pattern A aggregations (aggregate_then_feature).

        CORRECTED: Explicit decorator for aggregate_raw callables.

        Args:
            name: Aggregation identifier
            semantic_type: Data type category (e.g., "size", "price", "notional")
            mode_allowlist: Research modes where allowed (e.g., ["HFT", "MFT", "LFT"])
            required_columns: Columns that must exist in input data
            pit_policy: PIT correctness policy

        Returns:
            Decorator function

        Example:
            @AggregationRegistry.register_aggregate_raw(
                name="sum",
                semantic_type="size",
                mode_allowlist=["HFT", "MFT", "LFT"],
            )
            def agg_sum(source_col: str) -> pl.Expr:
                return pl.col(source_col).sum()
        """

        def decorator(func: AggregateRawCallable):
            metadata = AggregationMetadata(
                name=name,
                stage="aggregate_then_feature",
                semantic_type=semantic_type,
                mode_allowlist=mode_allowlist,
                required_columns=required_columns or [],
                pit_policy=pit_policy or {"feature_direction": "backward_only"},
                determinism={"required_sort": ["exchange_id", "symbol", "ts_local_us"]},
                impl_ref=f"{func.__module__}.{func.__name__}",
                version="2.0",
                aggregate_raw=func,
                compute_features=None,
            )
            cls._registry[name] = metadata
            return func

        return decorator

    @classmethod
    def register_compute_features(
        cls,
        name: str,
        *,
        semantic_type: str,
        mode_allowlist: list[str],
        required_columns: list[str],
        pit_policy: dict | None = None,
    ):
        """Decorator for Pattern B aggregations (feature_then_aggregate).

        CORRECTED: Explicit decorator for compute_features callables.

        Args:
            name: Aggregation identifier
            semantic_type: Data type category
            mode_allowlist: Research modes where allowed
            required_columns: Columns that must exist in input data
            pit_policy: PIT correctness policy

        Returns:
            Decorator function

        Example:
            @AggregationRegistry.register_compute_features(
                name="microprice_close",
                semantic_type="book_top",
                mode_allowlist=["HFT", "MFT"],
                required_columns=["bids_px_int", "asks_px_int"],
            )
            def microprice(lf: pl.LazyFrame, spec: AggregationSpec) -> pl.LazyFrame:
                return lf.with_columns([...])
        """

        def decorator(func: ComputeFeaturesCallable):
            metadata = AggregationMetadata(
                name=name,
                stage="feature_then_aggregate",
                semantic_type=semantic_type,
                mode_allowlist=mode_allowlist,
                required_columns=required_columns,
                pit_policy=pit_policy or {"feature_direction": "backward_only"},
                determinism={"required_sort": ["exchange_id", "symbol", "ts_local_us"]},
                impl_ref=f"{func.__module__}.{func.__name__}",
                version="2.0",
                aggregate_raw=None,
                compute_features=func,
            )
            cls._registry[name] = metadata
            return func

        return decorator

    @classmethod
    def get(cls, name: str) -> AggregationMetadata:
        """Retrieve aggregation by name.

        Args:
            name: Aggregation identifier

        Returns:
            AggregationMetadata for the aggregation

        Raises:
            ValueError: If aggregation not registered
        """
        if name not in cls._registry:
            raise ValueError(f"Aggregation {name} not registered")
        return cls._registry[name]

    @classmethod
    def validate_for_mode(cls, name: str, research_mode: str) -> None:
        """Validate aggregation is allowed for research mode (HFT/MFT/LFT).

        Args:
            name: Aggregation identifier
            research_mode: Research mode (HFT, MFT, or LFT)

        Raises:
            ValueError: If aggregation not allowed in specified mode
        """
        meta = cls.get(name)
        if research_mode not in meta.mode_allowlist:
            raise ValueError(
                f"{name} not allowed in {research_mode} research mode. "
                f"Allowed modes: {meta.mode_allowlist}"
            )

    @classmethod
    def get_profile(cls, profile: str) -> set[str]:
        """Get aggregation set for profile.

        Args:
            profile: Profile name (e.g., "hft_default", "mft_default")

        Returns:
            Set of aggregation names in the profile

        Raises:
            ValueError: If profile not found
        """
        if profile not in cls._profiles:
            raise ValueError(
                f"Profile {profile} not found. Available profiles: {list(cls._profiles.keys())}"
            )
        return cls._profiles[profile]

    @classmethod
    def list_aggregations(cls) -> list[str]:
        """List all registered aggregations.

        Returns:
            List of aggregation names
        """
        return list(cls._registry.keys())

    @classmethod
    def list_by_stage(cls, stage: str) -> list[str]:
        """List aggregations by stage.

        Args:
            stage: Stage name ("aggregate_then_feature" or "feature_then_aggregate")

        Returns:
            List of aggregation names for the specified stage
        """
        return [name for name, meta in cls._registry.items() if meta.stage == stage]


# Semantic type policies
SEMANTIC_POLICIES = {
    "price": {
        "allowed_aggs": ["last", "mean", "min", "max"],
        "forbidden_aggs": ["sum"],
        "description": "Price fields should not be summed",
    },
    "size": {
        "allowed_aggs": ["sum", "mean", "std", "max"],
        "forbidden_aggs": [],
        "description": "Size/volume fields",
    },
    "notional": {
        "allowed_aggs": ["sum", "mean", "std", "max"],
        "forbidden_aggs": [],
        "description": "Notional/dollar volume fields",
    },
    "event_id": {
        "allowed_aggs": ["count", "nunique", "last"],
        "forbidden_aggs": ["sum", "mean"],
        "description": "Event identifiers should not be summed or averaged",
    },
    "state_variable": {
        "allowed_aggs": ["last", "mean", "diff"],
        "forbidden_aggs": ["sum"],
        "description": "State variables (OI, funding rate) should not be summed",
    },
}
