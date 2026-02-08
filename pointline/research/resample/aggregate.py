"""Aggregation execution for resample operations.

This module implements aggregation with:
- Pattern A (aggregate_then_feature): Aggregate raw values
- Pattern B (feature_then_aggregate): Compute features on ticks, then aggregate
- Registry-driven validation
- Semantic type enforcement
"""

import polars as pl

from .config import AggregateConfig, AggregationSpec
from .registry import SEMANTIC_POLICIES, AggregationMetadata, AggregationRegistry


def aggregate(
    bucketed_data: pl.LazyFrame,
    config: AggregateConfig,
    *,
    spine: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """Apply aggregations to bucketed data.

    CORRECTED: Uses typed callables from metadata.

    Args:
        bucketed_data: Data with bucket_ts from assign_to_buckets()
            Required columns: bucket_ts, exchange_id, symbol_id, ...
        config: Aggregation configuration
            - by: Grouping columns (typically ["exchange_id", "symbol_id", "bucket_ts"])
            - aggregations: List of aggregation specs
            - mode: Pipeline mode (for logging/observability)
            - research_mode: Research mode for registry validation (HFT/MFT/LFT)
        spine: Optional spine for left join preservation
            If provided, all spine points will be preserved with nulls for missing data

    Returns:
        Aggregated bars (one row per bucket)

    Raises:
        ValueError: If validation fails

    Example:
        >>> bucketed = assign_to_buckets(trades, spine)
        >>> config = AggregateConfig(
        ...     by=["exchange_id", "symbol_id", "bucket_ts"],
        ...     aggregations=[
        ...         AggregationSpec(name="volume", source_column="qty", agg="sum"),
        ...         AggregationSpec(name="trade_count", source_column="trade_id", agg="count"),
        ...     ],
        ...     mode="bar_then_feature",
        ...     research_mode="MFT",
        ... )
        >>> result = aggregate(bucketed, config)
    """
    # Step 1: Pre-execution validation
    _validate_aggregate_config(bucketed_data, config)

    # Step 2: Separate aggregations by stage
    stage_1_aggs: list[tuple[AggregationSpec, AggregationMetadata | None]] = []  # Pattern A
    stage_2_aggs: list[tuple[AggregationSpec, AggregationMetadata]] = []  # Pattern B

    for spec in config.aggregations:
        if spec.agg in AggregationRegistry._registry:
            # Registered custom aggregation
            meta = AggregationRegistry.get(spec.agg)
            AggregationRegistry.validate_for_mode(spec.agg, config.research_mode)

            if meta.stage == "aggregate_then_feature":
                stage_1_aggs.append((spec, meta))
            else:
                stage_2_aggs.append((spec, meta))
        else:
            # Built-in Polars aggregation
            stage_1_aggs.append((spec, None))

    # Step 3: Apply Pattern A (aggregate raw values)
    agg_exprs: list[pl.Expr] = []
    for spec, meta in stage_1_aggs:
        # Use aggregate_raw callable if meta exists, otherwise use built-in
        expr = meta.aggregate_raw(spec.source_column) if meta else _build_builtin_agg_expr(spec)
        agg_exprs.append(expr.alias(spec.name))

    result = bucketed_data.group_by(config.by).agg(agg_exprs)

    # Step 4: Apply Pattern B (compute features then aggregate)
    if stage_2_aggs:
        # Compute features on original data
        feature_data = bucketed_data
        for spec, meta in stage_2_aggs:
            # CORRECTED: Use compute_features callable
            feature_data = meta.compute_features(feature_data, spec)

        # Aggregate computed features
        feature_agg_exprs: list[pl.Expr] = []
        for spec, _meta in stage_2_aggs:
            feature_col = f"_{spec.name}_feature"

            # Standard distribution statistics for Pattern B
            feature_agg_exprs.extend(
                [
                    pl.col(feature_col).mean().alias(f"{spec.name}_mean"),
                    pl.col(feature_col).std().alias(f"{spec.name}_std"),
                    pl.col(feature_col).min().alias(f"{spec.name}_min"),
                    pl.col(feature_col).max().alias(f"{spec.name}_max"),
                ]
            )

        feature_result = feature_data.group_by(config.by).agg(feature_agg_exprs)

        # Join Pattern A and Pattern B results
        result = result.join(
            feature_result,
            on=config.by,
            how="left",
        )

    # Step 5: Left join with spine to preserve all spine points
    if spine is not None:
        result = spine.join(
            result,
            left_on=["exchange_id", "symbol_id", "ts_local_us"],
            right_on=config.by,
            how="left",
        )

    return result


def _validate_aggregate_config(bucketed_data: pl.LazyFrame, config: AggregateConfig) -> None:
    """Pre-execution validation.

    Args:
        bucketed_data: Data with bucket_ts
        config: Aggregation configuration

    Raises:
        ValueError: If validation fails
    """
    # Check grouping columns exist
    for col in config.by:
        if col not in bucketed_data.columns:
            raise ValueError(f"Grouping column {col} not found in data")

    # Validate each aggregation
    for spec in config.aggregations:
        # Check semantic type compatibility
        if spec.semantic_type:
            _validate_semantic_type(spec.agg, spec.semantic_type)

        # Check required columns for custom aggregations
        if spec.agg in AggregationRegistry._registry:
            meta = AggregationRegistry.get(spec.agg)
            for col in meta.required_columns:
                if col not in bucketed_data.columns:
                    raise ValueError(f"Required column {col} not found for {spec.agg}")

        # Check source column exists
        if spec.source_column and spec.source_column not in bucketed_data.columns:
            raise ValueError(f"Source column {spec.source_column} not found")


def _validate_semantic_type(agg: str, semantic_type: str) -> None:
    """Validate aggregation allowed for semantic type.

    Args:
        agg: Aggregation name
        semantic_type: Semantic type (e.g., "price", "size")

    Raises:
        ValueError: If aggregation not allowed for semantic type
    """
    policy = SEMANTIC_POLICIES.get(semantic_type, {})

    if agg in policy.get("forbidden_aggs", []):
        raise ValueError(
            f"Aggregation {agg} not allowed for semantic type {semantic_type}. "
            f"Reason: {policy.get('description', 'Policy violation')}"
        )

    allowed = policy.get("allowed_aggs", [])
    if allowed and agg not in allowed:
        raise ValueError(
            f"Aggregation {agg} not in allowlist for {semantic_type}. " f"Allowed: {allowed}"
        )


def _build_builtin_agg_expr(spec: AggregationSpec) -> pl.Expr:
    """Build Polars expression for built-in aggregations.

    Args:
        spec: Aggregation specification

    Returns:
        Polars expression

    Raises:
        ValueError: If aggregation unknown
    """
    col = pl.col(spec.source_column)

    match spec.agg:
        case "sum":
            return col.sum()
        case "mean":
            return col.mean()
        case "std":
            return col.std()
        case "min":
            return col.min()
        case "max":
            return col.max()
        case "last":
            return col.last()
        case "first":
            return col.first()
        case "count":
            return col.count()
        case "nunique":
            return col.n_unique()
        case _:
            raise ValueError(f"Unknown aggregation: {spec.agg}")
