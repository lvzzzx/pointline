"""Test aggregation execution with Pattern A and Pattern B.

This module tests:
- Pattern A (aggregate raw values) with correct callable
- Pattern B (compute features on ticks) with correct callable
- Registry validation uses research_mode
- Semantic type validation
- Spine preservation
"""

import polars as pl
import pytest

from pointline.research.resample import (
    AggregateConfig,
    AggregationRegistry,
    AggregationSpec,
    aggregate,
)


class TestAggregatePatternA:
    """Test Pattern A: aggregate raw values with correct callable."""

    def test_aggregate_pattern_a_sum(self):
        """Test Pattern A: aggregate raw values with correct callable.

        COMPLETE: Full test specification.
        """
        # Bucketed data
        data = pl.LazyFrame(
            {
                "exchange_id": [1, 1, 1, 1],
                "symbol_id": [12345, 12345, 12345, 12345],
                "bucket_ts": [60_000_000, 60_000_000, 120_000_000, 120_000_000],
                "qty": [10, 20, 15, 25],
            }
        )

        config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(name="volume", source_column="qty", agg="sum"),
            ],
            mode="bar_then_feature",
            research_mode="MFT",
        )

        result = aggregate(data, config).collect()

        # First bucket: 10 + 20 = 30
        assert result.filter(pl.col("bucket_ts") == 60_000_000)["volume"][0] == 30
        # Second bucket: 15 + 25 = 40
        assert result.filter(pl.col("bucket_ts") == 120_000_000)["volume"][0] == 40

    def test_aggregate_pattern_a_multiple_aggs(self):
        """Test Pattern A with multiple aggregations.

        COMPLETE: Full test specification.
        """
        data = pl.LazyFrame(
            {
                "exchange_id": [1, 1, 1],
                "symbol_id": [12345, 12345, 12345],
                "bucket_ts": [60_000_000, 60_000_000, 60_000_000],
                "qty": [10, 20, 30],
                "price": [100, 110, 90],
            }
        )

        config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(name="volume", source_column="qty", agg="sum"),
                AggregationSpec(name="avg_price", source_column="price", agg="mean"),
                AggregationSpec(name="min_price", source_column="price", agg="min"),
                AggregationSpec(name="max_price", source_column="price", agg="max"),
            ],
            mode="bar_then_feature",
            research_mode="MFT",
        )

        result = aggregate(data, config).collect()

        assert result["volume"][0] == 60  # 10 + 20 + 30
        assert result["avg_price"][0] == 100  # (100 + 110 + 90) / 3
        assert result["min_price"][0] == 90
        assert result["max_price"][0] == 110


class TestAggregatePatternB:
    """Test Pattern B uses compute_features callable correctly."""

    def test_aggregate_pattern_b_with_typed_callable(self):
        """Test Pattern B uses compute_features callable correctly.

        COMPLETE: Full test specification.
        """

        # Register test aggregation
        @AggregationRegistry.register_compute_features(
            name="test_feature_dist",
            semantic_type="test",
            mode_allowlist=["MFT"],
            required_columns=["value"],
        )
        def test_feature(lf: pl.LazyFrame, spec: AggregationSpec) -> pl.LazyFrame:
            # Compute feature on each row
            return lf.with_columns([(pl.col("value") * 2).alias("_test_feature_dist_feature")])

        # Bucketed data
        data = pl.LazyFrame(
            {
                "exchange_id": [1, 1, 1],
                "symbol_id": [12345, 12345, 12345],
                "bucket_ts": [60_000_000, 60_000_000, 60_000_000],
                "value": [10, 20, 30],
            }
        )

        config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(
                    name="test_feature_dist",
                    source_column="value",
                    agg="test_feature_dist",
                ),
            ],
            mode="tick_then_bar",
            research_mode="MFT",
        )

        result = aggregate(data, config).collect()

        # Should have distribution stats
        assert "test_feature_dist_mean" in result.columns
        assert "test_feature_dist_std" in result.columns

        # Mean of (10*2, 20*2, 30*2) = mean of (20, 40, 60) = 40
        assert result["test_feature_dist_mean"][0] == 40


class TestAggregateSemanticValidation:
    """Test semantic type policy enforcement."""

    def test_aggregate_semantic_validation_forbid_sum_on_price(self):
        """Test semantic type policy enforcement.

        COMPLETE: Full test specification.
        """
        data = pl.LazyFrame(
            {
                "exchange_id": [1],
                "symbol_id": [12345],
                "bucket_ts": [60_000_000],
                "price": [50000],
            }
        )

        # sum not allowed for price semantic type
        config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(
                    name="price_sum",
                    source_column="price",
                    agg="sum",
                    semantic_type="price",
                ),
            ],
            mode="bar_then_feature",
            research_mode="MFT",
        )

        with pytest.raises(ValueError, match="not allowed for semantic type"):
            aggregate(data, config)

    def test_aggregate_semantic_validation_allow_mean_on_price(self):
        """Test semantic validation allows mean on price.

        COMPLETE: Full test specification.
        """
        data = pl.LazyFrame(
            {
                "exchange_id": [1, 1],
                "symbol_id": [12345, 12345],
                "bucket_ts": [60_000_000, 60_000_000],
                "price": [100, 110],
            }
        )

        # mean allowed for price semantic type
        config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(
                    name="avg_price",
                    source_column="price",
                    agg="mean",
                    semantic_type="price",
                ),
            ],
            mode="bar_then_feature",
            research_mode="MFT",
        )

        result = aggregate(data, config).collect()
        assert result["avg_price"][0] == 105


class TestAggregateSpinePreservation:
    """Test spine preservation via left join."""

    def test_aggregate_spine_preservation(self):
        """Test spine preservation via left join.

        COMPLETE: Full test specification.
        """
        # Spine with 3 buckets
        spine = pl.LazyFrame(
            {
                "ts_local_us": [60_000_000, 120_000_000, 180_000_000],
                "exchange_id": [1, 1, 1],
                "symbol_id": [12345, 12345, 12345],
            }
        )

        # Data only in first and third buckets (second is empty)
        data = pl.LazyFrame(
            {
                "exchange_id": [1, 1],
                "symbol_id": [12345, 12345],
                "bucket_ts": [60_000_000, 180_000_000],
                "qty": [10, 20],
            }
        )

        config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(name="volume", source_column="qty", agg="sum"),
            ],
            mode="bar_then_feature",
            research_mode="MFT",
        )

        result = aggregate(data, config, spine=spine).collect()

        # Should have 3 rows (all spine points preserved)
        assert len(result) == 3

        # Second bucket should have null volume
        assert result.filter(pl.col("ts_local_us") == 120_000_000)["volume"][0] is None


class TestAggregateModeValidation:
    """Test mode allowlist validation."""

    def test_mode_validation_passes(self):
        """Test mode validation passes for allowed mode.

        COMPLETE: Full test specification.
        """
        data = pl.LazyFrame(
            {
                "exchange_id": [1],
                "symbol_id": [12345],
                "bucket_ts": [60_000_000],
                "qty": [10],
            }
        )

        config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(name="volume", source_column="qty", agg="sum"),
            ],
            mode="bar_then_feature",
            research_mode="MFT",  # sum is allowed in MFT
        )

        # Should not raise
        result = aggregate(data, config).collect()
        assert result["volume"][0] == 10

    def test_mode_validation_fails_for_restricted_agg(self):
        """Test mode validation fails for restricted aggregation.

        COMPLETE: Full test specification.
        """

        # Register HFT-only aggregation
        @AggregationRegistry.register_aggregate_raw(
            name="test_hft_only_agg",
            semantic_type="size",
            mode_allowlist=["HFT"],  # Only HFT
        )
        def hft_agg(source_col: str) -> pl.Expr:
            return pl.col(source_col).sum()

        data = pl.LazyFrame(
            {
                "exchange_id": [1],
                "symbol_id": [12345],
                "bucket_ts": [60_000_000],
                "qty": [10],
            }
        )

        config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(name="test_hft", source_column="qty", agg="test_hft_only_agg"),
            ],
            mode="bar_then_feature",
            research_mode="MFT",  # MFT not allowed!
        )

        with pytest.raises(ValueError, match="not allowed in MFT"):
            aggregate(data, config)


class TestAggregateValidation:
    """Test validation and error handling."""

    def test_missing_grouping_column(self):
        """Test validation fails when grouping column missing.

        COMPLETE: Full test specification.
        """
        data = pl.LazyFrame(
            {
                "exchange_id": [1],
                "symbol_id": [12345],
                # Missing bucket_ts!
                "qty": [10],
            }
        )

        config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(name="volume", source_column="qty", agg="sum"),
            ],
            mode="bar_then_feature",
            research_mode="MFT",
        )

        with pytest.raises(ValueError, match="Grouping column.*not found"):
            aggregate(data, config)

    def test_missing_source_column(self):
        """Test validation fails when source column missing.

        COMPLETE: Full test specification.
        """
        data = pl.LazyFrame(
            {
                "exchange_id": [1],
                "symbol_id": [12345],
                "bucket_ts": [60_000_000],
                # Missing qty!
            }
        )

        config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(name="volume", source_column="qty", agg="sum"),
            ],
            mode="bar_then_feature",
            research_mode="MFT",
        )

        with pytest.raises(ValueError, match="Source column.*not found"):
            aggregate(data, config)
