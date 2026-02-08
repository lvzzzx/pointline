"""Test aggregation registry with typed callables.

This module tests:
- Stage validation (Pattern A vs Pattern B)
- Typed callable registration
- Mode allowlist validation
- Semantic type policy enforcement
"""

import polars as pl
import pytest

from pointline.research.resample import (
    SEMANTIC_POLICIES,
    AggregationMetadata,
    AggregationRegistry,
)


class TestRegistryStageValidation:
    """Test that stage validation enforces exactly one callable."""

    def test_valid_pattern_a(self):
        """Test valid Pattern A registration.

        COMPLETE: Full test specification.
        """
        # Valid Pattern A: aggregate_then_feature with aggregate_raw
        meta = AggregationMetadata(
            name="test_a",
            stage="aggregate_then_feature",
            semantic_type="size",
            mode_allowlist=["MFT"],
            required_columns=[],
            pit_policy={},
            determinism={},
            aggregate_raw=lambda x: pl.col(x).sum(),
            compute_features=None,
        )
        # Should not raise
        assert meta.aggregate_raw is not None
        assert meta.compute_features is None

    def test_valid_pattern_b(self):
        """Test valid Pattern B registration.

        COMPLETE: Full test specification.
        """
        # Valid Pattern B: feature_then_aggregate with compute_features
        meta = AggregationMetadata(
            name="test_b",
            stage="feature_then_aggregate",
            semantic_type="book_top",
            mode_allowlist=["HFT"],
            required_columns=["bids_px_int"],
            pit_policy={},
            determinism={},
            aggregate_raw=None,
            compute_features=lambda lf, spec: lf,
        )
        # Should not raise
        assert meta.aggregate_raw is None
        assert meta.compute_features is not None

    def test_invalid_pattern_a_with_compute_features(self):
        """Test invalid Pattern A with compute_features raises error.

        COMPLETE: Full test specification.
        """
        with pytest.raises(ValueError, match="cannot have compute_features"):
            AggregationMetadata(
                name="test_invalid",
                stage="aggregate_then_feature",
                semantic_type="size",
                mode_allowlist=["MFT"],
                required_columns=[],
                pit_policy={},
                determinism={},
                aggregate_raw=lambda x: pl.col(x).sum(),
                compute_features=lambda lf, spec: lf,  # WRONG!
            )

    def test_invalid_pattern_a_missing_aggregate_raw(self):
        """Test invalid Pattern A without aggregate_raw raises error.

        COMPLETE: Full test specification.
        """
        with pytest.raises(ValueError, match="requires aggregate_raw"):
            AggregationMetadata(
                name="test_invalid",
                stage="aggregate_then_feature",
                semantic_type="size",
                mode_allowlist=["MFT"],
                required_columns=[],
                pit_policy={},
                determinism={},
                aggregate_raw=None,  # WRONG!
                compute_features=None,
            )

    def test_invalid_pattern_b_with_aggregate_raw(self):
        """Test invalid Pattern B with aggregate_raw raises error.

        COMPLETE: Full test specification.
        """
        with pytest.raises(ValueError, match="cannot have aggregate_raw"):
            AggregationMetadata(
                name="test_invalid",
                stage="feature_then_aggregate",
                semantic_type="book_top",
                mode_allowlist=["HFT"],
                required_columns=[],
                pit_policy={},
                determinism={},
                aggregate_raw=lambda x: pl.col(x).sum(),  # WRONG!
                compute_features=lambda lf, spec: lf,
            )

    def test_invalid_pattern_b_missing_compute_features(self):
        """Test invalid Pattern B without compute_features raises error.

        COMPLETE: Full test specification.
        """
        with pytest.raises(ValueError, match="requires compute_features"):
            AggregationMetadata(
                name="test_invalid",
                stage="feature_then_aggregate",
                semantic_type="book_top",
                mode_allowlist=["HFT"],
                required_columns=[],
                pit_policy={},
                determinism={},
                aggregate_raw=None,
                compute_features=None,  # WRONG!
            )


class TestRegistryRegistration:
    """Test aggregation registration with correct decorators."""

    def test_builtins_registered(self):
        """Test built-in aggregations are registered.

        COMPLETE: Full test specification.
        """
        expected = ["sum", "mean", "std", "min", "max", "last", "first", "count", "nunique"]

        for name in expected:
            assert name in AggregationRegistry._registry, f"{name} not registered"

    def test_sum_metadata(self):
        """Test sum aggregation metadata.

        COMPLETE: Full test specification.
        """
        sum_meta = AggregationRegistry.get("sum")
        assert sum_meta.stage == "aggregate_then_feature"
        assert sum_meta.aggregate_raw is not None
        assert sum_meta.compute_features is None
        assert sum_meta.semantic_type == "size"
        assert "HFT" in sum_meta.mode_allowlist
        assert "MFT" in sum_meta.mode_allowlist
        assert "LFT" in sum_meta.mode_allowlist

    def test_mean_metadata(self):
        """Test mean aggregation metadata.

        COMPLETE: Full test specification.
        """
        mean_meta = AggregationRegistry.get("mean")
        assert mean_meta.stage == "aggregate_then_feature"
        assert mean_meta.aggregate_raw is not None
        assert mean_meta.compute_features is None

    def test_get_nonexistent(self):
        """Test getting non-existent aggregation raises error.

        COMPLETE: Full test specification.
        """
        with pytest.raises(ValueError, match="not registered"):
            AggregationRegistry.get("nonexistent_agg")

    def test_list_aggregations(self):
        """Test listing all aggregations.

        COMPLETE: Full test specification.
        """
        aggs = AggregationRegistry.list_aggregations()
        assert isinstance(aggs, list)
        assert "sum" in aggs
        assert "mean" in aggs
        assert len(aggs) >= 9  # At least the built-ins

    def test_list_by_stage_pattern_a(self):
        """Test listing aggregations by stage (Pattern A).

        COMPLETE: Full test specification.
        """
        pattern_a = AggregationRegistry.list_by_stage("aggregate_then_feature")
        assert "sum" in pattern_a
        assert "mean" in pattern_a
        assert all(
            AggregationRegistry.get(name).stage == "aggregate_then_feature" for name in pattern_a
        )


class TestRegistryModeValidation:
    """Test mode allowlist validation."""

    def test_validate_for_mode_allowed(self):
        """Test validation passes for allowed mode.

        COMPLETE: Full test specification.
        """
        # sum is allowed in HFT, MFT, LFT
        AggregationRegistry.validate_for_mode("sum", "HFT")
        AggregationRegistry.validate_for_mode("sum", "MFT")
        AggregationRegistry.validate_for_mode("sum", "LFT")
        # Should not raise

    def test_validate_for_mode_disallowed(self):
        """Test validation fails for disallowed mode.

        COMPLETE: Full test specification.
        """

        # Register a custom aggregation with restricted modes
        @AggregationRegistry.register_aggregate_raw(
            name="test_hft_only",
            semantic_type="size",
            mode_allowlist=["HFT"],  # Only HFT
        )
        def test_hft_agg(source_col: str) -> pl.Expr:
            return pl.col(source_col).sum()

        # Should pass for HFT
        AggregationRegistry.validate_for_mode("test_hft_only", "HFT")

        # Should fail for MFT and LFT
        with pytest.raises(ValueError, match="not allowed in MFT"):
            AggregationRegistry.validate_for_mode("test_hft_only", "MFT")

        with pytest.raises(ValueError, match="not allowed in LFT"):
            AggregationRegistry.validate_for_mode("test_hft_only", "LFT")


class TestSemanticPolicyEnforcement:
    """Test semantic type policy enforcement."""

    def test_semantic_policies_defined(self):
        """Test semantic policies are defined.

        COMPLETE: Full test specification.
        """
        expected_types = ["price", "size", "notional", "event_id", "state_variable"]

        for semantic_type in expected_types:
            assert semantic_type in SEMANTIC_POLICIES

    def test_price_policy(self):
        """Test price semantic type policy.

        COMPLETE: Full test specification.
        """
        price_policy = SEMANTIC_POLICIES["price"]

        # sum not allowed for price
        assert "sum" in price_policy["forbidden_aggs"]

        # mean allowed for price
        assert "mean" in price_policy["allowed_aggs"]
        assert "last" in price_policy["allowed_aggs"]

    def test_size_policy(self):
        """Test size semantic type policy.

        COMPLETE: Full test specification.
        """
        size_policy = SEMANTIC_POLICIES["size"]

        # sum allowed for size
        assert "sum" in size_policy["allowed_aggs"]
        assert "mean" in size_policy["allowed_aggs"]
        assert "std" in size_policy["allowed_aggs"]

        # No forbidden aggs for size
        assert len(size_policy["forbidden_aggs"]) == 0

    def test_event_id_policy(self):
        """Test event_id semantic type policy.

        COMPLETE: Full test specification.
        """
        event_id_policy = SEMANTIC_POLICIES["event_id"]

        # count allowed for event_id
        assert "count" in event_id_policy["allowed_aggs"]
        assert "nunique" in event_id_policy["allowed_aggs"]

        # sum and mean not allowed
        assert "sum" in event_id_policy["forbidden_aggs"]
        assert "mean" in event_id_policy["forbidden_aggs"]

    def test_state_variable_policy(self):
        """Test state_variable semantic type policy.

        COMPLETE: Full test specification.
        """
        state_policy = SEMANTIC_POLICIES["state_variable"]

        # last allowed for state variables
        assert "last" in state_policy["allowed_aggs"]

        # sum not allowed
        assert "sum" in state_policy["forbidden_aggs"]


class TestRegistryProfiles:
    """Test registry profiles for different research workflows."""

    def test_profiles_defined(self):
        """Test profiles are defined.

        COMPLETE: Full test specification.
        """
        expected_profiles = ["hft_default", "mft_default", "lft_default"]

        for profile in expected_profiles:
            assert profile in AggregationRegistry._profiles

    def test_get_profile(self):
        """Test getting profile aggregations.

        COMPLETE: Full test specification.
        """
        hft_profile = AggregationRegistry.get_profile("hft_default")

        assert isinstance(hft_profile, set)
        assert "sum" in hft_profile
        assert "mean" in hft_profile
        assert "count" in hft_profile

    def test_get_nonexistent_profile(self):
        """Test getting non-existent profile raises error.

        COMPLETE: Full test specification.
        """
        with pytest.raises(ValueError, match="Profile.*not found"):
            AggregationRegistry.get_profile("nonexistent_profile")

    def test_hft_profile_includes_advanced(self):
        """Test HFT profile includes advanced aggregations.

        COMPLETE: Full test specification.
        """
        hft_profile = AggregationRegistry.get_profile("hft_default")

        # HFT should have microstructure aggregations
        # (These will be registered in Phase 3, but profile is defined now)
        assert (
            "microprice_close" in hft_profile
            or "microprice_close" not in AggregationRegistry._registry
        )
        assert "ofi_cont" in hft_profile or "ofi_cont" not in AggregationRegistry._registry

    def test_lft_profile_basic_only(self):
        """Test LFT profile has only basic aggregations.

        COMPLETE: Full test specification.
        """
        lft_profile = AggregationRegistry.get_profile("lft_default")

        # LFT should have basic aggregations only
        assert "sum" in lft_profile
        assert "mean" in lft_profile
        assert "last" in lft_profile
        assert "count" in lft_profile

        # LFT should not have advanced microstructure aggs
        assert "microprice_close" not in lft_profile
        assert "ofi_cont" not in lft_profile


class TestAggregateRawCallable:
    """Test Pattern A aggregations execute correctly."""

    def test_sum_callable(self):
        """Test sum aggregation callable.

        COMPLETE: Full test specification.
        """
        sum_meta = AggregationRegistry.get("sum")
        assert sum_meta.aggregate_raw is not None

        # Create test data
        df = pl.DataFrame({"qty": [10, 20, 30]})

        # Apply aggregation
        result = df.select([sum_meta.aggregate_raw("qty").alias("total_qty")])

        assert result["total_qty"][0] == 60

    def test_mean_callable(self):
        """Test mean aggregation callable.

        COMPLETE: Full test specification.
        """
        mean_meta = AggregationRegistry.get("mean")
        assert mean_meta.aggregate_raw is not None

        df = pl.DataFrame({"price": [100, 110, 90]})

        result = df.select([mean_meta.aggregate_raw("price").alias("avg_price")])

        assert result["avg_price"][0] == 100

    def test_count_callable(self):
        """Test count aggregation callable.

        COMPLETE: Full test specification.
        """
        count_meta = AggregationRegistry.get("count")
        assert count_meta.aggregate_raw is not None

        df = pl.DataFrame({"trade_id": [1, 2, 3, 4, 5]})

        result = df.select([count_meta.aggregate_raw("trade_id").alias("trade_count")])

        assert result["trade_count"][0] == 5
