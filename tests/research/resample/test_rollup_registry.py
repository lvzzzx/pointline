"""Tests for user-defined feature rollup registry."""

from __future__ import annotations

import polars as pl
import pytest

from pointline.research.resample.rollups import FeatureRollupRegistry


def test_registry_has_starter_rollups():
    assert FeatureRollupRegistry.exists("weighted_close")
    assert FeatureRollupRegistry.exists("trimmed_mean_10pct")
    assert FeatureRollupRegistry.exists("tail_ratio_p95_p50")


def test_validate_params_missing_required():
    with pytest.raises(ValueError, match="missing required params"):
        FeatureRollupRegistry.validate_params("weighted_close", {})


def test_validate_params_unknown_param_rejected():
    with pytest.raises(ValueError, match="unknown params"):
        FeatureRollupRegistry.validate_params(
            "weighted_close",
            {"weight_column": "qty_int", "foo": 1},
        )


def test_validate_for_mode_rejects_disallowed_mode():
    @FeatureRollupRegistry.register_feature_rollup(
        name="test_mft_only_rollup",
        required_params={},
        required_columns=[],
        mode_allowlist=["MFT"],
        semantic_allowlist=["any"],
    )
    def test_mft_only_rollup(feature_col: str, params: dict):
        return pl.col(feature_col).mean()

    with pytest.raises(ValueError, match="not allowed in HFT"):
        FeatureRollupRegistry.validate_for_mode("test_mft_only_rollup", "HFT")


def test_validate_semantic_rejects_disallowed_semantic():
    @FeatureRollupRegistry.register_feature_rollup(
        name="test_price_semantic_rollup",
        required_params={},
        required_columns=[],
        mode_allowlist=["HFT", "MFT"],
        semantic_allowlist=["price"],
    )
    def test_price_semantic_rollup(feature_col: str, params: dict):
        return pl.col(feature_col).mean()

    with pytest.raises(ValueError, match="not allowed for semantic type size"):
        FeatureRollupRegistry.validate_semantic("test_price_semantic_rollup", "size")


def test_validate_required_columns_checks_param_column_references():
    with pytest.raises(ValueError, match="references missing column"):
        FeatureRollupRegistry.validate_required_columns(
            "weighted_close",
            available_columns={"bid_px_int", "ask_px_int"},
            params={"weight_column": "qty_int"},
        )


def test_build_expr_applies_default_params():
    expr = FeatureRollupRegistry.build_expr("tail_ratio_p95_p50", "_x", params=None)
    assert isinstance(expr, pl.Expr)
