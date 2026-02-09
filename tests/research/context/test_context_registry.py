"""Tests for context/risk registry and engine foundations."""

from __future__ import annotations

import polars as pl
import pytest

from pointline.research.context import ContextRegistry, apply_context_plugins


def test_context_registry_register_and_get():
    @ContextRegistry.register_context(
        name="test_ctx_register_get",
        required_columns=["x"],
        mode_allowlist=["MFT"],
    )
    def test_ctx_register_get(lf: pl.LazyFrame, spec) -> pl.LazyFrame:
        return lf.with_columns([pl.col("x").alias(f"{spec.name}_x")])

    assert ContextRegistry.exists("test_ctx_register_get")
    meta = ContextRegistry.get("test_ctx_register_get")
    assert meta.name == "test_ctx_register_get"
    assert meta.required_columns == ["x"]
    assert meta.mode_allowlist == ["MFT"]


def test_apply_context_plugins_noop_when_empty():
    frame = pl.LazyFrame({"x": [1, 2, 3]})
    out = apply_context_plugins(frame, [], research_mode="MFT").collect()
    assert out.columns == ["x"]


def test_apply_context_plugins_rejects_unknown_plugin():
    frame = pl.LazyFrame({"x": [1, 2, 3]})
    payload = [
        {
            "name": "ctx",
            "plugin": "missing_plugin",
            "required_columns": ["x"],
            "params": {},
            "mode_allowlist": ["MFT"],
            "pit_policy": {"feature_direction": "backward_only"},
            "determinism_policy": {"required_sort": ["exchange_id", "symbol_id", "ts_local_us"]},
            "version": "2.0",
        }
    ]

    with pytest.raises(ValueError, match="not registered"):
        apply_context_plugins(frame, payload, research_mode="MFT")


def test_apply_context_plugins_validates_mode_allowlist():
    @ContextRegistry.register_context(
        name="test_ctx_mft_only",
        required_columns=["x"],
        mode_allowlist=["MFT"],
    )
    def test_ctx_mft_only(lf: pl.LazyFrame, spec) -> pl.LazyFrame:
        return lf.with_columns([pl.col("x").alias(f"{spec.name}_x")])

    frame = pl.LazyFrame({"x": [1, 2, 3]})
    payload = [
        {
            "name": "ctx",
            "plugin": "test_ctx_mft_only",
            "required_columns": ["x"],
            "params": {},
            "mode_allowlist": ["MFT"],
            "pit_policy": {"feature_direction": "backward_only"},
            "determinism_policy": {"required_sort": ["exchange_id", "symbol_id", "ts_local_us"]},
            "version": "2.0",
        }
    ]

    with pytest.raises(ValueError, match="not allowed in HFT"):
        apply_context_plugins(frame, payload, research_mode="HFT")


def test_apply_context_plugins_validates_required_columns():
    @ContextRegistry.register_context(
        name="test_ctx_requires_col",
        required_columns=["needed_col"],
        mode_allowlist=["MFT"],
    )
    def test_ctx_requires_col(lf: pl.LazyFrame, spec) -> pl.LazyFrame:
        return lf

    frame = pl.LazyFrame({"x": [1, 2, 3]})
    payload = [
        {
            "name": "ctx",
            "plugin": "test_ctx_requires_col",
            "required_columns": ["needed_col"],
            "params": {},
            "mode_allowlist": ["MFT"],
            "pit_policy": {"feature_direction": "backward_only"},
            "determinism_policy": {"required_sort": ["exchange_id", "symbol_id", "ts_local_us"]},
            "version": "2.0",
        }
    ]

    with pytest.raises(ValueError, match="missing required columns"):
        apply_context_plugins(frame, payload, research_mode="MFT")


def test_apply_context_plugins_validates_params_and_applies_defaults():
    @ContextRegistry.register_context(
        name="test_ctx_params_defaults",
        required_columns=["x"],
        mode_allowlist=["MFT"],
        required_params={"scale": "number"},
        optional_params={"offset": "number"},
        default_params={"offset": 1.0},
    )
    def test_ctx_params_defaults(lf: pl.LazyFrame, spec) -> pl.LazyFrame:
        scale = float(spec.params["scale"])
        offset = float(spec.params["offset"])
        return lf.with_columns([(pl.col("x") * scale + offset).alias(f"{spec.name}_scaled")])

    frame = pl.LazyFrame({"x": [1.0, 2.0]})
    payload = [
        {
            "name": "ctx",
            "plugin": "test_ctx_params_defaults",
            "required_columns": ["x"],
            "params": {"scale": 2.0},
            "mode_allowlist": ["MFT"],
            "pit_policy": {"feature_direction": "backward_only"},
            "determinism_policy": {"required_sort": ["exchange_id", "symbol_id", "ts_local_us"]},
            "version": "2.0",
        }
    ]

    out = apply_context_plugins(frame, payload, research_mode="MFT").collect()
    assert "ctx_scaled" in out.columns
    assert out["ctx_scaled"].to_list() == pytest.approx([3.0, 5.0], abs=1e-12)

    bad_payload = [
        {
            "name": "ctx",
            "plugin": "test_ctx_params_defaults",
            "required_columns": ["x"],
            "params": {"scale": "bad"},
            "mode_allowlist": ["MFT"],
            "pit_policy": {"feature_direction": "backward_only"},
            "determinism_policy": {"required_sort": ["exchange_id", "symbol_id", "ts_local_us"]},
            "version": "2.0",
        }
    ]
    with pytest.raises(ValueError, match="expected number"):
        apply_context_plugins(frame, bad_payload, research_mode="MFT")
