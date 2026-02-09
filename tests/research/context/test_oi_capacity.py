"""Tests for the built-in oi_capacity context/risk plugin."""

from __future__ import annotations

import polars as pl
import pytest

from pointline.research.context import apply_context_plugins


def _capacity_spec(name: str = "cap", params: dict | None = None) -> list[dict]:
    return [
        {
            "name": name,
            "plugin": "oi_capacity",
            "required_columns": ["exchange_id", "symbol_id", "ts_local_us"],
            "params": params
            or {
                "oi_col": "oi_last",
                "price_col": "mark_px",
                "base_notional": 1_000.0,
                "lookback_bars": 2,
                "min_ratio": 0.9,
                "clip_min": 0.5,
                "clip_max": 1.5,
            },
            "mode_allowlist": ["MFT", "LFT"],
            "pit_policy": {"feature_direction": "backward_only"},
            "determinism_policy": {"required_sort": ["exchange_id", "symbol_id", "ts_local_us"]},
            "impl_ref": "pointline.research.context.plugins.oi_capacity.oi_capacity",
            "version": "2.0",
        }
    ]


def test_oi_capacity_emits_expected_columns_and_values():
    frame = pl.LazyFrame(
        {
            "exchange_id": [1, 1, 1],
            "symbol_id": [11, 11, 11],
            "ts_local_us": [10, 20, 30],
            "oi_last": [100.0, 120.0, 80.0],
            "mark_px": [10.0, 12.0, 9.0],
        }
    )
    out = apply_context_plugins(frame, _capacity_spec(), research_mode="MFT").collect()

    assert "cap_oi_notional" in out.columns
    assert "cap_oi_level_ratio" in out.columns
    assert "cap_capacity_ok" in out.columns
    assert "cap_capacity_mult" in out.columns
    assert "cap_max_trade_notional" in out.columns

    assert out["cap_oi_notional"].to_list() == pytest.approx([1000.0, 1440.0, 720.0], abs=1e-12)
    assert out["cap_oi_level_ratio"].to_list() == pytest.approx(
        [1.0, 120.0 / 110.0, 80.0 / 100.0], abs=1e-12
    )
    assert out["cap_capacity_ok"].to_list() == [True, True, False]
    assert out["cap_capacity_mult"].to_list() == pytest.approx([1.0, 120.0 / 110.0, 0.8], abs=1e-12)
    assert out["cap_max_trade_notional"].to_list() == pytest.approx(
        [1000.0, 1000.0 * (120.0 / 110.0), 800.0], abs=1e-12
    )


def test_oi_capacity_zero_denominator_falls_back_to_clip_min():
    frame = pl.LazyFrame(
        {
            "exchange_id": [1, 1],
            "symbol_id": [11, 11],
            "ts_local_us": [10, 20],
            "oi_last": [0.0, 0.0],
        }
    )
    spec = _capacity_spec(
        params={
            "oi_col": "oi_last",
            "base_notional": 500.0,
            "lookback_bars": 2,
            "min_ratio": 0.9,
            "clip_min": 0.25,
            "clip_max": 2.0,
            "epsilon": 1e-9,
        }
    )
    out = apply_context_plugins(frame, spec, research_mode="MFT").collect()

    assert "cap_oi_notional" not in out.columns
    assert out["cap_oi_level_ratio"].null_count() == out.height
    assert out["cap_capacity_ok"].to_list() == [False, False]
    assert out["cap_capacity_mult"].to_list() == pytest.approx([0.25, 0.25], abs=1e-12)
    assert out["cap_max_trade_notional"].to_list() == pytest.approx([125.0, 125.0], abs=1e-12)


def test_oi_capacity_rejects_invalid_clip_bounds():
    frame = pl.LazyFrame(
        {
            "exchange_id": [1],
            "symbol_id": [11],
            "ts_local_us": [10],
            "oi_last": [100.0],
        }
    )
    spec = _capacity_spec(
        params={
            "oi_col": "oi_last",
            "base_notional": 500.0,
            "clip_min": 2.0,
            "clip_max": 1.0,
        }
    )

    with pytest.raises(ValueError, match="clip_max >= clip_min"):
        apply_context_plugins(frame, spec, research_mode="MFT")
