"""Tests for the v2 contract-first research pipeline."""

from __future__ import annotations

import importlib
from copy import deepcopy

import polars as pl
import pytest

from pointline.research import (
    compile_request,
    pipeline,
    validate_quant_research_input_v2,
    validate_quant_research_output_v2,
)
from pointline.research.contracts import SchemaValidationError
from pointline.research.resample import AggregationRegistry
from pointline.research.resample.rollups import FeatureRollupRegistry

pipeline_module = importlib.import_module("pointline.research.pipeline")


def _operator_contract(
    agg: str,
    *,
    source_column: str,
    name: str | None = None,
    feature_rollups: list[str] | None = None,
    feature_rollup_params: dict | None = None,
) -> dict:
    meta = AggregationRegistry.get(agg)
    op_name = name or agg
    contract = {
        "name": op_name,
        "output_name": op_name,
        "stage": meta.stage,
        "agg": agg,
        "source_column": source_column,
        "required_columns": meta.required_columns,
        "mode_allowlist": meta.mode_allowlist,
        "pit_policy": meta.pit_policy,
        "determinism_policy": {**meta.determinism, "stateful": False},
        "impl_ref": meta.impl_ref,
        "version": meta.version,
    }
    if feature_rollups is not None:
        contract["feature_rollups"] = feature_rollups
    if feature_rollup_params is not None:
        contract["feature_rollup_params"] = feature_rollup_params
    return contract


def _base_request(mode: str) -> dict:
    return {
        "schema_version": "2.0",
        "request_id": f"req-{mode}",
        "mode": mode,
        "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
        "sources": [
            {
                "name": "trades",
                "inline_rows": [
                    {
                        "ts_local_us": 10_000_000,
                        "exchange_id": 1,
                        "symbol_id": 12345,
                        "qty_int": 100,
                        "px_int": 50000,
                        "side": 0,
                        "file_id": 1,
                        "file_line_number": 10,
                    },
                    {
                        "ts_local_us": 70_000_000,
                        "exchange_id": 1,
                        "symbol_id": 12345,
                        "qty_int": 200,
                        "px_int": 50010,
                        "side": 1,
                        "file_id": 1,
                        "file_line_number": 11,
                    },
                ],
            }
        ],
        "spine": {
            "type": "clock",
            "step_ms": 60_000,
            "start_ts_us": 0,
            "end_ts_us": 120_000_000,
        },
        "operators": [_operator_contract("sum", source_column="qty_int", name="volume")],
        "labels": [
            {
                "name": "future_volume_1",
                "source_column": "volume",
                "direction": "forward",
                "horizon_bars": 1,
            }
        ],
        "evaluation": {"metrics": ["row_count", "non_null_ratio"], "split_method": "fixed"},
        "constraints": {
            "pit_timeline": "ts_local_us",
            "forbid_lookahead": True,
            "cost_model": {"fees_bps": 1.0, "slippage_bps": 2.0},
        },
        "artifacts": {"include_artifacts": False},
    }


def _context_contract(
    plugin: str = "oi_capacity",
    *,
    name: str | None = None,
    params: dict | None = None,
) -> dict:
    return {
        "name": name or plugin,
        "plugin": plugin,
        "required_columns": ["oi_last"],
        "params": params or {"oi_col": "oi_last", "base_notional": 100_000.0, "lookback_bars": 20},
        "mode_allowlist": ["MFT", "LFT"],
        "pit_policy": {"feature_direction": "backward_only"},
        "determinism_policy": {"required_sort": ["exchange_id", "symbol_id", "ts_local_us"]},
        "impl_ref": "pointline.research.context.plugins.oi_capacity",
        "version": "2.0",
    }


def test_v2_contract_validation_and_pipeline_output_validation():
    request = _base_request("bar_then_feature")
    validate_quant_research_input_v2(request)

    output = pipeline(request)
    validate_quant_research_output_v2(output)

    assert output["schema_version"] == "2.0"
    assert output["decision"]["status"] in {"go", "revise", "reject"}


def test_compile_request_produces_deterministic_hash():
    request = _base_request("bar_then_feature")
    compiled_a = compile_request(deepcopy(request))
    compiled_b = compile_request(deepcopy(request))

    assert compiled_a["config_hash"] == compiled_b["config_hash"]


def test_compile_request_hash_changes_when_context_risk_changes():
    request_a = _base_request("bar_then_feature")
    request_b = _base_request("bar_then_feature")
    request_b["context_risk"] = [
        _context_contract(
            params={"oi_col": "oi_last", "base_notional": 100_000.0, "lookback_bars": 96}
        )
    ]

    compiled_a = compile_request(deepcopy(request_a))
    compiled_b = compile_request(deepcopy(request_b))

    assert compiled_a["config_hash"] != compiled_b["config_hash"]


def test_pipeline_bar_then_feature_end_to_end():
    request = _base_request("bar_then_feature")
    output = pipeline(request)

    assert output["run"]["mode"] == "bar_then_feature"
    assert output["results"]["row_count"] >= 1
    assert "volume" in output["results"]["columns"]


def test_pipeline_tick_then_bar_end_to_end():
    request = _base_request("tick_then_bar")
    request["sources"][0]["name"] = "quotes"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50000,
            "ask_px_int": 50005,
            "file_id": 1,
            "file_line_number": 1,
        },
        {
            "ts_local_us": 20_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50010,
            "ask_px_int": 50015,
            "file_id": 1,
            "file_line_number": 2,
        },
    ]
    request["operators"] = [
        _operator_contract(
            "spread_distribution",
            source_column="bid_px_int",
            name="spread_stats",
        )
    ]
    request["labels"] = []

    output = pipeline(request)
    assert output["run"]["mode"] == "tick_then_bar"
    assert "spread_stats_mean" in output["results"]["columns"]


def test_pipeline_tick_then_bar_custom_rollups_sum_last_close():
    request = _base_request("tick_then_bar")
    request["sources"][0]["name"] = "quotes"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50000,
            "ask_px_int": 50005,
            "file_id": 1,
            "file_line_number": 1,
        },
        {
            "ts_local_us": 20_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50010,
            "ask_px_int": 50015,
            "file_id": 1,
            "file_line_number": 2,
        },
    ]
    request["operators"] = [
        _operator_contract(
            "spread_distribution",
            source_column="bid_px_int",
            name="spread_stats",
            feature_rollups=["sum", "last", "close"],
        )
    ]
    request["labels"] = []

    output = pipeline(request)
    cols = output["results"]["columns"]
    assert "spread_stats_sum" in cols
    assert "spread_stats_last" in cols
    assert "spread_stats_close" in cols
    assert "spread_stats_mean" not in cols


def test_pipeline_tick_then_bar_custom_rollup_weighted_close():
    request = _base_request("tick_then_bar")
    request["sources"][0]["name"] = "quotes"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50000,
            "ask_px_int": 50005,
            "file_id": 1,
            "file_line_number": 1,
        },
        {
            "ts_local_us": 20_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50010,
            "ask_px_int": 50015,
            "file_id": 1,
            "file_line_number": 2,
        },
    ]
    request["operators"] = [
        _operator_contract(
            "spread_distribution",
            source_column="bid_px_int",
            name="spread_stats",
            feature_rollups=["weighted_close"],
            feature_rollup_params={"weighted_close": {"weight_column": "ask_px_int"}},
        )
    ]
    request["labels"] = []

    output = pipeline(request)
    assert "spread_stats_weighted_close" in output["results"]["columns"]


def test_pipeline_tick_then_bar_custom_rollup_ofi_imbalance():
    request = _base_request("tick_then_bar")
    request["sources"][0]["name"] = "books"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bids_px_int": [100],
            "asks_px_int": [101],
            "bids_sz_int": [10],
            "asks_sz_int": [12],
            "file_id": 1,
            "file_line_number": 1,
        },
        {
            "ts_local_us": 20_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bids_px_int": [101],
            "asks_px_int": [102],
            "bids_sz_int": [8],
            "asks_sz_int": [9],
            "file_id": 1,
            "file_line_number": 2,
        },
        {
            "ts_local_us": 30_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bids_px_int": [101],
            "asks_px_int": [101],
            "bids_sz_int": [6],
            "asks_sz_int": [15],
            "file_id": 1,
            "file_line_number": 3,
        },
    ]
    request["operators"] = [
        _operator_contract(
            "ofi_cont",
            source_column="bids_sz_int",
            name="ofi_stats",
            feature_rollups=["sum", "ofi_imbalance"],
            feature_rollup_params={"ofi_imbalance": {"epsilon": 1e-9}},
        )
    ]
    request["labels"] = []

    output = pipeline(request)
    assert "ofi_stats_sum" in output["results"]["columns"]
    assert "ofi_stats_ofi_imbalance" in output["results"]["columns"]


def test_pipeline_bar_then_feature_derivatives_funding_features():
    request = _base_request("bar_then_feature")
    request["sources"][0]["name"] = "derivative_ticker"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "funding_rate": 0.00010,
            "predicted_funding_rate": 0.00011,
            "open_interest": 1_000_000.0,
            "file_id": 1,
            "file_line_number": 1,
        },
        {
            "ts_local_us": 20_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "funding_rate": 0.00020,
            "predicted_funding_rate": 0.00018,
            "open_interest": 1_005_000.0,
            "file_id": 1,
            "file_line_number": 2,
        },
        {
            "ts_local_us": 30_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "funding_rate": 0.00015,
            "predicted_funding_rate": 0.00014,
            "open_interest": 1_008_000.0,
            "file_id": 1,
            "file_line_number": 3,
        },
    ]
    request["operators"] = [
        _operator_contract("funding_close", source_column="funding_rate"),
        _operator_contract("funding_step", source_column="funding_rate"),
        _operator_contract("funding_carry_8h_per_hour", source_column="funding_rate"),
        _operator_contract("funding_surprise", source_column="funding_rate"),
        _operator_contract("funding_pressure", source_column="funding_rate"),
    ]
    request["labels"] = []

    output = pipeline(request)
    cols = output["results"]["columns"]
    assert "funding_close" in cols
    assert "funding_step" in cols
    assert "funding_carry_8h_per_hour" in cols
    assert "funding_surprise" in cols
    assert "funding_pressure" in cols


def test_pipeline_bar_then_feature_derivatives_oi_features():
    request = _base_request("bar_then_feature")
    request["sources"][0]["name"] = "derivative_ticker"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "open_interest": 1_000_000.0,
            "file_id": 1,
            "file_line_number": 1,
        },
        {
            "ts_local_us": 20_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "open_interest": 1_005_000.0,
            "file_id": 1,
            "file_line_number": 2,
        },
        {
            "ts_local_us": 30_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "open_interest": 1_008_000.0,
            "file_id": 1,
            "file_line_number": 3,
        },
    ]
    request["operators"] = [
        _operator_contract("oi_open", source_column="open_interest"),
        _operator_contract("oi_high", source_column="open_interest"),
        _operator_contract("oi_low", source_column="open_interest"),
        _operator_contract("oi_range", source_column="open_interest"),
        _operator_contract("oi_change", source_column="open_interest"),
        _operator_contract("oi_last", source_column="open_interest"),
        _operator_contract("oi_pct_change", source_column="open_interest"),
        _operator_contract("oi_pressure", source_column="open_interest"),
    ]
    request["labels"] = []

    output = pipeline(request)
    cols = output["results"]["columns"]
    assert "oi_open" in cols
    assert "oi_high" in cols
    assert "oi_low" in cols
    assert "oi_range" in cols
    assert "oi_change" in cols
    assert "oi_last" in cols
    assert "oi_pct_change" in cols
    assert "oi_pressure" in cols


def test_pipeline_tick_then_bar_rejects_unknown_custom_rollup():
    request = _base_request("tick_then_bar")
    request["sources"][0]["name"] = "quotes"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50000,
            "ask_px_int": 50005,
            "file_id": 1,
            "file_line_number": 1,
        },
    ]
    request["operators"] = [
        _operator_contract(
            "spread_distribution",
            source_column="bid_px_int",
            name="spread_stats",
            feature_rollups=["my_unknown_rollup"],
        )
    ]
    request["labels"] = []

    with pytest.raises(ValueError, match="Feature rollup not registered"):
        pipeline(request)


def test_pipeline_tick_then_bar_rejects_missing_custom_rollup_params():
    request = _base_request("tick_then_bar")
    request["sources"][0]["name"] = "quotes"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50000,
            "ask_px_int": 50005,
            "file_id": 1,
            "file_line_number": 1,
        },
    ]
    request["operators"] = [
        _operator_contract(
            "spread_distribution",
            source_column="bid_px_int",
            name="spread_stats",
            feature_rollups=["weighted_close"],
        )
    ]
    request["labels"] = []

    with pytest.raises(ValueError, match="missing required params"):
        pipeline(request)


def test_pipeline_tick_then_bar_rejects_params_for_unknown_rollup_key():
    request = _base_request("tick_then_bar")
    request["sources"][0]["name"] = "quotes"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50000,
            "ask_px_int": 50005,
            "file_id": 1,
            "file_line_number": 1,
        },
    ]
    request["operators"] = [
        _operator_contract(
            "spread_distribution",
            source_column="bid_px_int",
            name="spread_stats",
            feature_rollups=["sum"],
            feature_rollup_params={"weighted_close": {"weight_column": "ask_px_int"}},
        )
    ]
    request["labels"] = []

    with pytest.raises(ValueError, match="contains unknown rollups"):
        pipeline(request)


def test_pipeline_tick_then_bar_rejects_disallowed_mode_for_custom_rollup():
    @FeatureRollupRegistry.register_feature_rollup(
        name="test_mft_only_rollup_pipeline",
        required_params={},
        required_columns=[],
        mode_allowlist=["MFT"],
        semantic_allowlist=["any"],
    )
    def test_mft_only_rollup_pipeline(feature_col: str, params: dict) -> object:
        return pl.col(feature_col).mean()

    request = _base_request("tick_then_bar")
    request["sources"][0]["name"] = "quotes"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50000,
            "ask_px_int": 50005,
            "file_id": 1,
            "file_line_number": 1,
        },
    ]
    request["operators"] = [
        _operator_contract(
            "spread_distribution",
            source_column="bid_px_int",
            name="spread_stats",
            feature_rollups=["test_mft_only_rollup_pipeline"],
        )
    ]
    request["labels"] = []

    with pytest.raises(ValueError, match="not allowed in HFT"):
        pipeline(request)


def test_pipeline_event_joined_end_to_end():
    request = _base_request("event_joined")
    request["spine"] = {"type": "trades", "source": "trades"}
    request["operators"] = []
    request["labels"] = []
    request["sources"].append(
        {
            "name": "quotes",
            "inline_rows": [
                {
                    "ts_local_us": 9_000_000,
                    "exchange_id": 1,
                    "symbol_id": 12345,
                    "bid_px_int": 49990,
                    "ask_px_int": 50010,
                    "file_id": 2,
                    "file_line_number": 20,
                },
                {
                    "ts_local_us": 60_000_000,
                    "exchange_id": 1,
                    "symbol_id": 12345,
                    "bid_px_int": 50005,
                    "ask_px_int": 50020,
                    "file_id": 2,
                    "file_line_number": 21,
                },
            ],
        }
    )

    output = pipeline(request)
    assert output["run"]["mode"] == "event_joined"
    assert output["results"]["row_count"] == 2


def test_quality_gate_blocks_forward_feature_operator():
    request = _base_request("bar_then_feature")
    request["operators"][0]["pit_policy"] = {
        "feature_direction": "forward",
        "label_direction": "forward_allowed",
    }

    output = pipeline(request)
    assert output["decision"]["status"] == "reject"
    assert "lookahead_check" in output["quality_gates"]["failed_gates"]


def test_quality_gate_respects_forbid_lookahead_false():
    request = _base_request("bar_then_feature")
    request["operators"][0]["pit_policy"] = {
        "feature_direction": "forward",
        "label_direction": "forward_allowed",
    }
    request["constraints"]["forbid_lookahead"] = False

    output = pipeline(request)
    assert "lookahead_check" not in output["quality_gates"]["failed_gates"]
    assert output["quality_gates"]["lookahead_check"]["passed"] is True
    assert output["quality_gates"]["lookahead_check"]["forbid_lookahead"] is False


def test_reproducibility_gate_records_hash_evidence():
    request = _base_request("bar_then_feature")
    output = pipeline(request)

    repro = output["quality_gates"]["reproducibility_check"]
    assert repro["passed"] is True
    assert isinstance(repro["output_hash"], str)
    assert isinstance(repro["rerun_output_hash"], str)
    assert repro["output_hash"] == repro["rerun_output_hash"]


def test_reproducibility_gate_rejects_hash_mismatch(monkeypatch):
    request = _base_request("bar_then_feature")
    calls = {"n": 0}

    def _fake_hash(_frame):
        calls["n"] += 1
        return f"hash_{calls['n']}"

    monkeypatch.setattr(pipeline_module, "_hash_output_frame", _fake_hash)

    output = pipeline(request)
    assert output["decision"]["status"] == "reject"
    assert "reproducibility_check" in output["quality_gates"]["failed_gates"]


def test_artifacts_gate_metrics_include_cost_model_evidence():
    request = _base_request("bar_then_feature")
    output = pipeline(request)

    gate_metrics = output["artifacts"]["gate_metrics"]
    assert gate_metrics["forbid_lookahead"] is True
    assert gate_metrics["cost_model"]["fees_bps"] == 1.0
    assert gate_metrics["cost_model"]["slippage_bps"] == 2.0


def test_input_contract_requires_operator_contract_fields():
    request = _base_request("bar_then_feature")
    del request["operators"][0]["impl_ref"]

    with pytest.raises(SchemaValidationError):
        validate_quant_research_input_v2(request)


def test_input_contract_allows_custom_rollup_identifier():
    request = _base_request("tick_then_bar")
    request["sources"][0]["name"] = "quotes"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50000,
            "ask_px_int": 50005,
            "file_id": 1,
            "file_line_number": 1,
        },
    ]
    request["operators"] = [
        _operator_contract(
            "spread_distribution",
            source_column="bid_px_int",
            name="spread_stats",
            feature_rollups=["weighted_close"],
            feature_rollup_params={"weighted_close": {"weight_column": "ask_px_int"}},
        )
    ]
    request["labels"] = []
    validate_quant_research_input_v2(request)


def test_input_contract_rejects_invalid_custom_rollup_identifier():
    request = _base_request("tick_then_bar")
    request["sources"][0]["name"] = "quotes"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50000,
            "ask_px_int": 50005,
            "file_id": 1,
            "file_line_number": 1,
        },
    ]
    request["operators"] = [
        _operator_contract(
            "spread_distribution",
            source_column="bid_px_int",
            name="spread_stats",
            feature_rollups=["Weighted-Close"],
        )
    ]
    request["labels"] = []

    with pytest.raises(SchemaValidationError):
        validate_quant_research_input_v2(request)


def test_input_contract_allows_context_risk_spec():
    request = _base_request("bar_then_feature")
    request["context_risk"] = [_context_contract()]
    validate_quant_research_input_v2(request)


def test_input_contract_rejects_context_risk_missing_required_params_field():
    request = _base_request("bar_then_feature")
    bad = _context_contract()
    del bad["params"]
    request["context_risk"] = [bad]

    with pytest.raises(SchemaValidationError):
        validate_quant_research_input_v2(request)


def test_input_contract_rejects_invalid_context_plugin_identifier():
    request = _base_request("bar_then_feature")
    request["context_risk"] = [_context_contract(plugin="OI-Capacity")]

    with pytest.raises(SchemaValidationError):
        validate_quant_research_input_v2(request)


def test_compile_request_rejects_context_plugin_disallowed_for_mode():
    request = _base_request("tick_then_bar")
    request["sources"][0]["name"] = "quotes"
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50000,
            "ask_px_int": 50005,
            "file_id": 1,
            "file_line_number": 1,
        }
    ]
    request["operators"] = [
        _operator_contract(
            "spread_distribution",
            source_column="bid_px_int",
            name="spread_stats",
        )
    ]
    request["labels"] = []
    request["context_risk"] = [_context_contract()]

    with pytest.raises(ValueError, match="not allowed in HFT"):
        compile_request(request)


def test_pipeline_applies_context_risk_after_aggregation():
    request = _base_request("bar_then_feature")
    request["sources"][0]["inline_rows"] = [
        {
            "ts_local_us": 10_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "oi_raw": 100.0,
            "mark_px": 10.0,
            "file_id": 1,
            "file_line_number": 10,
        },
        {
            "ts_local_us": 70_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "oi_raw": 120.0,
            "mark_px": 12.0,
            "file_id": 1,
            "file_line_number": 11,
        },
    ]
    request["operators"] = [
        _operator_contract("last", source_column="oi_raw", name="oi_last"),
        _operator_contract("last", source_column="mark_px", name="mark_px_last"),
    ]
    request["labels"] = []
    request["context_risk"] = [
        _context_contract(
            name="oi_cap",
            params={
                "oi_col": "oi_last",
                "price_col": "mark_px_last",
                "base_notional": 1_000.0,
                "lookback_bars": 2,
                "min_ratio": 0.9,
            },
        )
    ]

    output = pipeline(request)
    columns = set(output["results"]["columns"])
    assert "oi_cap_oi_notional" in columns
    assert "oi_cap_oi_level_ratio" in columns
    assert "oi_cap_capacity_ok" in columns
    assert "oi_cap_capacity_mult" in columns
    assert "oi_cap_max_trade_notional" in columns
    assert output["decision"]["status"] in {"go", "revise"}


def test_pit_gate_fails_when_events_arrive_after_last_boundary():
    request = _base_request("bar_then_feature")
    request["sources"][0]["inline_rows"].append(
        {
            "ts_local_us": 130_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "qty_int": 50,
            "px_int": 50020,
            "side": 0,
            "file_id": 1,
            "file_line_number": 12,
        }
    )

    output = pipeline(request)
    assert output["decision"]["status"] == "reject"
    assert "pit_ordering_check" in output["quality_gates"]["failed_gates"]
    assert output["quality_gates"]["pit_ordering_check"]["violations"] >= 1


def test_tick_then_bar_requires_feature_then_aggregate_operator():
    request = _base_request("tick_then_bar")
    request["labels"] = []

    with pytest.raises(ValueError, match="tick_then_bar requires at least one operator stage"):
        pipeline(request)


def test_event_joined_rejects_operator_payload():
    request = _base_request("event_joined")
    request["spine"] = {"type": "trades", "source": "trades"}
    request["labels"] = []

    with pytest.raises(ValueError, match="event_joined mode does not support operators"):
        pipeline(request)
