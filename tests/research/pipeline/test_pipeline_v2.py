"""Tests for the v2 contract-first research pipeline."""

from __future__ import annotations

from copy import deepcopy

import pytest

from pointline.research import (
    compile_request,
    pipeline,
    validate_quant_research_input_v2,
    validate_quant_research_output_v2,
)
from pointline.research.contracts import SchemaValidationError
from pointline.research.resample import AggregationRegistry


def _operator_contract(agg: str, *, source_column: str, name: str | None = None) -> dict:
    meta = AggregationRegistry.get(agg)
    op_name = name or agg
    return {
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


def test_input_contract_requires_operator_contract_fields():
    request = _base_request("bar_then_feature")
    del request["operators"][0]["impl_ref"]

    with pytest.raises(SchemaValidationError):
        validate_quant_research_input_v2(request)


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
