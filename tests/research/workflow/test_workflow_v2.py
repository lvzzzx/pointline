"""Tests for hybrid workflow orchestration over pipeline v2 kernels."""

from __future__ import annotations

from copy import deepcopy

import pytest

from pointline.research import (
    compile_workflow_request,
    validate_quant_research_workflow_input_v2,
    validate_quant_research_workflow_output_v2,
    workflow,
)
from pointline.research.resample import AggregationRegistry
from pointline.research.workflow import WorkflowError


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


def _stage_constraints() -> dict:
    return {
        "pit_timeline": "ts_local_us",
        "forbid_lookahead": True,
        "cost_model": {"fees_bps": 1.0, "slippage_bps": 2.0},
    }


def _base_workflow_request() -> dict:
    return {
        "schema_version": "2.0",
        "request_id": "wf-req-001",
        "workflow_id": "hybrid-001",
        "base_sources": [
            {
                "name": "quotes",
                "inline_rows": [
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
                ],
            },
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
                        "file_id": 2,
                        "file_line_number": 1,
                    },
                    {
                        "ts_local_us": 70_000_000,
                        "exchange_id": 1,
                        "symbol_id": 12345,
                        "qty_int": 200,
                        "px_int": 50020,
                        "side": 1,
                        "file_id": 2,
                        "file_line_number": 2,
                    },
                ],
            },
        ],
        "stages": [
            {
                "stage_id": "s1",
                "mode": "tick_then_bar",
                "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
                "spine": {
                    "type": "clock",
                    "step_ms": 60_000,
                    "start_ts_us": 0,
                    "end_ts_us": 120_000_000,
                },
                "sources": [{"name": "quotes", "ref": "base:quotes"}],
                "operators": [
                    _operator_contract(
                        "spread_distribution",
                        source_column="bid_px_int",
                        name="spread_stats",
                    )
                ],
                "labels": [],
                "evaluation": {"metrics": ["row_count"], "split_method": "fixed"},
                "constraints": _stage_constraints(),
                "outputs": [{"name": "bars"}],
            },
            {
                "stage_id": "s2",
                "mode": "bar_then_feature",
                "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
                "spine": {
                    "type": "clock",
                    "step_ms": 60_000,
                    "start_ts_us": 0,
                    "end_ts_us": 180_000_000,
                },
                "sources": [{"name": "bars", "ref": "artifact:s1:bars"}],
                "operators": [_operator_contract("sum", source_column="spread_stats_mean")],
                "labels": [],
                "evaluation": {"metrics": ["row_count", "non_null_ratio"], "split_method": "fixed"},
                "constraints": _stage_constraints(),
                "outputs": [{"name": "bars2"}],
            },
            {
                "stage_id": "s3",
                "mode": "event_joined",
                "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
                "spine": {"type": "trades", "source": "trades"},
                "sources": [
                    {"name": "trades", "ref": "base:trades"},
                    {"name": "bars2", "ref": "artifact:s2:bars2"},
                ],
                "operators": [],
                "labels": [],
                "evaluation": {"metrics": ["row_count"], "split_method": "fixed"},
                "constraints": _stage_constraints(),
                "outputs": [{"name": "frame"}],
            },
        ],
        "final_stage_id": "s3",
        "constraints": {"fail_fast": True},
        "artifacts": {"include_artifacts": False},
    }


def test_workflow_end_to_end_validates_output():
    request = _base_workflow_request()
    validate_quant_research_workflow_input_v2(request)

    output = workflow(request)
    validate_quant_research_workflow_output_v2(output)

    assert output["run"]["status"] == "success"
    assert output["decision"]["status"] in {"go", "revise"}
    assert output["results"]["final_stage_id"] == "s3"
    assert len(output["stage_runs"]) == 3


def test_workflow_fail_fast_on_stage_gate_failure():
    request = _base_workflow_request()
    request["stages"] = [
        {
            "stage_id": "s1",
            "mode": "bar_then_feature",
            "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
            "spine": {
                "type": "clock",
                "step_ms": 60_000,
                "start_ts_us": 0,
                "end_ts_us": 120_000_000,
            },
            "sources": [{"name": "trades", "ref": "base:trades"}],
            "operators": [_operator_contract("sum", source_column="qty_int", name="volume")],
            "labels": [],
            "evaluation": {"metrics": ["row_count"], "split_method": "fixed"},
            "constraints": _stage_constraints(),
            "outputs": [{"name": "frame"}],
        },
        {
            "stage_id": "s2",
            "mode": "event_joined",
            "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
            "spine": {"type": "trades", "source": "trades"},
            "sources": [{"name": "trades", "ref": "base:trades"}],
            "operators": [],
            "labels": [],
            "evaluation": {"metrics": ["row_count"], "split_method": "fixed"},
            "constraints": _stage_constraints(),
            "outputs": [{"name": "frame"}],
        },
    ]
    request["final_stage_id"] = "s2"
    request["base_sources"][1]["inline_rows"].append(
        {
            "ts_local_us": 130_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "qty_int": 50,
            "px_int": 50020,
            "side": 0,
            "file_id": 2,
            "file_line_number": 3,
        }
    )

    output = workflow(request)
    assert output["run"]["status"] == "failed"
    assert output["decision"]["status"] == "reject"
    assert len(output["stage_runs"]) == 1
    assert output["quality_gates"]["failed_gates"]


def test_compile_workflow_detects_stage_cycle():
    request = _base_workflow_request()
    request["stages"] = [
        {
            "stage_id": "s1",
            "mode": "bar_then_feature",
            "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
            "spine": {
                "type": "clock",
                "step_ms": 60_000,
                "start_ts_us": 0,
                "end_ts_us": 120_000_000,
            },
            "sources": [{"name": "from_s2", "ref": "artifact:s2:out"}],
            "operators": [_operator_contract("sum", source_column="qty_int")],
            "labels": [],
            "evaluation": {"metrics": ["row_count"], "split_method": "fixed"},
            "constraints": _stage_constraints(),
            "outputs": [{"name": "out"}],
        },
        {
            "stage_id": "s2",
            "mode": "bar_then_feature",
            "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
            "spine": {
                "type": "clock",
                "step_ms": 60_000,
                "start_ts_us": 0,
                "end_ts_us": 120_000_000,
            },
            "sources": [{"name": "from_s1", "ref": "artifact:s1:out"}],
            "operators": [_operator_contract("sum", source_column="qty_int")],
            "labels": [],
            "evaluation": {"metrics": ["row_count"], "split_method": "fixed"},
            "constraints": _stage_constraints(),
            "outputs": [{"name": "out"}],
        },
    ]
    request["final_stage_id"] = "s2"

    with pytest.raises(WorkflowError, match="cycle"):
        compile_workflow_request(request)


def test_compile_workflow_rejects_unknown_artifact_output():
    request = _base_workflow_request()
    request["stages"][1]["sources"] = [{"name": "bars", "ref": "artifact:s1:missing"}]

    with pytest.raises(WorkflowError, match="Unknown artifact output"):
        compile_workflow_request(request)


def test_compile_workflow_hash_is_deterministic():
    request = _base_workflow_request()
    compiled_a = compile_workflow_request(deepcopy(request))
    compiled_b = compile_workflow_request(deepcopy(request))
    assert compiled_a["config_hash"] == compiled_b["config_hash"]
