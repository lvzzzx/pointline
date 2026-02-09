"""Tests for hybrid workflow orchestration over pipeline v2 kernels."""

from __future__ import annotations

import importlib
import json
from copy import deepcopy

import polars as pl
import pytest

from pointline.research import (
    compile_workflow_request,
    validate_quant_research_workflow_input_v2,
    validate_quant_research_workflow_output_v2,
    workflow,
)
from pointline.research.context import ContextRegistry
from pointline.research.contracts import SchemaValidationError
from pointline.research.resample import AggregationRegistry
from pointline.research.workflow import WorkflowError

workflow_module = importlib.import_module("pointline.research.workflow")


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


def _stage_constraints() -> dict:
    return {
        "pit_timeline": "ts_local_us",
        "forbid_lookahead": True,
        "cost_model": {"fees_bps": 1.0, "slippage_bps": 2.0},
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
        "context_risk": [],
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


def test_workflow_supports_custom_rollups_in_intermediate_stage():
    request = _base_workflow_request()
    request["stages"][0]["operators"] = [
        _operator_contract(
            "spread_distribution",
            source_column="bid_px_int",
            name="spread_stats",
            feature_rollups=["weighted_close"],
            feature_rollup_params={"weighted_close": {"weight_column": "ask_px_int"}},
        )
    ]
    request["stages"][1]["operators"] = [
        _operator_contract("sum", source_column="spread_stats_weighted_close", name="sum_weighted")
    ]
    request["stages"][1]["spine"]["end_ts_us"] = 180_000_000
    request["stages"][1]["evaluation"] = {"metrics": ["row_count"], "split_method": "fixed"}

    output = workflow(request)
    assert output["run"]["status"] == "success"
    stage1 = next(item for item in output["stage_runs"] if item["stage_id"] == "s1")
    assert "spread_stats_weighted_close" in stage1["columns"]


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


def test_compile_workflow_hash_changes_when_context_risk_changes():
    request_a = _base_workflow_request()
    request_b = _base_workflow_request()
    request_b["context_risk"] = [
        _context_contract(
            params={"oi_col": "oi_last", "base_notional": 100_000.0, "lookback_bars": 96}
        )
    ]

    compiled_a = compile_workflow_request(deepcopy(request_a))
    compiled_b = compile_workflow_request(deepcopy(request_b))
    assert compiled_a["config_hash"] != compiled_b["config_hash"]


def test_workflow_schema_allows_stage_and_workflow_context_risk():
    request = _base_workflow_request()
    request["context_risk"] = [_context_contract(name="wf_capacity")]
    request["stages"][1]["context_risk"] = [_context_contract(name="stage_capacity")]
    validate_quant_research_workflow_input_v2(request)


def test_workflow_schema_rejects_invalid_context_plugin_identifier():
    request = _base_workflow_request()
    request["stages"][0]["context_risk"] = [_context_contract(plugin="OI-Capacity")]
    with pytest.raises(SchemaValidationError):
        validate_quant_research_workflow_input_v2(request)


def test_workflow_applies_stage_context_risk_plugin():
    request = _base_workflow_request()
    request["stages"][1]["operators"] = [
        _operator_contract("last", source_column="spread_stats_mean", name="oi_last")
    ]
    request["stages"][1]["context_risk"] = [
        _context_contract(
            name="stage_cap",
            params={"oi_col": "oi_last", "base_notional": 10_000.0, "lookback_bars": 2},
        )
    ]

    output = workflow(request)
    stage2 = next(item for item in output["stage_runs"] if item["stage_id"] == "s2")
    assert "stage_cap_oi_level_ratio" in stage2["columns"]
    assert "stage_cap_capacity_ok" in stage2["columns"]
    assert "stage_cap_capacity_mult" in stage2["columns"]
    assert "stage_cap_max_trade_notional" in stage2["columns"]


def test_workflow_global_context_risk_is_merged_into_stage_requests():
    @ContextRegistry.register_context(
        name="workflow_tag",
        required_columns=["exchange_id", "symbol_id", "ts_local_us"],
        mode_allowlist=["HFT", "MFT", "LFT"],
    )
    def workflow_tag(lf: pl.LazyFrame, spec) -> pl.LazyFrame:
        return lf.with_columns([pl.lit(1).alias(f"{spec.name}_flag")])

    request = _base_workflow_request()
    request["context_risk"] = [
        {
            "name": "wf_tag",
            "plugin": "workflow_tag",
            "required_columns": ["exchange_id", "symbol_id", "ts_local_us"],
            "params": {},
            "mode_allowlist": ["HFT", "MFT", "LFT"],
            "pit_policy": {"feature_direction": "backward_only"},
            "determinism_policy": {"required_sort": ["exchange_id", "symbol_id", "ts_local_us"]},
            "impl_ref": "tests.research.workflow.test_workflow_v2.workflow_tag",
            "version": "2.0",
        }
    ]
    request["stages"][0]["context_risk"] = []
    request["stages"][1]["context_risk"] = []
    request["stages"][2]["context_risk"] = []

    output = workflow(request)
    for stage_id in ["s1", "s2", "s3"]:
        stage_run = next(item for item in output["stage_runs"] if item["stage_id"] == stage_id)
        assert "wf_tag_flag" in stage_run["columns"]


def test_stage_config_hash_changes_when_artifact_ref_changes():
    request_a = _base_workflow_request()
    request_a["stages"][0]["outputs"] = [{"name": "bars"}, {"name": "bars_alt"}]
    request_a["stages"][1]["sources"] = [{"name": "bars", "ref": "artifact:s1:bars"}]

    request_b = _base_workflow_request()
    request_b["stages"][0]["outputs"] = [{"name": "bars"}, {"name": "bars_alt"}]
    request_b["stages"][1]["sources"] = [{"name": "bars", "ref": "artifact:s1:bars_alt"}]

    output_a = workflow(request_a)
    output_b = workflow(request_b)

    stage2_hash_a = next(
        stage_run["config_hash"]
        for stage_run in output_a["stage_runs"]
        if stage_run["stage_id"] == "s2"
    )
    stage2_hash_b = next(
        stage_run["config_hash"]
        for stage_run in output_b["stage_runs"]
        if stage_run["stage_id"] == "s2"
    )
    assert stage2_hash_a != stage2_hash_b


def test_workflow_schema_rejects_fail_fast_false():
    request = _base_workflow_request()
    request["constraints"]["fail_fast"] = False
    with pytest.raises(SchemaValidationError):
        validate_quant_research_workflow_input_v2(request)


def test_compile_workflow_rejects_fail_fast_false_defensive():
    request = _base_workflow_request()
    request["constraints"]["fail_fast"] = False
    with pytest.raises(WorkflowError, match="fail_fast must be true"):
        compile_workflow_request(request)


def test_workflow_rejects_when_workflow_level_lineage_gate_fails(monkeypatch):
    request = _base_workflow_request()
    original_publish = workflow_module._publish_stage_outputs

    def _drop_lineage_entry(stage, frame, artifact_store, lineage):
        refs = original_publish(stage, frame, artifact_store, lineage)
        if lineage:
            lineage.pop()
        return refs

    monkeypatch.setattr(workflow_module, "_publish_stage_outputs", _drop_lineage_entry)

    output = workflow(request)
    assert output["decision"]["status"] == "reject"
    assert "workflow:lineage_completeness_check" in output["quality_gates"]["failed_gates"]


def test_stage_artifact_plan_includes_constraints(tmp_path):
    request = _base_workflow_request()
    request["artifacts"] = {
        "include_artifacts": True,
        "output_dir": str(tmp_path),
        "persist_stage_snapshots": False,
    }

    output = workflow(request)
    stage_plan_path = next(
        path
        for path in output["artifacts"]["paths"]
        if path.endswith("/stages/s1/resolved_plan.json")
    )
    with open(stage_plan_path, encoding="utf-8") as file_obj:
        payload = json.load(file_obj)

    assert "constraints" in payload
    assert payload["constraints"]["cost_model"]["fees_bps"] == 1.0
