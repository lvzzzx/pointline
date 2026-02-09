"""North-star acceptance checks for hybrid research.workflow v2."""

from __future__ import annotations

import importlib
from copy import deepcopy

from pointline.research import (
    validate_quant_research_workflow_output_v2,
    workflow,
)

workflow_module = importlib.import_module("pointline.research.workflow")


def _base_request() -> dict:
    return {
        "schema_version": "2.0",
        "request_id": "wf-north-star-001",
        "workflow_id": "wf-north-star",
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
                    }
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
                    {
                        "name": "spread_stats",
                        "output_name": "spread_stats",
                        "stage": "feature_then_aggregate",
                        "source_column": "bid_px_int",
                        "agg": "spread_distribution",
                        "required_columns": ["bid_px_int", "ask_px_int"],
                        "mode_allowlist": ["HFT", "MFT"],
                        "pit_policy": {"feature_direction": "backward_only"},
                        "determinism_policy": {
                            "required_sort": ["exchange_id", "symbol_id", "ts_local_us"],
                            "stateful": False,
                        },
                        "impl_ref": "pointline.research.resample.aggregations.microstructure.compute_spread_distribution",
                        "version": "2.0",
                    }
                ],
                "labels": [],
                "evaluation": {"metrics": ["row_count"], "split_method": "fixed"},
                "constraints": {
                    "pit_timeline": "ts_local_us",
                    "forbid_lookahead": True,
                    "cost_model": {"fees_bps": 1.0, "slippage_bps": 2.0},
                },
                "outputs": [{"name": "bars"}],
            },
            {
                "stage_id": "s2",
                "mode": "event_joined",
                "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
                "spine": {"type": "trades", "source": "trades"},
                "sources": [
                    {"name": "trades", "ref": "base:trades"},
                    {"name": "bars", "ref": "artifact:s1:bars"},
                ],
                "operators": [],
                "labels": [],
                "evaluation": {"metrics": ["row_count"], "split_method": "fixed"},
                "constraints": {
                    "pit_timeline": "ts_local_us",
                    "forbid_lookahead": True,
                    "cost_model": {"fees_bps": 1.0, "slippage_bps": 2.0},
                },
                "outputs": [{"name": "frame"}],
            },
        ],
        "final_stage_id": "s2",
        "constraints": {"fail_fast": True},
        "artifacts": {"include_artifacts": False},
    }


def test_workflow_runs_hybrid_and_validates_output_contract():
    output = workflow(_base_request())
    validate_quant_research_workflow_output_v2(output)
    assert output["run"]["status"] == "success"
    assert len(output["stage_runs"]) == 2


def test_workflow_fail_fast_stops_after_first_critical_failure():
    request = _base_request()
    request["base_sources"][0]["inline_rows"].append(
        {
            "ts_local_us": 130_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "bid_px_int": 50011,
            "ask_px_int": 50016,
            "file_id": 1,
            "file_line_number": 99,
        }
    )

    output = workflow(request)
    assert output["decision"]["status"] == "reject"
    assert output["run"]["status"] == "failed"
    assert len(output["stage_runs"]) == 1


def test_workflow_level_gate_failure_forces_reject(monkeypatch):
    request = deepcopy(_base_request())
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
