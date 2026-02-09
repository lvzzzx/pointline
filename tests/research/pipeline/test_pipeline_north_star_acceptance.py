"""North-star acceptance checks for single-mode research.pipeline v2."""

from __future__ import annotations

import importlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from pointline.research import pipeline, validate_quant_research_output_v2

pipeline_module = importlib.import_module("pointline.research.pipeline")


def _fixture(name: str) -> dict:
    base = Path(__file__).resolve().parent / "fixtures"
    with (base / name).open("r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


@pytest.mark.parametrize(
    "fixture_name",
    [
        "input_v2_bar_then_feature.json",
        "input_v2_tick_then_bar.json",
        "input_v2_event_joined.json",
    ],
)
def test_pipeline_modes_run_and_validate_output_contract(fixture_name: str):
    request = _fixture(fixture_name)
    output = pipeline(request)
    validate_quant_research_output_v2(output)
    assert output["run"]["status"] == "success"


def test_pipeline_critical_pit_failure_forces_reject():
    request = _fixture("input_v2_bar_then_feature.json")
    request["sources"][0]["inline_rows"].append(
        {
            "ts_local_us": 130_000_000,
            "exchange_id": 1,
            "symbol_id": 12345,
            "qty_int": 1,
            "px_int": 50020,
            "side": 0,
            "file_id": 1,
            "file_line_number": 99,
        }
    )

    output = pipeline(request)
    assert output["decision"]["status"] == "reject"
    assert "pit_ordering_check" in output["quality_gates"]["failed_gates"]


def test_pipeline_rejects_unknown_registry_operator():
    request = _fixture("input_v2_bar_then_feature.json")
    request["operators"][0]["agg"] = "unknown_registry_op"
    with pytest.raises(ValueError, match="not registered"):
        pipeline(request)


def test_pipeline_reproducibility_gate_is_mandatory(monkeypatch):
    request = _fixture("input_v2_bar_then_feature.json")
    state = {"n": 0}

    def _fake_hash(_frame):
        state["n"] += 1
        return f"hash_{state['n']}"

    monkeypatch.setattr(pipeline_module, "_hash_output_frame", _fake_hash)
    output = pipeline(deepcopy(request))

    assert output["decision"]["status"] == "reject"
    assert "reproducibility_check" in output["quality_gates"]["failed_gates"]
