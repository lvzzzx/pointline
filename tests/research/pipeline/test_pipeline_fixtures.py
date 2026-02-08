"""Golden contract fixture checks for v2 pipeline modes."""

from __future__ import annotations

import json
from pathlib import Path

from pointline.research import (
    pipeline,
    validate_quant_research_input_v2,
    validate_quant_research_output_v2,
)

_FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"


def _load_fixture(name: str) -> dict:
    with (_FIXTURE_DIR / name).open("r", encoding="utf-8") as f:
        return json.load(f)


def test_fixture_bar_then_feature_runs_and_validates():
    request = _load_fixture("input_v2_bar_then_feature.json")
    validate_quant_research_input_v2(request)

    output = pipeline(request)
    validate_quant_research_output_v2(output)

    assert output["run"]["mode"] == "bar_then_feature"
    assert "volume" in output["results"]["columns"]


def test_fixture_tick_then_bar_runs_and_validates():
    request = _load_fixture("input_v2_tick_then_bar.json")
    validate_quant_research_input_v2(request)

    output = pipeline(request)
    validate_quant_research_output_v2(output)

    assert output["run"]["mode"] == "tick_then_bar"
    assert "spread_stats_mean" in output["results"]["columns"]


def test_fixture_event_joined_runs_and_validates():
    request = _load_fixture("input_v2_event_joined.json")
    validate_quant_research_input_v2(request)

    output = pipeline(request)
    validate_quant_research_output_v2(output)

    assert output["run"]["mode"] == "event_joined"
    assert output["results"]["row_count"] == 2
