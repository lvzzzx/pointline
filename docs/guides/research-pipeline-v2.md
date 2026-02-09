# Research Pipeline v2

`research.pipeline(request)` is the canonical production execution path for feature engineering, resample, and aggregate workflows.

For multi-stage hybrid compositions across modes, use:
- `research.workflow(request)` (see `docs/guides/research-workflow-hybrid.md`)

## Contract Schemas

- Input: `schemas/quant_research_input.v2.json`
- Output: `schemas/quant_research_output.v2.json`

Validate payloads directly:

```python
from pointline.research import (
    pipeline,
    validate_quant_research_input_v2,
    validate_quant_research_output_v2,
)

validate_quant_research_input_v2(request)
output = pipeline(request)
validate_quant_research_output_v2(output)
```

Hybrid workflow validation:

```python
from pointline.research import (
    workflow,
    validate_quant_research_workflow_input_v2,
    validate_quant_research_workflow_output_v2,
)

validate_quant_research_workflow_input_v2(workflow_request)
workflow_output = workflow(workflow_request)
validate_quant_research_workflow_output_v2(workflow_output)
```

## Modes

- `bar_then_feature`: build bars first, then derive labels/features from bar outputs.
- `tick_then_bar`: compute tick-level features through typed operators, then aggregate.
- `event_joined`: PIT as-of align multi-stream event tables on a shared event spine.

For `tick_then_bar` operators (`stage="feature_then_aggregate"`), you can customize bar rollups via
`feature_rollups`, for example: `["sum", "last", "close"]`.

You can also use registry-backed custom rollups with typed params:

```python
{
  "name": "spread_stats",
  "output_name": "spread_stats",
  "stage": "feature_then_aggregate",
  "agg": "spread_distribution",
  "source_column": "bid_px_int",
  "feature_rollups": ["mean", "weighted_close", "tail_ratio_p95_p50"],
  "feature_rollup_params": {
    "weighted_close": {"weight_column": "ask_px_int"},
    "tail_ratio_p95_p50": {"epsilon": 1e-6}
  }
}
```

## Required Input Top-Level Fields

- `schema_version`
- `request_id`
- `mode`
- `timeline`
- `sources`
- `spine`
- `operators`
- `labels`
- `evaluation`
- `constraints`
- `artifacts`

## Required Output Top-Level Fields

- `schema_version`
- `request_id`
- `run`
- `resolved_plan`
- `data_feasibility`
- `quality_gates`
- `results`
- `decision`
- `artifacts`

## Gate Behavior

The pipeline evaluates and reports:

- lookahead check
- PIT ordering check
- partition safety check
- reproducibility check

If any critical gate fails, decision status is `reject`.
