# Research Pipeline v2

`research.pipeline(request)` is the canonical production execution path for feature engineering, resample, and aggregate workflows.

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

## Modes

- `bar_then_feature`: build bars first, then derive labels/features from bar outputs.
- `tick_then_bar`: compute tick-level features through typed operators, then aggregate.
- `event_joined`: PIT as-of align multi-stream event tables on a shared event spine.

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
