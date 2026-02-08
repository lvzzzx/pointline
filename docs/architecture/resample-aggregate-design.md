# Resample and Aggregate Design (v1)

This document defines a robust, PIT-safe, deterministic design for first-class resampling and aggregation in Pointline research workflows.

## Goals

- Provide stable, first-class APIs for resample and aggregate operations.
- Support three production modes: `event_joined`, `tick_then_bar`, `bar_then_feature`.
- Enable advanced custom microstructure and MFT aggregations with guardrails.
- Guarantee PIT correctness and deterministic reruns.

## Non-Goals

- Replacing all low-level Polars operations.
- Supporting arbitrary user-defined Python in execution path.
- Optimizing every edge case before semantic correctness is locked.

## Core Design Principles

- Contract-first: explicit config schema and validation before execution.
- PIT-first: feature windows are backward-only; labels may be forward.
- Deterministic-first: canonical sort and tie-break policy enforced.
- Extensible-by-registry: custom aggregations are registered, typed, and policy-checked.

## API Surface (Proposed)

### 1) Resample API

```python
research.resample(
    lf: pl.LazyFrame,
    *,
    time_col: str = "ts_local_us",
    by: list[str] = ["exchange_id", "symbol_id"],
    every: str,
    period: str | None = None,
    closed: str = "left",
    label: str = "left",
    mode: str = "bar_then_feature",
    fill_policy: str = "none",
    deterministic: bool = True,
) -> pl.LazyFrame
```

### 2) Aggregate API

```python
research.aggregate(
    lf: pl.LazyFrame,
    *,
    by: list[str],
    aggregations: list[dict],
    mode: str,
    registry_profile: str = "default",
    deterministic: bool = True,
) -> pl.LazyFrame
```

Example aggregation item:

```json
{
  "name": "signed_flow_1m",
  "source_column": "signed_qty",
  "agg": "sum"
}
```

### 3) Pipeline API

```python
research.pipeline(
    *,
    mode: str,  # event_joined | tick_then_bar | bar_then_feature
    sources: dict[str, pl.LazyFrame],
    config: dict,
) -> pl.LazyFrame
```

## Pipeline Modes

### event_joined

- Build event-level frame via backward as-of joins.
- Compute features on event timeline.
- Optional downstream resample.

### tick_then_bar

- Compute microfeatures at tick/event level.
- Aggregate microfeatures into bars.
- Best for microstructure-native alpha.

### bar_then_feature

- Resample streams to bars first.
- Compute features on bar data.
- Best for scalable MFT baselines.

## Typed Aggregation Registry

Registry governs both built-in and custom aggregations.

## Registry Contract

```json
{
  "name": "microprice_close",
  "stage": "feature_then_aggregate",
  "semantic_type": "book_top",
  "mode_allowlist": ["HFT", "MFT"],
  "inputs": {
    "source_table": "book_snapshot_25",
    "required_columns": [
      "bids_px_int", "asks_px_int", "bids_sz_int", "asks_sz_int", "ts_local_us"
    ]
  },
  "pit_policy": {
    "feature_direction": "backward_only",
    "label_direction": "forward_allowed"
  },
  "determinism": {
    "required_sort": [
      "exchange_id", "symbol_id", "ts_local_us", "file_id", "file_line_number"
    ]
  },
  "impl_ref": "pointline.research.aggregations.microprice_close"
}
```

## Stage Types

- `feature_then_aggregate`: compute event-level feature then roll up to bars.
- `aggregate_then_feature`: aggregate raw fields then derive features.
- `hybrid`: multi-step composition with explicit join/alignment steps.

## Built-In Aggregations (v1)

- Numeric: `sum`, `mean`, `std`, `min`, `max`, `last`
- Event counters: `count`, `nunique`
- Optional later: `quantile` (v1.1+)

## Custom Aggregations (starter set)

- `microprice_close`
- `ofi_cont`
- `signed_trade_imbalance`
- `liq_qty_sum`
- `liq_count`
- `liq_oi_pressure`

## Semantic Type Policies

Allowed aggregations are constrained by semantic type, not only dtype.

- `price`: `last`, `mean`, `min`, `max`
- `size` / `notional`: `sum`, `mean`, `std`, `max`
- `event_id`: `count`, `nunique`, `last`
- `state_variable` (OI/funding): `last`, `mean`, `diff`-style derived custom aggs

## PIT and Determinism Rules

Applied globally before time-aware operations:

1. Sort by:
- `exchange_id`, `symbol_id`, `ts_local_us`, `file_id`, `file_line_number`
2. As-of joins:
- `strategy="backward"` by default
3. Window direction:
- features backward-only
- labels may be forward
4. Execution must fail fast if required sort/tie-break columns are absent.

## Configuration Schema (Proposed)

```json
{
  "schema_version": "1.0",
  "mode": "tick_then_bar",
  "timeline": {
    "time_col": "ts_local_us",
    "timezone": "UTC"
  },
  "bucketing": {
    "every": "1m",
    "period": "1m",
    "closed": "left",
    "label": "left"
  },
  "fill_policy": "none",
  "join_policy": {
    "asof_strategy": "backward",
    "by": ["exchange_id", "symbol_id"]
  },
  "aggregations": [
    {"name": "signed_flow_1m", "source_column": "signed_qty", "agg": "sum"},
    {"name": "trade_count_1m", "source_column": "trade_id", "agg": "count"}
  ],
  "registry_profile": "mft_default"
}
```

## Validation Layer

Pre-execution checks:

- valid `mode`, durations, boundary settings
- required columns present for all selected aggregations
- aggregation allowed for semantic type and mode
- feature/label direction policy compliance
- table compatibility and join-key compatibility

Runtime checks:

- bucket completeness and row-loss diagnostics
- null inflation diagnostics after joins
- lag distribution diagnostics for as-of joins

## Observability and Artifacts

Each run should persist:

- normalized config hash
- mode and profile
- registry entry versions used
- input table coverage and probe evidence
- quality gate outcomes
- output schema version and run status

Suggested paths:

- `artifacts/resample_aggregate/inputs/<run_id>.json`
- `artifacts/resample_aggregate/runs/<run_id>/metrics.json`
- `artifacts/resample_aggregate/outputs/<run_id>.json`

## Testing Strategy

### Unit

- bucket boundary correctness (`closed`, `label`, `period`)
- aggregation correctness by semantic type
- registry validation and policy enforcement

### Property-based

- no future dependency in features
- deterministic rerun equivalence for same inputs

### Integration

- end-to-end tests for all three modes
- cross-table PIT join validation
- custom aggregation regression fixtures

## Rollout Plan

### Phase 1

- Publish config/schema + API contracts
- Implement `resample` and `aggregate` built-ins
- Add strict validation and deterministic ordering enforcement

### Phase 2

- Implement registry framework and starter custom aggs
- Add mode-aware policy profiles (`hft_default`, `mft_default`)

### Phase 3

- Add `pipeline(...)` orchestration with mode templates
- Integrate run artifacts and quality-gate telemetry

### Phase 4

- Integrate with Feature DSL/Quant Agent planning stage
- LLM emits contract-compliant mode + aggregation plan

## Acceptance Criteria

- Same input + config yields identical output across reruns.
- PIT gate catches all forward-looking feature attempts.
- All three modes pass integration tests on representative symbols.
- Custom aggregations can be added via registry without core API changes.
