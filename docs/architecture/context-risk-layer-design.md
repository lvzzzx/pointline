# Context/Risk Layer Architecture (North-Star Extension)

## Summary

Add a first-class **Context/Risk Layer** to the research framework as a post-aggregation module:

`resample/aggregate -> context/risk -> labels -> evaluation/gates -> output`

This keeps core feature engineering clean while enabling production-safe tradeability and sizing context.

`oi_capacity` is the first context plugin.

## Goals

1. Keep `research.pipeline(...)` and `research.workflow(...)` canonical.
2. Separate alpha feature construction from context/risk controls.
3. Use registry-backed, typed plugins (no ad-hoc runtime UDF execution).
4. Preserve PIT/determinism and fail-fast governance.
5. Support both LLM-agent and human-researcher usage with low cognitive load.

## Non-Goals

1. Live trading/routing/portfolio execution.
2. Broker/exchange-specific risk engines.
3. Replacing existing resample/aggregation contracts.

## Layer Placement

The new context layer runs on **bar-level outputs** (not raw tick events):

1. Sources + spine + bucket assignment
2. Aggregations/rollups (existing)
3. Context/Risk plugins (new)
4. Labels
5. Metrics, gates, decision, artifacts

## Module Design

New package:

- `pointline/research/context/__init__.py`
- `pointline/research/context/config.py`
- `pointline/research/context/registry.py`
- `pointline/research/context/engine.py`
- `pointline/research/context/plugins/oi_capacity.py`

### Responsibilities

1. `config.py`
   - Typed request-side config objects for context plugins.
2. `registry.py`
   - Plugin registration, metadata contracts, mode/param validation.
3. `engine.py`
   - Applies context plugins in deterministic sequence.
4. `plugins/oi_capacity.py`
   - First implementation of OI-based capacity context metrics.

## Public Contract Changes

Add optional field to both request schemas:

- `schemas/quant_research_input.v2.json`
- `schemas/quant_research_workflow_input.v2.json`

New optional top-level field:

- `context_risk: ContextSpec[]` (default: `[]`)

No change required for existing requests when `context_risk` is omitted.

## Context Plugin Contract

Each context spec is decision-complete:

```json
{
  "name": "oi_capacity",
  "plugin": "oi_capacity",
  "required_columns": ["oi_last"],
  "params": {
    "oi_col": "oi_last",
    "price_col": "mark_px",
    "base_notional": 100000,
    "lookback_bars": 96,
    "min_ratio": 0.6,
    "clip_min": 0.5,
    "clip_max": 1.5
  },
  "mode_allowlist": ["MFT", "LFT"],
  "pit_policy": {"feature_direction": "backward_only"},
  "determinism_policy": {
    "required_sort": ["exchange_id", "symbol_id", "ts_local_us"],
    "partition_by": ["exchange_id", "symbol_id"]
  },
  "version": "2.0"
}
```

## First Plugin: `oi_capacity`

### Input assumptions

1. Bar-level OI feature exists (typically `oi_last`).
2. Optional price column for notional conversion (for example `mark_px`).

### Output metrics

1. `<name>_oi_notional` = `oi_last * mark_px` (if `price_col` provided)
2. `<name>_oi_level_ratio` = `oi_last / rolling_mean(oi_last, lookback_bars)`
3. `<name>_capacity_ok` = `oi_level_ratio >= min_ratio`
4. `<name>_capacity_mult` = `clip(oi_level_ratio, clip_min, clip_max)`
5. `<name>_max_trade_notional` = `base_notional * capacity_mult`

### Why this belongs in context/risk layer

1. These metrics are execution context and sizing controls.
2. They are post-feature controls, not raw aggregation primitives.
3. They should be reusable across many strategies without polluting aggregation modules.

## Execution Semantics

1. Context engine runs after aggregate result is materialized as LazyFrame.
2. Context plugins execute in request order.
3. Plugin outputs are appended as new columns.
4. Fail-fast on missing required columns, invalid params, or disallowed modes.

## Governance and Gates

Mandatory checks:

1. `context_contract_check`
   - Plugin exists, metadata complete, params valid.
2. `context_required_columns_check`
   - Required columns and referenced params columns exist.
3. `context_determinism_check`
   - Required sort/partition policy present for rolling/stateful context logic.
4. `context_output_completeness_check`
   - Declared outputs are emitted.

Decision rule:

1. Any critical context gate failure => `decision.status = reject`.

## Artifacts and Lineage

Add context evidence to artifacts:

1. Resolved `context_risk` plan
2. Per-plugin params and metadata snapshot
3. Output column list per plugin
4. Gate outcomes per plugin

## Testing Strategy

1. Unit tests (context plugin formulas)
   - zero/near-zero OI, nulls, short lookback behavior.
2. Unit tests (registry)
   - validation, mode allowlist, unknown plugin rejection.
3. Pipeline integration tests
   - context columns present in output for bar_then_feature runs.
4. Workflow integration tests
   - context layer applied on stage outputs deterministically.
5. Determinism tests
   - identical input/config produces identical output hashes.

## Rollout Plan

1. M1: add context package, registry, contracts, schema fields.
2. M2: wire context engine into pipeline/workflow execution path.
3. M3: implement `oi_capacity` plugin with full tests.
4. M4: add docs/examples and artifact/gate evidence.
5. M5: CI governance checks for context contract and deterministic reruns.

## Acceptance Criteria

1. Requests without `context_risk` behave exactly as today.
2. Requests with `context_risk` emit deterministic context columns.
3. `oi_capacity` outputs are schema-valid and PIT-safe.
4. Critical context failures block `go` decision.
5. Full test suite remains green.
