# Implementation Plan: Context/Risk Layer (`oi_capacity` first)

## Scope

Implement a new high-level context/risk layer in the research framework:

`aggregate -> context/risk -> labels -> evaluation/gates`

First plugin: `oi_capacity`.

## Commit Series (P1-first)

### Commit 1: Contracts + Schema (No execution changes)

**Goal**
Add request contract support for optional context/risk config.

**Changes**
1. Update `schemas/quant_research_input.v2.json`:
   - Add optional `context_risk` array.
2. Update `schemas/quant_research_workflow_input.v2.json`:
   - Add optional `context_risk` array for stage-level usage (or workflow-level if chosen).
3. Update `pointline/research/contracts.py` validators:
   - Validate shape and required fields for each context spec.
4. Add schema fixtures:
   - Valid payloads with/without `context_risk`.
   - Invalid payloads (missing plugin, bad params types, bad mode allowlist).

**Tests**
1. Contract validation tests (positive + negative).
2. Ensure existing requests without `context_risk` still pass unchanged.

---

### Commit 2: New Context Package + Registry

**Goal**
Create context module primitives with strict registry governance.

**Changes**
1. Add package:
   - `pointline/research/context/__init__.py`
   - `pointline/research/context/config.py`
   - `pointline/research/context/registry.py`
   - `pointline/research/context/engine.py`
2. Define `ContextSpec`/metadata contract:
   - `name`, `plugin`, `required_columns`, `params`, `mode_allowlist`,
     `pit_policy`, `determinism_policy`, `version`.
3. Add `ContextRegistry`:
   - register/get/exists
   - mode validation
   - params validation
   - required-columns validation
4. Add `apply_context_plugins(frame, specs, mode)` in `engine.py`.

**Tests**
1. Registry tests:
   - registration/retrieval
   - unknown plugin rejection
   - mode mismatch rejection
   - params/required-columns validation.
2. Engine tests:
   - no-op when specs empty
   - deterministic application order.

---

### Commit 3: `oi_capacity` Plugin

**Goal**
Ship first production context plugin with useful capacity controls.

**Changes**
1. Add `pointline/research/context/plugins/oi_capacity.py`.
2. Register plugin with metadata and defaults:
   - params: `oi_col`, `price_col?`, `base_notional`, `lookback_bars`,
     `min_ratio`, `clip_min`, `clip_max`.
3. Emit columns:
   - `<name>_oi_notional` (if `price_col` present)
   - `<name>_oi_level_ratio`
   - `<name>_capacity_ok`
   - `<name>_capacity_mult`
   - `<name>_max_trade_notional`
4. PIT-safe rolling semantics:
   - partition by `exchange_id,symbol_id`
   - backward-only computation.

**Tests**
1. Formula unit tests:
   - standard case
   - zero OI
   - short history
   - null handling.
2. Determinism test:
   - identical input => identical output.

---

### Commit 4: Pipeline + Workflow Integration

**Goal**
Wire context/risk execution into canonical APIs.

**Changes**
1. Update `pointline/research/pipeline.py`:
   - after aggregate result, before labels:
     - `result = apply_context_plugins(result, compiled["context_risk"], mode)`
   - include context plan in `resolved_plan`.
2. Update `pointline/research/workflow.py`:
   - apply context plugins per stage according to contract.
3. Ensure output artifacts include context columns and plugin evidence.

**Tests**
1. Pipeline integration test:
   - bar_then_feature + `context_risk=[oi_capacity]` emits expected columns.
2. Workflow integration test:
   - stage with context plugin propagates outputs and lineage.
3. Regression tests:
   - runs without `context_risk` unchanged.

---

### Commit 5: Gates + Decision Integration

**Goal**
Make context/risk governance first-class in quality gates.

**Changes**
1. Add context gate checks in pipeline/workflow:
   - contract validity
   - required column presence
   - output completeness
   - determinism rerun consistency (context columns included in hash)
2. Add gate evidence to output payload under `quality_gates` and `artifacts`.
3. Enforce fail-fast:
   - critical context gate failure => `decision.status = reject`.

**Tests**
1. Gate failure tests:
   - missing columns
   - bad params
   - plugin missing.
2. Decision tests:
   - critical context failure blocks `go`.

---

### Commit 6: Docs + Usage Templates

**Goal**
Make human and agent usage easy and low mental load.

**Changes**
1. Add/update docs:
   - `docs/guides/research-pipeline-v2.md`
   - `docs/guides/research-workflow-hybrid.md`
   - include `context_risk` examples.
2. Add canonical snippets:
   - MFT bar request with OI features + `oi_capacity`.
   - workflow stage variant.
3. Add concise “when to use context/risk” guidance.

**Tests**
1. Example payload smoke tests (if test harness supports fixture execution).

---

### Commit 7: CI Governance Lock

**Goal**
Prevent architecture drift.

**Changes**
1. Add CI checks:
   - schema validation for `context_risk`.
   - plugin registry integrity checks.
   - deterministic rerun check for context-enabled fixtures.
2. Add guardrail test that context plugins are registry-backed only.

**Tests**
1. CI job green with context on/off scenarios.

## Acceptance Criteria

1. `context_risk` is optional and backward-safe.
2. `oi_capacity` is usable from both `research.pipeline` and `research.workflow`.
3. Context outputs are deterministic and artifacted.
4. Context gate failures are fail-fast and block `go`.
5. Full repository test suite remains green.

## Recommended Execution Order

1. Commit 1
2. Commit 2
3. Commit 3
4. Commit 4
5. Commit 5
6. Commit 6
7. Commit 7
