# ADR: Research Framework v2 Clean Architecture

- Status: Proposed
- Date: 2026-02-08
- Decision Makers: Research Engineer, Quant Researcher, Data Infra Engineer
- Scope: `pointline/research` framework architecture

## Context

The current research framework has multiple overlapping paths:

- Event-joined feature orchestration via `pointline.research.features`.
- Emerging resample/aggregate primitives via `pointline.research.resample`.
- Mixed semantics and mixed discoverability at the top-level API.

This creates ambiguity for users and operational risk for production research:

- Different paths can produce different timeline semantics.
- PIT and determinism controls are not centralized in one mandatory execution path.
- Registry-driven contracts and family-style feature functions coexist without a single governance boundary.

The team explicitly prefers a clean architecture with no backward compatibility constraints.

## Decision

Adopt a contract-first, pipeline-first architecture where all production research execution goes through one canonical entrypoint and one canonical timeline policy.

## Accepted Decisions

### D1. One Production API

Use a single official API for production execution:

- `research.pipeline(request: InputSchemaV1) -> OutputSchemaV1`

No alternate production path is supported.

### D2. Contract-First Execution

All runs must use explicit versioned schemas:

- `input.v1` for request and configuration
- `output.v1` for results, diagnostics, gates, and artifacts

Implicit defaults that change behavior are disallowed.

### D3. Centralized PIT Timeline Semantics

Use one canonical bucket/timeline contract across all modes:

- Bar/window semantics: `[T_prev, T)` with strict upper bound enforcement (`ts < T`)
- As-of joins default to backward
- Feature windows backward-only; forward windows label-only

No secondary clock or bucket semantics are allowed in production code.

### D4. Mode-First, Same Interface

Treat these as first-class execution modes under the same API:

- `event_joined`
- `tick_then_bar`
- `bar_then_feature`

Modes differ in execution plan, not in public API shape.

### D5. Registry-First Operators

All production feature and aggregation logic must be registered typed operators with:

- stage
- required columns
- mode allowlist
- PIT policy
- determinism requirements

Ad-hoc family-chaining is not a production orchestration model.

### D6. Determinism Is Mandatory

Execution must enforce deterministic ordering keys (including tie-breakers where required).

Stateful transforms (for example `diff`, rolling, `pct_change`) must be partitioned by keys (`exchange_id`, `symbol_id`) unless explicitly justified in operator policy.

### D7. Quality Gates and Artifacts Are Mandatory

Every run emits:

- normalized config hash
- input data evidence (coverage/probes)
- PIT/leakage checks
- determinism checks
- output schema version
- run status and decision payload

Critical gate failures block `go` outcomes.

## Rejected Alternatives

### R1. Keep dual production paths (`features` and `resample`) long-term

Rejected because it preserves ambiguity and weakens governance.

### R2. Keep manual resample/aggregate as a recommended production pattern

Rejected because it bypasses centralized PIT and determinism enforcement.

### R3. Maintain backward compatibility as primary constraint

Rejected by explicit project preference for a clean architecture cutover.

### R4. Allow operator code outside registry contracts

Rejected because it undermines validation, mode safety, and reproducibility.

## Target High-Level Architecture

1. Data Access Layer
- Deterministic table access, symbol resolution, schema validation

2. Timeline Layer
- Spine builders, bucket assignment, join semantics, partition policy

3. Operator Layer
- Typed feature/aggregation registry and semantic policies

4. Pipeline Compiler
- Compiles `input.v1` into an executable mode-specific DAG

5. Execution + Validation Layer
- Executes DAG and enforces PIT, leakage, and determinism gates

6. Artifact Layer
- Persists run metadata, diagnostics, metrics, and decision outputs

## Cutover Plan (No Backward Compatibility)

### Phase 0: Freeze

- Freeze new capability additions to legacy orchestration paths.
- Allow only bug fixes required for stability before cutover.

### Phase 1: Canonical Contract

- Finalize `input.v1` and `output.v1`.
- Publish mode semantics and required invariants.

### Phase 2: Pipeline Compiler + Registry Completion

- Route all three modes through compiler/execution pipeline.
- Migrate family formulas into typed registry operators.

### Phase 3: Hard Switch

- Remove deprecated production entrypoints.
- Remove compatibility wrappers and alternate orchestration APIs.

### Phase 4: Cleanup

- Remove obsolete docs and examples that describe manual production patterns.
- Keep one canonical user guide and one architecture reference.

## Cutover Acceptance Criteria

All criteria must pass before declaring v2 complete:

1. One production entrypoint exists and is documented.
2. All production modes execute through the same compiler/validator pipeline.
3. No conflicting bucket semantics remain.
4. PIT, leakage, and determinism gates are mandatory and tested.
5. Registry coverage includes all production feature/aggregation operators.
6. Integration tests exist for each mode with multi-symbol partition safety.
7. Run artifacts are complete and machine-validated against `output.v1`.

## Consequences

Positive:

- Cleaner mental model and onboarding.
- Stronger reproducibility and governance.
- Lower long-term maintenance cost from reduced surface duplication.

Trade-offs:

- Short-term migration effort.
- Breaking changes for existing internal callers that bypass pipeline contracts.

## Open Issues To Resolve In Design Spec

1. Final shape of `input.v1` and `output.v1` fields.
2. Operator packaging and versioning scheme for registry entries.
3. Policy for sandboxing/approval of new custom operators.
4. Performance baselines and SLOs by mode.
