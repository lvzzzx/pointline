# Collaboration Playbook

This playbook defines how Pointline teams collaborate when humans and LLM agents work together.

It is operational guidance for day-to-day delivery and should be used with:
- `docs/roles-and-responsibilities.md` (role ownership and PR routing)
- `docs/architecture/design.md` (architecture constraints)

## Purpose

- Move faster without losing correctness.
- Make cross-role handoffs explicit and repeatable.
- Use LLM agents safely as collaborators and consumers.

## Operating Model

- One human DRI per initiative is required.
- Humans own decisions; LLM agents accelerate execution.
- Contract-first delivery: define semantics before implementation.
- PR is the integration gate, not the full collaboration process.

## End-to-End Workflow

### 1) Intake and Task Envelope

Before implementation, create a short task brief with:
- `Context`: affected modules/tables, links to existing docs
- `Goal`: exact expected outcome
- `Inputs/Outputs`: schema/API/CLI expectations and artifact paths
- `Constraints`: PIT, determinism/idempotency, performance bounds
- `Acceptance Checks`: tests, sample queries, expected pass criteria
- `Failure + Rollback`: abort/recovery plan

If this envelope is incomplete, work is not ready.

### 2) Ownership and Routing

Assign one DRI and required reviewer roles up front:
- Ingestion/storage semantics: lead `Data Infra`, reviewer `Data Quality`
- Validation/schema contracts: lead `Data Quality`, reviewer `Data Infra`
- Research API/features: lead `Research Engineer`, reviewer `Quant Researcher`
- Tooling/CI: lead `Platform/DevEx`

When scope crosses domains, the DRI remains single-threaded, with required co-reviewers.

### 3) Contract-First Design

Before writing implementation code, publish a minimal contract:
- schema and field requirements (required/optional)
- timestamp semantics (`ts_local_us`, `ts_exch_us`)
- deterministic ordering/tie-break keys
- data quality invariants and caveats
- compatibility and breaking-change notes

Any unresolved contract question blocks implementation.

### 4) Implementation and Collaboration

Use short execution cycles:
- LLM agent drafts code/tests/docs from the task envelope
- Human DRI reviews direction early (not only at final PR)
- Required reviewer role validates domain semantics continuously

Preferred split:
- LLM: scaffolding, test drafting, docs updates, repetitive refactors
- Human: risk tradeoffs, contract decisions, release-critical calls

### 5) Validation Gates

A change is not ready without all applicable gates:
- Infra gate: deterministic rerun/replay behavior
- DQ gate: contract checks, validation rule coverage, drift checks
- Research gate: PIT-safe behavior and no-lookahead assertions
- Quant gate: feature semantics and experiment usability
- DevEx gate: CI reliability and local developer ergonomics

Repository quality gates remain mandatory (`pytest`, lint, type checks, and required pre-commit hooks).

### 6) PR Integration Gate

PR must include:
- task envelope summary
- contract deltas (if any)
- test evidence and acceptance query results
- required role approvals

Merge only after all required role approvals and checks are green.

### 7) Post-Merge Feedback Loop

After merge:
- monitor ingestion/backfill and DQ signals
- validate research usability with canonical examples
- capture incidents and update contracts/tests/docs

Every incident should map to one of: missing contract, missing test, unclear ownership, or tooling gap.

## LLM Autonomy Levels

Default to `Level 1` unless explicitly raised/lowered.

- `Level 0 (Auto)`:
  - docs fixes, formatting, non-semantic cleanups
- `Level 1 (Guarded)`:
  - implementation allowed with tests and mandatory quality gates
- `Level 2 (Approval Required)`:
  - schema contracts, PIT semantics, storage/replay behavior, release-critical logic

For `Level 2`, explicit owner approval is required before merge.

## Escalation Rules

Escalate immediately when:
- ownership is ambiguous
- contract text conflicts with implementation
- required envelope context is missing
- a reviewer identifies PIT/leakage or determinism risk

Escalation path:
1. Pause implementation.
2. Assign/confirm DRI using `Decision Guidelines` in `docs/roles-and-responsibilities.md`.
3. Resolve contract ambiguity in writing.
4. Resume with updated acceptance checks.

## Working Example: New Options Table Request

Use this flow when `Quant Researcher` requests options data.

1. Intake (`Quant Researcher`)
- Provide hypothesis, required fields/granularity, PIT constraints, acceptance queries.

2. Ownership
- Lead: `Data Infra`
- Required reviewers: `Data Quality`, `Research Engineer`, `Quant Researcher`

3. Contract
- Define schema, timestamps, ordering keys, partition/backfill strategy, caveats.

4. Build
- LLM assists with ingestion/table plumbing/tests/docs under `Level 1`.
- Human owner approves any `Level 2` contract decisions.

5. Validate
- Determinism + DQ + PIT/no-lookahead + research usability checks all pass.

6. Merge + Follow-up
- Merge with required approvals.
- Run initial backfill monitoring and update docs from learnings.

## Cadence and Metrics

Weekly (30 min):
- ingestion reliability, DQ incidents, research blockers, DevEx regressions

Monthly:
- contract drift review
- routing/approval bottleneck review
- autonomy policy tuning

Track:
- review turnaround time
- handoff cycle time
- rework/reopen rate
- incident rate from boundary ambiguity
- first-pass CI success for LLM-assisted changes
