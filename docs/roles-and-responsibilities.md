# Roles and Responsibilities

This document defines practical role boundaries for Pointline so we can move faster with clear ownership and clean handoffs.

## Core Roles

### 1) Data Infra Engineer

**Mission:** Keep ingestion and storage reliable, scalable, and reproducible.

**Owns:**
- Bronze to Silver ingestion pipelines and operational reliability
- Delta Lake layout, partitioning strategy, compaction/maintenance
- Backfills, reprocessing, and manifest-driven idempotency
- Runtime/performance tuning for large historical loads

**Primary code areas:**
- `pointline/io/`
- `pointline/services/`
- `pointline/cli/commands/ingest.py`
- `pointline/cli/commands/delta.py`
- `pointline/io/delta_manifest_repo.py`

**Key PR review focus:**
- Deterministic behavior and re-run safety
- Failure handling and recovery semantics
- Data layout/performance impact

### 2) Data Quality Engineer

**Mission:** Enforce trust in datasets through explicit validation and contracts.

**Owns:**
- Table-level validation rules and quality checks
- Schema contract integrity and migration safety
- Data drift detection and incident triage
- Cross-table consistency checks and DQ reporting

**Primary code areas:**
- `pointline/tables/` (`validate_*`, schema normalization)
- `pointline/dq/`
- `pointline/tables/validation_log.py`
- `tests/` (validation and edge-case coverage)

**Key PR review focus:**
- False positive/negative risk in rules
- Contract-breaking schema changes
- Test coverage for bad data paths

### 3) Research Engineer

**Mission:** Build PIT-correct, reproducible research interfaces and feature pipelines.

**Owns:**
- Core and convenience research APIs
- PIT-safe joins and deterministic query patterns
- Feature framework and reusable research utilities
- Research-facing docs and examples

**Primary code areas:**
- `pointline/research/`
- `pointline/research/features/`
- `docs/guides/`
- `examples/`

**Key PR review focus:**
- Lookahead bias prevention
- API usability and reproducibility
- Feature correctness and composability

### 4) Quant Researcher

**Mission:** Define and validate alpha hypotheses with robust experimental design.

**Owns:**
- Signal hypotheses and feature requirements
- Experiment specs, metrics, and interpretation
- Regime analysis and strategy diagnostics

**Primary artifacts:**
- Research notes, experiment scripts/notebooks, feature specs
- PRs proposing new features or transformations with rationale

**Key PR review focus:**
- Signal validity and leakage risk
- Statistical robustness and regime behavior
- Practical tradability assumptions

### 5) Platform / DevEx

**Mission:** Keep local and CI workflows fast, reliable, and easy to adopt.

**Owns:**
- Tooling (`uv`, pre-commit, lint/type/test setup)
- CI guardrails and developer workflow docs
- Build/release hygiene and repo ergonomics

**Primary code areas:**
- `pyproject.toml`
- `.pre-commit-config.yaml` (if present)
- CI config files
- `docs/README.md`, development setup docs

**Key PR review focus:**
- Developer friction and onboarding quality
- CI reliability and runtime
- Consistency of quality gates

## Ownership Map (By Area)

- `pointline/io/**`, `pointline/services/**`, ingest CLI: `Data Infra` + `Data Quality`
- `pointline/tables/**`, `pointline/dq/**`: `Data Quality` + `Data Infra`
- `pointline/research/**`, `pointline/research/features/**`: `Research Engineer` + `Quant Researcher`
- `docs/guides/**`, `examples/**`: `Research Engineer` + `Quant Researcher`
- Tooling and CI files: `Platform/DevEx`

## PR Routing Rules

Use these as default approval expectations:

1. Ingestion/storage semantics changes:
- Required: `Data Infra`
- Required: `Data Quality` when schema/validation changes

2. Validation/schema rule changes:
- Required: `Data Quality`
- Required: `Data Infra` if ingestion behavior or storage contracts change

3. Research API/feature changes:
- Required: `Research Engineer`
- Required: `Quant Researcher` for signal/feature semantics

4. Tooling/CI workflow changes:
- Required: `Platform/DevEx`
- Optional: role-specific owner if runtime behavior changes

## Handoff Contracts

### Infra -> Research

Before research consumes a table:
- Schema and required columns are documented
- PIT timeline semantics are explicit (`ts_local_us` vs `ts_exch_us`)
- Deterministic ordering keys are defined
- Known quality caveats are listed

### Research -> Infra

When requesting new data support:
- Feature/use-case and required granularity are explicit
- Latency/timestamp expectations are explicit
- Backfill window and volume estimates are provided
- Acceptance tests and minimal sample queries are included

## Decision Guidelines

When ownership is unclear:
1. If it changes storage/replay semantics, `Data Infra` leads.
2. If it changes validity/contract semantics, `Data Quality` leads.
3. If it changes user-facing research behavior, `Research Engineer` leads.
4. If it changes signal definition or evaluation, `Quant Researcher` leads.
5. If it changes team workflow/tooling, `Platform/DevEx` leads.

## Research Persona Requirements

`Quant Researcher` is the primary end user of the data lake and research APIs, but not all quant workflows have the same requirements. We should design interfaces and contracts with persona-specific constraints in mind.

### HFT Research Persona

**Typical concerns:**
- Microsecond timestamp fidelity and deterministic event ordering
- Replay correctness for high message-rate streams
- Latency sensitivity and execution modeling assumptions
- Queue-position and fill-quality proxies

**Implications for the repo:**
- Preserve strict ordering semantics (`ts_local_us`, tie-break keys)
- Keep lineage fields and deterministic replay guarantees first-class
- Document timestamp semantics and edge cases clearly

### MFT Research Persona

**Typical concerns:**
- PIT-correct multi-stream alignment over longer horizons (seconds to minutes)
- Feature stability across regimes and time periods
- Cost-aware alpha signals (spread, slippage, funding, fees)
- Cross-venue dislocation analysis (perp/spot, funding, OI)

**Implications for the repo:**
- Provide composable PIT-safe feature pipelines
- Emphasize reproducible feature generation and clear versioning
- Make cross-table joins ergonomic and bias-resistant

### Shared Requirements Across Personas

- No lookahead bias in APIs and examples
- Reproducibility and idempotent pipelines
- Stable, explicit schema contracts
- Reliable data quality checks with actionable diagnostics

## LLM Collaboration Protocol

This protocol applies when LLM agents are used as collaborators and when LLMs are end users of Pointline interfaces.

### Principles

- Human role owners remain accountable for correctness, risk decisions, and final approvals.
- LLM outputs are treated as proposals unless the task is explicitly marked as safe for autonomous execution.
- Any artifact intended for LLM consumption must be explicit, machine-readable where possible, and versioned.

### Human-in-the-Loop Ownership

- Every task must declare one human lead role (`Data Infra`, `Data Quality`, `Research Engineer`, `Quant Researcher`, or `Platform/DevEx`).
- Required reviewers follow the PR routing rules in this document.
- If an LLM proposes a contract-affecting change, the owning human role must approve before merge.

### Standard LLM Task Envelope

Any task delegated to an LLM (or handed off between personas through LLM tooling) must include:

- `Context`: relevant tables/modules, current behavior, and linked docs
- `Goal`: exact expected outcome
- `Inputs/Outputs`: paths, schemas, CLI/API signatures, and artifact format
- `Constraints`: PIT rules, determinism/idempotency requirements, performance bounds
- `Acceptance Checks`: tests, queries, and quality gates that must pass
- `Failure + Rollback`: what to do if checks fail or regressions are detected

Tasks without this envelope are considered not ready for LLM execution.

### LLM-Ready Artifacts By Role

- `Data Infra`:
  - Replay/order invariants, partitioning rules, and idempotency guarantees are documented alongside ingestion code.
  - Backfill/reprocess procedures include deterministic rerun expectations.
- `Data Quality`:
  - Validation rules are explicit about severity, failure conditions, and remediation guidance.
  - Schema contracts define required/optional fields and breaking-change criteria.
- `Research Engineer`:
  - Research APIs and feature pipelines include PIT-safe usage examples and anti-leakage guidance.
  - Join semantics and timeline assumptions are documented in examples and references.
- `Quant Researcher`:
  - Feature requests and experiment specs include leakage checks, metrics, and regime assumptions.
  - Strategy diagnostics are reproducible from committed artifacts.
- `Platform/DevEx`:
  - Canonical local/CI commands and expected pass conditions are documented and kept current.
  - Tooling updates include migration notes for humans and automation/agents.

### Autonomy Levels

- `Level 0 (Auto)`:
  - Safe mechanical changes (formatting, typo/docs fixes, non-semantic refactors).
- `Level 1 (Guarded)`:
  - Code/test changes allowed only with required tests and quality gates passing.
- `Level 2 (Approval Required)`:
  - Schema contracts, PIT semantics, storage/replay behavior, or release-critical logic require explicit human owner approval.

Default level is `Level 1` unless the task is explicitly marked otherwise.

### Validation and CI Expectations

- CI must fail when contract-critical documentation is missing or inconsistent with implementation.
- Any change affecting PIT semantics must include tests that demonstrate no lookahead behavior.
- Any change affecting ingestion or storage semantics must include deterministic rerun/replay validation.
- LLM-generated or LLM-assisted changes follow the same `pytest`, lint, and type-check gates as human-authored changes.

### Escalation Rules

- If ownership is ambiguous, use the Decision Guidelines section and assign a temporary lead before implementation continues.
- If an LLM output conflicts with documented contracts, stop and escalate to the owning human role.
- If required context is missing, do not proceed with autonomous changes; first complete the task envelope.

## Lightweight Team Cadence

- Weekly 30-minute sync with role leads:
- Infra/DQ: ingestion health, validation incidents, backfill status
- Research: feature requests, PIT issues, experiment blockers
- Platform: CI/tooling regressions and workflow improvements

- Monthly contract review:
- Validate schema docs against implementation
- Reconfirm PR routing and ownership map
