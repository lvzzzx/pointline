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

## Lightweight Team Cadence

- Weekly 30-minute sync with role leads:
- Infra/DQ: ingestion health, validation incidents, backfill status
- Research: feature requests, PIT issues, experiment blockers
- Platform: CI/tooling regressions and workflow improvements

- Monthly contract review:
- Validate schema docs against implementation
- Reconfirm PR routing and ownership map
