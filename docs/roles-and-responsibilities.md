# Domain Ownership Map

Single-owner project. This document maps code areas to domain concerns for self-review and LLM agent context.

For workflow and collaboration protocol, see `docs/development/collaboration-playbook.md`.

## Domain Areas

### Ingestion & Storage
**Concern:** Reliable, reproducible, idempotent data pipelines.

**Code:**
- `pointline/io/`
- `pointline/services/`
- `pointline/cli/commands/ingest.py`
- `pointline/cli/commands/delta.py`
- `pointline/io/delta_manifest_repo.py`

**Review focus:** deterministic behavior, re-run safety, failure recovery, data layout impact.

### Data Quality & Schema
**Concern:** Trust in datasets through validation and contracts.

**Code:**
- `pointline/tables/` (schemas, `validate_*`, normalization)
- `pointline/dq/`
- `pointline/tables/validation_log.py`
- `tests/` (validation and edge-case coverage)

**Review focus:** false positive/negative risk, contract-breaking changes, bad-data path coverage.

### Research API
**Concern:** PIT-correct, reproducible research interfaces.

**Code:**
- `pointline/research/`
- `pointline/research/features/`
- `docs/guides/`
- `examples/`

**Review focus:** lookahead bias prevention, API usability, feature correctness.

### Experiment & Signal
**Concern:** Alpha hypotheses with robust experimental design.

**Artifacts:**
- `research/03_experiments/`
- Research notes, experiment scripts, feature specs

**Review focus:** signal validity, leakage risk, regime behavior, tradability.

### Tooling & DevEx
**Concern:** Fast, reliable local and CI workflows.

**Code:**
- `pyproject.toml`
- `.pre-commit-config.yaml`
- CI config files
- Development setup docs

**Review focus:** developer friction, CI reliability, quality gate consistency.

## Risk Priority

When a change spans multiple domains, prioritize review by correctness impact:

1. PIT / lookahead bias (highest risk — silent, hard to detect)
2. Schema / storage contracts (breaking changes propagate widely)
3. Determinism / idempotency (affects reproducibility)
4. Data quality rules (false negatives let bad data through)
5. API usability / tooling (lowest blast radius)
