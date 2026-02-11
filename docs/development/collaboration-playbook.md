# Collaboration Playbook

How Pointline gets built: one human (DRI for everything) + LLM agents as execution partners.

Read with: `docs/architecture/design.md` (architecture constraints).

## Principles

- You own all decisions. LLM agents accelerate execution, not judgment.
- LLM outputs are proposals until you approve.
- Contract-first for risky changes: define semantics before implementation.
- Quality gates are non-negotiable regardless of who wrote the code.

## Workflow

### 1) Decide scope and risk level

Before starting, decide the autonomy level:

| Level | What | Task brief needed | Your involvement |
|-------|------|-------------------|------------------|
| L0 (Auto) | Formatting, typo fixes, non-semantic refactors | None | Review diff |
| L1 (Guarded) | Code/test changes with clear requirements | Goal + constraints | Review direction early, verify tests pass |
| L2 (Approval) | Schema contracts, PIT semantics, storage/replay behavior | Full envelope | Approve contract before implementation starts |

**Default is L1.**

### 2) Task brief (L1 and L2)

**L1 (short form):** State the goal and any constraints inline. Example:
> Add kline_1h ingestion for tardis. Follow existing trades ingestion pattern. Must be idempotent. Tests required.

**L2 (full envelope):** Required fields:
- `Context`: affected modules/tables, current behavior
- `Goal`: exact expected outcome
- `Constraints`: PIT, determinism, performance bounds
- `Acceptance`: tests and quality gates that must pass
- `Rollback`: what to do if it breaks

If the envelope is incomplete for an L2 change, stop and fill it in first.

### 3) Build

Preferred split:
- **LLM:** scaffolding, test drafting, docs, repetitive refactors, exploratory analysis
- **You:** risk tradeoffs, contract decisions, release-critical calls, final review

For L1+, review direction early (not only at final diff).

### 4) Validate

Every change must pass applicable gates before merge:

- **Always:** `pytest`, `ruff check`, `ruff format`, pre-commit hooks
- **Ingestion/storage changes:** deterministic rerun produces identical output
- **Schema/validation changes:** contract checks, edge-case coverage
- **Research API changes:** PIT-safe behavior, no lookahead bias
- **Feature/signal changes:** leakage checks, regime robustness

### 5) Post-merge

- Monitor ingestion/backfill and DQ signals
- If something breaks, classify the root cause: missing contract, missing test, or tooling gap
- Update contracts/tests/docs accordingly

## Self-Review Checklists

Since there's no second reviewer, use these checklists to catch domain-specific issues.

### Ingestion & Storage
- [ ] Deterministic: same inputs + metadata produce same outputs?
- [ ] Idempotent: safe to re-run without duplicates or corruption?
- [ ] Failure recovery: what happens on partial failure mid-write?
- [ ] Data layout: partition strategy and Z-ordering still optimal?
- [ ] Performance: acceptable for full historical backfill?

### Data Quality & Schema
- [ ] Schema contract documented and matches implementation?
- [ ] Validation rules cover known bad-data paths?
- [ ] Breaking change: any downstream consumer affected?
- [ ] False positive/negative risk in validation rules?

### Research & PIT Correctness
- [ ] No lookahead bias: uses `ts_local_us` (arrival time) for replay?
- [ ] Joins are as-of joins, not exact joins?
- [ ] Symbol resolution via `dim_symbol`, not raw exchange symbols?
- [ ] Fixed-point decoded only at final output, not mid-pipeline?
- [ ] Reproducible: symbol_ids, timestamp column, and params recorded?

### Signal & Feature
- [ ] Signal definition is leakage-free?
- [ ] Tested across multiple regimes?
- [ ] Practical tradability assumptions documented?

### Tooling & DevEx
- [ ] CI still passes?
- [ ] Local dev workflow unbroken?
- [ ] Pre-commit hooks still work?

## LLM-Specific Review Items

LLM agents have specific failure modes. Watch for these:
- **Hallucinated APIs:** Verify every function call exists in the codebase
- **Plausible but wrong math:** Double-check fixed-point encoding, timestamp conversions, bitwise operations
- **Over-engineering:** Reject unnecessary abstractions, feature flags, or "just in case" error handling
- **Stale patterns:** LLM may use patterns from older code that has since been refactored

## Escalation

Stop and think before proceeding when:
- You're unsure about PIT/determinism implications
- A change touches schema contracts or storage layout
- LLM output conflicts with documented contracts
- The task feels underspecified for its risk level

Escalation for a solo team means: pause, write down the ambiguity, resolve it in docs, then resume.

## Research Personas

Design APIs and contracts with these personas in mind.

### HFT Researcher
- Microsecond timestamp fidelity and deterministic event ordering
- Replay correctness for high message-rate streams
- Queue-position and fill-quality proxies
- **Implication:** preserve strict ordering (`ts_local_us`, tie-break keys), keep lineage first-class

### MFT Researcher
- PIT-correct multi-stream alignment over longer horizons
- Cost-aware alpha signals (spread, slippage, funding, fees)
- Cross-venue dislocation analysis
- **Implication:** composable PIT-safe feature pipelines, ergonomic cross-table joins

### Shared
- No lookahead bias
- Reproducible and idempotent pipelines
- Stable, explicit schema contracts
- Actionable data quality diagnostics

## Working Example: New Table Request

1. **Scope it:** Is this L1 (follows existing pattern) or L2 (new schema contract)?
2. **Brief:** Write goal + constraints (L1) or full envelope (L2)
3. **Contract first (L2):** Define schema, timestamps, ordering keys, partition strategy, caveats
4. **Build:** LLM drafts ingestion/table/tests/docs. You review direction early.
5. **Validate:** Run all applicable gates from the checklist above
6. **Merge + monitor:** Watch first backfill, update docs from learnings
