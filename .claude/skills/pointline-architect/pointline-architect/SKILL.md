---
name: pointline-architect
description: >-
  Pointline data lake architecture and design guidance. Use when:
  (1) designing new tables, modules, or subsystems for the pointline data lake,
  (2) evaluating design trade-offs (encoding, partitioning, storage, schema evolution),
  (3) writing or reviewing ExecPlans or research proposals,
  (4) assessing change risk levels (L0/L1/L2) for PRs,
  (5) reviewing code for PIT correctness, determinism, or invariant violations,
  (6) planning new vendor integrations or data source onboarding,
  (7) making architectural decisions about schema, storage, or pipeline extensions,
  (8) understanding why existing design choices were made (function-first, fixed-point,
  quarantine-over-drop, no backward compatibility, SCD2),
  (9) extending the module dependency graph without introducing cycles.
---

# Pointline Data Lake — Architecture & Design

Architecture guidance for extending pointline's PIT-accurate offline data lake. Use this skill when making design decisions, not when following existing patterns (use **pointline-infra** for execution).

## Design Principles

These are non-negotiable. Every design decision must satisfy all five:

1. **PIT correctness** — No lookahead bias. Symbol resolution via as-of joins on validity windows. Event ordering by `ts_event_us`. Forward-only alignment.
2. **Deterministic replay** — Same inputs + metadata → byte-identical outputs. Composite tie-break keys for total ordering. No random, no wall-clock timestamps in data path.
3. **Idempotent ingestion** — Re-processing the same file produces no duplicates. Identity = `(vendor, data_type, bronze_path, file_hash)`.
4. **Lineage traceability** — Every Silver row traces back to Bronze via `file_id` + `file_seq`. No data appears without provenance.
5. **No silent data loss** — Invalid rows quarantined with reason, never dropped. Coverage gaps are visible.

## Architectural Decisions & Rationale

| Decision | Rationale | Alternative Rejected |
|---|---|---|
| **Function-first** over classes | Testable (pure in/out), composable, debuggable, no hidden state | OOP with stateful pipeline objects |
| **Fixed-point Int64** over float/Decimal | Exact equality, O(1) comparison, no precision drift mid-pipeline, 8 bytes vs 16+ | Float64 (rounding), Decimal128 (slow, large) |
| **Quarantine over drop** | Auditability, coverage reporting, debug-ability. Every row accounted for | Silent filter (invisible data loss) |
| **No backward compat** | Bounded re-ingestion cost vs unbounded compatibility complexity. Bronze is immutable → rebuild is always possible | Migration scripts (compound over time) |
| **Delta Lake** | Partition pruning, ACID, time travel, schema enforcement, compaction. Single library, no external service | Raw Parquet (no ACID), DuckDB (no partitioned writes), PostgreSQL (no columnar) |
| **SCD Type 2** for symbols | PIT-correct metadata at any historical timestamp. Validity windows enable as-of joins | SCD1 (lose history), snapshot tables (expensive joins) |
| **Protocols over ABCs** | Structural typing, no inheritance hierarchy, test doubles without mocking frameworks | Abstract base classes (rigid hierarchy) |

Full rationale with trade-off analysis: [references/design-rationale.md](references/design-rationale.md)

## Module Dependency Graph

```
schemas (leaf)
   ↑
protocols ← depends on schemas for type hints
   ↑
   ├── ingestion ← schemas, protocols, dim_symbol
   ├── storage   ← schemas, protocols
   └── research  ← schemas, dim_symbol

vendors ← schemas only (no other internal deps)
```

**Rules for extending:**
- `schemas/` has zero internal dependencies. Keep it leaf.
- `vendors/` depends only on `schemas/`. Never import from `ingestion/`, `storage/`, or `research/`.
- `research/` never imports from `ingestion/` or `storage/` (except via protocols).
- New modules must fit the DAG. Circular dependencies = design smell.

## Change Risk Assessment

Before any PR, classify the risk level:

| Level | Scope | Review Required | Examples |
|---|---|---|---|
| **L0** | Formatting, typos, non-semantic | Self-merge OK | Ruff fixes, docstring typos |
| **L1** | Code/test with clear requirements | Tests pass, standard review | New parser, new validation rule, new test |
| **L2** | Schema, PIT semantics, storage/replay | Explicit approval needed | New table spec, change tie-break keys, modify dim_symbol upsert logic, change partitioning |

**L2 triggers** — any change to:
- `pointline/schemas/` (table definitions)
- `pointline/dim_symbol.py` (SCD2 logic)
- `pointline/storage/contracts.py` (storage protocols)
- Tie-break keys or partition_by in any spec
- PIT join semantics in `ingestion/pit.py` or `research/primitives.py`
- Encoding rules (PRICE_SCALE, QTY_SCALE, timestamp format)

## Design Review Checklist

Apply when reviewing any L1+ change:

- [ ] **Deterministic?** Same inputs → same outputs? No wall-clock, no random?
- [ ] **Idempotent?** Re-running safe? No duplicate rows?
- [ ] **No lookahead?** Symbol resolution via as-of join? Forward-only alignment?
- [ ] **`ts_local_us` preserved?** Replay fidelity maintained?
- [ ] **As-of joins, not exact?** Validity windows, not equality?
- [ ] **Fixed-point intact?** No decode mid-pipeline? Decode only at research output?
- [ ] **Quarantine, not drop?** Invalid rows captured with reason?
- [ ] **Tie-break keys correct?** Full deterministic ordering?
- [ ] **Module boundaries respected?** No dependency cycle introduced?
- [ ] **Schema-as-code?** Spec read before touching table definitions?

## Planning & Proposal Standards

**ExecPlans** (for features/refactors) — follow `.agent/PLANS.md`:
- Self-contained: a novice can implement end-to-end
- Living document with Progress, Surprises, Decision Log, Outcomes
- Concrete steps with exact file paths and expected output
- Validation and acceptance criteria
- Idempotence and recovery plan

**Research Proposals** — follow `.agent/PROPOSALS.md`:
- Testable hypothesis in plain language
- Data sources with PIT constraints
- Experiment design: splits, baselines, ablations
- Evaluation metrics with pass/fail thresholds
- 1-5 clarifying questions (decision-oriented, with defaults)

Full planning reference: [references/planning.md](references/planning.md)

## Design Patterns for Extension

### Adding a New Event Table

1. **Ask:** Does it belong in the existing schema hierarchy? Could it be a column on an existing table instead?
2. **Define spec** in `pointline/schemas/` — set columns, tie-break keys, partition_by, scaled columns
3. **Determine validation rules** — what constitutes an invalid row?
4. **Ensure PIT coverage** — does the new table need dim_symbol resolution?
5. **Update pipeline** — add table alias, validation dispatch, any canonicalization
6. **Write research API** — does `load_events()` need special handling?

### Adding a New Vendor

1. **Map vendor schema** to canonical pointline tables (which existing tables? new tables needed?)
2. **Create parser module** at `pointline/vendors/<vendor>/` — depends only on `schemas`
3. **Handle exchange mapping** — add to `EXCHANGE_TIMEZONE_MAP` if new exchanges
4. **Plan canonicalization** — how do vendor column names/encodings map to canonical form?
5. **Define Bronze layout** — archive format, file naming convention, extraction pipeline
6. **Test with real data** — sample files in `tests/fixtures/`

### Adding a New Dimension

1. **Evaluate SCD type** — Type 1 (overwrite) vs Type 2 (versioned) vs Type 3 (limited history)
2. **Define natural key** and tracked columns (which changes trigger new version?)
3. **Design validity semantics** — `valid_from_ts_us <= event_ts < valid_until_ts_us`?
4. **Plan bootstrap path** — initial load from vendor snapshot
5. **Plan incremental path** — upsert logic, delisting/removal handling
6. **Add storage contract** — new protocol or extend `DimensionStore`?

### Modifying Schema Encoding

**This is always L2.** Schema changes = rebuild. Checklist:
1. Is the change necessary or can the goal be achieved differently?
2. What's the rebuild cost? (time, compute, Bronze still available?)
3. Does the change affect downstream consumers (research API, feature pipelines)?
4. Update spec, rebuild, validate, update research API

Full extension patterns: [references/extension-patterns.md](references/extension-patterns.md)

## Data Modeling Decisions

### Why Int64 Microseconds for Timestamps

- Microsecond resolution matches exchange feed precision
- Int64 arithmetic is fast and exact (no float rounding)
- UTC avoids timezone ambiguity in storage
- Exchange-local conversion only for `trading_date` derivation

### Why Fixed-Point 10^9 Scale

- 9 decimal places covers all exchange price/quantity precision
- Single scale factor simplifies code (no per-column scale lookup at runtime)
- Int64 range: ±9.2 × 10^18 → max representable value ~9.2 × 10^9 in decoded units
- Sufficient for crypto (BTC ~$100k = 100,000 × 10^9 fits) and equities

### Why (exchange, trading_date) Partitioning

- Query pattern: researchers load single exchange + date range
- Partition pruning eliminates 99%+ of data on typical queries
- One partition per exchange-day keeps file counts manageable
- `trading_date` derived from exchange-local time (not UTC date) matches researcher mental model

### Why Composite Tie-Break Keys

- Exchange timestamps have ties (multiple events at same microsecond)
- `file_id` + `file_seq` provide total ordering within ties
- Deterministic sort = reproducible research results
- Different tables need different keys (orderbook adds `book_seq`)

## Non-Goals (Explicit Boundaries)

- **Not real-time.** Batch ingestion only. No streaming, no pub/sub.
- **Not multi-user.** Single-machine, single-writer. No concurrent write conflict resolution beyond OCC on dim_symbol.
- **Not distributed.** No Spark, no cluster. Polars on one machine.
- **Not a trading system.** Research only. No order management, no execution.
- **Not backward compatible.** Schema change = rebuild from Bronze. Explicitly chosen trade-off.
