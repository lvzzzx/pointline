# Pointline Architecture (One-Page Design)

**Status:** Live
**Scope:** Local-host offline research data lake (single machine)

## 1) System Purpose

Pointline provides deterministic, point-in-time-safe ingestion and research on market data using a local Delta Lake stack.

Core goals:
- PIT correctness for replay and feature computation.
- Deterministic reruns with stable lineage.
- Fast local iteration with operational simplicity.

## 2) Canonical Stack

- Storage: Delta Lake (`delta-rs`)
- Compute: Polars (primary), DuckDB (ad hoc SQL)
- Runtime model: local filesystem only (no distributed/cloud backend)

## 3) Data Lake Model

- **Bronze**: Immutable raw vendor files and API captures.
- **Silver**: Typed normalized tables with lineage (`file_id`, `file_line_number`).
- **Gold**: Optional derived tables when justified by query demand.

Default partitioning: `exchange`, `date`.

## 4) Ingestion Contract

Pipeline stages:
1. Discover Bronze files.
2. Skip already successful files via `silver.ingest_manifest`.
3. Parse/normalize to Silver schema.
4. Resolve symbols via `dim_symbol` (SCD2, PIT validity windows).
5. Apply validation + quarantine rules.
6. Write Silver table (idempotent semantics).
7. Record status in manifest and validation log.

Identity key for file-level tracking:
`(vendor, data_type, bronze_file_name, sha256)`.

## 5) Research Contract

Canonical APIs:
- `research.pipeline(request: QuantResearchInputV2) -> QuantResearchOutputV2`
- `research.workflow(request: QuantResearchWorkflowInputV2) -> QuantResearchWorkflowOutputV2`

Execution invariants:
- Half-open windows: `[T_prev, T)`.
- Backward-only feature direction (forward logic is label-only).
- Deterministic sort/tie-break keys:
  `exchange_id, symbol_id, ts_local_us, file_id, file_line_number`.
- Registry-governed operators/rollups; fail-fast gates on PIT/determinism violations.

## 6) Layering Rules

- **Domain (`pointline/tables/*`)**: pure schema/transform logic, no IO side effects.
- **Services (`pointline/services/*`)**: orchestration and sequencing.
- **IO (`pointline/io/*`)**: Delta read/write/merge, maintenance, physical layout.

## 7) Operational Defaults

- Correctness over throughput.
- Explicit operator-driven maintenance (`optimize`, `vacuum`).
- No backward compatibility guarantee for legacy contracts in this clean-break architecture.

## 8) Out of Scope

- Live execution/routing.
- Cloud object storage.
- Distributed compute/storage backends.
- Automatic migration of historical legacy formats.
