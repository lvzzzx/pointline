# Local-Host Data Lake Design (Clean Break)

This document defines the **current target architecture** for Pointline on a single machine.

Scope:
- local filesystem only
- Delta Lake + Polars + DuckDB only
- deterministic ingestion/research workflows
- **no backward compatibility guarantees**

Out of scope:
- distributed storage
- cloud object stores
- legacy schema migrations
- non-local execution backends

---

## 1) Design Goals

1. Point-in-time (PIT) correctness for replay and research.
2. Deterministic re-runs with stable lineage keys.
3. Fast local iteration for ingestion and query workloads.
4. Operational simplicity over cross-platform compatibility.

---

## 2) Timeline and Ordering Semantics

Default replay timeline is `ts_local_us` (arrival/observation time).

Store both timestamps when available:
- `ts_local_us` (required)
- `ts_exch_us` (optional but recommended)

Stable ordering key for event tables:
- `(exchange_id, symbol_id, date, ts_local_us, file_id, file_line_number)`

Lineage requirements:
- every Silver row must include `file_id` and `file_line_number`
- source file provenance is resolved via `silver.ingest_manifest`

---

## 3) Lake Layout

### Bronze
Raw vendor files are stored exactly as downloaded.

Layout:
- `<LAKE_ROOT>/bronze/<vendor>/exchange=<exchange>/type=<data_type>/date=<date>/symbol=<symbol>/...`

Notes:
- vendor republish events are stored as new immutable files
- checksums are tracked in `silver.ingest_manifest`

### Silver
Typed, normalized Delta tables used by ingestion and research.

Core properties:
- integer timestamp fields (`*_us`)
- fixed-point numeric encoding where applicable
- deterministic lineage columns

### Gold
Optional derived tables only when query demand justifies precomputation.

Current default:
- Bronze + Silver are mandatory
- Gold is optional

---

## 4) Storage Defaults (Local Delta)

- Format: Delta Lake (`delta-rs`)
- Compression: ZSTD
- Typical file target: 256MB to 1GB
- Partitioning (default): `exchange`, `date`

Type conventions:
- use signed integer types compatible with Delta/Parquet (`Int16`, `Int32`, `Int64`)
- avoid unsigned types that downcast on write

Within-partition data organization:
- sort/z-order by `symbol_id`, `ts_local_us` when beneficial

---

## 5) Symbol and Numeric Semantics

### Symbol dimension
Use `silver.dim_symbol` as SCD2 for metadata changes over time.

Join rule (PIT-safe):
- match on `exchange_id` + symbol key
- constrain by validity window using event `ts_local_us`

### Fixed-point encoding
Preferred:
- `px_int = round(price / price_increment)`
- `qty_int = round(qty / amount_increment)`

Fallback for poor metadata quality:
- global multiplier (for example `1e8`) with explicit documentation

---

## 6) Ingestion Pipeline Contract

Stages:
1. Discover Bronze files.
2. Filter already-successful files via manifest.
3. Parse + normalize into Silver schema.
4. Attach lineage (`file_id`, `file_line_number`).
5. Validate table invariants.
6. Write Silver partition(s).
7. Update manifest status.
8. Run table maintenance as needed (`delta optimize` / `delta vacuum`).

Idempotency expectations:
- re-running the same input with same metadata should not duplicate successful files
- ordering keys and lineage fields must remain stable

---

## 7) Manifest Contract (`silver.ingest_manifest`)

Purpose:
- file-level ingest ledger
- skip logic
- provenance and audit trail

Identity key:
- `(vendor, data_type, bronze_file_name, sha256)`

Required operational behavior:
- `created_at_us` records first discovery and must remain stable
- `processed_at_us` records latest ingest attempt
- status values are explicit (`pending`, `success`, `failed`, `quarantined`)

---

## 8) Local Operations

Use local commands only:
- ingest: `pointline bronze ingest ...`
- maintenance: `pointline delta optimize ...`, `pointline delta vacuum ...`
- checks: `pointline dq run --table all`, `pointline dq summary`

For local-host deployments, do not assume background schedulers.
Maintenance cadence is explicit and operator-driven.

---

## 9) Research Access Patterns

Primary interfaces:
- Polars: `pl.scan_delta(...)` for lazy pipelines
- DuckDB: `delta_scan(...)` for ad hoc SQL

Safe default patterns:
- filter early on `date` and `exchange`
- use as-of joins on `ts_local_us`
- preserve tie-break keys (`file_id`, `file_line_number`) in replay workflows

---

## 10) Versioning Policy

This architecture is a clean-break baseline.

- No compatibility promise with previous manifest/table contracts.
- Schema changes may require full local rebuilds.
- Migration helpers are optional tooling, not architectural requirements.

For schemas, see `/Users/zjx/.codex/worktrees/62ef/pointline/docs/reference/schemas.md`.
