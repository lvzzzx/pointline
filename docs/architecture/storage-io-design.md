# Storage-IO Architecture (Local Delta + Polars)

This document defines the storage architecture for Pointline on one local machine.

Constraints:
- local filesystem only
- Delta Lake via `delta-rs`
- no backward compatibility requirement

---

## 1) Layering Model

### Domain layer
Location: `pointline/tables/*` and pure transformation modules.

Rules:
- pure Polars logic
- schema/validation/transformation only
- no filesystem paths
- no Delta write/read side effects

### Service layer
Location: `pointline/services/*`

Rules:
- orchestrates ingestion/update flows
- owns sequencing (read -> transform -> validate -> write)
- depends on repository interfaces, not storage internals

### IO layer
Location: `pointline/io/*`

Rules:
- owns Delta read/write/merge/maintenance calls
- owns physical layout and partition details
- converts Polars <-> Arrow as needed

---

## 2) Repository Interfaces

Core repository protocol should stay small and explicit:
- `read_all()`
- `write_full(df)`
- `append(df)`
- `merge(df, keys)`
- maintenance operations (`optimize_partition`, `vacuum`) where applicable

Ingestion-specific protocols:
- `BronzeSource`: discover local files
- `IngestionManifestRepository`: resolve `file_id`, pending filtering, status updates

---

## 3) Local Path Contract

All table paths resolve from `LAKE_ROOT` (config/env).

Avoid hardcoded `/lake/...` assumptions in runtime logic. Use path helpers from:
- `/Users/zjx/.codex/worktrees/62ef/pointline/pointline/config.py`

Recommended structure:
- `<LAKE_ROOT>/bronze/...`
- `<LAKE_ROOT>/silver/...`

---

## 4) Write Semantics

Preferred defaults:
- overwrite for deterministic table rebuilds
- append for event ingestion
- native Delta merge for keyed upserts

Manifest-specific requirement:
- preserve immutable discovery metadata (for example `created_at_us`) when updating status rows

---

## 5) Partition and File Management

Default partitioning:
- `exchange`, `date` for high-volume event tables

Maintenance:
- optimize compaction and optional z-ordering for hot partitions
- vacuum under explicit retention policy

On localhost, these remain explicit operational steps; no distributed scheduler is assumed.

---

## 6) Failure and Recovery Semantics

Required behavior:
- ingest status is durably recorded per file
- failed/quarantined files can be retried without losing lineage
- successful files are skipped by manifest identity key

Because this is a clean-break architecture, failed historical formats are not migrated automatically.

---

## 7) Non-Goals

- cloud object store abstractions in core flow
- multi-engine execution backends
- compatibility shims for older table contracts
