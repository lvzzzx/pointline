# V2-Owned Storage Adapter Layer (Manifest, Event, Dimension, Quarantine)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan must be maintained in accordance with `PLANS.md`.

## Purpose / Big Picture

After this change, v2 ingestion will persist and recover state through v2-owned storage adapters instead of legacy `pointline/io/*` repositories. A contributor will be able to run the full v2 path (manifest identity/idempotency, event writes, quarantine writes, and dimension reads) without importing legacy ingestion I/O modules.

The user-visible behavior is a clean v2 storage boundary: reruns are deterministic, manifest statuses are stable (`pending`, `success`, `failed`, `quarantined`), and failures are restart-safe without hidden fallback to legacy repository code. CLI refactor remains explicitly out of scope.

## Progress

- [x] (2026-02-13 13:25Z) Authored initial ExecPlan with v2-owned storage scope, file-level targets, acceptance criteria, and hard boundary rules.
- [x] (2026-02-13 13:44Z) Added v2 storage contracts/models and Delta adapter package under `pointline/v2/storage/` with manifest, event, dimension, quarantine, and layout/util modules.
- [x] (2026-02-13 13:47Z) Wired `pointline/v2/ingestion/pipeline.py` to accept v2 storage adapters directly (event store object and optional quarantine store) while preserving callable writer compatibility.
- [x] (2026-02-13 13:50Z) Added storage-focused tests under `tests/v2/storage/` plus runtime boundary guard `tests/v2/test_core_integration_no_legacy_imports.py`.
- [x] (2026-02-13 13:52Z) Verified implementation with `uv run pytest tests/v2 -q` and targeted Ruff checks.
- [ ] Update architecture/docs references for v2 storage ownership (completed: new ExecPlan and evidence updates; remaining: architecture/data-source pages if needed in follow-up doc pass).

## Surprises & Discoveries

- Observation: The current v2 ingestion pipeline is function-first and already injects `manifest_repo` and `writer`, which lowers migration risk.
  Evidence: `pointline/v2/ingestion/pipeline.py` accepts injected dependencies rather than constructing legacy repositories internally.

- Observation: Active concrete manifest persistence is still implemented under legacy `pointline/io/delta_manifest_repo.py`.
  Evidence: no `pointline/v2/storage/` package exists yet, and current Delta manifest implementation is in `pointline/io/delta_manifest_repo.py`.

- Observation: Quant360 upstream v2 now provides deterministic extracted-file handoff and archive-level restart semantics, so storage migration can focus on ingestion/silver correctness rather than archive extraction.
  Evidence: `pointline/v2/vendors/quant360/upstream/*` plus `tests/v2/quant360/test_upstream_runner.py`.

- Observation: Using strict schema validation in storage adapters catches drift early and kept pipeline regressions at zero while adding adapter support.
  Evidence: `tests/v2/storage/test_event_store_delta.py` and full `tests/v2` pass after adapter wiring.

## Decision Log

- Decision: Introduce a dedicated `pointline/v2/storage/` package and keep all v2 runtime storage interfaces there.
  Rationale: Ownership clarity is the core goal of this plan; v2 runtime must not depend on legacy repository modules.
  Date/Author: 2026-02-13 / Codex

- Decision: Keep v2 storage adapters function-first and explicit, with thin Delta implementations under `pointline/v2/storage/delta/`.
  Rationale: This preserves testability and allows deterministic behavior checks without CLI coupling.
  Date/Author: 2026-02-13 / Codex

- Decision: Enforce a hard runtime boundary with dedicated tests that fail on legacy imports from v2 modules.
  Rationale: Without an explicit guard, accidental reintroduction of legacy I/O coupling is likely during future changes.
  Date/Author: 2026-02-13 / Codex

- Decision: Maintain manifest identity as `(vendor, data_type, bronze_file_path, sha256)` and keep status transitions explicit and monotonic.
  Rationale: This is already the v2 design invariant and must remain stable across adapter migration.
  Date/Author: 2026-02-13 / Codex

- Decision: Implement the v2 manifest adapter with lock + deterministic overwrite of canonical schema rows keyed by `file_id`.
  Rationale: This keeps behavior deterministic and simple for single-node v2 execution while preserving restart safety and stable IDs.
  Date/Author: 2026-02-13 / Codex

## Outcomes & Retrospective

The v2-owned storage adapter layer is now implemented under `pointline/v2/storage/` and integrated into the v2 ingestion pipeline surface. The runtime can ingest through v2 Delta adapters for manifest/event/quarantine flows with deterministic behavior verified by tests.

The primary remaining risk is documentation drift, not runtime coupling. A boundary guard test now prevents forbidden legacy repository imports from v2 runtime modules, reducing regression risk for future changes.

## Context and Orientation

Pointline v2 ingestion currently lives in `pointline/v2/ingestion/`. In this plan, "storage adapter" means the concrete code that reads/writes persistent state for ingestion, including manifest records, silver event tables, quarantine rows, and symbol dimension reads.

Today, v2 ingestion is already structured for dependency injection, but concrete persistence implementations are still legacy-owned. `pointline/v2/ingestion/pipeline.py` relies on caller-provided `manifest_repo` and `writer`, while active Delta manifest behavior is implemented in `pointline/io/delta_manifest_repo.py`. Legacy services still exist under `pointline/services/*`.

This plan introduces a v2-owned adapter layer under `pointline/v2/storage/` and wires v2 runtime code to those interfaces. "v2-owned" means interfaces and implementations both live in `pointline/v2/*` namespaces. "Hard boundary" means v2 runtime modules (ingestion, v2 adapters, v2 entrypoints) must not import legacy repository modules.

Out of scope: CLI command rewiring and legacy service deletion beyond what is required to enforce the v2 runtime boundary.

## Plan of Work

Milestone 1 establishes contracts and test-first boundaries. Create `pointline/v2/storage/contracts.py`, `pointline/v2/storage/models.py`, and `pointline/v2/storage/__init__.py`. Define plain-language interfaces for manifest, event, dimension, and quarantine operations used by v2 ingestion. Add failing tests under `tests/v2/storage/test_contracts.py` and `tests/v2/storage/test_no_legacy_imports.py` that codify the boundary and required method surface before implementation.

Milestone 2 implements the v2 Delta manifest adapter. Add `pointline/v2/storage/delta/manifest_store.py` with deterministic identity resolution, pending-record persistence, status updates, and file-id allocation semantics compatible with v2 invariants. Keep file-locking and retry-safe behavior explicit and covered by tests under `tests/v2/storage/test_manifest_store_delta.py`.

Milestone 3 implements v2 Delta event, quarantine, and dimension adapters. Add `pointline/v2/storage/delta/event_store.py`, `pointline/v2/storage/delta/quarantine_store.py`, and `pointline/v2/storage/delta/dimension_store.py`. Event store writes normalized frames to canonical tables; quarantine store records quarantined rows with reasons and lineage context; dimension store provides point-in-time lookup inputs for PIT checks. Add tests under `tests/v2/storage/` that verify schema adherence and deterministic writes.

Milestone 4 wires the v2 ingestion pipeline to v2 adapters only. Update `pointline/v2/ingestion/pipeline.py` and v2 entrypoints so runtime usage references `pointline/v2/storage/contracts.py` interfaces and v2 Delta implementations. Do not add CLI work. Ensure manifest status writes happen through v2 manifest store and writer behavior through v2 event/quarantine stores.

Milestone 5 adds reliability checks and migration guards. Add tests for rerun idempotency, partial failure recovery, and boundary enforcement (`rg`/AST-based checks or import tests) so v2 modules fail CI if they import legacy repository modules. Add one end-to-end test that runs extracted Quant360 input through v2 ingestion with v2 adapters on a temporary lake root.

Milestone 6 aligns documentation and final acceptance. Update architecture and internal docs so storage ownership is explicit, and record command outputs in the plan’s evidence section. Keep this file updated with surprises, decisions, and retrospective notes as implementation proceeds.

## Concrete Steps

Run all commands from `/Users/zjx/Documents/pointline`.

Start with contract and boundary tests (expected to fail before implementation):

    uv run pytest tests/v2/storage/test_contracts.py -q
    uv run pytest tests/v2/storage/test_no_legacy_imports.py -q

Expected result: tests fail with missing modules/interfaces until `pointline/v2/storage/*` is created.

Implement manifest store and run focused validation:

    uv run pytest tests/v2/storage/test_manifest_store_delta.py -q
    uv run pytest tests/v2/test_manifest_semantics.py -q

Expected result: v2 manifest adapter passes deterministic identity/status tests and existing v2 manifest semantics stay green.

Implement event/dimension/quarantine stores and run integration checks:

    uv run pytest tests/v2/storage/test_event_store_delta.py -q
    uv run pytest tests/v2/storage/test_dimension_store_delta.py -q
    uv run pytest tests/v2/storage/test_quarantine_store_delta.py -q

Expected result: adapters persist/read expected columns and partition behavior remains deterministic.

Wire pipeline and run v2 regression coverage:

    uv run pytest tests/v2/test_ingestion_pipeline_contract.py -q
    uv run pytest tests/v2/test_core_integration_no_legacy_imports.py -q
    uv run pytest tests/v2/quant360/test_pipeline_cn_quant360.py -q

Expected result: v2 ingestion path succeeds using v2-owned adapters and no-legacy-import boundary tests pass.

Run final checks:

    uv run pytest tests/v2 -q
    uv run ruff check pointline/v2 tests/v2
    uv run ruff format --check pointline/v2 tests/v2

Expected result: all tests and lint/format checks pass; capture outputs in `Artifacts and Notes`.

## Validation and Acceptance

Acceptance is behavior-based.

First, ingesting a valid Bronze file through v2 with v2 adapters writes the expected Silver rows and marks manifest `success`, with deterministic `file_id`/lineage behavior.

Second, ingesting the exact same file again without force does not duplicate writes and returns a deterministic skip/no-op outcome.

Third, when validation or PIT checks quarantine all rows, quarantine records are written through the v2 quarantine adapter and manifest status is `quarantined` with stable failure reason.

Fourth, if an adapter write fails mid-run, rerunning after fix resumes safely without corrupting prior successful writes.

Fifth, boundary enforcement tests demonstrate that active v2 runtime modules do not import `pointline/io/*` repository implementations.

## Idempotence and Recovery

All milestone steps are designed to be repeatable. Tests can be rerun safely. Adapter implementations must avoid non-deterministic status transitions and must preserve successful writes across retries.

If a milestone fails, revert only milestone-local edits and rerun the milestone commands. Do not clear full lake roots in shared environments. For local validation, use temporary roots under `/tmp` and isolated test fixtures.

If manifest records become inconsistent during development, repair only affected test fixtures or temporary tables. Do not apply destructive resets to production-like data paths as part of this plan.

## Artifacts and Notes

Record concise evidence snippets here during implementation.

    2026-02-13 13:52Z
    Command: uv run pytest tests/v2/storage/test_manifest_store_delta.py -q
    Output: 2 passed

    2026-02-13 13:52Z
    Command: uv run pytest tests/v2/test_core_integration_no_legacy_imports.py -q
    Output: 1 passed

    2026-02-13 13:52Z
    Command: uv run pytest tests/v2 -q
    Output: 94 passed

## Interfaces and Dependencies

Create v2-owned interfaces in `pointline/v2/storage/contracts.py` with explicit method signatures used by v2 ingestion.

Define a manifest interface:

    class ManifestStore(Protocol):
        def resolve_file_id(self, meta: BronzeFileMetadata) -> int: ...
        def filter_pending(self, candidates: list[BronzeFileMetadata]) -> list[BronzeFileMetadata]: ...
        def update_status(self, file_id: int, status: str, meta: BronzeFileMetadata, result: IngestionResult) -> None: ...

Define an event interface:

    class EventStore(Protocol):
        def append(self, table_name: str, df: pl.DataFrame) -> None: ...

Define a dimension interface:

    class DimensionStore(Protocol):
        def load_dim_symbol(self) -> pl.DataFrame: ...

Define a quarantine interface:

    class QuarantineStore(Protocol):
        def append(self, table_name: str, df: pl.DataFrame, *, reason: str, file_id: int) -> None: ...

Implement Delta adapters under:

- `pointline/v2/storage/delta/manifest_store.py`
- `pointline/v2/storage/delta/event_store.py`
- `pointline/v2/storage/delta/dimension_store.py`
- `pointline/v2/storage/delta/quarantine_store.py`
- `pointline/v2/storage/delta/__init__.py`

Dependencies should remain aligned with current project toolchain (`polars`, `deltalake`, `filelock`). Reuse stable schema definitions from `pointline/schemas/*` and v2 ingestion models from `pointline/v2/ingestion/*`.

Do not import legacy repository implementations from active v2 runtime modules. Any temporary bridging code must be isolated, explicitly documented, and removed before acceptance is marked complete.

---

Revision Note (2026-02-13 13:25Z): Initial ExecPlan created for v2-owned storage adapter layer to remove legacy repository coupling from active v2 ingestion runtime while keeping CLI rewrite out of scope.
Revision Note (2026-02-13 13:52Z): Implemented `pointline/v2/storage/*` Delta adapters, integrated v2 pipeline adapter support, and added storage + no-legacy-import test coverage with full `tests/v2` pass.
