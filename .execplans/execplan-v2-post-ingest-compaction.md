# V2 Post-Ingest Partition Compaction (Storage Primitive Only)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan must be maintained in accordance with `PLANS.md`.

## Purpose / Big Picture

After this change, a v2 caller can explicitly run partition compaction on a Delta table after ingestion and reduce small-file pressure without changing ingestion correctness rules. In plain language, compaction means rewriting many tiny data files in one partition into fewer larger files so reads and metadata scans become faster and cheaper. A user can verify the behavior by running a focused test that creates many tiny files in one partition, runs compaction, and observes fewer files in that partition while row counts stay unchanged.

The user-visible contract is intentionally narrow: storage exposes explicit maintenance primitives (`compact_partitions(...)` and `vacuum_table(...)`), and the caller decides when to invoke them. No scheduler, no implicit background jobs, and no CLI rewiring are part of this plan.

## Progress

- [x] (2026-02-13 16:05Z) Drafted this ExecPlan with strict v2 boundary: storage compaction primitive only; caller/orchestrator out of scope.
- [x] (2026-02-13 16:24Z) Added v2 storage compaction contract (`PartitionOptimizer`) and compaction report models in `pointline/v2/storage/contracts.py` and `pointline/v2/storage/models.py`.
- [x] (2026-02-13 16:27Z) Implemented `DeltaPartitionOptimizer` in `pointline/v2/storage/delta/optimizer_store.py` with partition-key validation, deterministic ordering, dry-run, threshold skip policy, and continue-on-error behavior.
- [x] (2026-02-13 16:29Z) Added tests in `tests/v2/storage/test_compaction_contracts.py` and `tests/v2/storage/test_compaction_delta.py` for target-only compaction, dry-run, invalid key rejection, error isolation, and idempotent rerun.
- [x] (2026-02-13 16:31Z) Exported new APIs in `pointline/v2/storage/__init__.py` and `pointline/v2/storage/delta/__init__.py`, then validated full v2 storage suite.
- [x] (2026-02-13 16:42Z) Extended storage maintenance with explicit vacuum primitive (`TableVacuum` + `DeltaPartitionOptimizer.vacuum_table`) and added `tests/v2/storage/test_vacuum_delta.py`.

## Surprises & Discoveries

- Observation: the current v2 ingestion helper performs one `ingest_file(...)` call per extracted source file, which naturally creates many small Delta appends.
  Evidence: `scripts/ingest_v2_quant360_partition.py` loops over discovered files and calls `ingest_file(...)` once per file.

- Observation: v2 storage currently has manifest/event/dimension/quarantine adapters but no dedicated maintenance or optimization adapter.
  Evidence: files under `pointline/v2/storage/delta/` currently include `manifest_store.py`, `event_store.py`, `dimension_store.py`, and `quarantine_store.py` only.

- Observation: existing generic optimize CLI wiring in legacy config does not define v2 CN tables, so v2 needs its own clean storage-level primitive rather than borrowing legacy table registries.
  Evidence: previously observed `pointline/config.py` table map excludes `cn_order_events`.

- Observation: `deltalake` optimize no-op compaction does not create a new table version when nothing is rewritten.
  Evidence: local probe with `DeltaTable.optimize.compact(...)` on a single-file partition returned `numFilesAdded=0`/`numFilesRemoved=0` and table version stayed unchanged.

- Observation: vacuum should remain table-level and explicit; it cannot be safely expressed as a partition-scoped delete primitive.
  Evidence: `DeltaTable.vacuum(...)` API operates on table tombstones, not a partition-filtered scope.

## Decision Log

- Decision: keep ingestion write-path behavior unchanged and add compaction as a separate explicit step.
  Rationale: preserves ingestion determinism, quarantine semantics, and file-level lineage while solving small-file pressure independently.
  Date/Author: 2026-02-13 / Codex + User

- Decision: caller/orchestrator is explicitly out of scope for this plan.
  Rationale: aligns with v2 clean-boundary philosophy; storage owns primitives, caller owns workflow.
  Date/Author: 2026-02-13 / Codex + User

- Decision: compact only explicitly provided partitions, never whole-table by default.
  Rationale: predictable blast radius, safer reruns, and lower operational risk.
  Date/Author: 2026-02-13 / Codex + User

- Decision: return a structured compaction report with per-partition outcomes.
  Rationale: makes behavior auditable and easy for future orchestration without coupling now.
  Date/Author: 2026-02-13 / Codex

- Decision: add `min_small_files` skip policy with default `8`.
  Rationale: avoid unnecessary optimize commits/cost for low-impact partitions and keep behavior explicit in reports (`below_min_small_files`).
  Date/Author: 2026-02-13 / Codex + User direction

- Decision: add a separate explicit vacuum primitive with safe defaults (`dry_run=True`, `retention_hours=168`, `enforce_retention_duration=True`).
  Rationale: reclaim storage safely without coupling deletion to compaction and without hidden retention risks.
  Date/Author: 2026-02-13 / Codex + User

## Outcomes & Retrospective

Implemented as planned and then extended by explicit user request. v2 storage now exposes a clean compaction primitive (`DeltaPartitionOptimizer.compact_partitions(...)`) and a separate vacuum primitive (`DeltaPartitionOptimizer.vacuum_table(...)`). Compaction remains partition-targeted with deterministic reporting, and vacuum remains table-level with safe defaults and explicit caller control. The result matches the clean-cut boundary: ingestion path remains unchanged; caller/orchestrator ownership remains explicit and out of scope.

The main lesson is that storage maintenance should be policy-driven but minimal. A tiny set of explicit controls for compaction (`target_file_size_bytes`, `min_small_files`, `dry_run`, `continue_on_error`) and vacuum (`retention_hours`, `dry_run`, `enforce_retention_duration`) is enough to make these primitives production-usable without introducing orchestration complexity.

## Context and Orientation

The v2 storage layer lives in `pointline/v2/storage/` with protocol contracts in `pointline/v2/storage/contracts.py`, shared types in `pointline/v2/storage/models.py`, and Delta implementations in `pointline/v2/storage/delta/`. The ingestion path currently writes event rows through `pointline/v2/storage/delta/event_store.py` and does not include a maintenance abstraction.

A partition in this repository means the canonical table partition keys defined by schema specs, such as `("exchange", "trading_date")` for CN event tables. A small-file problem means the partition has too many tiny Parquet files because input is appended per source file. Post-ingest compaction means rewriting files in selected partitions into larger files without changing row content.

This plan does not add scheduling, retry orchestration, backlog discovery, or CLI behavior. Those are caller concerns by design.

## Plan of Work

Milestone 1 introduces storage-level contracts and result models for compaction. Update `pointline/v2/storage/contracts.py` with a new runtime-checkable protocol (for example `PartitionOptimizer`) that defines a single explicit method for partition compaction. Add compaction result dataclasses to `pointline/v2/storage/models.py` (or a new focused models module under `pointline/v2/storage/`) so outcomes are typed and stable. Include fields for table name, attempted partitions, skipped partitions, failures, and per-partition before/after file counts.

Milestone 2 implements a Delta-backed optimizer in `pointline/v2/storage/delta/optimizer_store.py`. The implementation must validate table existence, verify provided partition keys exactly match schema partition columns, and then call Delta optimize-compact per partition. Keep execution deterministic by sorting partition inputs before work. Add dry-run mode that performs validation and planning but no rewrite. Add optional policy guards such as minimum file threshold to skip low-impact partitions.

Milestone 3 adds robust tests under `tests/v2/storage/`. Add protocol/model tests in `tests/v2/storage/test_compaction_contracts.py` and behavior tests in `tests/v2/storage/test_compaction_delta.py`. Behavior tests must cover: targeted partition-only compaction, dry-run no-op behavior, invalid partition key rejection, continue-on-error partition isolation, and idempotent rerun (second run reports zero or minimal rewrites).

Milestone 4 exports and documents the primitive boundary. Export the Delta implementation from `pointline/v2/storage/delta/__init__.py` and, if useful, the protocol/model types from `pointline/v2/storage/__init__.py`. Add a short internal usage note to this plan’s artifacts section showing a direct call example with explicit partitions. Do not wire caller/orchestrator code in this milestone.

## Concrete Steps

Run all commands from `/Users/zjx/Documents/pointline`.

Start with failing tests for the new contract and behavior.

    uv run pytest tests/v2/storage/test_compaction_contracts.py -q
    uv run pytest tests/v2/storage/test_compaction_delta.py -q

Expected before implementation: import failures or missing-interface failures.

Implement contracts/models and Delta optimizer, then rerun focused tests.

    uv run pytest tests/v2/storage/test_compaction_contracts.py -q
    uv run pytest tests/v2/storage/test_compaction_delta.py -q
    uv run ruff check pointline/v2/storage tests/v2/storage

Expected after implementation: both test modules pass and lint is clean.

Run targeted v2 regression to ensure no storage regressions.

    uv run pytest tests/v2/storage -q
    uv run pytest tests/v2 -q

Expected result: v2 storage suite passes, and broader v2 tests remain green.

## Validation and Acceptance

Acceptance is behavior-based.

First, a caller can pass explicit partitions to `compact_partitions(...)` and receive a structured report that names what was compacted, skipped, or failed.

Second, running compaction on one partition does not rewrite unrelated partitions.

Third, row-level content is unchanged before and after compaction for compacted partitions.

Fourth, dry-run validates inputs and returns a plan-like report without creating new Delta versions.

Fifth, rerunning compaction on the same partition is safe and deterministic; the second run should be mostly skipped or report no meaningful rewrites.

## Idempotence and Recovery

This plan is safe to rerun because compaction is explicit and partition-scoped. If one partition fails, `continue_on_error=True` must allow remaining partitions to proceed, and the report must record the failure detail. Recovery is straightforward: fix the issue and rerun the same partition set. No schema migration and no destructive table drops are involved.

## Artifacts and Notes

Record command evidence here during implementation.

    2026-02-13 16:25Z
    Command: uv run pytest tests/v2/storage/test_compaction_contracts.py -q
    Output: 2 passed

    2026-02-13 16:25Z
    Command: uv run pytest tests/v2/storage/test_compaction_delta.py -q
    Output: 5 passed

    2026-02-13 16:31Z
    Command: uv run pytest tests/v2/storage -q
    Output: 22 passed

    2026-02-13 16:30Z
    Command: uv run ruff check pointline/v2/storage tests/v2/storage
    Output: All checks passed

    2026-02-13 16:32Z
    Command: uv run pytest tests/v2 -q
    Output: 148 passed

    2026-02-13 16:41Z
    Command: uv run pytest tests/v2/storage/test_compaction_contracts.py tests/v2/storage/test_compaction_delta.py tests/v2/storage/test_vacuum_delta.py -q
    Output: 12 passed

    2026-02-13 16:42Z
    Command: uv run pytest tests/v2/storage -q
    Output: 27 passed

    2026-02-13 16:42Z
    Command: uv run pytest tests/v2 -q
    Output: 153 passed

Illustrative direct-call usage (caller-owned workflow, not implemented here):

    optimizer = DeltaPartitionOptimizer(silver_root=silver_root)
    report = optimizer.compact_partitions(
        table_name="cn_order_events",
        partitions=[
            {"exchange": "sse", "trading_date": date(2024, 1, 2)},
            {"exchange": "sse", "trading_date": date(2024, 1, 3)},
        ],
        target_file_size_bytes=134_217_728,
        min_small_files=8,
        dry_run=False,
        continue_on_error=True,
    )

    vacuum = optimizer.vacuum_table(
        table_name="cn_order_events",
        retention_hours=168,
        dry_run=True,
        enforce_retention_duration=True,
    )

## Interfaces and Dependencies

Use the existing Delta dependency (`deltalake`) and v2 schema registry (`pointline/schemas/registry.py`) to validate table partition keys. Do not depend on legacy config table maps.

Define or extend stable interfaces in `pointline/v2/storage/contracts.py`:

    @runtime_checkable
    class PartitionOptimizer(Protocol):
        def compact_partitions(
            self,
            *,
            table_name: str,
            partitions: list[dict[str, object]],
            target_file_size_bytes: int | None = None,
            min_small_files: int = 8,
            dry_run: bool = False,
            continue_on_error: bool = True,
        ) -> CompactionReport: ...

    @runtime_checkable
    class TableVacuum(Protocol):
        def vacuum_table(
            self,
            *,
            table_name: str,
            retention_hours: int | None = 168,
            dry_run: bool = True,
            enforce_retention_duration: bool = True,
            full: bool = False,
        ) -> VacuumReport: ...

Define stable result models in `pointline/v2/storage/models.py` (or a focused sibling module), for example:

    @dataclass(frozen=True)
    class PartitionCompactionResult:
        partition: tuple[tuple[str, str], ...]
        before_file_count: int
        after_file_count: int
        skipped_reason: str | None = None
        error: str | None = None

    @dataclass(frozen=True)
    class CompactionReport:
        table_name: str
        partition_keys: tuple[str, ...]
        planned_partitions: int
        attempted_partitions: int
        succeeded_partitions: int
        skipped_partitions: int
        failed_partitions: int
        partitions: tuple[PartitionCompactionResult, ...]

    @dataclass(frozen=True)
    class VacuumReport:
        table_name: str
        dry_run: bool
        retention_hours: int | None
        enforce_retention_duration: bool
        full: bool
        deleted_count: int
        deleted_files: tuple[str, ...]

Implement Delta adapter in `pointline/v2/storage/delta/optimizer_store.py`:

    class DeltaPartitionOptimizer(PartitionOptimizer, TableVacuum):
        def __init__(self, *, silver_root: Path, table_paths: Mapping[str, Path] | None = None) -> None: ...

        def compact_partitions(...) -> CompactionReport: ...
        def vacuum_table(...) -> VacuumReport: ...

The implementation must sort partition work items deterministically and normalize partition value rendering in reports so repeated runs produce comparable output.

---

Revision Note (2026-02-13 16:05Z): Initial plan drafted after confirming small-file pressure from file-by-file ingestion; scope explicitly constrained to storage compaction primitives with caller/orchestrator excluded.
Revision Note (2026-02-13 16:31Z): Plan updated after implementation completion with final progress, decisions, discoveries, and validation evidence.
Revision Note (2026-02-13 16:42Z): Scope extended by user request to include explicit table vacuum primitive with safe defaults and dedicated tests.
