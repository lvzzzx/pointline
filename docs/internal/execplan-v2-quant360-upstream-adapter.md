# V2 Quant360 Upstream Adapter (Archive to Extracted Bronze Contract)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan must be maintained in accordance with `PLANS.md`.

## Purpose / Big Picture

After this change, a contributor can point Pointline at raw Quant360 `.7z` drops and deterministically produce extracted Bronze files that the v2 ingestion core can consume without any archive-specific logic. The v2 core remains stable and simple: it only ingests extracted per-symbol files and never needs to know about `.7z` members, extraction tooling, or archive retry behavior.

The user-visible behavior is that upstream processing becomes a separate, explicit step with clear idempotency and restart semantics. Running the upstream step twice on the same input does not duplicate work, and partial failures can be retried safely without corrupting already-published extracted files.

## Progress

- [x] (2026-02-13 00:00Z) Authored initial ExecPlan for Quant360 upstream adapter with clean v2 boundary and no CLI scope.
- [x] (2026-02-13 01:30Z) Created v2 upstream adapter package under `pointline/v2/vendors/quant360/upstream/` with contracts, models, discovery, extraction, publish, ledger, and runner modules.
- [x] (2026-02-13 01:35Z) Implemented deterministic archive-to-extracted publish flow using `py7zr`, deterministic layout paths, and atomic temp-file rename.
- [x] (2026-02-13 01:45Z) Added adapter-facing tests for success, idempotency rerun skip, partial retry after injected failure, archive name validation, corrupted archive continuation, and ledger state persistence.
- [x] (2026-02-13 01:55Z) Integrated active docs references (`docs/references/quant360_cn_l2.md` and sibling integration ExecPlan) to make the extracted-file contract explicit and keep archive handling upstream-only.
- [x] (2026-02-13 12:10Z) Refactored ledger from member-level to archive-level (one record per `.7z`) and updated runner/tests for archive-granularity skip/retry.

## Surprises & Discoveries

- Observation: The repository already has Quant360 archive reorganization logic under `pointline/io/vendors/quant360/reorganize.py`, but it is tied to legacy plugin/runtime paths and shelling out to `7z`.
  Evidence: `pointline/io/vendors/quant360/plugin.py` and `pointline/io/vendors/quant360/reorganize.py`.

- Observation: The v2 package already has deterministic filename/member parsers and canonical stream parser dispatch that we should reuse instead of rebuilding.
  Evidence: `pointline/v2/vendors/quant360/filenames.py`, `pointline/v2/vendors/quant360/dispatch.py`, and `pointline/v2/vendors/quant360/parsers.py`.

- Observation: `py7zr` is already a project dependency, so upstream extraction can avoid external `7z` command dependencies and become cross-platform and testable.
  Evidence: `pyproject.toml` includes `py7zr>=0.21`.

- Observation: `py7zr` extraction from archives created by `writestr(...)` can yield files with permission bits set to `000`, which causes read failures in tests unless permissions are normalized first.
  Evidence: Upstream runner tests initially failed with `Permission denied` on extracted member paths until `extract.py` applied explicit `chmod` before `read_bytes()`.

## Decision Log

- Decision: Extend `pointline/v2/vendors/quant360/` with an `upstream` subpackage instead of creating a parallel vendor tree.
  Rationale: Existing v2 Quant360 modules already define parsing/canonical boundaries; colocating upstream logic keeps ownership clear and avoids split abstractions.
  Date/Author: 2026-02-13 / Codex

- Decision: Keep v2 ingestion core unchanged and extracted-file-only; archive handling will not be added to `pointline/v2/ingestion/pipeline.py`.
  Rationale: This preserves clean architecture and prevents technical debt from bleeding archive concerns into core correctness logic.
  Date/Author: 2026-02-13 / Codex

- Decision: Use `py7zr` for extraction, not shell calls to `7z`.
  Rationale: Reduces operational drift, improves testability, and removes host-tool dependency from the critical path.
  Date/Author: 2026-02-13 / Codex

- Decision: Maintain a dedicated upstream ledger for archive processing state (one record per `.7z`), separate from Silver ingest manifest.
  Rationale: Upstream extraction idempotency and Silver ingestion idempotency solve different problems and must not be conflated, and archive-level state keeps ledger size bounded and simple.
  Date/Author: 2026-02-13 / Codex

- Decision: Keep upstream extracted output layout as `exchange=<...>/type=<stream_type>/date=<YYYY-MM-DD>/symbol=<...>/<symbol>.csv.gz`.
  Rationale: This aligns with the v2 extracted-file contract already exercised by Quant360 ingestion tests and preserves stable `data_type` aliases (`order_new`, `tick_new`, `L2_new`).
  Date/Author: 2026-02-13 / Codex

- Decision: Normalize extracted file permissions before reading payload bytes.
  Rationale: Prevents platform-specific extraction permission issues from breaking deterministic upstream processing.
  Date/Author: 2026-02-13 / Codex

- Decision: Refactor extraction to one-pass per archive, preferring `7z x` when available and falling back to `py7zr`.
  Rationale: Per-member extraction is too slow on large archives; one-pass extraction mirrors proven script performance while preserving v2 function-first boundaries.
  Date/Author: 2026-02-13 / Codex

## Outcomes & Retrospective

This plan now has a working implementation for the upstream adapter core: archive discovery, member planning, extraction, publish, ledger idempotency, and runner retry semantics are implemented and validated with dedicated tests. The handoff contract is stable and extracted-file-only, and v2 ingestion remains archive-agnostic.

Primary remaining work is documentation alignment in active data-source/architecture pages so the upstream boundary is explicit for contributors. Scope creep into CLI remains controlled by keeping runtime integration function-first and adapter-local.

## Context and Orientation

Quant360 delivers China A-share data as `.7z` archives where each archive contains many per-symbol CSV files. In this repository, the active v2 ingestion flow is centered on `pointline/v2/ingestion/pipeline.py`, which expects a `BronzeFileMetadata` and a parser function and then performs canonicalization, timezone partitioning, PIT checks, validation, lineage assignment, and writes.

Today, v2 already has Quant360 parsing primitives in `pointline/v2/vendors/quant360/` for filename metadata parsing, symbol extraction from archive member path, timestamp parsing, stream parsing, and canonicalization. What is missing is a clean upstream adapter that turns raw archives into stable extracted Bronze files and emits deterministic metadata for ingestion.

In this plan, "upstream adapter" means the code that performs archive discovery, member selection, extraction/decompression, normalized file publishing, and stateful idempotency tracking before ingestion. "Extracted Bronze contract" means ingestion inputs are plain extracted files on disk with stable metadata; ingestion does not open archives.

## Plan of Work

Milestone 1 defines explicit upstream contracts and package boundaries. Create `pointline/v2/vendors/quant360/upstream/` and add `contracts.py`, `models.py`, `discover.py`, `extract.py`, `publish.py`, `ledger.py`, and `runner.py`. `models.py` must define strongly typed data classes for archive jobs, member jobs, publish outcomes, and run summaries. `contracts.py` must define status values and failure reasons as stable constants so diagnostics are deterministic.

Milestone 2 implements deterministic archive discovery and member planning. In `discover.py`, scan a configured source directory for files matching Quant360 archive naming and parse each using `parse_archive_filename` from `pointline/v2/vendors/quant360/filenames.py`. For each valid archive, enumerate member CSV paths and derive symbols with `parse_symbol_from_member_path`. Build member jobs in deterministic sort order by `(trading_date, exchange, stream_type, archive_filename, member_path)`.

Milestone 3 implements extraction and normalized publish with atomic writes. In `extract.py`, use `py7zr` to read archive members and stream bytes to publisher input without shelling out. In `publish.py`, write outputs as gzipped CSV files to a stable layout under the configured Bronze root, first writing to a temporary file in the same directory and then atomically renaming to final target. The output path must be deterministic and immutable for a given archive content hash and member path, so reruns do not mutate previously published content.

Milestone 4 adds idempotent ledger behavior and failure recovery. In `ledger.py`, store per-archive state keyed by archive identity (source filename + archive content hash) and include status and run counters. The runner in `runner.py` must skip archives already in `success`, retry archives in `failed`, and never duplicate successful outputs because publish remains deterministic and existence-safe. If processing fails mid-run, subsequent reruns resume at archive granularity.

Milestone 5 defines v2 handoff and verification tests. The runner must return a deterministic list of published extracted files and enough metadata to construct `BronzeFileMetadata` objects for downstream ingestion. Add tests under `tests/v2/quant360/` to prove success path, idempotent rerun, partial retry after injected failure, invalid archive filename rejection, corrupted archive handling, malformed member path handling, and stable ordering of produced metadata.

Milestone 6 updates docs and active references. Update `docs/references/quant360_cn_l2.md` and this plan’s sibling v2 integration plan to clearly state that v2 core contract is extracted files, while archive extraction is handled by the upstream adapter in `pointline/v2/vendors/quant360/upstream/`.

## Concrete Steps

Run all commands from repository root `/Users/zjx/Documents/pointline`.

Start with contract and import scaffolding:

    uv run pytest tests/v2/quant360/test_filename_and_symbol_extraction.py -q
    uv run pytest tests/v2/quant360/test_upstream_contracts.py -q

Expected result: existing filename tests pass; new upstream contract tests fail before implementation and pass after adding models/contracts.

Implement discovery, extraction, publish, and ledger behavior:

    uv run pytest tests/v2/quant360/test_upstream_discover.py -q
    uv run pytest tests/v2/quant360/test_upstream_publish.py -q
    uv run pytest tests/v2/quant360/test_upstream_ledger.py -q

Expected result: deterministic planning, atomic publish behavior, and idempotent ledger state transitions are validated independently.

Implement end-to-end upstream runner behavior:

    uv run pytest tests/v2/quant360/test_upstream_runner.py -q

Expected result: a synthetic `.7z` fixture produces deterministic extracted outputs and rerun skip behavior; injected failures are recoverable on retry.

Run v2 Quant360 regression checks:

    uv run pytest tests/v2/quant360 -q
    uv run ruff check pointline/v2/vendors/quant360 tests/v2/quant360

Expected result: no regressions in existing Quant360 v2 parsing/canonicalization tests, and all new upstream tests pass.

## Validation and Acceptance

Acceptance is behavior-based and requires three demonstrations.

First, the upstream adapter must transform valid Quant360 archives into extracted per-symbol gzipped CSV files in deterministic layout, and emit deterministic metadata ordering.

Second, rerunning the exact same upstream command on unchanged inputs must produce zero new writes and report all members skipped because their archive is already `success` in the idempotency ledger.

Third, after an injected partial failure (for example one corrupted member), fixing the input and rerunning must publish only remaining failed/pending members while preserving already successful outputs without rewrite.

A final architecture acceptance check is that `pointline/v2/ingestion/pipeline.py` remains archive-agnostic and unchanged in behavior, still consuming extracted files through the existing parser/canonicalization path.

## Idempotence and Recovery

All upstream steps are designed to be safely repeatable. Discovery is read-only. Publish writes are atomic, so process interruption cannot leave partially visible final files. The ledger records per-archive outcomes; reruns skip successful archives and retry only failed archives.

If a run fails, do not delete successful outputs. Fix the failing input or code path and rerun the same command. If a ledger entry is incorrect due to a bug, repair only the affected ledger records and rerun; do not clear the entire ledger unless intentionally rebuilding from scratch.

When developing locally, use temporary directories for source, Bronze output, and ledger storage to avoid polluting shared data roots.

## Artifacts and Notes

Record concise evidence snippets here during implementation.

    2026-02-13 01:46Z
    Command: uv run pytest tests/v2/quant360/test_upstream_runner.py -q
    Output: 3 passed

    2026-02-13 01:46Z
    Command: uv run pytest tests/v2/quant360 -q
    Output: 37 passed

    2026-02-13 01:46Z
    Command: uv run ruff check pointline/v2/vendors/quant360 tests/v2/quant360
    Output: All checks passed

    2026-02-13 01:56Z
    Command: uv run pytest tests/v2 -q
    Output: 54 passed

## Interfaces and Dependencies

Create the following v2-local interfaces under `pointline/v2/vendors/quant360/upstream/`.

In `pointline/v2/vendors/quant360/upstream/runner.py`, define:

    def run_quant360_upstream(
        source_dir: Path,
        bronze_root: Path,
        ledger_path: Path,
        *,
        dry_run: bool = False,
    ) -> Quant360UpstreamRunResult:
        ...

In `pointline/v2/vendors/quant360/upstream/discover.py`, define:

    def discover_quant360_archives(source_dir: Path) -> list[Quant360ArchiveJob]:
        ...

In `pointline/v2/vendors/quant360/upstream/extract.py`, define:

    def iter_archive_members(job: Quant360ArchiveJob) -> Iterator[Quant360MemberPayload]:
        ...

In `pointline/v2/vendors/quant360/upstream/publish.py`, define:

    def publish_member_payload(
        payload: Quant360MemberPayload,
        bronze_root: Path,
    ) -> Quant360PublishedFile:
        ...

In `pointline/v2/vendors/quant360/upstream/ledger.py`, define:

    class Quant360UpstreamLedger:
        def load(self) -> None: ...
        def should_skip(self, key: Quant360ArchiveKey) -> bool: ...
        def mark_success(self, record: Quant360LedgerRecord) -> None: ...
        def mark_failure(self, record: Quant360LedgerRecord) -> None: ...

Dependencies and constraints:

- Use `py7zr` for archive access.
- Reuse v2 Quant360 helpers from `pointline/v2/vendors/quant360/filenames.py`.
- Keep this adapter independent from legacy `pointline/io/vendors/quant360/*`.
- Do not add CLI or orchestration command surfaces in this plan.
- Keep storage contract extracted-file based; do not expose archive-member processing to v2 ingestion core.

---

Revision Note (2026-02-13 00:00Z): Initial ExecPlan created to define a clean, idempotent Quant360 upstream adapter for v2, with archive handling isolated from ingestion core and CLI explicitly out of scope.
Revision Note (2026-02-13 01:47Z): Implemented upstream adapter core in `pointline/v2/vendors/quant360/upstream/` and added dedicated upstream tests under `tests/v2/quant360/`; validated idempotent rerun, partial retry, corrupted archive continuation, and deterministic publish behavior.
Revision Note (2026-02-13 01:56Z): Added `Quant360PublishedFile.to_bronze_file_metadata()` handoff helper and aligned active docs to the upstream/extracted-file boundary.
Revision Note (2026-02-13 12:10Z): Switched upstream ledger semantics to archive-level state (one record per `.7z`) and updated runner/tests/docs to match.
Revision Note (2026-02-13 12:25Z): Replaced per-member extraction loop with one-pass archive extraction (prefer `7z`, fallback `py7zr`) and updated upstream tests for extract-path failure behavior.
