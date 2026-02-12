# V2 Clean-Cut Architecture Cutover (No Backward Compatibility)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan must be maintained in accordance with `/Users/zjx/Documents/pointline/PLANS.md`.

## Purpose / Big Picture

After this change, Pointline will run on one new v2 ingestion and schema core that matches the target architecture exactly: event partitions are `(exchange, trading_date)` derived from exchange-local time, manifest identity is deterministic, point-in-time symbol coverage is enforced, and event lineage is `file_id` plus `file_seq`. A contributor will be able to read one set of schema modules and one ingestion pipeline and understand the entire data path without compatibility shims or mixed legacy contracts.

The user-visible result is that `pointline bronze discover`, `pointline bronze ingest`, `pointline manifest show`, and validation commands continue to work, but they run through the new v2 core only. Legacy schema/version compatibility code is removed, and rebuilding or re-ingesting old data is the supported migration path.

## Progress

- [x] (2026-02-12 15:24Z) Authored this ExecPlan with concrete milestones, file-level targets, cutover rules, and validation criteria for the v2 clean cut.
- [ ] Establish v2 package scaffolding and test-first contracts (completed: none; remaining: add `pointline/schemas/*`, `pointline/v2/*`, and first failing tests).
- [ ] Implement canonical v2 schema registry and table specs (completed: none; remaining: event/dimension/control specs, dtype helpers, schema introspection APIs).
- [ ] Implement v2 single-file ingestion pipeline with deterministic manifest and lineage semantics (completed: none; remaining: parser dispatch, idempotency gate, timezone partition derivation, PIT quarantine, validation logging, Delta writes).
- [ ] Port trades, quotes, and orderbook update flows to v2 and prove determinism with tests (completed: none; remaining: parser adapters, tie-break ordering, replay determinism tests).
- [ ] Cut CLI orchestration to v2 path and remove compatibility branches from runtime code (completed: none; remaining: ingestion factory and command rewiring, removal of legacy service entrypoints).
- [ ] Remove legacy schema/version shims and obsolete modules, then update docs to one architecture story (completed: none; remaining: delete old compatibility paths, refresh architecture/development/reference docs).
- [ ] Run full quality gates and record final evidence in this plan (completed: none; remaining: `pytest`, `ruff check`, `ruff format --check`, and any targeted integration tests).

## Surprises & Discoveries

- Observation: Current event modules and ingestion flow still use `date` as the partition column name in core paths, while the target design requires `trading_date` derived from exchange-local timezone conversion.
  Evidence: `/Users/zjx/Documents/pointline/pointline/tables/trades.py` and `/Users/zjx/Documents/pointline/pointline/services/generic_ingestion_service.py`.

- Observation: Legacy compatibility behavior is still active in runtime code and not only in archived docs.
  Evidence: `/Users/zjx/Documents/pointline/pointline/services/api_snapshot_service.py` supports both v1 and v2 formats; `/Users/zjx/Documents/pointline/pointline/services/generic_ingestion_service.py` includes backward-compat wrappers.

- Observation: The architecture docs already define a no-compatibility policy, so the operational risk is not policy disagreement but execution drift during transition.
  Evidence: `/Users/zjx/Documents/pointline/docs/architecture/design.md` and `/Users/zjx/Documents/pointline/docs/architecture/schema-design-v4.md`.

## Decision Log

- Decision: Build the new implementation as an in-repo v2 core (`pointline/schemas` and `pointline/v2`) before the hard cut, then switch CLI/runtime entrypoints in one cutover milestone.
  Rationale: This keeps implementation isolated while preserving fast feedback from existing tooling and tests, then avoids long-term dual-path maintenance.
  Date/Author: 2026-02-12 / Codex

- Decision: Enforce non-negotiable v2 invariants at pipeline boundaries instead of relying on implicit conventions inside table modules.
  Rationale: Explicit checks at ingest boundaries make failures deterministic and easier to debug for new contributors.
  Date/Author: 2026-02-12 / Codex

- Decision: Prefer deletion over adapters once v2 acceptance criteria pass.
  Rationale: The stated architecture allows rebuild/re-ingest; keeping adapters would reintroduce the mixed architecture this cutover is intended to remove.
  Date/Author: 2026-02-12 / Codex

## Outcomes & Retrospective

At plan creation time, no code has been cut over yet. The intended outcome is a single active architecture that matches the v2 design docs and eliminates compatibility-era ambiguity.

The main delivery risk is transitional coupling between existing CLI commands and legacy services. This plan addresses that risk by introducing test-first boundaries, then a single explicit CLI cutover milestone, followed by immediate legacy deletion and full-suite validation.

## Context and Orientation

Pointline is a single-machine data lake project. Bronze is immutable raw vendor input files and API captures. Silver is typed canonical tables used for deterministic replay and analysis. Gold is optional derived output. In this plan, an event table means append-only market facts such as trades, quotes, or order book updates. A dimension table means historical metadata with validity windows, such as `dim_symbol`.

Point-in-time correctness (PIT correctness) means answering symbol validity based on the exact event timestamp, not on present-day metadata. SCD2 means storing history as half-open windows (`valid_from_ts_us <= ts < valid_until_ts_us`) so every event can be mapped to the correct symbol definition at that time.

The target architecture requires these invariants in code, not only in docs: deterministic manifest identity (`vendor`, `data_type`, `bronze_path`, `file_hash`), deterministic lineage (`file_id`, `file_seq`), exchange-timezone-aware `trading_date`, deterministic quarantine/failure statuses (`pending`, `success`, `failed`, `quarantined`), and explicit replay tie-break ordering.

Relevant current modules are `/Users/zjx/Documents/pointline/pointline/services/generic_ingestion_service.py`, `/Users/zjx/Documents/pointline/pointline/tables/*.py`, `/Users/zjx/Documents/pointline/pointline/io/delta_manifest_repo.py`, `/Users/zjx/Documents/pointline/pointline/cli/commands/ingest.py`, and `/Users/zjx/Documents/pointline/pointline/cli/ingestion_factory.py`. This plan introduces new canonical modules under `/Users/zjx/Documents/pointline/pointline/schemas/` and `/Users/zjx/Documents/pointline/pointline/v2/` and then rewires CLI runtime to those modules.

## Plan of Work

Milestone 1 creates a stable skeleton and failing contract tests. Add the new package roots `pointline/schemas/` and `pointline/v2/`, then add tests that assert the target schema contracts and pipeline invariants before implementation. The tests must fail first, proving that we are not silently inheriting legacy behavior. This milestone is complete when v2 contract tests exist and fail for expected missing functionality, while existing tests remain runnable.

Milestone 2 implements canonical schema ownership. Create `pointline/schemas/types.py`, `pointline/schemas/events.py`, `pointline/schemas/dimensions.py`, `pointline/schemas/control.py`, and `pointline/schemas/registry.py`. These files define all canonical columns, dtypes, partition keys, business keys, tie-break keys, and helper APIs used by ingestion and introspection. Keep one active schema per table and remove version branching. This milestone is complete when schema registry tests pass and ingestion can import specs without referencing `pointline/tables/*` schemas.

Milestone 3 implements the v2 ingestion core as one function-first pipeline. Add `pointline/v2/ingestion/pipeline.py` with `ingest_file(meta, *, force=False, dry_run=False) -> IngestionResult` and supporting modules for manifest gating, timezone partition derivation, lineage assignment, PIT symbol checks, validation/quarantine, and Delta writes. The pipeline must follow deterministic status transitions and must never write partial success without manifest/log updates. This milestone is complete when unit tests cover each failure path and a positive-path integration test writes correct Silver partitions.

Milestone 4 ports event flows to v2 semantics. Build parser adapters so existing vendor parser outputs can be canonicalized to the new event schemas (`trades`, `quotes`, `orderbook_updates`), then enforce explicit replay tie-break ordering and deterministic `file_seq`. Keep behavior deterministic across reruns with the same input. This milestone is complete when new determinism tests pass and sample replay order is stable across repeated runs.

Milestone 5 performs the runtime hard cut. Rewire `pointline/cli/commands/ingest.py`, `pointline/cli/ingestion_factory.py`, and related service wiring to call the v2 pipeline only. Remove legacy runtime compatibility branches in the same milestone so the default path cannot fall back to old internals. This milestone is complete when CLI ingestion commands succeed using only v2 imports.

Milestone 6 removes obsolete code and updates docs. Delete superseded ingestion/schema compatibility modules, remove legacy version references in runtime docs, and align architecture and development docs with the single active v2 design. This milestone is complete when repository search no longer shows active runtime references to removed compatibility interfaces and docs point to the new modules.

Milestone 7 runs full validation and captures final evidence. Execute the full local quality gate sequence and targeted end-to-end ingestion checks, then update this ExecPlan with observed outputs, final decisions, and retrospective outcomes. This milestone is complete when all required checks pass and this living document reflects final state.

## Concrete Steps

Run all commands from `/Users/zjx/Documents/pointline`.

Begin Milestone 1 with test-first scaffolding:

    rg --files pointline/schemas pointline/v2 tests/v2
    uv run pytest tests/v2/test_schema_contracts.py -q
    uv run pytest tests/v2/test_ingestion_pipeline_contract.py -q

Expected result: first command shows new files after creation; the two tests fail before implementation with clear missing-function or assertion errors.

Implement Milestone 2 schema modules and rerun:

    uv run pytest tests/v2/test_schema_contracts.py -q
    uv run ruff check pointline/schemas tests/v2/test_schema_contracts.py

Expected result: schema contract tests pass and lint is clean for new schema files.

Implement Milestone 3 ingestion core and verify deterministic failure/positive paths:

    uv run pytest tests/v2/test_ingestion_pipeline_contract.py -q
    uv run pytest tests/v2/test_manifest_semantics.py -q
    uv run pytest tests/v2/test_timezone_partitioning.py -q

Expected result: pending/success/failed/quarantined semantics and partition derivation behaviors are deterministic and repeatable.

Implement Milestone 4 event table ports and replay determinism checks:

    uv run pytest tests/v2/test_trades_v2.py tests/v2/test_quotes_v2.py tests/v2/test_orderbook_updates_v2.py -q
    uv run pytest tests/v2/test_replay_determinism_v2.py -q

Expected result: canonical event schemas and tie-break ordering are stable across reruns.

Execute Milestone 5 hard cut and verify CLI behavior on v2-only runtime:

    uv run pytest tests/test_cli.py -q
    uv run pointline bronze discover --help
    uv run pointline bronze ingest --help

Expected result: CLI commands still resolve and test suite passes without importing removed legacy ingestion services.

Execute Milestone 6 cleanup checks:

    rg -n "SNAPSHOT_SCHEMA_VERSION_V1|_write_v1_snapshot|backward compat|compatibility" pointline docs | sed -n '1,200p'
    rg -n "pointline\\.tables\\.|generic_ingestion_service" pointline/cli pointline/services pointline/v2 | sed -n '1,200p'

Expected result: remaining hits are either deleted paths or intentionally historical docs; active runtime files should not depend on legacy compat branches.

Execute Milestone 7 full quality gates:

    uv run pytest
    uv run ruff check .
    uv run ruff format --check .
    uv run mypy pointline --ignore-missing-imports

Expected result: all commands pass; if any command fails, capture the failure in this document and resolve before marking completion.

## Validation and Acceptance

Acceptance is behavior-driven. A contributor must be able to ingest Bronze files and observe Silver output partitioned by exchange-local `trading_date`, not by naive UTC date. The same file ingested twice without force must be skipped through manifest idempotency, and forced replay must retain deterministic lineage and replay order.

A second acceptance signal is PIT correctness: when `dim_symbol` history contains multiple validity windows, events at boundary timestamps must resolve to the correct window (`valid_from_ts_us <= ts < valid_until_ts_us`), with uncovered rows deterministically quarantined.

A third acceptance signal is architecture clarity: running a repository search should show one active schema source (`pointline/schemas/*`) and one active ingestion path (`pointline/v2/ingestion/*`) used by CLI commands. There must be no hidden runtime fallback to removed v1/vcompat paths.

Final acceptance requires passing full quality gates and targeted v2 tests, with outputs recorded in this plan.

## Idempotence and Recovery

This cutover is intentionally non-backward-compatible at runtime, but implementation steps are still repeatable. Each milestone is safe to re-run because it is test-driven and file-local. If a milestone fails mid-change, revert only that milestone’s edits and rerun its commands until clean.

For data safety, do not mutate existing production lake paths during development validation. Use temporary test lake roots and fixture datasets. The supported migration path for existing historical data is rebuild or re-ingest under v2 contracts, not in-place schema migration.

If the hard-cut milestone introduces regressions, recover by reverting the cutover commit(s) as a unit and re-running the previous milestone tests before retrying. Do not reintroduce permanent dual-runtime paths as a recovery shortcut.

## Artifacts and Notes

During implementation, keep short evidence snippets in this section as indented text. Include one snippet for each major acceptance claim: schema contract pass, deterministic manifest behavior, timezone partition correctness, PIT quarantine correctness, CLI cutover success, and full quality gate pass.

Example evidence format to add as work progresses:

    2026-02-13 09:14Z
    Command: uv run pytest tests/v2/test_manifest_semantics.py -q
    Output: 6 passed in 0.84s

    2026-02-13 10:02Z
    Command: uv run pytest tests/v2/test_replay_determinism_v2.py -q
    Output: 3 passed in 0.41s

## Interfaces and Dependencies

The new schema interfaces must exist under `/Users/zjx/Documents/pointline/pointline/schemas/` with stable import paths. Define `ColumnSpec` and `TableSpec` dataclasses with required metadata (`name`, `dtype`, nullability, partition keys, business keys, tie-break keys, schema version) and helper methods for Polars schema conversion and required column inspection.

The v2 ingestion interface must exist at `/Users/zjx/Documents/pointline/pointline/v2/ingestion/pipeline.py`:

    ingest_file(meta: BronzeFileMetadata, *, force: bool = False, dry_run: bool = False) -> IngestionResult

Supporting interfaces must include explicit, testable functions for:

    derive_trading_date(ts_event_us, exchange) -> trading_date
    assign_lineage(df, file_id) -> df_with_file_id_and_file_seq
    check_pit_coverage(df, dim_symbol_df) -> (valid_df, quarantined_df, reason)
    update_manifest_status(meta, file_id, status, result) -> None

Use existing repository dependencies where practical (`polars`, `delta-rs` via current repository wrappers), but keep ownership clear: schema ownership in `pointline/schemas/*`, pipeline ownership in `pointline/v2/ingestion/*`, CLI orchestration in `pointline/cli/commands/*`. If an existing dependency cannot satisfy v2 determinism requirements, document the limitation in `Surprises & Discoveries` and adjust the interface in `Decision Log` before implementation continues.

---

Revision Note (2026-02-12 15:24Z): Initial ExecPlan created for the v2 clean-cut rewrite and hard cutover to a single non-compatible runtime path, per explicit request to choose a fresh-start architecture over incremental legacy refactor.
