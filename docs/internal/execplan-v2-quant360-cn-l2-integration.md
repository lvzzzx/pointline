# V2 Quant360 CN L2/L3 Integration (Clean Core, No Legacy Constraints)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan must be maintained in accordance with `/Users/zjx/Documents/pointline/PLANS.md`.

## Purpose / Big Picture

After this change, Pointline v2 will ingest Quant360 China A-share archives (`order_new`, `tick_new`, `L2_new`) into deterministic, typed Silver tables through the new v2 ingestion core only. A contributor will be able to process both SZSE and SSE order/trade streams plus SZSE L2 snapshots without relying on legacy table modules, legacy orchestration, or CLI compatibility behavior.

The user-visible result is simple: given Quant360 `.7z` files and a populated `dim_symbol`, v2 ingestion produces deterministic Silver outputs partitioned by `(exchange, trading_date)` with stable manifest idempotency, PIT correctness, and explicit replay ordering keys.

## Progress

- [x] (2026-02-12 16:12Z) Authored this ExecPlan with a clean-cut architecture and concrete milestones for Quant360 CN integration into v2 core.
- [x] (2026-02-12 16:17Z) Define and lock canonical v2 table contracts for Quant360 CN streams (`cn_order_events`, `cn_tick_events`, `cn_l2_snapshots`) with deterministic tie-break rules.
- [x] (2026-02-12 16:23Z) Build v2-local Quant360 source adapters (archive metadata parsing, symbol extraction, timestamp parsing, CSV parsing) with no dependency on legacy vendor plugin system.
- [ ] Implement canonicalization and validation rules for SSE vs SZSE differences, including cancel/fill semantics and L2 array integrity checks (completed: parser-level symbol checks, SSE fill default for missing `ExecType`, L2 depth-vector length validation, scaled-int canonicalization for CN event columns, and exec-type validation in canonicalization; remaining: pipeline-level quarantine rule mapping and richer rule-level diagnostics).
- [ ] Integrate Quant360 adapters with `pointline/v2/ingestion/pipeline.py` through explicit parser dispatch and table routing, preserving manifest/PIT/lineage invariants (completed: data-type alias routing in pipeline and Quant360 canonicalization hook; remaining: end-to-end parser dispatch wiring from Bronze row/frame readers).
- [ ] Add robust v2 contract/integration tests and fixture set for SZSE + SSE + L2 snapshot paths, including failure and quarantine behaviors (completed: schema/filename/timestamp/parser/dispatch/pipeline route tests; remaining: fixture-backed multi-file integration and quarantine diagnostics assertions).
- [ ] Update architecture/data-source docs to reflect the single v2 path and deprecate legacy Quant360 runtime routes from active docs.

## Surprises & Discoveries

- Observation: The Quant360 data source combines three materially different streams (order events, tick events, and snapshot arrays) that cannot be represented cleanly by forcing everything into existing generic `trades`/`orderbook_updates` schemas.
  Evidence: `/Users/zjx/Documents/pointline/docs/data_sources/quant360_cn_l2.md` sections 3, 4, and 7 define distinct semantics and payload shapes.

- Observation: Current Quant360 runtime path is coupled to legacy plugin and table-domain modules (`l3_orders`, `l3_ticks`) and uses `date` + `file_line_number` conventions that differ from new v2 core contracts (`trading_date`, `file_seq`).
  Evidence: `/Users/zjx/Documents/pointline/pointline/io/vendors/quant360/plugin.py`, `/Users/zjx/Documents/pointline/pointline/tables/l3_orders.py`, `/Users/zjx/Documents/pointline/pointline/tables/l3_ticks.py`.

- Observation: SZSE files omit symbol in CSV columns while SSE includes `SecurityID`; a robust adapter must normalize symbol sourcing from both file path and payload with explicit consistency checks.
  Evidence: `/Users/zjx/Documents/pointline/docs/data_sources/quant360_cn_l2.md` sections 3.1, 5.1, 6.1, and 8.

- Observation: SSE tick stream does not provide `ExecType`; every row is an execution and must be normalized with an explicit synthetic fill marker to keep downstream logic deterministic.
  Evidence: `/Users/zjx/Documents/pointline/docs/data_sources/quant360_cn_l2.md` section 4 and adapter test `tests/v2/quant360/test_parsers_cn_streams.py`.

## Decision Log

- Decision: Introduce dedicated Quant360 CN event tables in v2 (`cn_order_events`, `cn_tick_events`, `cn_l2_snapshots`) instead of forcing lossy mapping into existing generic tables.
  Rationale: Clean architecture requires preserving source semantics first; derived projections to generic tables can be added later without corrupting primary truth.
  Date/Author: 2026-02-12 / Codex

- Decision: Keep Quant360 integration in a new v2-local adapter package and avoid importing legacy plugin/table orchestration into the v2 path.
  Rationale: This avoids hidden coupling and preserves the “single clear v2 core path” objective.
  Date/Author: 2026-02-12 / Codex

- Decision: Keep CLI work out of scope for this plan.
  Rationale: The requested cut is core ingestion and schemas without technical debt from existing command surfaces.
  Date/Author: 2026-02-12 / Codex

- Decision: Keep parser outputs as canonical intermediate frames with `*_raw` enum/numeric columns, and defer final enum mapping and scaled-int conversion to later pipeline canonicalization step.
  Rationale: This preserves vendor evidence for audit and allows clean separation between parser correctness and ingestion normalization/validation.
  Date/Author: 2026-02-12 / Codex

- Decision: Route Quant360 legacy and source-native data type aliases (`order_new`, `tick_new`, `L2_new`, `l3_orders`, `l3_ticks`) to canonical v2 CN table names inside pipeline alias resolution.
  Rationale: This allows immediate cutover to v2 schemas without forcing old Bronze manifests to be rewritten first.
  Date/Author: 2026-02-12 / Codex

## Outcomes & Retrospective

This plan defines a full clean-cut route for Quant360 CN ingestion into v2 with explicit boundaries, deterministic contracts, and test-first verification. The intended outcome is a standalone, understandable v2 path where all vendor-specific complexity is isolated in adapters and all canonical invariants are enforced in one ingestion core.

The largest risk is accidental reuse of legacy modules due to convenience. This plan mitigates that by requiring v2-local modules, explicit no-legacy import tests, and behavior-based acceptance checks.

## Context and Orientation

Quant360 CN source files arrive as `.7z` archives named like `order_new_STK_SZ_20240102.7z` and contain one CSV per symbol. The data source has three families:

- Order stream (`order_new`) with order placements/cancellations and per-channel sequence identifiers.
- Tick stream (`tick_new`) with fills and cancels referencing order IDs.
- L2 snapshot stream (`L2_new`, SZSE) with top-of-book arrays and market state fields.

In this repository, v2 core lives under `/Users/zjx/Documents/pointline/pointline/schemas/` and `/Users/zjx/Documents/pointline/pointline/v2/ingestion/`. Legacy ingestion paths still exist under `/Users/zjx/Documents/pointline/pointline/services/` and `/Users/zjx/Documents/pointline/pointline/tables/`, but they are not the target for this work.

PIT correctness means every row must resolve to a valid `symbol_id` in `dim_symbol` for its exact event timestamp window (`valid_from_ts_us <= ts_event_us < valid_until_ts_us`). Deterministic replay means event ordering must be explicit and stable across reruns, not dependent on incidental file ordering.

## Plan of Work

Milestone 1 defines schema truth in v2. Extend `pointline/schemas/events.py` (or split to `pointline/schemas/events_cn.py` if readability demands) to add three canonical event specs:

- `cn_order_events`
- `cn_tick_events`
- `cn_l2_snapshots`

Each table must include required v2 core columns (`exchange`, `trading_date`, `symbol`, `symbol_id`, `ts_event_us`, `file_id`, `file_seq`) and explicit tie-break keys. For CN channelized streams, tie-break ordering must use canonical semantic identity:

- `cn_order_events`: `(exchange, symbol_id, trading_date, channel_id, event_seq, file_id, file_seq)`
- `cn_tick_events`: `(exchange, symbol_id, trading_date, channel_id, event_seq, file_id, file_seq)`
- `cn_l2_snapshots`: `(exchange, symbol_id, ts_event_us, snapshot_seq, file_id, file_seq)`

Milestone 2 creates a v2 Quant360 adapter package at `/Users/zjx/Documents/pointline/pointline/v2/vendors/quant360/`. Implement modules for filename parsing, exchange mapping, symbol extraction, and timestamp conversion (`YYYYMMDDHHMMSSmmm` in `Asia/Shanghai` -> UTC microseconds). Parsing must be deterministic and pure functions. The parser layer must produce canonical intermediate frames, and canonicalization must map those frames into vendor-agnostic table fields while preserving raw vendor provenance in `source_*_raw` columns.

Archive extraction/reorganization is intentionally separated from this ingestion plan and implemented by the dedicated upstream adapter plan at `docs/internal/execplan-v2-quant360-upstream-adapter.md`; v2 ingestion core consumes extracted files only.

Milestone 3 implements canonicalization and validation logic. Add v2-local transformations that map raw vendor enums to canonical values while retaining raw fields for auditability. Required checks include:

- SSE/SZSE symbol consistency checks when `SecurityID` exists.
- `ExecType` semantics in tick stream (`F` fill vs `4` cancel).
- Sequence integrity checks for non-null positive canonical sequencing fields (`event_seq`, `channel_id`) and required SSE provenance sequence fields.
- L2 array integrity checks (length and parse validity for top-10 depth vectors).

Invalid rows must be quarantined with rule-level reasons through validation log pathways, not silently dropped.

Milestone 4 integrates parser dispatch with v2 pipeline. Extend `pointline/v2/ingestion/pipeline.py` table routing and parser binding so Quant360 data types map to the new canonical tables. Keep manifest identity and status transitions unchanged. Preserve current v2 lineage contract (`file_id`, `file_seq`) and do not reintroduce `file_line_number` fallback semantics.

Milestone 5 adds tests and fixtures under `/Users/zjx/Documents/pointline/tests/v2/quant360/` and corresponding contract tests. Cover positive path and failure/quarantine path for:

- SZSE order/tick files (symbol from filename).
- SSE order/tick files (symbol from payload).
- SZSE L2 snapshot files with valid array fields.
- Invalid timestamp format, invalid enum values, malformed arrays, missing required columns.

Milestone 6 updates docs and removes active-doc ambiguity. Update `/Users/zjx/Documents/pointline/docs/data_sources/quant360_cn_l2.md` with canonical v2 mapping summary and point active implementation docs to this plan and v2 modules. Keep historical legacy design docs in archive only.

## Concrete Steps

Run all commands from `/Users/zjx/Documents/pointline`.

Create and verify schema contracts first:

    uv run pytest tests/v2/test_schema_contracts.py -q
    uv run pytest tests/v2/quant360/test_schema_cn_quant360.py -q

Expected result: new Quant360 schema contract tests fail before implementation and pass after schemas are added.

Implement v2 adapter parsing and timestamp normalization:

    uv run pytest tests/v2/quant360/test_timestamp_parser.py -q
    uv run pytest tests/v2/quant360/test_filename_and_symbol_extraction.py -q
    uv run pytest tests/v2/quant360/test_parsers_cn_streams.py -q

Expected result: deterministic parsing for both exchanges and deterministic timestamp conversion to UTC microseconds.

Implement ingestion wiring and deterministic semantics:

    uv run pytest tests/v2/quant360/test_pipeline_cn_quant360.py -q
    uv run pytest tests/v2/quant360/test_determinism_cn_sequences.py -q

Expected result: parser -> pipeline -> writer path succeeds for valid fixtures; idempotency and tie-break determinism are stable across reruns.

Run consolidated v2 quality checks:

    uv run pytest tests/v2 -q
    uv run ruff check pointline/schemas pointline/v2 tests/v2

Expected result: all v2 tests pass and lint is clean.

## Validation and Acceptance

The implementation is accepted only when a contributor can ingest representative Quant360 SZSE/SSE files and observe:

- Silver rows written to the new v2 CN tables with partitions `(exchange, trading_date)` derived from exchange-local time.
- Stable idempotency behavior: second ingestion without force is skipped via manifest identity.
- Stable ordering behavior: sorting by table tie-break keys produces identical sequence across reruns.
- PIT enforcement behavior: rows without valid `dim_symbol` window are quarantined with explicit reason.

A second acceptance gate is correctness of stream semantics:

- Tick fill/cancel events are distinguishable and validated.
- Order stream enums are canonicalized deterministically.
- L2 snapshot arrays are parsed with explicit validation and deterministic failure modes for malformed fields.

A third acceptance gate is architecture clarity: v2 Quant360 path is implemented in `pointline/v2/*` and schema ownership in `pointline/schemas/*`, with no runtime dependency on legacy ingestion/table modules for this flow.

## Idempotence and Recovery

All implementation steps are idempotent at source level because they are test-driven and additive. Re-running tests or parser commands should not mutate external lake data when fixture paths are used.

If a milestone fails mid-way, rollback only the in-progress milestone edits and re-run that milestone’s tests before proceeding. Do not introduce compatibility shims to “temporarily pass” tests; if a schema decision changes, record the decision in this document and update tests accordingly.

For any local end-to-end validation against real files, use isolated temporary lake roots so failures cannot pollute production Bronze/Silver paths.

## Artifacts and Notes

During implementation, keep concise evidence snippets here. Capture one snippet per major acceptance claim.

    2026-02-12 16:15Z
    Command: uv run pytest tests/v2/quant360/test_schema_cn_quant360.py -q
    Output: 4 failed (expected, test-first before schema implementation)

    2026-02-12 16:16Z
    Command: uv run pytest tests/v2/quant360/test_schema_cn_quant360.py -q
    Output: 4 passed in 0.10s

    2026-02-12 16:16Z
    Command: uv run pytest tests/v2/test_schema_contracts.py -q
    Output: 5 passed in 0.05s

    2026-02-12 16:16Z
    Command: uv run ruff check pointline/schemas tests/v2/quant360/test_schema_cn_quant360.py
    Output: All checks passed!

    2026-02-12 16:17Z
    Command: uv run pytest tests/v2 -q
    Output: 20 passed in 0.13s

    2026-02-12 16:22Z
    Command: uv run pytest tests/v2/quant360/test_timestamp_parser.py tests/v2/quant360/test_filename_and_symbol_extraction.py tests/v2/quant360/test_parsers_cn_streams.py -q
    Output: 14 passed in 0.16s

    2026-02-12 16:22Z
    Command: uv run ruff check pointline/v2/vendors tests/v2/quant360
    Output: All checks passed!

    2026-02-12 16:23Z
    Command: uv run pytest tests/v2 -q
    Output: 34 passed in 0.20s

    2026-02-12 16:23Z
    Command: uv run pytest tests/v2/quant360/test_dispatch.py tests/v2/quant360/test_pipeline_cn_quant360.py tests/v2/quant360/test_parsers_cn_streams.py -q
    Output: 11 passed in 0.21s

    2026-02-12 16:23Z
    Command: uv run ruff check pointline/v2/ingestion/pipeline.py pointline/v2/vendors tests/v2/quant360
    Output: All checks passed!

    2026-02-12 16:23Z
    Command: uv run pytest tests/v2 -q
    Output: 39 passed in 0.19s

## Interfaces and Dependencies

Define new v2-local interfaces under `/Users/zjx/Documents/pointline/pointline/v2/vendors/quant360/`.

Required functions:

    parse_archive_filename(name: str) -> Quant360ArchiveMeta
    parse_quant360_timestamp(value: str | int) -> int
    parse_order_stream(df: pl.DataFrame, *, exchange: str, symbol: str) -> pl.DataFrame
    parse_tick_stream(df: pl.DataFrame, *, exchange: str, symbol: str) -> pl.DataFrame
    parse_l2_snapshot_stream(df: pl.DataFrame, *, exchange: str, symbol: str) -> pl.DataFrame

Required integration points:

- Extend `pointline/schemas/registry.py` with new table specs.
- Extend `pointline/v2/ingestion/pipeline.py` table routing aliases for Quant360 data types.
- Keep usage of existing v2 helpers for trading date derivation, PIT coverage, lineage assignment, schema normalization, and manifest status updates.

Constraints:

- No CLI interface work in this plan.
- No runtime backward-compatibility fallback to legacy Quant360/table modules.
- No float storage in canonical Silver numeric columns.

---

Revision Note (2026-02-12 16:12Z): Initial ExecPlan created for clean v2 Quant360 CN L2/L3 integration, explicitly excluding CLI and legacy compatibility constraints.
Revision Note (2026-02-12 16:17Z): Completed Milestone 1 schema contracts test-first; added `pointline/schemas/events_cn.py`, registry wiring, and Quant360 schema contract tests.
Revision Note (2026-02-12 16:23Z): Completed Milestone 2 v2-local Quant360 adapter package (`pointline/v2/vendors/quant360/*`) with filename/symbol parsing, timestamp conversion, and parser contracts for order/tick/L2 streams.
Revision Note (2026-02-12 16:23Z): Added pipeline alias routing + Quant360 canonicalization hook, plus dispatch and pipeline route tests for `order_new`/`tick_new`/`L2_new` aliases.
