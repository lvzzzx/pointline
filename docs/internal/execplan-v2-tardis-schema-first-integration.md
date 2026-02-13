# V2 Tardis Schema-First Integration Into The Data Lake

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan must be maintained in accordance with `.agent/PLANS.md`.

## Purpose / Big Picture

After this change, Pointline will ingest Tardis CSV datasets into canonical Silver Delta tables through the current v2 ingestion core (`pointline/schemas`, `pointline/ingestion`, `pointline/storage`) with deterministic ordering, PIT symbol coverage checks, and manifest-based idempotency. A contributor should be able to take Bronze files under `bronze/tardis/...`, run ingestion, and observe stable writes to Silver event tables without vendor-specific logic leaking into core modules.

The user-visible outcome is that Tardis trades, quotes, and L2 updates become production-ready in the existing lake contract first, then derivative and options datasets are added under explicit new schema specs with tests and replay-safe ordering keys.

## Progress

- [x] (2026-02-13 12:29Z) Authored initial schema-first Tardis integration ExecPlan with phased delivery, file-level targets, and acceptance criteria.
- [x] (2026-02-13) Refactored Tardis parsers to read exchange/symbol from CSV row data (grouped-symbol support). Removed path-level `exchange`/`symbol` kwargs from `parse_tardis_trades` and `parse_tardis_incremental_l2`. Unified all parsers to `(df) -> df` signature. Updated dispatch, tests, and ExecPlan. 169/169 tests pass.
- [ ] Implement schema contracts for Tardis coverage tiers (completed: design decisions and table mapping in this plan; remaining: `pointline/schemas/*` and `tests/test_schema_contracts.py` updates).
- [ ] Implement Tardis vendor adapters and parser dispatch (completed: target module layout and function signatures in this plan; remaining: `pointline/vendors/tardis/*` implementation and tests).
- [ ] Integrate Tardis routing into ingestion pipeline and exchange timezone map (completed: contract-level behavior defined; remaining: pipeline alias wiring, parser binding, and timezone coverage updates).
- [ ] Add end-to-end tests proving idempotent ingest, PIT quarantine behavior, and deterministic ordering for Tardis streams.
- [ ] Update active docs so the listed Tardis coverage matches the implemented v2 runtime.

## Surprises & Discoveries

- Observation: Current runtime code has no active Tardis adapter package under `pointline/vendors/`; only Quant360 and Tushare are implemented.
  Evidence: `find pointline/vendors -maxdepth 3 -type f | sort` output contains `pointline/vendors/quant360/*` and `pointline/vendors/tushare/*`, but no `pointline/vendors/tardis/*`.

- Observation: The active v2 ingestion path is schema-registry driven and expects parser outputs already in canonical column names and scaled integer numeric fields.
  Evidence: `pointline/ingestion/pipeline.py` and `pointline/ingestion/normalize.py` enforce `get_table_spec(table_name)` and pre-scaled Int64 checks.

- Observation: Documentation currently overstates available runtime Tardis tables and query helpers compared with the active codebase.
  Evidence: `docs/guides/researcher-guide.md` and `docs/quickstart.md` mention `book_snapshot_25`/`derivative_ticker` query flows, while `pointline/schemas/events.py` currently defines only `trades`, `quotes`, and `orderbook_updates`.

## Decision Log

- Decision: Use a two-tier rollout. Tier 1 lands Tardis `trades`, `quotes`, and `incremental_book_L2` on existing canonical tables without schema changes; Tier 2 adds new event tables for `book_snapshot_25`, `derivative_ticker`, `liquidations`, and `options_chain`.
  Rationale: This yields fast production value on the stable core path while keeping larger schema expansion explicit and testable.
  Date/Author: 2026-02-13 / Codex

- Decision: Keep canonical numeric storage as scaled Int64 for price/size/open-interest style columns; keep non-price analytics fields (for example `funding_rate`, IV, Greeks) as Float64 nullable.
  Rationale: Aligns with current canonical contract (`pointline/schemas/types.py`) while avoiding artificial scaling complexity for dimensionless rates/Greeks.
  Date/Author: 2026-02-13 / Codex

- Decision: Do not create a separate canonical `book_snapshot_5` table in the initial plan; ingest `book_snapshot_25` first and treat `book_snapshot_5` as optional follow-up.
  Rationale: Reduces table sprawl and prioritizes the depth format already referenced by project docs and expected research workflows.
  Date/Author: 2026-02-13 / Codex

- Decision: Keep CLI work out of scope; integration targets pure Python modules and tests only.
  Rationale: Current repo explicitly removed CLI scripts during v2 cleanup, and this plan should avoid coupling to command-surface churn.
  Date/Author: 2026-02-13 / Codex

- Decision: Use Tardis grouped symbols as the bronze file unit. Bronze path uses `symbol=SPOT` (or `FUTURES`, `PERPETUALS`, `OPTIONS`) as a placeholder partition — not an actual instrument symbol. The CSV is self-contained: `exchange` and `symbol` are read from row data, not from the path. Parsers accept `(df: pl.DataFrame) -> pl.DataFrame` with no path-level `exchange`/`symbol` kwargs.
  Rationale: Grouped-symbol files are the natural Tardis download unit (one file per exchange/data_type/date/market_type). Avoids splitting raw vendor files in bronze (bronze is immutable). The pipeline is already row-level for trading_date derivation, PIT checks, and lineage, so multi-symbol files work without core changes.
  Date/Author: 2026-02-13

## Outcomes & Retrospective

This initial version delivers a complete implementation roadmap but no code changes yet. The biggest risk is drifting further between docs and runtime contracts if schema expansion and parser wiring are done piecemeal. This plan mitigates that by requiring test-first schema additions, explicit table-by-table acceptance checks, and an active-doc cleanup milestone before completion.

## Context and Orientation

Pointline currently uses a schema-registry ingestion model:

- Canonical table specs live in `pointline/schemas/`.
- Ingestion orchestration lives in `pointline/ingestion/pipeline.py`.
- Delta writing and manifest/quarantine persistence live in `pointline/storage/delta/`.
- PIT checks join incoming rows to `dim_symbol` using `(exchange, symbol, ts_event_us)` via `pointline/ingestion/pit.py`.

Key terms used in this plan:

- Bronze means immutable vendor files in Hive-style partitions. Tardis uses grouped-symbol files: `bronze/tardis/exchange=binance-futures/type=trades/date=2024-05-01/symbol=PERPETUALS/binance-futures_trades_2024-05-01_PERPETUALS.csv.gz`. The `symbol=` partition is a placeholder for the grouped symbol name (e.g. `SPOT`, `FUTURES`, `PERPETUALS`, `OPTIONS`), not an instrument symbol; each CSV row carries its own `exchange` and `symbol` fields.
- Silver means typed canonical Delta tables validated against `TableSpec`.
- PIT (point in time) means symbol resolution uses the validity window `valid_from_ts_us <= ts_event_us < valid_until_ts_us`.
- Tie-break keys mean deterministic ordering columns used for replay-safe sorting.

Current gap: Tardis source semantics are documented in `docs/references/tardis.md`, but there is no active Tardis parser/dispatch module in runtime code.

## Plan of Work

Milestone 1 is schema design and contract locking. Start by defining the exact canonical mapping from Tardis data types to Silver tables and codify that in `pointline/schemas/events.py` (and a new `pointline/schemas/events_tardis.py` if readability requires it). Do this test-first by extending `tests/test_schema_contracts.py` and adding a dedicated `tests/tardis/test_schema_tardis.py` contract suite. Tier 1 should require no schema expansion (`trades`, `quotes`, `orderbook_updates` already exist). Tier 2 adds new specs for `book_snapshot_25`, `derivative_ticker`, `liquidations`, and `options_chain`.

Milestone 2 is parser and canonicalization implementation under a new package `pointline/vendors/tardis/`. Build deterministic pure functions that transform raw Tardis CSV frames into canonical schema columns with scaled Int64 values where required. Implement explicit timestamp semantics: `ts_event_us` uses `timestamp` with fallback to `local_timestamp`; `ts_local_us` uses `local_timestamp` (nullable). For `incremental_book_L2`, preserve the vendor semantics that amount values are absolute depth levels and map directly to `orderbook_updates.qty`.

Milestone 3 is ingestion wiring. Extend `_TABLE_ALIASES` in `pointline/ingestion/pipeline.py` for Tardis stream names and add a small parser-dispatch layer used by ingestion scripts/tests. Extend `pointline/ingestion/exchange.py` timezone map for all Tardis exchanges we intend to ingest in phase 1 and phase 2. Keep failure behavior strict: unsupported data types or unknown exchanges must raise explicit errors.

Milestone 4 is symbol-dimension readiness for PIT. Implement a Tardis symbol snapshot adapter in `pointline/vendors/tardis/symbols.py` that converts instrument metadata snapshots into `DIM_SYMBOL`-compatible frames (nullable tick/lot sizes allowed when source is incomplete). Wire this into existing `pointline/dim_symbol.py` bootstrap/upsert flow so PIT checks stop being a blocker for Tardis ingestion.

Milestone 5 is verification and determinism hardening. Add parser unit tests, ingestion contract tests, and Delta adapter integration tests for positive and quarantine paths. Include tests for manifest idempotency (second run skipped unless forced), scaled-int enforcement, symbol PIT failure quarantine, and deterministic sorting by tie-break keys.

Milestone 6 is documentation alignment and cutover evidence. Update active docs (`docs/guides/researcher-guide.md`, `docs/tutorial.md`, and Tardis-specific references) so documented table availability matches actual runtime. Capture final evidence in this plan’s artifacts section and close with an outcomes update.

## Canonical Table Mapping (Schema-First)

Tier 1 mapping (implement first):

- `tardis/trades` -> `trades`
- `tardis/quotes` -> `quotes`
- `tardis/incremental_book_L2` -> `orderbook_updates`

Tier 2 mapping (implement second):

- `tardis/book_snapshot_25` -> `book_snapshot_25` (new spec)
- `tardis/derivative_ticker` -> `derivative_ticker` (new spec)
- `tardis/liquidations` -> `liquidations` (new spec)
- `tardis/options_chain` -> `options_chain` (new spec)

Proposed Tier 2 schema notes:

- `book_snapshot_25` stores four level arrays: `bid_price_levels`, `bid_qty_levels`, `ask_price_levels`, `ask_qty_levels` as `List(Int64)` with price/qty scaling applied element-wise.
- `derivative_ticker` stores price-like fields as scaled Int64 (`mark_price`, `index_price`, `last_price`, `open_interest`) and keeps rates/Greeks-style fields as nullable Float64.
- `liquidations` mirrors trade-style schema with `side`, scaled `price`, scaled `qty`, and nullable vendor identifier.
- `options_chain` stores option symbol timeline rows with scaled strike/price/size columns and nullable Float64 IV/Greek columns.

## Concrete Steps

Run all commands from `/Users/zjx/Documents/pointline`.

Implement schema contracts first:

    uv run pytest tests/test_schema_contracts.py -q

Add and run Tardis schema tests:

    uv run pytest tests/tardis/test_schema_tardis.py -q

Implement parser modules and validate parser behavior:

    uv run pytest tests/tardis/test_parsers_tardis.py -q

Wire ingestion and verify pipeline behavior:

    uv run pytest tests/test_ingestion_pipeline_contract.py tests/tardis/test_pipeline_tardis.py -q

Verify Delta integration path:

    uv run pytest tests/storage/test_pipeline_delta_adapters.py tests/tardis/test_pipeline_tardis_delta.py -q

Run final quality gates:

    uv run pytest -q
    uv run ruff check pointline tests
    uv run ruff format --check pointline tests

## Validation and Acceptance

Acceptance is behavior-based and must be demonstrable:

1. Ingesting a valid Tardis trades Bronze file produces one or more rows in `silver/trades`, with non-null `file_id`, `file_seq`, `symbol_id`, and correctly scaled integer `price`/`qty`.
2. Re-ingesting the same Bronze identity without force is skipped by manifest semantics; with `force=True` it reprocesses.
3. A row without PIT symbol coverage is quarantined with reason `missing_pit_symbol_coverage`, and no event rows are written for the all-quarantine case.
4. Ordering by each table’s tie-break keys yields deterministic sequence across repeated runs.
5. Docs describing available Tardis-backed tables match the actual schema registry and tests.

## Idempotence and Recovery

All implementation steps are additive and can be safely re-run. If a milestone fails:

- revert only that milestone’s partial edits;
- rerun the milestone-local tests before continuing;
- avoid introducing compatibility shims that bypass schema contracts.

For manual data validation, use temporary lake roots under `/tmp` to avoid polluting persistent Bronze/Silver paths.

## Artifacts and Notes

Capture proof snippets during implementation in this section.

    2026-02-13 12:29Z
    Command: find pointline/vendors -maxdepth 3 -type f | sort
    Output: quant360 and tushare adapters exist; no tardis adapter files present.

    2026-02-13 12:29Z
    Command: sed -n '1,260p' pointline/schemas/events.py
    Output: canonical event specs currently include only trades, quotes, and orderbook_updates.

## Interfaces and Dependencies

New modules to define:

- `pointline/vendors/tardis/__init__.py`
- `pointline/vendors/tardis/parsers.py`
- `pointline/vendors/tardis/dispatch.py`
- `pointline/vendors/tardis/symbols.py`

Required functions (all parsers take a single DataFrame — exchange/symbol from row data):

    parse_tardis_trades(df: pl.DataFrame) -> pl.DataFrame
    parse_tardis_quotes(df: pl.DataFrame) -> pl.DataFrame
    parse_tardis_incremental_l2(df: pl.DataFrame) -> pl.DataFrame
    parse_tardis_book_snapshot_25(df: pl.DataFrame) -> pl.DataFrame
    parse_tardis_derivative_ticker(df: pl.DataFrame) -> pl.DataFrame
    parse_tardis_liquidations(df: pl.DataFrame) -> pl.DataFrame
    parse_tardis_options_chain(df: pl.DataFrame) -> pl.DataFrame
    get_tardis_parser(data_type: str) -> Callable[[pl.DataFrame], pl.DataFrame]
    tardis_symbols_to_snapshot(raw: pl.DataFrame, *, effective_ts_us: int) -> pl.DataFrame

Dependencies and constraints:

- Use existing project dependencies only (`polars`, `deltalake`, `tardis-dev`).
- Keep runtime pure-function style for parser/canonicalization modules.
- Keep canonical writes aligned to `TableSpec` via `normalize_to_table_spec`.
- Do not add CLI coupling or legacy module dependencies.

---

Revision Note (2026-02-13 12:29Z): Initial ExecPlan created to define a schema-first, test-driven Tardis integration path for the active v2 ingestion core and to close the gap between source documentation and runtime implementation.
