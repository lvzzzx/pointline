# V2 Quant360 Strict Parser-to-Canonical Contract (No Alias Fallbacks)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan must be maintained in accordance with `PLANS.md`.

## Purpose / Big Picture

After this change, Quant360 ingestion will run on one strict, deterministic contract:

1. parser emits one fixed intermediate schema per stream type;
2. canonicalizer expects exactly that schema (no alias fallback branches);
3. ingestion validates and writes canonical tables.

User-visible outcome: if upstream data is valid, ingest succeeds deterministically; if parser/canonicalizer inputs are off-contract, ingestion fails fast with explicit error messages instead of silently trying fallback column names.

## Progress

- [x] (2026-02-13 16:05Z) Drafted strict-contract ExecPlan with milestones and acceptance gates.
- [x] (2026-02-13 16:20Z) Frozen parser intermediate schemas in tests and source docs.
- [x] (2026-02-13 16:23Z) Removed alias fallback logic from Quant360 canonicalizer required paths.
- [x] (2026-02-13 16:26Z) Added strict negative tests proving alias-only parser payloads are rejected.
- [x] (2026-02-13 16:28Z) Ran quant360 + full v2 regression and lint checks.

## Surprises & Discoveries

- Observation: Quant360 parser outputs are mostly deterministic today, but canonicalizer still accepts multiple candidate names (legacy and alias variants).
  Evidence: `pointline/v2/vendors/quant360/canonicalize.py`.

- Observation: CN schemas are already canonicalized to non-`source_*` naming (`exchange_seq`, `exchange_order_index`, `exchange_trade_index`, `image_status`, `trading_phase_code`).
  Evidence: `pointline/schemas/events_cn.py`.

- Observation: docs define deterministic raw CSV schemas by exchange/stream, so strict parser contracts are aligned with source reality and clean v2 design.
  Evidence: `docs/references/quant360_cn_l2.md`.

- Observation: removing fallback code exposed a null-dtype edge where string normalization needed explicit `cast(pl.Utf8)` before string transforms.
  Evidence: strict test run failure in `tests/v2/quant360/test_pipeline_cn_quant360.py` and subsequent fix in `pointline/v2/vendors/quant360/canonicalize.py`.

## Decision Log

- Decision: keep two-stage normalization (`parser` -> `canonicalizer`) but make the boundary strict.
  Rationale: preserves clean architecture while removing technical-debt fallback behavior.
  Date/Author: 2026-02-13 / Codex + User

- Decision: parser is the only place allowed to handle raw SSE/SZSE schema differences.
  Rationale: avoid vendor-specific branching leaking into canonical domain mapping.
  Date/Author: 2026-02-13 / Codex + User

- Decision: canonicalizer must not accept alias columns for required fields.
  Rationale: fail-fast contracts are simpler, safer, and easier to reason about.
  Date/Author: 2026-02-13 / Codex + User

- Decision: no backward compatibility layer for old `source_*` event column names.
  Rationale: v2 clean-cut policy and no legacy constraints.
  Date/Author: 2026-02-13 / Codex + User

## Outcomes & Retrospective

Implemented. Quant360 parser-to-canonicalizer boundary is now strict for required fields, with parser contract assertions and negative tests that reject alias-only payloads. The resulting behavior is simpler and clearer: parser owns raw-schema normalization; canonicalizer consumes one deterministic intermediate schema; ingestion fails fast on contract violations.

## Context and Orientation

Current Quant360 flow in v2:

1. `parse_*_stream` converts raw CSV columns into stream-specific intermediate columns.
2. `canonicalize_quant360_frame` maps intermediate columns into canonical table columns.
3. ingestion derives `trading_date`, applies CN rules, performs PIT symbol coverage, assigns lineage, and normalizes to `TableSpec`.

Relevant files:

- `pointline/v2/vendors/quant360/parsers.py`
- `pointline/v2/vendors/quant360/canonicalize.py`
- `pointline/v2/ingestion/pipeline.py`
- `pointline/v2/ingestion/cn_validation.py`
- `pointline/schemas/events_cn.py`
- `tests/v2/quant360/*`
- `docs/references/quant360_cn_l2.md`

## Strict Boundary Contract

The parser output is the contract that canonicalizer consumes. Canonicalizer should require these fields directly (no candidate lists for required fields).

### Contract: `order_new` intermediate frame

Required columns:

- `symbol`, `exchange`, `ts_event_us`
- `appl_seq_num`, `channel_no`
- `side_raw`, `ord_type_raw`, `order_action_raw`
- `price_raw`, `qty_raw`
- `biz_index_raw`, `order_index_raw`

### Contract: `tick_new` intermediate frame

Required columns:

- `symbol`, `exchange`, `ts_event_us`
- `appl_seq_num`, `channel_no`
- `bid_appl_seq_num`, `offer_appl_seq_num`
- `exec_type_raw`, `trade_bs_flag_raw`
- `price_raw`, `qty_raw`
- `biz_index_raw`, `trade_index_raw`

### Contract: `L2_new` intermediate frame

Required columns:

- `symbol`, `exchange`, `ts_event_us`
- `ts_local_us`, `msg_seq_num`
- `image_status`, `trading_phase_code_raw`
- `bid_price_levels`, `bid_qty_levels`, `ask_price_levels`, `ask_qty_levels`

## Plan of Work

Milestone 1 locks contracts in tests first.

1. Add parser contract tests that assert exact output columns for each stream.
2. Add canonicalizer contract tests that require parser-contract names and reject alias-only payloads.

Milestone 2 removes canonicalizer fallback behavior.

1. Refactor `_canonicalize_order_events`, `_canonicalize_tick_events`, and `_canonicalize_l2_snapshots` to read required parser columns directly.
2. Remove alias candidate resolution for required fields in those paths.
3. Keep null handling only for truly optional semantics, not compatibility aliases.

Milestone 3 tightens docs and validation language.

1. Update `docs/references/quant360_cn_l2.md` with explicit parser contract appendix.
2. Ensure error messages from parser/canonicalizer name missing columns clearly.
3. Keep CN validation on canonical columns only.

Milestone 4 runs full quality gates and records evidence.

1. Run focused quant360 tests.
2. Run full `tests/v2`.
3. Run lint/format checks.

## Concrete Steps

Run all commands from `/Users/zjx/Documents/pointline`.

First, enforce parser contract expectations in tests.

    uv run pytest tests/v2/quant360/test_parsers_cn_streams.py -q

Then add/adjust canonicalizer strictness tests.

    uv run pytest tests/v2/quant360/test_pipeline_cn_quant360.py -q
    uv run pytest tests/v2/quant360/test_schema_cn_quant360.py -q

After tests are ready, implement strict canonicalizer changes.

    uv run pytest tests/v2/quant360 -q
    uv run ruff check pointline/v2/vendors/quant360 pointline/v2/ingestion tests/v2/quant360

Finally run full v2 regression.

    uv run pytest tests/v2 -q
    uv run ruff check pointline/v2 tests/v2
    uv run ruff format --check pointline/v2 tests/v2

## Validation and Acceptance

Acceptance is behavior-based.

1. Canonicalizer no longer accepts alias-only required inputs for Quant360 streams.
2. Parser output columns are deterministic and fully documented.
3. `cn_order_events`, `cn_tick_events`, and `cn_l2_snapshots` writes remain green under existing ingestion integration tests.
4. SSE row-level quarantine behavior for missing `exchange_seq` and exchange indices is unchanged in semantics.
5. Full `tests/v2` and `ruff check` pass.

## Idempotence and Recovery

This refactor is code-contract-only (no data migration). If an implementation step fails:

1. revert only the failing local change chunk;
2. rerun focused quant360 tests;
3. reapply with smaller patch scope.

No destructive data-lake operations are required.

## Artifacts and Notes

Record implementation evidence here.

    2026-02-13 16:27Z
    Command: uv run pytest tests/v2/quant360 -q
    Output: 42 passed

    2026-02-13 16:28Z
    Command: uv run pytest tests/v2 -q
    Output: 141 passed

    2026-02-13 16:28Z
    Command: uv run ruff check pointline/v2 tests/v2
    Output: All checks passed

## Interfaces and Dependencies

No new runtime dependencies.

Primary interfaces in scope:

- `parse_order_stream(df, exchange, symbol) -> pl.DataFrame`
- `parse_tick_stream(df, exchange, symbol) -> pl.DataFrame`
- `parse_l2_snapshot_stream(df, exchange, symbol) -> pl.DataFrame`
- `canonicalize_quant360_frame(df, table_name) -> pl.DataFrame`

Non-goals for this plan:

- introducing plugin systems;
- adding backward compatibility alias layers;
- changing upstream `.7z` processing contract;
- changing research-layer APIs.

---

Revision Note (2026-02-13 16:05Z): Initial strict-contract ExecPlan created before implementation to lock scope and avoid fallback-style technical debt.
