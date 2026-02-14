# V2 Research Spine Module (Internal Builders, No Legacy Coupling)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan must be maintained in accordance with `PLANS.md`.

## Purpose / Big Picture

After this change, v2 research will provide a clean spine API owned entirely by `pointline/v2/research/*`, with first-class support for `clock`, `trades`, `volume`, and `dollar` spines. A contributor will be able to build PIT-safe spines and align events to those spines without importing legacy `pointline/research/*` code, without persistence/cache concerns, and without hidden metadata joins.

The user-visible behavior is explicit and deterministic:
- one canonical spine contract
- explicit builder selection and typed configs
- strict `[start, end)` window semantics
- deterministic ordering
- no implicit enrichment side effects

## Progress

- [x] (2026-02-13 14:05Z) Authored initial ExecPlan with v2-only scope, no-plugin rule, builder list (`clock`, `trades`, `volume`, `dollar`), and extension path for `tick`/`imbalance`.
- [x] (2026-02-13 14:28Z) Added failing-then-passing tests for spine contract, builder semantics, PIT alignment, and no-legacy-import boundary under `tests/v2/research/test_spine_*`.
- [x] (2026-02-13 14:31Z) Implemented v2 spine public API and internal builder dispatcher in `pointline/v2/research/spine.py`.
- [x] (2026-02-13 14:34Z) Implemented `clock`, `trades`, `volume`, `dollar` builders in `pointline/v2/research/_spine_builders.py` with v2-only dependencies.
- [x] (2026-02-13 14:36Z) Implemented explicit `align_to_spine(...)` primitive with boundary-safe forward assignment (`ts+1` join key).
- [ ] Add researcher-facing guide and examples for explicit usage patterns.
- [x] (2026-02-13 14:38Z) Ran full v2 regression and lint checks; all green.

## Surprises & Discoveries

- Observation: The repository already has extensive spine logic under legacy `pointline/research/spines/*`.
  Evidence: `pointline/research/spines/base.py`, `pointline/research/spines/trades.py`, and `tests/test_spine_builders.py`.

- Observation: Existing spine semantics are strong and worth preserving (bar-end timestamps, deterministic sorting, PIT alignment).
  Evidence: `docs/guides/resampling-methods.md` and `tests/research/features/spines/test_bucket_semantics.py`.

- Observation: v2 research already has clean explicit APIs (`discover_symbols`, `load_events`, `load_symbol_meta`) that avoid implicit dim joins by default.
  Evidence: `pointline/v2/research/discovery.py`, `pointline/v2/research/query.py`, `pointline/v2/research/metadata.py`.

- Observation: Dollar-bar notional math can overflow `int64` on intermediate `price * qty` even when final scaled notional fits.
  Evidence: initial implementation failures in `tests/v2/research/test_spine_builders.py`; fixed with per-row Python big-int arithmetic before casting.

## Decision Log

- Decision: Do not introduce a plugin system for v2 spines.
  Rationale: All supported builders are owned and controlled by the project; dynamic registration adds complexity without value.
  Date/Author: 2026-02-13 / Codex + User

- Decision: Phase 1 must include four builders: `clock`, `trades`, `volume`, `dollar`.
  Rationale: These are required now; architecture must still make future builders (`tick`, `imbalance`) easy to add.
  Date/Author: 2026-02-13 / Codex + User

- Decision: No persistence/cache layer in this phase.
  Rationale: Keep v2 core simple; optimize execution semantics first, then caching only when proven necessary.
  Date/Author: 2026-02-13 / Codex + User

- Decision: No implicit joins or hidden enrichment in spine APIs.
  Rationale: Predictable research semantics and explicit user intent are core v2 principles.
  Date/Author: 2026-02-13 / Codex + User

- Decision: Hard boundary: v2 spine module must not import `pointline/research/*`.
  Rationale: Avoid legacy coupling and protect the clean-cut v2 design.
  Date/Author: 2026-02-13 / Codex + User

- Decision: Threshold math (`volume`, `dollar`) should be integer-first on canonical scaled columns.
  Rationale: Deterministic arithmetic and reproducible results across environments.
  Date/Author: 2026-02-13 / Codex

## Outcomes & Retrospective

Implemented outcome: v2 now has a complete internal spine core for `clock`, `trades`, `volume`, and `dollar` with explicit API surface (`build_spine`, `align_to_spine`) and no `pointline/research/*` coupling.

Tradeoff accepted: dollar-bar notional currently uses per-row Python big-int arithmetic to avoid intermediate overflow and keep deterministic integer semantics. This is correct and simple, with potential future optimization if performance requires.

Remaining item: add a concise researcher-facing usage guide (recipes only).

## Context and Orientation

Current v2 research scope now includes:
- symbol discovery (`discover_symbols`)
- event loading (`load_events`)
- explicit symbol metadata loading (`load_symbol_meta`)

Missing v2 capability: a first-class spine layer for resampling and PIT alignment.

This plan introduces a v2-owned spine module that sits above `load_events(...)` and remains explicit by design. It must not pull from legacy research builders or workflow code. It must not auto-join metadata or apply hidden transforms.

Out of scope:
- CLI/UI wiring
- persistence/cache of generated spines
- full feature-engineering orchestration
- migration/deletion of legacy research modules

## Target Module Surface

Public API (initial):

- `build_spine(...) -> pl.DataFrame`
- `align_to_spine(...) -> pl.DataFrame`

Internal ownership (suggested files):

- `pointline/v2/research/spine.py`
- `pointline/v2/research/_spine_builders.py`
- `pointline/v2/research/_spine_types.py`
- `pointline/v2/research/__init__.py` (export public APIs)

No plugin registry. Use a project-owned internal dispatcher (`if/elif` or static mapping) with typed config models.

## Canonical Spine Contract

Every builder must return a DataFrame with:

- `exchange` (`Utf8`)
- `symbol` (`Utf8`)
- `symbol_id` (`Int64`)
- `ts_spine_us` (`Int64`) as bar-end timestamp

Required semantics:

- deterministic sort by `(exchange, symbol, ts_spine_us)`
- strict window semantics `[start, end)`
- bar-end interpretation: each row represents right boundary of `[prev, ts_spine_us)`
- PIT-safe alignment support via next-bar assignment (implemented as forward as-of on `ts+1`)

## Builder Scope (Phase 1)

`clock`
- fixed-step spine using `step_us` (positive integer)

`trades`
- one spine point per unique trade timestamp from `trades`
- deduplicate by `(exchange, symbol, ts_event_us)` then rename to `ts_spine_us`

`volume`
- cumulative absolute quantity threshold on trades (`qty`)
- emit `ts_spine_us` when running sum crosses `volume_threshold_scaled`

`dollar`
- cumulative notional threshold on trades
- integer notional from canonical scaled ints: `notional_scaled = (price * qty) // QTY_SCALE`
- emit `ts_spine_us` when running sum crosses `dollar_threshold_scaled`

Extension-ready (future):
- `tick` bars
- `imbalance` bars

## Plan of Work

Milestone 1: Contract tests and boundary tests first.
- Add failing tests for canonical columns/order/dtypes and deterministic behavior.
- Add boundary test ensuring v2 spine modules do not import `pointline/research/*`.
- Add PIT boundary tests (`event at boundary goes to next bar`).

Milestone 2: Implement minimal public API and internal types.
- Create `build_spine(...)` API with explicit `builder` and typed config.
- Define builder-specific config dataclasses or typed objects.
- Add strict validation and clear error messages.

Milestone 3: Implement builders (`clock`, `trades`, `volume`, `dollar`).
- Source data only via v2 research table loading/query path.
- Keep pure deterministic DataFrame logic.
- Enforce `max_rows` safety limits to prevent accidental huge output.

Milestone 4: Implement explicit alignment primitive.
- Add `align_to_spine(events_df, spine_df, ...)` with strict next-bar PIT semantics.
- Validate join keys and ordering prerequisites.
- Keep no implicit metadata joins.

Milestone 5: Documentation and examples.
- Add a minimal guide with three copy-paste recipes:
  - build clock spine
  - build volume/dollar spine
  - load events + explicit align + optional explicit metadata fetch

Milestone 6: End-to-end verification and acceptance.
- Run focused tests, then full `tests/v2`.
- Record evidence in `Artifacts and Notes`.

## Concrete Steps

Run all commands from `/Users/zjx/Documents/pointline`.

Start with failing tests for the new module:

    uv run pytest tests/v2/research/test_spine_contract.py -q
    uv run pytest tests/v2/research/test_spine_builders.py -q
    uv run pytest tests/v2/research/test_spine_alignment.py -q

Expected result: failing tests until new v2 spine modules are implemented.

Implement API + builders and run focused checks:

    uv run pytest tests/v2/research/test_spine_contract.py -q
    uv run pytest tests/v2/research/test_spine_builders.py -q
    uv run pytest tests/v2/research/test_spine_alignment.py -q
    uv run ruff check pointline/v2/research tests/v2/research

Expected result: all focused spine tests pass and lint is clean.

Run full v2 validation:

    uv run pytest tests/v2 -q
    uv run ruff check pointline/v2 tests/v2
    uv run ruff format --check pointline/v2 tests/v2

Expected result: no regressions across v2 modules.

## Validation and Acceptance

Acceptance is behavior-based.

First, `build_spine(...)` returns canonical spine columns and deterministic ordering for all four builders.

Second, `[start, end)` semantics are enforced consistently, including boundary behavior in PIT alignment.

Third, `volume` and `dollar` builders produce deterministic cut points using integer scaled math.

Fourth, `align_to_spine(...)` is explicitly called by users and performs strict next-bar PIT-safe alignment.

Fifth, v2 spine modules pass boundary checks showing no imports from `pointline/research/*`.

Sixth, full `tests/v2` remains green.

## Idempotence and Recovery

This plan is compute-only and non-persistent by design. Re-running builder functions with identical inputs must produce identical outputs.

If a milestone fails:
- fix within milestone scope
- rerun focused tests
- avoid widening module surface until contract tests pass

No destructive lake operations are required for this plan.

## Artifacts and Notes

Record concise evidence snippets here during implementation.

    2026-02-13 14:36Z
    Command: uv run pytest tests/v2/research/test_spine_contract.py -q
    Output: pass (as part of spine-focused suite)

    2026-02-13 14:36Z
    Command: uv run pytest tests/v2/research/test_spine_builders.py -q
    Output: pass (as part of spine-focused suite)

    2026-02-13 14:36Z
    Command: uv run pytest tests/v2/research/test_spine_alignment.py -q
    Output: pass (as part of spine-focused suite)

    2026-02-13 14:38Z
    Command: uv run pytest tests/v2 -q
    Output: 132 passed

    2026-02-13 14:38Z
    Command: uv run ruff check pointline/v2 tests/v2
    Output: All checks passed

## Interfaces and Dependencies

Suggested public API sketch:

    def build_spine(
        *,
        silver_root: Path,
        exchange: str,
        symbol: str | list[str],
        start: TimestampInput,
        end: TimestampInput,
        builder: Literal["clock", "trades", "volume", "dollar"],
        config: SpineConfig,
    ) -> pl.DataFrame: ...

    def align_to_spine(
        *,
        events: pl.DataFrame,
        spine: pl.DataFrame,
        ts_col: str = "ts_event_us",
        by: tuple[str, str] = ("exchange", "symbol"),
    ) -> pl.DataFrame: ...

Config types should be explicit and internal to v2, for example:
- `ClockSpineConfig(step_us: int, max_rows: int = ...)`
- `TradesSpineConfig(max_rows: int = ...)`
- `VolumeSpineConfig(volume_threshold_scaled: int, max_rows: int = ...)`
- `DollarSpineConfig(dollar_threshold_scaled: int, max_rows: int = ...)`

Dependencies:
- `polars`
- v2 schema registry and scale constants (`pointline/schemas/types.py`)
- v2 research APIs only (`pointline/v2/research/*`)

Forbidden runtime dependency:
- `pointline/research/*`

---

Revision Note (2026-02-13 14:05Z): Initial ExecPlan created for v2 research spine module with internal non-plugin builders (`clock`, `trades`, `volume`, `dollar`), explicit PIT alignment API, and hard no-legacy-coupling boundary.
