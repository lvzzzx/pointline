# V2 Research Primitive Layer (Explicit Primitives First)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan must be maintained in accordance with `PLANS.md`.

## Purpose / Big Picture

After this change, a quant researcher can build factors and labels directly from trusted, explicit building blocks in `pointline/v2/research/primitives.py`, without hidden joins, hidden caching, or coupling to legacy `pointline/research/*`. The user-visible behavior is that every transformation is called directly, has a clear input/output column contract, and produces deterministic results. A novice can verify this by running primitive contract tests that fail on missing columns or invalid parameters, and pass when the primitive is used correctly.

## Progress

- [x] (2026-02-13 15:16Z) Created initial primitive-only ExecPlan with explicit scope, milestones, and acceptance criteria.
- [ ] Implement `pointline/v2/research/primitives.py` with strict validation and deterministic behavior.
- [ ] Add primitive contract tests and golden behavior tests under `tests/v2/research/`.
- [ ] Export stable primitive APIs from `pointline/v2/research/__init__.py`.
- [ ] Add concise user-facing examples for direct primitive usage.

## Surprises & Discoveries

- Observation: v2 research already has explicit modules for discovery, event loading, metadata loading, and spine building, but no dedicated primitive module yet.
  Evidence: `pointline/v2/research/discovery.py`, `pointline/v2/research/query.py`, `pointline/v2/research/metadata.py`, `pointline/v2/research/spine.py`.

- Observation: the new v2 spine module already enforces explicit semantics, which is a strong base for primitive-first research workflows.
  Evidence: `align_to_spine(...)` and builder contracts in `pointline/v2/research/spine.py`.

## Decision Log

- Decision: implement the research module in a strict primitive-first order and defer pipeline/spec/DSL layers.
  Rationale: this keeps semantics simple, auditable, and stable before adding higher-level ergonomics.
  Date/Author: 2026-02-13 / Codex + User

- Decision: no implicit joins or hidden enrichment inside primitives.
  Rationale: user intent must be explicit to avoid data leakage and confusing behavior.
  Date/Author: 2026-02-13 / Codex + User

- Decision: no plugin system for primitives.
  Rationale: the project owns all primitives, so static module ownership is simpler and more maintainable.
  Date/Author: 2026-02-13 / Codex + User

- Decision: no coupling to legacy `pointline/research/*`.
  Rationale: preserve clean-cut v2 boundaries and avoid technical debt carry-over.
  Date/Author: 2026-02-13 / Codex + User

## Outcomes & Retrospective

This plan is not implemented yet. The intended outcome is a stable primitive API that researchers can call directly for event-level, bar-level, and labeling workflows, with deterministic behavior and no hidden side effects. The main risk is under-specifying contracts; this plan addresses that risk by requiring explicit required-column checks and contract tests for each primitive.

## Context and Orientation

In this repository, the v2 research surface currently includes symbol discovery, pure event query, explicit metadata APIs, and spine generation. The relevant files are `pointline/v2/research/discovery.py`, `pointline/v2/research/query.py`, `pointline/v2/research/metadata.py`, and `pointline/v2/research/spine.py`. There is not yet a dedicated primitive transformation module for factor construction.

A primitive in this plan means one small, trusted, explicit DataFrame-to-DataFrame operation with a documented contract. For example, a primitive can filter rows, aggregate by time, apply lag, or perform a point-in-time as-of join. Trusted means the operation is deterministic, has validated inputs, and is covered by tests. Explicit means the user calls it directly and can see exactly which columns are required and produced.

This plan intentionally does not add orchestration, caching, a plugin framework, or a DSL. Those can be added later on top of the primitive layer once primitive contracts are stable.

## Plan of Work

Milestone 1 introduces a new module `pointline/v2/research/primitives.py` with shared validation helpers and a minimal but complete primitive set. At the end of this milestone, a researcher can import primitives directly and run them with fail-fast schema checks. The first group will include shape and ordering operations (`select_cols`, `rename_cols`, `filter_rows`, `sort_rows`) and time-series basics (`lag`, `lead`, `resample_time`).

Milestone 2 extends the module with deterministic aggregation and join primitives. This includes `group_agg`, `rolling_agg`, `join_left`, `join_asof_backward`, and `join_asof_forward`. Every function must validate required columns, type assumptions, and sort preconditions before running work. Output ordering rules must be explicit and stable.

Milestone 3 adds explicit label-oriented primitives that are frequently needed in research but must remain transparent. This includes `forward_return` and `bar_move`, plus explicit reuse of `align_to_spine(...)` from `pointline/v2/research/spine.py` without hidden enrichment. At the end of this milestone, users can build basic supervised learning targets with explicit point-in-time semantics.

Milestone 4 adds tests and API exports. New tests under `tests/v2/research/` will include contract tests for each primitive, edge-case tests for null and empty frames, and deterministic output tests. `pointline/v2/research/__init__.py` will export only primitives that are stable and documented.

Milestone 5 writes a concise guide showing direct primitive usage. The guide should demonstrate two explicit patterns: calculate on events first then aggregate, and resample to a spine first then calculate. The examples must show no implicit joins and no hidden metadata enrichment.

## Concrete Steps

Run all commands from `/Users/zjx/Documents/pointline`.

First, create failing tests for primitive contracts and expected behavior.

    uv run pytest tests/v2/research/test_primitives_contract.py -q
    uv run pytest tests/v2/research/test_primitives_behavior.py -q

Expected result before implementation: import or behavior failures indicating missing primitives.

Then implement `pointline/v2/research/primitives.py` and export stable APIs.

    uv run pytest tests/v2/research/test_primitives_contract.py -q
    uv run pytest tests/v2/research/test_primitives_behavior.py -q
    uv run ruff check pointline/v2/research tests/v2/research

Expected result after implementation: primitive tests pass and lint is clean.

Finally run full v2 regression to ensure no module regressions.

    uv run pytest tests/v2 -q
    uv run ruff check pointline/v2 tests/v2
    uv run ruff format --check pointline/v2 tests/v2

Expected result: full v2 tests and style checks pass.

## Validation and Acceptance

Acceptance is behavior-based.

First, each primitive fails fast with a clear message when required columns are missing or invalid parameters are provided.

Second, each primitive returns deterministic output ordering when given the same input.

Third, as-of join primitives enforce explicit direction semantics (`backward` or `forward`) and never silently switch behavior.

Fourth, label primitives produce point-in-time safe results and do not depend on implicit metadata joins.

Fifth, v2 research continues to pass existing tests, proving no regressions in discovery/query/metadata/spine modules.

## Idempotence and Recovery

The primitive layer is compute-only and has no persistent state. Re-running the same primitive with the same inputs must produce the same outputs. If a test fails mid-implementation, recovery is safe: fix the primitive or the contract test, then rerun focused tests first before rerunning full v2 checks. No destructive data-lake operations are required.

## Artifacts and Notes

Record short command evidence here during implementation.

    2026-02-13 HH:MMZ
    Command: uv run pytest tests/v2/research/test_primitives_contract.py -q
    Output: <to be filled during implementation>

    2026-02-13 HH:MMZ
    Command: uv run pytest tests/v2/research/test_primitives_behavior.py -q
    Output: <to be filled during implementation>

    2026-02-13 HH:MMZ
    Command: uv run pytest tests/v2 -q
    Output: <to be filled during implementation>

## Interfaces and Dependencies

Dependencies are intentionally minimal: `polars`, existing v2 research time/spine helpers, and canonical v2 schemas where needed. Runtime dependency on `pointline/research/*` is forbidden.

In `pointline/v2/research/primitives.py`, define explicit stable interfaces with DataFrame input and DataFrame output:

    def select_cols(*, df: pl.DataFrame, columns: list[str]) -> pl.DataFrame: ...

    def rename_cols(*, df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame: ...

    def filter_rows(*, df: pl.DataFrame, expr: pl.Expr) -> pl.DataFrame: ...

    def sort_rows(*, df: pl.DataFrame, by: list[str], descending: list[bool] | None = None) -> pl.DataFrame: ...

    def lag(*, df: pl.DataFrame, value_col: str, periods: int, by: list[str], order_col: str, out_col: str | None = None) -> pl.DataFrame: ...

    def lead(*, df: pl.DataFrame, value_col: str, periods: int, by: list[str], order_col: str, out_col: str | None = None) -> pl.DataFrame: ...

    def resample_time(*, events: pl.DataFrame, by: list[str], ts_col: str, every_us: int, closed: str = "right") -> pl.DataFrame: ...

    def group_agg(*, df: pl.DataFrame, by: list[str], aggs: list[pl.Expr], sort_by: list[str] | None = None) -> pl.DataFrame: ...

    def rolling_agg(*, df: pl.DataFrame, by: list[str], order_col: str, value_col: str, window: int, agg: str, out_col: str) -> pl.DataFrame: ...

    def join_left(*, left: pl.DataFrame, right: pl.DataFrame, on: list[str]) -> pl.DataFrame: ...

    def join_asof_backward(*, left: pl.DataFrame, right: pl.DataFrame, by: list[str], left_on: str, right_on: str) -> pl.DataFrame: ...

    def join_asof_forward(*, left: pl.DataFrame, right: pl.DataFrame, by: list[str], left_on: str, right_on: str) -> pl.DataFrame: ...

    def forward_return(*, bars: pl.DataFrame, price_col: str, periods: int, by: list[str], order_col: str, out_col: str = "fwd_ret") -> pl.DataFrame: ...

    def bar_move(*, bars: pl.DataFrame, open_col: str, close_col: str, out_col: str = "bar_move") -> pl.DataFrame: ...

All primitives must document required columns, null behavior, and deterministic ordering rules in their docstrings. Error messages must name the missing or invalid input explicitly.

---

Revision Note (2026-02-13 15:16Z): Initial plan created to lock primitive-only v2 research scope before implementation. This prevents accidental scope creep into pipeline/spec/DSL layers.
