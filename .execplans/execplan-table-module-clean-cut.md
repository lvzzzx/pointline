# Clean-Cut Table Module Architecture (No Backward Compatibility)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan must be maintained in accordance with `/Users/zjx/Documents/pointline/PLANS.md`.

## Purpose / Big Picture

After this change, the repository will have one clear table architecture with no dual registries or compatibility shims for event-table semantics. A developer will be able to answer, from code alone, where table metadata lives, where business semantics live, and which pipeline owns each table kind. Event tables (append-only market facts) and dimension tables (state history with validity windows) will use separate contracts under one registry so orchestration stays simple and predictable.

The observable result is that event-table modules register only table domains, event ingestion uses those domains directly, and `dim_symbol` is represented by a dimension-domain contract instead of being forced into event-specific methods. Runtime behavior is validated by domain-focused test suites and end-to-end ingestion/query checks.

## Progress

- [x] (2026-02-12 06:58Z) Authored this ExecPlan with final target architecture, migration sequence, and acceptance criteria.
- [x] (2026-02-12 07:21Z) Implemented contract split in `/Users/zjx/Documents/pointline/pointline/tables/domain_contract.py` (`EventTableDomain`, `DimensionTableDomain`, `AnyTableDomain`) while keeping shared `TableSpec`.
- [x] (2026-02-12 07:21Z) Implemented unified registry behavior in `/Users/zjx/Documents/pointline/pointline/tables/domain_registry.py` with table-kind aware APIs (`get_event_domain`, `get_dimension_domain`, `list_event_domains`, `list_dimension_domains`).
- [ ] Migrate all event tables to domain-only registration and domain-owned semantics (completed: all event `TableSpec` now declare `table_kind="event"` and register in domain registry; remaining: remove residual module-level compatibility wrappers where still present).
- [x] (2026-02-12 07:21Z) Migrated `dim_symbol` to `DimensionTableDomain` and registered it via domain registry.
- [ ] Remove runtime dependence on `schema_registry` for event/dimension table semantics and update related tests/docs (completed: event/dim runtime paths now use domain/introspection APIs; remaining: cleanup of non-domain legacy schema_registry uses and associated docs).
- [ ] Run full validation commands and record evidence in this plan (completed: targeted lint + tests; remaining: full repository test run).

## Surprises & Discoveries

- Observation: The current `TableDomain` protocol is explicitly event-oriented and encodes assumptions that do not fit SCD2 dimensions.
  Evidence:
    `/Users/zjx/Documents/pointline/pointline/tables/domain_contract.py` defines `canonicalize_vendor_frame`, `encode_storage`, and `decode_storage*`, which are not appropriate for `dim_symbol` lifecycle operations.

- Observation: `dim_symbol` is stateful SCD2 logic, not event normalization logic.
  Evidence:
    `/Users/zjx/Documents/pointline/pointline/dim_symbol.py` contains `scd2_bootstrap`, `scd2_upsert`, `diff_snapshots`, and `apply_scd2_diff`.

- Observation: Renaming `research.core` dependency from `get_domain` to `get_event_domain` broke a unit test patch target.
  Evidence:
    `tests/test_research.py::test_load_trades_decoded_keeps_ints_for_requested_columns` failed until the patch target changed to `pointline.research.core.get_event_domain`.

## Decision Log

- Decision: Use one registry (`domain_registry`) for both table kinds, but split contracts by semantics.
  Rationale: One registry preserves discoverability and single metadata ownership; separate contracts avoid fake no-op methods and semantic leakage.
  Date/Author: 2026-02-12 / Codex

- Decision: Treat `schema_registry` as non-canonical and remove it from runtime table semantics.
  Rationale: Dual metadata systems create drift and hidden coupling. The architecture goal is a single source of truth.
  Date/Author: 2026-02-12 / Codex

- Decision: Keep `TableSpec` as shared metadata across domain kinds.
  Rationale: Partitioning, schema, and layer metadata are cross-cutting and should stay uniform for orchestration and discovery.
  Date/Author: 2026-02-12 / Codex

- Decision: Add `table_kind` directly to `TableSpec` instead of maintaining a parallel kind registry.
  Rationale: Domain kind should travel with table metadata to keep dispatch rules explicit and prevent drift.
  Date/Author: 2026-02-12 / Codex

- Decision: Introduce explicit event/dimension getters (`get_event_domain`, `get_dimension_domain`) and migrate orchestration/query decode call sites to event-specific lookups.
  Rationale: Fail-fast type boundaries are clearer than generic runtime assumptions and prevent accidental decode calls on dimension tables.
  Date/Author: 2026-02-12 / Codex

- Decision: Define `dim_symbol` dimension validation invariants in domain `validate()` (window ordering, single current row per natural key, overlap check).
  Rationale: Dimension-domain ownership should include temporal correctness guarantees, not only schema casting.
  Date/Author: 2026-02-12 / Codex

## Outcomes & Retrospective

Milestones 1-3 are materially started and mostly implemented: contracts are split, registry is kind-aware, and `dim_symbol` is now a dimension domain. Event modules now declare kind metadata, and event orchestration/query paths use event-specific domain getters.

Remaining gaps are cleanup and completeness work: remove residual wrapper surfaces in event modules, finish deprecating non-domain runtime schema registry usage, and run the full repository validation pass.

## Context and Orientation

This repository currently has a strong move toward table-domain ownership for event tables, but still contains remnants of compatibility-era structure and legacy schema registration patterns. The relevant files are:

- `/Users/zjx/Documents/pointline/pointline/tables/domain_contract.py` for split event/dimension domain protocols and shared `TableSpec`.
- `/Users/zjx/Documents/pointline/pointline/tables/domain_registry.py` for runtime registration and lookup of table domains.
- `/Users/zjx/Documents/pointline/pointline/tables/*.py` (event modules) for schema, canonicalization, validation, and encode/decode logic.
- `/Users/zjx/Documents/pointline/pointline/dim_symbol.py` for SCD2 dimension operations.
- `/Users/zjx/Documents/pointline/pointline/services/generic_ingestion_service.py` and `/Users/zjx/Documents/pointline/pointline/cli/ingestion_factory.py` for ingestion orchestration.
- `/Users/zjx/Documents/pointline/pointline/research/core.py` and `/Users/zjx/Documents/pointline/pointline/research/discovery.py` for query/discovery behavior.

Definitions used in this plan:

An event table is an append-only stream of observed market facts (for example trades, quotes, L3 ticks). A dimension table is a historical state table that answers “what definition was valid at time T,” implemented with SCD2 validity intervals. A contract is the Python protocol that table modules must implement. A registry is the runtime map from table name to domain object.

## Plan of Work

Milestone 1 defines clear contracts and keeps names simple. `TableSpec` remains shared. The current `TableDomain` protocol is renamed to `EventTableDomain` (or equivalent) and a new `DimensionTableDomain` is introduced for SCD2 operations. This milestone is complete when both contracts are expressed in code and all existing event modules type-check against the event contract.

Milestone 2 updates registry and orchestration to be explicit about table kind. `domain_registry` will register and resolve both domain kinds and provide metadata listing APIs that annotate kind. Event ingestion continues through the event pipeline only. A separate dimension maintenance path is introduced (or existing flow is refactored) to call dimension-specific methods. This milestone is complete when no orchestration code uses event-only method names for dimension tables.

Milestone 3 migrates modules. Event tables keep semantics in their domain modules with no compatibility wrappers required by the old architecture. `dim_symbol` becomes a first-class dimension domain implementation with methods for bootstrap and upsert/diff application and with explicit invariant checks (validity window monotonicity, overlap prevention policy, key uniqueness constraints). This milestone is complete when all current table modules are represented in the unified registry with the correct domain kind.

Milestone 4 removes legacy runtime paths and finalizes documentation/tests. Runtime schema lookup for event and dimension semantics must no longer rely on `schema_registry`. Tests are updated to validate domain registry ownership and pipeline behavior. Internal docs are updated so newcomers see one architecture, not a mixed transition story.

## Concrete Steps

Run all commands from `/Users/zjx/Documents/pointline`.

During Milestone 1, implement contracts and run:

    uv run ruff check pointline/tables/domain_contract.py pointline/tables/domain_registry.py
    uv run pytest tests/test_table_domain_registry.py -q

Expected result: lint passes; registry tests pass and include assertions for domain kind registration.

During Milestone 2, wire orchestrators and run:

    uv run ruff check pointline/services/generic_ingestion_service.py pointline/cli/ingestion_factory.py pointline/research/core.py
    uv run pytest tests/test_trades.py tests/test_research.py -q

Expected result: no regressions in event ingestion/query paths.

During Milestone 3, migrate `dim_symbol` and run:

    uv run ruff check pointline/dim_symbol.py pointline/tables/domain_contract.py pointline/tables/domain_registry.py
    uv run pytest tests/test_dim_symbol.py -q

Expected result: SCD2 invariants and update behavior remain correct under the new dimension-domain contract.

During Milestone 4, run architecture-level checks:

    uv run rg -n "schema_registry" pointline | sed -n '1,200p'
    uv run pytest -q

Expected result: no runtime-critical `schema_registry` references for event/dimension semantics and green test suite.

## Validation and Acceptance

Acceptance is behavior-based:

Event path acceptance means a normal event ingestion command still ingests rows with canonical schema and validation semantics, and query decode behavior remains consistent with the event domain methods.

Dimension path acceptance means `dim_symbol` updates still produce correct SCD2 intervals, with current rows and history rows behaving as before under bootstrap and upsert scenarios.

Architecture acceptance means a newcomer can inspect `/Users/zjx/Documents/pointline/pointline/tables/domain_contract.py` and `/Users/zjx/Documents/pointline/pointline/tables/domain_registry.py` and understand all table kinds without reading legacy compatibility modules.

Test acceptance means targeted suites pass and the full `uv run pytest -q` run passes after migration.

## Idempotence and Recovery

Each milestone is designed to be safe to re-run. Contract and registry changes are additive first; deletions happen only after tests pass for the new path. If a milestone fails midway, revert only the milestone-local edits, keep the plan updated, and rerun the milestone commands. Avoid mixed partial migrations where a table is registered but does not satisfy its contract.

Because this plan intentionally removes backward compatibility, rollback is repository-level (git revert of the milestone commit), not runtime dual-path fallback. This constraint must be respected to keep architecture clear.

## Artifacts and Notes

Evidence to capture during implementation:

- Short diffs showing contract definitions and registry APIs.
- Test output snippets showing domain-kind registration coverage.
- Test output snippets for SCD2 behavior after `dim_symbol` migration.
- Final `rg` output proving legacy runtime coupling is removed or intentionally retained only for non-table legacy utilities.

## Interfaces and Dependencies

The final architecture must expose stable interfaces in `/Users/zjx/Documents/pointline/pointline/tables/domain_contract.py`.

`TableSpec` remains shared and must continue to include table name, schema, partitioning, layer, and timestamp metadata needed by orchestration and discovery.

`EventTableDomain` must include canonicalization, storage encode/decode, schema normalization, validation, and decode-required-column semantics.

`DimensionTableDomain` must include schema normalization, validation, and SCD2 lifecycle operations. The preferred explicit methods are:

    bootstrap(snapshot_df: pl.DataFrame) -> pl.DataFrame
    upsert(current_df: pl.DataFrame, updates_df: pl.DataFrame) -> pl.DataFrame

If the implementation uses a diff/apply split, the contract may expose that split as long as it is documented and used consistently by the dimension orchestrator.

`domain_registry` must provide typed lookup and listing for both kinds and must remain the canonical runtime owner of table domain objects.

`schema_registry` may remain only as a lightweight utility for explicitly registered non-domain uses, but it must not be the source of truth for event or dimension table architecture.

---

Revision Note (2026-02-12 06:58Z): Initial ExecPlan created to replace compatibility-era table architecture with a clean domain-registry-centered design, per request for no backward compatibility.
Revision Note (2026-02-12 07:21Z): Updated plan status after implementing contract split, kind-aware registry, dim_symbol dimension domain, and targeted test/lint validation.
