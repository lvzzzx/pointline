# CLI Migration Plan

Date: 2026-01-13

## Summary
We will restructure the CLI to use clear, domain-based subcommands and consistent verbs while
preserving existing behavior via compatibility shims. The goal is improved discoverability,
consistent flag naming, and easier extension.

## Goals
- Align commands to data-lake layers (`bronze`, `silver`, `gold`) and domains (`symbol`, `assets`,
  `manifest`, `delta`).
- Standardize verbs and flags across commands.
- Preserve backward compatibility during a deprecation window.

## Non-Goals
- Change business logic, IO formats, or ETL semantics.
- Modify data schemas or table layouts.

## Proposed Command Tree
```
pointline
├─ symbol
│  ├─ search [QUERY] [--exchange] [--base-asset] [--quote-asset]
│  └─ sync [--source api|file] [--exchange] [--symbol] [--filter] [--rebuild]
│
├─ bronze
│  ├─ download --exchange --data-types --symbols --start-date --end-date
│  ├─ discover [--glob] [--data-type] [--pending-only]
│  └─ ingest [--glob] [--data-type] [--force] [--retry-quarantined] [--validate]
│
├─ silver
│  ├─ validate quotes --file [--file-id] [--exchange] [--symbol] [--date]
│  └─ validate trades --file [--file-id] [--exchange] [--symbol] [--date]
│
├─ manifest
│  ├─ show [--detailed] [--status] [--exchange] [--data-type] [--symbol]
│  └─ backfill-sha256 [--dry-run] [--limit] [--batch-size]
│
├─ assets
│  ├─ sync --date [--base-assets]
│  └─ backfill --start-date --end-date [--base-assets]
│
├─ delta
│  ├─ optimize --table --partition KEY=VALUE [--target-file-size] [--zorder]
│  └─ vacuum --table --retention-hours --execute
│
└─ gold
   └─ l2-state-checkpoint --symbol-id --start-date --end-date [--checkpoint-*]
```

## Flag Standards
- Date ranges: `--start-date` / `--end-date` (replace `--from-date`/`--to-date`).
- Consistent filters: `--exchange`, `--symbol`, `--data-type`.
- Uniform output flags (future): `--json`, `--quiet`.

## Compatibility & Deprecation Strategy
1) **Phase 1 (compatibility)**
   - Keep existing commands functional.
   - Add new command tree in parallel.
   - Emit deprecation warnings when old paths are used.

2) **Phase 2 (migration)**
   - Update docs and examples to new command tree.
   - Add `pointline help` sections that recommend new paths.
   - Provide a mapping table in docs and release notes.

3) **Phase 3 (cleanup)**
   - Remove deprecated command paths and old flag aliases.
   - Remove warnings and compatibility shims.

## Command Mapping (Old → New)
- `pointline download` → `pointline bronze download`
- `pointline ingest discover` → `pointline bronze discover`
- `pointline ingest run` → `pointline bronze ingest`
- `pointline validate quotes` → `pointline silver validate quotes`
- `pointline validate trades` → `pointline silver validate trades`
- `pointline manifest show` → `pointline manifest show` (unchanged)
- `pointline manifest backfill-sha256` → `pointline manifest backfill-sha256` (unchanged)
- `pointline dim-symbol upsert` → `pointline symbol sync --source <file>`
- `pointline dim-symbol sync` → `pointline symbol sync`
- `pointline dim-asset-stats sync` → `pointline assets sync`
- `pointline dim-asset-stats backfill` → `pointline assets backfill`
- `pointline delta optimize` → `pointline delta optimize` (unchanged)
- `pointline delta vacuum` → `pointline delta vacuum` (unchanged)
- `pointline gold l2-state-checkpoint` → `pointline gold l2-state-checkpoint` (unchanged)

## Implementation Plan
1) Add new subcommand structure in `pointline/cli/parser.py`.
2) Implement alias subcommands that dispatch to existing handlers.
3) Add flag aliases and normalize in a shared helper.
4) Update docs and examples.
5) Add tests:
   - new command path parity tests
   - deprecated path still works with warning

## Risks & Mitigations
- **User scripts break**: mitigate with shims and warnings.
- **Flag ambiguity**: ensure clear precedence in normalization logic.
- **Help output complexity**: keep top-level help concise with grouped sections.

## Acceptance Criteria
- All existing command paths still function.
- New command paths produce identical behavior.
- CI covers at least one new-path command per domain.
