# Canonical Table-Domain Interface Refactor (Event Tables + Shared Primitives)

## Summary
Unify all event-table semantics behind one canonical domain interface, make table modules the single source of truth for schema + validation + encode/decode behavior, and keep shared primitives cross-table only. Vendor plugins become adapters only. No backward compatibility layer.

## Scope
- In scope: `trades`, `quotes`, `book_snapshot_25`, `kline_1h`, `kline_1d`, `derivative_ticker`, `liquidations`, `options_chain`, `l3_orders`, `l3_ticks`.
- In scope: ingestion orchestration, research decode ownership consolidation, table metadata source-of-truth migration.
- Out of scope: changing query defaults (`decoded=False` stays), non-event table semantics (`dim_*`, `validation_log`, `dq_summary`, `stock_basic_cn`).

## Canonical Interface

### `pointline/tables/domain_contract.py`
Define:

- `TableSpec`
  - `table_name: str`
  - `schema: dict[str, pl.DataType]`
  - `partition_by: tuple[str, ...]`
  - `has_date: bool`
  - `layer: str`
  - `allowed_exchanges: frozenset[str] | None`
  - `ts_column: str`

- `TableDomain` protocol
  - `spec: TableSpec`
  - `canonicalize_vendor_frame(df) -> pl.DataFrame`
  - `encode_storage(df) -> pl.DataFrame`
  - `normalize_schema(df) -> pl.DataFrame`
  - `validate(df) -> pl.DataFrame`
  - `required_decode_columns() -> tuple[str, ...]`
  - `decode_storage(df, keep_ints=False) -> pl.DataFrame`
  - `decode_storage_lazy(lf, keep_ints=False) -> pl.LazyFrame`

### `pointline/tables/domain_registry.py`
- `register_domain(domain)`
- `get_domain(table_name)`
- `list_domains()`
- `get_table_spec(table_name)`
- `list_table_specs()`

`TableSpec` becomes metadata source of truth (replacing config-level table metadata constants).

## Responsibilities

### Vendor plugins (adapter only)
- Read raw files/API snapshots.
- Parse vendor formats.
- Output vendor-neutral intermediate frame + required metadata (`exchange`, `exchange_symbol`, `date`, `file_line_number`).
- No table business rules.

### Table domain modules (canonical truth)
- Own canonical schema, validation, enum semantics, encode/decode, and decode lazy plan.
- Reuse across vendors for same table.

### Shared primitives
- Cross-table utilities only (encoding profiles, scalar attachment, decode expression helpers).
- No table-specific column decisions.

### Ingestion orchestrator
- Pipeline only: adapter -> domain transforms -> write -> lineage/logging.
- No embedded table semantics.

## Concrete Changes

### 1) Ingestion interface migration
- Replace `TableStrategy` callable bundle in `/Users/zjx/Documents/pointline/pointline/services/generic_ingestion_service.py` with `TableDomain`.
- Update `/Users/zjx/Documents/pointline/pointline/cli/ingestion_factory.py` to resolve domains from registry.

### 2) Ingestion pipeline order
1. plugin `read_and_parse`
2. required metadata checks
3. rename `exchange_symbol -> symbol`
4. `domain.canonicalize_vendor_frame`
5. exchange validation + `spec.allowed_exchanges` check
6. date alignment + quarantine checks
7. `domain.encode_storage`
8. lineage add
9. `domain.normalize_schema`
10. `domain.validate`
11. write

### 3) Table module migration (all event tables)
- Add domain object per table.
- Move remaining semantic mappings into domain canonicalization.
- Keep schema + validation in table module.
- Provide eager + lazy decode methods from domain.

### 4) Parser adjustments (remove table semantics in parser)
- `tardis trades`: emit `side_raw` (domain maps to `side`).
- `quant360 l3_orders`: emit `side_raw`, `ord_type_raw`.
- `quant360 l3_ticks`: emit `exec_type_raw`.
- Keep adapter-level parsing and metadata enrichment only.

### 5) Shared primitives
- Extend `/Users/zjx/Documents/pointline/pointline/encoding.py` with lazy scalar attachment helper and generic decode expr helpers.
- Keep it table-agnostic.

### 6) Research decode consolidation
- Remove table-specific `_decode_*_lazy` logic from `/Users/zjx/Documents/pointline/pointline/research/core.py`.
- Decode routed via `domain.decode_storage` / `domain.decode_storage_lazy`.
- Keep query defaults unchanged (`decoded=False`).

### 7) Metadata SOT migration
- Move table metadata truth from config mirrors to domain registry.
- Migrate callers in:
  - `/Users/zjx/Documents/pointline/pointline/config.py` (`get_table_path` path resolution logic to registry-backed)
  - `/Users/zjx/Documents/pointline/pointline/research/core.py`
  - `/Users/zjx/Documents/pointline/pointline/research/discovery.py`
  - `/Users/zjx/Documents/pointline/pointline/dq/runner.py`
  - `/Users/zjx/Documents/pointline/pointline/services/generic_ingestion_service.py`
  - `/Users/zjx/Documents/pointline/pointline/cli/parser.py`
  - `/Users/zjx/Documents/pointline/pointline/introspection.py`
  - tests expecting `TABLE_PATHS`/`TABLE_HAS_DATE`.

## Public API / Interface Changes
- New: domain registry API in `pointline.tables.domain_registry`.
- Ingestion consumes `TableDomain` objects, not free-form strategy callables.
- Event-table encode interfaces become domain-owned (`encode_storage(df)`).
- Metadata source of truth moves to domain registry.
- Error text references “table/domain registry” instead of “TABLE_PATHS registry”.

## Testing Plan

### Domain registry tests
- Every event table registers exactly one domain.
- `spec.has_date` == `'date' in schema`.
- Table-name/type sync remains enforced.

### Table unit tests
- Encode/decode round-trip for each event table.
- Mixed-exchange encode/decode coverage.
- Null/unknown exchange failure behavior.

### Parser/domain boundary tests
- Parser outputs raw enum fields.
- Domain canonicalization maps raw to canonical consistently.

### Ingestion integration
- End-to-end ingest per event table through domain object.
- Allowed-exchange checks from `spec.allowed_exchanges`.
- Date alignment/quarantine still correct.

### Research API tests
- Decoded eager/lazy parity.
- Column projection with decode-required column auto-merge.
- Existing default (`decoded=False`) behavior unchanged.

### Discovery/DQ/CLI/introspection tests
- Table listing, table path, date partition behavior use registry metadata.

## Acceptance Criteria
- No event-table business semantics in vendor plugins.
- No event-table metadata truth in duplicated config constants.
- No table-specific decode logic in `research/core.py`.
- All event-table ingestion paths use domain registry interface.
- Updated docs and green targeted test suite.

## Assumptions
- No backward compatibility needed.
- All event tables refactor in one pass.
- Query default remains encoded for now.
- Non-event table semantics untouched in this refactor.
