# Pointline Architecture

**Status:** Target
**Scope:** Single-node offline market data lake for deterministic ingestion and research

## 1) Philosophy

Keep it clean, clear, and simple:

- Schemas are code.
- File ingestion is function-first.
- Keep only mechanisms that protect PIT correctness and replay determinism.

## 2) Canonical Stack

- Storage: Delta Lake (`delta-rs`)
- Compute: Polars
- Runtime: local filesystem, single machine only

No cloud/distributed assumptions.

## 3) Data Lake Model

- **Bronze**: immutable raw vendor files and API captures
- **Silver**: typed normalized event/dimension/control tables
- **Gold**: optional derived tables

Lineage fields in Silver event tables:
- `file_id`
- `file_seq`

## 4) Partition Contract (Timezone Aware)

Event table partitions are:

- `(exchange, trading_date)`

`trading_date` is derived from event timestamp in exchange local timezone:

1. `ts_event_us` is UTC microseconds.
2. Convert using canonical `exchange -> timezone` mapping.
3. `trading_date = local_datetime.date()`.

Required initial mappings:
- Crypto exchanges: UTC (unless explicitly overridden)
- China A-share exchanges (`sse`, `szse`): `Asia/Shanghai`

## 5) Ingestion Contract

For each source file:

1. Discover candidate Bronze files.
2. Idempotency gate using manifest success state.
3. Parse and canonicalize rows.
4. Derive and validate `(exchange, trading_date)` partition alignment.
5. Resolve/check PIT symbol coverage via `dim_symbol`.
6. Apply validation and quarantine rules.
7. Write Silver with deterministic lineage (`file_id`, `file_seq`).
8. Record status and diagnostics in control tables.

Manifest identity key:
`(vendor, data_type, bronze_path, file_hash)`.

Status model:
`pending | success | failed | quarantined`.

## 6) Schema Contract

- Event timestamps use `Int64` microseconds (`*_ts_us`).
- Monetary/quantity fields use scaled `Int64` (no float in canonical storage).
- `symbol_id` is deterministic and stable across reruns.
- `dim_symbol` uses SCD2 validity windows:
  `valid_from_ts_us <= ts < valid_until_ts_us`.

Canonical schema definitions live in code under `pointline/schemas/*`.

## 7) Determinism Contract

Replay ordering must be explicit and stable.

Default tie-break keys:
- trades/quotes: `(exchange, symbol_id, ts_event_us, file_id, file_seq)`
- orderbook updates: `(exchange, symbol_id, ts_event_us, book_seq, file_id, file_seq)`

## 8) Operational Defaults

- Correctness over throughput.
- Explicit maintenance (`optimize`, `vacuum`) by operator choice.
- No backward compatibility guarantee for legacy contracts.
- No mandatory migration path for old schema versions; rebuild/reingest is acceptable.

## 9) Design References

- Ingestion design: `docs/architecture/simplified-ingestion-design-v2.md`
- Schema design: `docs/architecture/schema-design-v4.md`

## 10) Out of Scope

- Live execution/routing.
- Cloud object storage.
- Distributed compute/storage backends.
