# Schema Design v4

**Status:** Proposal
**Scope:** Robust, Polars-native schema contracts for mixed crypto + China A-share ingestion and replay

---

## 1. Philosophy

> **Schemas are code, not configuration.**

- Single source of truth in Python.
- Polars-native dtypes.
- Deterministic replay and PIT correctness are first-class concerns.
- Keep schema contracts explicit and minimal.
- Keep schema definitions implementation-agnostic.

---

## 2. File Layout

```text
pointline/
  schemas/
    __init__.py
    types.py         # canonical dtypes, scales, status constants
    events.py        # trades, quotes, orderbook_updates schemas
    dimensions.py    # dim_symbol schema (SCD2)
    control.py       # ingest_manifest, validation_log schemas
    registry.py      # table name -> schema spec registry
```

This layout is intentionally small. Split further only when table count or ownership requires it.

---

## 3. Non-Negotiable Invariants

1. Event timestamps are `Int64` microseconds UTC (`*_ts_us`).
2. Event tables include deterministic lineage keys: `file_id`, `file_seq`.
3. `symbol_id` is deterministic and stable across reruns.
4. Fixed-point numeric fields use scaled integers (`Int64`) with explicit scale constants.
5. Partition keys for event tables are `(exchange, trading_date)`.
6. `trading_date` is exchange-timezone-aware, not naive UTC date.
7. Ingestion identity key is `(vendor, data_type, bronze_path, file_hash)`.

---

## 4. Exchange-Timezone-Aware Partition Contract

For each row:

1. Read `ts_event_us` as UTC microseconds.
2. Convert to exchange local time using canonical `exchange -> tz` mapping.
3. Derive `trading_date = local_datetime.date()`.
4. Write partition path: `exchange=<exchange>/trading_date=<YYYY-MM-DD>/`.

Rules:

- A single source file may produce multiple `trading_date` partitions.
- Partition validation is required: row-level derived `trading_date` must match write partition.
- Quarantine or fail rows/files that violate partition alignment rules.

Initial timezone mapping requirements:

- Crypto venues (for example Binance) map to UTC unless explicitly defined otherwise.
- China A-share venues (`sse`, `szse`) map to `Asia/Shanghai`.

---

## 5. Core Schema Abstractions

Use two code-level primitives:

```python
@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: pl.DataType
    nullable: bool = False
    description: str = ""
    scale: int | None = None

@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[ColumnSpec, ...]
    partition_by: tuple[str, ...]
    business_keys: tuple[str, ...]
    tie_break_keys: tuple[str, ...]
    schema_version: str
```

`TableSpec` must expose `to_polars()`, `columns()`, and `required_columns()`.

---

## 6. Event Table Contracts

### 6.1 trades

Required columns:

- `exchange`, `trading_date`
- `symbol`, `symbol_id`
- `ts_event_us`, `ts_local_us` (nullable if vendor does not provide)
- `side`, `is_buyer_maker`
- `price`, `qty` (scaled `Int64`)
- `file_id`, `file_seq`

Contract:

- Partition: `(exchange, trading_date)`
- Tie-break: `(exchange, symbol_id, ts_event_us, file_id, file_seq)`

### 6.2 quotes

Required columns:

- `exchange`, `trading_date`
- `symbol`, `symbol_id`
- `ts_event_us`, `ts_local_us` (nullable)
- `bid_price`, `bid_qty`, `ask_price`, `ask_qty` (scaled `Int64`)
- `seq_num` (nullable)
- `file_id`, `file_seq`

Contract:

- Partition: `(exchange, trading_date)`
- Tie-break: `(exchange, symbol_id, ts_event_us, file_id, file_seq)`

### 6.3 orderbook_updates

Required columns:

- `exchange`, `trading_date`
- `symbol`, `symbol_id`
- `ts_event_us`, `ts_local_us` (nullable)
- `book_seq`, `side`, `price`, `qty`
- `is_snapshot`
- `file_id`, `file_seq`

Contract:

- Partition: `(exchange, trading_date)`
- Tie-break: `(exchange, symbol_id, ts_event_us, book_seq, file_id, file_seq)`

Note: do not partition by `symbol` to avoid high-cardinality tiny-file layouts.

---

## 7. Dimension Contract (`dim_symbol`)

Purpose: PIT symbol resolution and metadata history.

Required columns:

- `symbol_id` (deterministic)
- `exchange`, `exchange_symbol`, `canonical_symbol`
- `market_type`, `base_asset`, `quote_asset`
- `valid_from_ts_us`, `valid_until_ts_us` (half-open interval)
- `is_current`
- `tick_size`, `lot_size`, `contract_size` (scaled numeric form, nullable where not applicable)
- `updated_at_ts_us`

Contract:

- PIT coverage check uses `valid_from_ts_us <= ts < valid_until_ts_us` (or open-ended current row).

---

## 8. Control Table Contracts

### 8.1 ingest_manifest

Required columns:

- `file_id`
- `vendor`, `data_type`, `bronze_path`, `file_hash`
- `status` (`pending|success|failed|quarantined`)
- `rows_total`, `rows_written`, `rows_quarantined`
- `trading_date_min`, `trading_date_max` (partition coverage summary)
- `created_at_ts_us`, `processed_at_ts_us`
- `status_reason`

Contract:

- Uniqueness by identity key `(vendor, data_type, bronze_path, file_hash)`.
- `file_id` is stable for that identity key across retries.

### 8.2 validation_log

Required columns:

- `file_id`
- `rule_name`
- `severity`
- `logged_at_ts_us`

Optional columns:

- `file_seq` (nullable for file-level errors)
- `field_name`, `field_value`
- `ts_event_us`, `symbol`, `symbol_id`
- `message`

---

## 9. Data Type and Precision Policy

- Prices and quantities are stored as scaled `Int64`.
- Scale constants are centrally defined in `types.py` (for example `PRICE_SCALE`, `QTY_SCALE`).
- Avoid `Float64` in canonical storage schemas for monetary/quantity fields.

---

## 10. Schema Change Policy

- No backward compatibility guarantee.
- No migration requirement for legacy schema versions.
- Keep only one active canonical schema per table in code.
- When schema changes, old data can be rebuilt/reingested under the new schema contract.

---

## 11. Acceptance Criteria

1. All event tables use `(exchange, trading_date)` with exchange-timezone-aware derivation.
2. PIT symbol resolution is valid at microsecond granularity.
3. Replay ordering is deterministic from explicit tie-break keys.
4. Manifest identity and status semantics remain stable across reruns.
5. Schema specs are importable, introspectable, and usable directly in Polars.

---

## 12. Related Docs

- Architecture: `docs/architecture/design.md`
- Ingestion: `docs/architecture/simplified-ingestion-design-v2.md`
