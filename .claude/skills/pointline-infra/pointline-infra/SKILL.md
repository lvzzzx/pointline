---
name: pointline-infra
description: >-
  Pointline data lake infrastructure engineering reference. Use when:
  (1) writing or modifying ingestion pipelines (ingest_file, parsers, validation),
  (2) adding new vendor parsers (Tardis, Quant360, or new vendors),
  (3) working with storage contracts (EventStore, ManifestStore, DimensionStore,
  QuarantineStore, PartitionOptimizer, TableVacuum),
  (4) modifying or adding table schemas (TableSpec, ColumnSpec, registry),
  (5) working with dim_symbol SCD Type 2 operations (bootstrap, upsert, validate),
  (6) writing or reviewing ETL code, Bronze→Silver transformations,
  (7) implementing idempotent ingestion or lineage tracking,
  (8) adding exchange timezone mappings or trading_date derivation,
  (9) writing tests for ingestion, storage, or schema code.
---

# Pointline Data Lake — Infrastructure Engineering

PIT-accurate offline data lake. Polars + Delta Lake, single-machine. Function-first architecture.

**Core invariants:** PIT correctness, deterministic replay, idempotent ingestion, lineage via `file_id` + `file_seq`.

## Ingestion Pipeline

`ingest_file()` is the single entry point. 13-stage function pipeline:

```python
from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata

result = ingest_file(
    meta=BronzeFileMetadata(vendor="tardis", data_type="trades", ...),
    parser=my_parser, writer=my_writer,
    manifest_repo=manifest_store, dim_symbol_df=dim_df,
    quarantine_store=quarantine_store,
    force=False, dry_run=False,
)
```

**Stages (in order):**

1. **Resolve table** — `_TABLE_ALIASES` maps vendor data_types to canonical table names
2. **Idempotency check** — Skip if `(vendor, data_type, bronze_path, file_hash)` already processed (unless `force=True`)
3. **Parse** — `parser(meta)` returns `pl.DataFrame` with vendor-native columns
4. **Canonicalize** — Vendor-specific transforms (e.g., Quant360 column renaming)
5. **Trading date** — `derive_trading_date_frame(df)` using exchange timezone
6. **Generic validation** — `apply_event_validations(df, table_name)` — quarantines invalid rows
7. **CN validation** — `apply_cn_validations(df, table_name)` — CN-specific rules (SSE missing sequences)
8. **PIT coverage** — `check_pit_coverage(df, dim_symbol_df)` — as-of join against dim_symbol
9. **Lineage** — `assign_lineage(df, file_id)` — adds `file_id` column + `file_seq` (1-indexed)
10. **Normalize** — `normalize_to_table_spec(df, spec)` — cast to canonical types, add missing nullable columns
11. **Write** — `writer(table_name, df)` appends to Delta Lake
12. **Write quarantine** — Invalid rows → `validation_log` with reason
13. **Manifest update** — Mark file as processed with row counts

Full pipeline details: [references/pipeline.md](references/pipeline.md)

## Storage Contracts

Protocol-based abstractions in `pointline/storage/contracts.py`:

| Protocol | Key Methods | Purpose |
|---|---|---|
| `ManifestStore` | `resolve_file_id`, `filter_pending`, `update_status` | Idempotency + lineage |
| `EventStore` | `append(table_name, df)` | Write event partitions |
| `DimensionStore` | `load_dim_symbol`, `save_dim_symbol` (OCC) | SCD2 dimension management |
| `QuarantineStore` | `append(table_name, df, reason, file_id)` | Invalid row isolation |
| `PartitionOptimizer` | `compact_partitions` → `CompactionReport` | Small-file compaction |
| `TableVacuum` | `vacuum_table` → `VacuumReport` | Tombstone cleanup |

Delta Lake implementations: `pointline/storage/delta/`. Use `table_path(silver_root, table_name)` from `pointline.storage.delta.layout` to resolve paths.

Full storage reference: [references/storage.md](references/storage.md)

## dim_symbol SCD Type 2

Pure-function SCD2 in `pointline/dim_symbol.py`. All functions are stateless — take and return DataFrames.

```python
from pointline.dim_symbol import bootstrap, upsert, validate, assign_symbol_ids

# Initial load from full snapshot
dim = bootstrap(snapshot_df, effective_ts_us=1700000000_000000)

# Incremental merge (closes old rows, opens new)
dim = upsert(dim, new_snapshot, effective_ts_us, delistings=delisted_df)

# Invariant checks (raises on violation)
validate(dim)

# Generate stable symbol_ids: blake2b(exchange|exchange_symbol|valid_from_ts_us) → Int64
dim = assign_symbol_ids(dim)
```

**Key constants:** `VALID_UNTIL_MAX = 2**63 - 1`, `NATURAL_KEY = ("exchange", "exchange_symbol")`.

**Tracked columns:** Changes to any of `canonical_symbol`, `market_type`, `base_asset`, `quote_asset`, `tick_size`, `lot_size`, `contract_size` trigger a new SCD2 version.

Full dim_symbol reference: [references/dim-symbol.md](references/dim-symbol.md)

## Schema Design

Schemas live in `pointline/schemas/`. One spec per table, versioned by git only. No migrations — schema change = rebuild.

```python
from pointline.schemas import get_table_spec, list_table_specs
from pointline.schemas.types import ColumnSpec, TableSpec, PRICE_SCALE, QTY_SCALE

spec = get_table_spec("trades")
spec.columns()           # tuple of column names
spec.to_polars()         # dict[str, pl.DataType]
spec.scaled_columns()    # columns with scale factor
spec.tie_break_keys      # deterministic sort order
spec.partition_by        # Delta Lake partitions ("exchange", "trading_date")
```

**Adding a new table:**

1. Define `TableSpec` in appropriate module (`events.py`, `events_cn.py`, `events_crypto.py`)
2. Add to the relevant `*_SPECS` dict
3. Registry (`registry.py`) auto-discovers via `TABLE_SPECS = {**EVENT_SPECS, **CN_EVENT_SPECS, ...}`
4. Set `tie_break_keys` for deterministic ordering
5. Set `partition_by` (typically `("exchange", "trading_date")` for events)
6. Mark scaled columns with `scale=PRICE_SCALE` or `QTY_SCALE`

**Rules:** All timestamps `Int64` UTC microseconds. Prices/quantities `Int64` scaled by `1_000_000_000`. Never decode mid-pipeline.

## Vendor Parser Development

Parsers are functions: `(BronzeFileMetadata) → pl.DataFrame`.

**Tardis (crypto):** `pointline/vendors/tardis/parsers.py` — 6 parsers for trades, quotes, incremental_l2, derivative_ticker, liquidations, options_chain. Helpers: `_require_columns`, `_scaled_expr`, `_resolve_ts_event_expr`, `_optional_utf8`, `_optional_scaled`, `_optional_float64`.

**Quant360 (CN L2/L3):** `pointline/vendors/quant360/dispatch.py` routes via `_PARSER_BY_DATA_TYPE` to `parse_order_stream`, `parse_tick_stream`, `parse_l2_snapshot_stream`.

Full parser guide: [references/parser-patterns.md](references/parser-patterns.md)

## Validation Rules

**Generic** (`pointline/ingestion/event_validation.py`):
- Trades: `side ∈ {buy, sell, unknown}`, `price > 0`, `qty > 0`
- Quotes: `bid/ask price > 0`, `qty >= 0`, no crossed quotes
- Orderbook: `side ∈ {bid, ask}`, `price > 0`, `qty >= 0`
- Derivative ticker: `mark_price > 0`
- Liquidations: `side ∈ {buy, sell}`, `price > 0`, `qty > 0`
- Options chain: `option_type ∈ {call, put}`, `strike > 0`, `expiration_ts_us > 0`

**CN-specific** (`pointline/ingestion/cn_validation.py`):
- SSE orders: quarantine rows missing `channel_biz_seq` or `symbol_order_seq`
- SSE ticks: quarantine rows missing `channel_biz_seq` or `symbol_trade_seq`

Invalid rows go to `QuarantineStore`, never silently dropped.

## Exchange Timezone Map

`pointline/ingestion/exchange.py` — `EXCHANGE_TIMEZONE_MAP` has 26+ entries. Crypto exchanges → UTC. CN exchanges (sse, szse) → Asia/Shanghai. `get_exchange_timezone(exchange)` resolves.

`trading_date` = `ts_event_us` converted to exchange-local date. Scalar: `derive_trading_date(ts_us, exchange)`. Vectorized: `derive_trading_date_frame(df)` (requires `exchange` column).

## Key Import Paths

```python
from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata
from pointline.storage.contracts import ManifestStore, EventStore, DimensionStore, QuarantineStore
from pointline.storage.delta import DeltaEventStore, DeltaDimensionStore
from pointline.storage.delta.layout import table_path
from pointline.schemas import get_table_spec, list_table_specs
from pointline.schemas.types import PRICE_SCALE, QTY_SCALE, TableSpec, ColumnSpec
from pointline.dim_symbol import bootstrap, upsert, validate, assign_symbol_ids
from pointline.ingestion.timezone import derive_trading_date, derive_trading_date_frame
from pointline.ingestion.exchange import EXCHANGE_TIMEZONE_MAP, get_exchange_timezone
from pointline.ingestion.lineage import assign_lineage
from pointline.ingestion.pit import check_pit_coverage
from pointline.ingestion.event_validation import apply_event_validations
from pointline.ingestion.normalize import normalize_to_table_spec
from pointline.ingestion.manifest import build_manifest_identity
```

## Critical Invariants

1. **Idempotent.** Same file ingested twice → no duplicates. Identity = `(vendor, data_type, bronze_path, file_hash)`.
2. **Deterministic ordering.** Always sort by tie-break keys. Never rely on insertion order.
3. **PIT-correct.** Symbol resolution via as-of join against dim_symbol validity windows. No lookahead.
4. **Lineage traceable.** Every Silver row has `file_id` + `file_seq` pointing back to Bronze source.
5. **No silent data loss.** Invalid rows quarantined with reason, never dropped.
6. **Schema-as-code.** Canonical spec in `pointline/schemas/`. Read spec before touching tables.
7. **No backward compatibility.** Schema changes = rebuild. No migration shims.
