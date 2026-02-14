# Pointline Architecture Design

> Canonical reference for contributors. Read this before changing schema definitions, ETL semantics, or storage contracts.

## 1. Mission

Pointline is a **point-in-time (PIT) accurate offline data lake** for quantitative trading research. It stores and serves tick-level market data — crypto (26+ exchanges, spot and derivatives) and Chinese A-shares (SSE/SZSE Level 2/3) — with five guarantees:

| Guarantee | Meaning |
|---|---|
| **PIT correctness** | No lookahead bias. Every symbol resolves through `dim_symbol` validity windows. |
| **Deterministic replay** | Identical inputs produce identical row ordering via composite tie-break keys. |
| **Idempotent ingestion** | Re-running the same file produces the same state. Manifest identity: `(vendor, data_type, bronze_path, file_hash)`. |
| **Lineage traceability** | Every Silver row carries `file_id` + `file_seq` linking back to its Bronze source. |
| **Schema-as-code** | One canonical `TableSpec` per table. No migration framework — schema changes mean rebuild. |

**Non-goals:** real-time streaming, multi-user concurrency, distributed compute. Pointline runs on a single machine with a function-first architecture built on Polars + Delta Lake.

---

## 2. Data Flow

```
Bronze (immutable raw files)
  │
  ▼  ingest_file()
Silver (typed Delta Lake tables, partitioned by exchange + trading_date)
  │
  ▼  load_events(), build_spine(), discover_symbols()
Research (PIT-correct decoded Polars DataFrames)
```

### 2.1 Bronze Layer

Immutable raw vendor files. Never modified after landing.

| Vendor | Format | Content |
|---|---|---|
| **Tardis** | CSV.gz | Crypto trades, quotes, orderbook, derivative tickers, liquidations, options chain |
| **Quant360** | 7z archives containing per-symbol CSVs | CN L2/L3 order events, tick events, snapshots |
| **Tushare** | API JSON | CN stock metadata (stock_basic) |

Each file is described by `BronzeFileMetadata`: vendor, data_type, path, file_size, last_modified, SHA-256 hash. Optional fields: date, interval, extra metadata.

### 2.2 Silver Layer

Typed, normalized Delta Lake tables with deterministic lineage. Twelve tables across three kinds:

**Event tables** (partitioned by `exchange, trading_date`):

| Table | Source | Key columns beyond common |
|---|---|---|
| `trades` | Tardis | trade_id, side, is_buyer_maker, price, qty |
| `quotes` | Tardis | bid/ask price/qty, seq_num |
| `orderbook_updates` | Tardis | book_seq, side, price, qty, is_snapshot |
| `derivative_ticker` | Tardis | mark/index/last price, open_interest, funding_rate |
| `liquidations` | Tardis | liquidation_id, side, price, qty |
| `options_chain` | Tardis | option_type, strike, expiration, Greeks, IVs |
| `cn_order_events` | Quant360 | channel_id/seq, order_ref, event_kind, side, order_type, price, qty |
| `cn_tick_events` | Quant360 | channel_id/seq, bid/ask_order_ref, event_kind, aggressor_side, price, qty |
| `cn_l2_snapshots` | Quant360 | 10-level bid/ask arrays, OHLCV, trading_phase_code |

**Dimension tables** (unpartitioned):

| Table | Content |
|---|---|
| `dim_symbol` | SCD Type 2 symbol registry with validity windows |

**Control tables** (unpartitioned):

| Table | Content |
|---|---|
| `ingest_manifest` | Idempotency ledger: one row per ingested file |
| `validation_log` | Quarantined rows with rule name and severity |

All event tables share a common column prefix:

```
exchange, trading_date, symbol, symbol_id,
ts_event_us, ts_local_us, file_id, file_seq
```

### 2.3 Research Layer

PIT-correct query and analysis APIs returning decoded Polars DataFrames. Fixed-point integers are decoded to Float64 only here, never mid-pipeline. Symbol metadata is attached via PIT as-of joins.

---

## 3. Data Encoding Conventions

### 3.1 Timestamps

All timestamps are **Int64 UTC microseconds** (`*_ts_us` suffix). Two timestamps per event row:

- `ts_event_us` — when the event occurred (exchange-reported time)
- `ts_local_us` — when the event was observed locally (nullable, used for replay fidelity)

`trading_date` (Date type) is derived from `ts_event_us` converted to exchange-local time:
- Crypto exchanges → UTC
- SSE/SZSE → Asia/Shanghai

This derived date drives Delta Lake partitioning and enables efficient pruning.

### 3.2 Fixed-Point Integers

Prices and quantities are stored as `Int64` scaled by constants:

```python
PRICE_SCALE = 1_000_000_000   # 10^9
QTY_SCALE   = 1_000_000_000   # 10^9
```

A price of `$42,350.75` is stored as `42_350_750_000_000`. This avoids floating-point imprecision for financial arithmetic and enables exact equality checks.

**Rules:**
- Scaling happens during ingestion (vendor parsers multiply raw values by the scale factor)
- Values remain scaled throughout the Silver layer — no decoding mid-pipeline
- `decode_scaled_columns()` in the Research API converts to Float64 at final output
- Dollar-denominated spine calculations use Python big-int to avoid Int64 overflow

### 3.3 Deterministic Ordering

Every event table defines tie-break keys that guarantee reproducible row ordering:

| Table family | Tie-break keys |
|---|---|
| Trades, quotes | `(exchange, symbol_id, ts_event_us, file_id, file_seq)` |
| Orderbook updates | `(exchange, symbol_id, ts_event_us, book_seq, file_id, file_seq)` |
| CN order/tick events | `(exchange, symbol_id, trading_date, channel_id, channel_seq, file_id, file_seq)` |
| CN L2 snapshots | `(exchange, symbol_id, ts_event_us, snapshot_seq, file_id, file_seq)` |

`file_id` is a monotonic integer assigned per ingested file. `file_seq` is the row's position within that file (1-indexed when assigned by ingestion lineage). Together they provide global deterministic ordering even when event timestamps collide.

---

## 4. Symbol Management (SCD Type 2)

`dim_symbol` tracks symbol metadata changes over time using Slowly Changing Dimension Type 2.

### 4.1 Schema

```
symbol_id           Int64     blake2b(exchange|exchange_symbol|valid_from_ts_us)
exchange            Utf8
exchange_symbol     Utf8      raw exchange symbol (e.g., "BTCUSDT", "000001")
canonical_symbol    Utf8      normalized identifier
market_type         Utf8      spot, perpetual, future, option, stock, ...
base_asset          Utf8
quote_asset         Utf8
valid_from_ts_us    Int64     validity window start (inclusive)
valid_until_ts_us   Int64     validity window end (exclusive), MAX_INT64 if current
is_current          Boolean
tick_size           Int64     scaled by PRICE_SCALE
lot_size            Int64     scaled by QTY_SCALE
contract_size       Int64     scaled by QTY_SCALE
updated_at_ts_us    Int64
```

**Natural key:** `(exchange, exchange_symbol)`. **Tracked columns** (changes trigger new version): canonical_symbol, market_type, base_asset, quote_asset, tick_size, lot_size, contract_size.

### 4.2 Operations

All operations are **pure functions** operating on Polars DataFrames — no I/O, no caching.

- **`bootstrap(snapshot, effective_ts_us)`** — Create initial dim_symbol from a full vendor snapshot. Every row becomes current with `valid_from = effective_ts_us`.
- **`upsert(dim, snapshot, effective_ts_us, delistings=None)`** — Full SCD2 update:
  - Detects changes in tracked columns → closes old version, opens new version
  - Handles delistings: implicit (missing from snapshot → closed) or explicit (delisting DataFrame with per-symbol timestamps)
  - New symbols → opened as current
- **`validate(dim)`** — Enforces invariants: `valid_until > valid_from`, no duplicate `is_current=True` per natural key, no overlapping validity windows, unique symbol_ids.

### 4.3 Symbol ID Generation

```python
symbol_id = int.from_bytes(
    blake2b(f"{exchange}|{exchange_symbol}|{valid_from_ts_us}".encode(), digest_size=8).digest(),
    byteorder="little", signed=True
)
```

Deterministic, collision-resistant, and stable across re-ingestions as long as the validity window start doesn't change.

### 4.4 PIT Resolution

During ingestion, `check_pit_coverage()` joins event rows against `dim_symbol` to resolve `symbol_id`. The join condition is:

```
dim.exchange == event.exchange
AND dim.exchange_symbol == event.symbol
AND dim.valid_from_ts_us <= event.ts_event_us
AND event.ts_event_us < dim.valid_until_ts_us
```

Rows without matching PIT coverage are quarantined, not silently dropped.

---

## 5. Ingestion Pipeline

### 5.1 Entry Point

```python
def ingest_file(
    meta: BronzeFileMetadata,
    *,
    parser: Parser,              # (BronzeFileMetadata) -> pl.DataFrame
    manifest_repo: ManifestStore,
    writer: Writer,              # Callable[[str, pl.DataFrame], None] | EventStore
    dim_symbol_df: pl.DataFrame,
    quarantine_store: QuarantineStore | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> IngestionResult
```

### 5.2 Pipeline Stages

```
┌─────────────────────────────────────────────────┐
│ 1. Table resolution                             │
│    Map vendor data_type to canonical table name  │
├─────────────────────────────────────────────────┤
│ 2. Idempotency check                            │
│    Skip if (vendor, data_type, path, hash)      │
│    already succeeded (unless force=True)         │
├─────────────────────────────────────────────────┤
│ 3. File ID assignment                           │
│    Mint monotonic file_id from manifest store    │
├─────────────────────────────────────────────────┤
│ 4. Parsing                                      │
│    Vendor-specific parser → raw DataFrame        │
├─────────────────────────────────────────────────┤
│ 5. Canonicalization                             │
│    Normalize vendor column names/values          │
│    (e.g., side codes, event kinds, scaling)      │
├─────────────────────────────────────────────────┤
│ 6. Trading date derivation                      │
│    ts_event_us → exchange-local date             │
├─────────────────────────────────────────────────┤
│ 7. Generic validation                           │
│    Quarantine: invalid sides, negative prices,   │
│    crossed quotes                                │
├─────────────────────────────────────────────────┤
│ 8. Exchange-specific validation                 │
│    Quarantine: SSE rows missing sequence fields  │
├─────────────────────────────────────────────────┤
│ 9. PIT coverage check                           │
│    Join dim_symbol → resolve symbol_id           │
│    Quarantine rows without coverage              │
├─────────────────────────────────────────────────┤
│ 10. Lineage assignment                          │
│     Stamp file_id + sequential file_seq          │
├─────────────────────────────────────────────────┤
│ 11. Schema normalization                        │
│     Cast to canonical TableSpec, add nullables   │
├─────────────────────────────────────────────────┤
│ 12. Write → Delta Lake                          │
├─────────────────────────────────────────────────┤
│ 13. Manifest update                             │
│     Record status, row counts, date ranges       │
└─────────────────────────────────────────────────┘
```

### 5.3 Quarantine Strategy

Invalid rows are **quarantined, not dropped**. Quarantine reasons accumulate across validation stages. Quarantined rows are written to `validation_log` with:

- `file_id` — source file
- `rule_name` — which validation rule triggered
- `severity` — always "error" for quarantine
- `field_name`, `field_value` — the offending data
- `message` — human-readable explanation

This preserves full auditability: you can always explain why a row was excluded.

---

## 6. Schema System

### 6.1 Schema Primitives

```python
@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: pl.DataType
    nullable: bool = False
    description: str = ""
    scale: int | None = None      # PRICE_SCALE or QTY_SCALE for fixed-point

@dataclass(frozen=True)
class TableSpec:
    name: str
    kind: str                      # "event" | "dimension" | "control"
    column_specs: tuple[ColumnSpec, ...]
    partition_by: tuple[str, ...]  # () for unpartitioned
    business_keys: tuple[str, ...]
    tie_break_keys: tuple[str, ...]
    schema_version: str            # e.g., "v2"
```

`TableSpec` validates at construction: no duplicate columns, all key columns exist in specs. Helper methods: `columns()`, `to_polars()`, `scaled_columns()`, `scale_for(column_name)`.

### 6.2 Registry

All twelve table specs are registered in `pointline/schemas/registry.py`:

```python
TABLE_SPECS: dict[str, TableSpec] = {
    "trades": TRADES,
    "quotes": QUOTES,
    "orderbook_updates": ORDERBOOK_UPDATES,
    "derivative_ticker": DERIVATIVE_TICKER,
    "liquidations": LIQUIDATIONS,
    "options_chain": OPTIONS_CHAIN,
    "cn_order_events": CN_ORDER_EVENTS,
    "cn_tick_events": CN_TICK_EVENTS,
    "cn_l2_snapshots": CN_L2_SNAPSHOTS,
    "dim_symbol": DIM_SYMBOL,
    "ingest_manifest": INGEST_MANIFEST,
    "validation_log": VALIDATION_LOG,
}
```

Access via `get_table_spec(name)` or `list_table_specs()`.

### 6.3 Schema Evolution Policy

**No backward compatibility.** Schema changes require full re-ingestion. There are no migration scripts, compatibility shims, or version negotiation. The schema version is tracked by git history only.

Rationale: the cost of re-ingestion is bounded (single-machine, offline workload), while the complexity of maintaining compatibility layers compounds over time. A clean rebuild is always possible because Bronze files are immutable.

---

## 7. Storage Layer

### 7.1 Protocol Contracts

The storage layer is defined by six runtime-checkable protocols in `pointline/storage/contracts.py`:

| Protocol | Responsibility |
|---|---|
| `ManifestStore` | Idempotency: file ID allocation, pending-file filtering, status updates |
| `EventStore` | Append-only writes to event tables |
| `DimensionStore` | Load/save `dim_symbol` with optimistic concurrency |
| `QuarantineStore` | Persist quarantined rows as validation_log records |
| `PartitionOptimizer` | Compact small Delta Lake files within partitions |
| `TableVacuum` | Remove old Delta Lake file versions |

### 7.2 Delta Lake Implementation

The default (and currently only) implementation uses Delta Lake via the `deltalake` Python library.

**`DeltaEventStore`**: Validates DataFrame against `TableSpec` before writing. Enforces `kind == "event"`. Writes via `write_deltalake()` with partition columns.

**`DeltaManifestStore`**: Uses file-lock-based monotonic ID allocation (`filelock.FileLock`). Manifest is read/written as full Delta table. Identity matching via `(vendor, data_type, bronze_path, file_hash)`.

**`DeltaDimensionStore`**: Supports optimistic concurrency via `expected_version` parameter on save. Validates `dim_symbol` invariants before persisting.

**`DeltaQuarantineStore`**: Converts quarantined rows into `validation_log` records (rule_name = quarantine reason, severity = "error").

**`DeltaPartitionOptimizer`**: Compacts partitions with many small files. Skips partitions below `min_small_files` threshold. Supports dry-run mode.

### 7.3 Path Layout

```
{lake_root}/
└── silver/
    ├── trades/                    # partitioned by exchange, trading_date
    │   ├── exchange=binance/
    │   │   └── trading_date=2024-01-15/
    │   └── exchange=okex/
    ├── quotes/
    ├── orderbook_updates/
    ├── ...
    ├── dim_symbol/                # unpartitioned
    ├── ingest_manifest/           # unpartitioned
    └── validation_log/            # unpartitioned
```

---

## 8. Research API

### 8.1 Event Loading

```python
def load_events(
    *, silver_root, table, exchange, symbol,
    start: int | str | date | datetime,
    end: int | str | date | datetime,
    columns=None, include_lineage=False,
) -> pl.DataFrame
```

- Time window is `[start, end)` on `ts_event_us`
- Derives trading_date bounds for Delta Lake partition pruning
- Sorts by tie-break keys
- Strips lineage columns by default

### 8.2 Symbol Discovery

```python
def discover_symbols(
    *, silver_root, exchange, q=None, as_of=None,
    include_meta=False, limit=50,
) -> pl.DataFrame
```

- Without `as_of`: returns currently-active symbols only
- With `as_of`: returns symbols valid at that timestamp (PIT semantics)
- Optional text search across exchange_symbol, canonical_symbol, base_asset

### 8.3 Spine System

Spines provide uniform sampling grids for time-series analysis. Four builders:

| Spine type | Config | Sampling logic |
|---|---|---|
| **Clock** | `ClockSpineConfig(step_us)` | Regular time intervals |
| **Trades** | `TradesSpineConfig()` | One point per unique trade timestamp |
| **Volume** | `VolumeSpineConfig(volume_threshold_scaled)` | Point when cumulative volume crosses bucket boundary |
| **Dollar** | `DollarSpineConfig(dollar_threshold_scaled)` | Point when cumulative notional crosses threshold |

All builders enforce `max_rows` limits and produce uniform output: `(exchange, symbol, symbol_id, ts_spine_us)`.

**Alignment:** `align_to_spine(events, spine)` performs a forward as-of join. Events at a spine boundary map to the **next** bar, preventing lookahead:

```
event at ts=100, spine at ts=100 → assigned to bar ending at next spine point
```

### 8.4 Research Primitives

- **`decode_scaled_columns(df, table)`** — Convert Int64 fixed-point to Float64. Adds `<col>_decoded` columns by default, preserving originals.
- **`join_symbol_meta(df, silver_root, columns, ts_col="ts_event_us")`** — PIT as-of join attaching requested dim_symbol metadata to event rows. Join condition: `valid_from_ts_us <= ts_col < valid_until_ts_us`.

### 8.5 CN Trading Phases

```python
class TradingPhase(Enum):
    CLOSED       = "CLOSED"
    PRE_OPEN     = "PRE_OPEN"       # 09:15–09:25
    MORNING      = "MORNING"        # 09:30–11:30
    NOON_BREAK   = "NOON_BREAK"     # 11:30–13:00
    AFTERNOON    = "AFTERNOON"      # 13:00–14:57
    CLOSING      = "CLOSING"        # 14:57–15:00 (SZSE only)
    AFTER_HOURS  = "AFTER_HOURS"    # 15:05–15:30 (STAR/Growth boards)
```

Functions: `add_phase_column()` (vectorized), `filter_by_phase()`. Supports SSE and SZSE only.

---

## 9. Vendor Integrations

### 9.1 Tardis (Crypto)

Six parsers in `pointline/vendors/tardis/parsers.py`, dispatched by data_type string:

| data_type | Parser | Target table |
|---|---|---|
| `trades` | `parse_tardis_trades` | trades |
| `quotes` | `parse_tardis_quotes` | quotes |
| `incremental_book_L2` | `parse_tardis_incremental_l2` | orderbook_updates |
| `derivative_ticker` | `parse_tardis_derivative_ticker` | derivative_ticker |
| `liquidations` | `parse_tardis_liquidations` | liquidations |
| `options_chain` | `parse_tardis_options_chain` | options_chain |

Tardis CSVs are self-describing (exchange and symbol in each row). Timestamps prefer the `timestamp` column, falling back to `local_timestamp`.

### 9.2 Quant360 (CN L2/L3)

Archives are 7z files with naming convention: `{stream_type}_{market}_{exchange}_{YYYYMMDD}.7z` (for example: `order_new_STK_SZ_20240115.7z`). Each archive contains per-symbol CSVs.

**Upstream pipeline** (`pointline/vendors/quant360/upstream/`):
1. `discover_quant360_archives()` — scan for *.7z, compute SHA-256
2. `plan_archive_members()` — list CSV members, extract symbol from filename
3. `iter_archive_members()` — yield CSV bytes from 7z
4. `publish_member_payload()` — write CSV.gz to Bronze directory
5. `Quant360UpstreamLedger` — JSON-based ledger tracking processed archives

**Parsers** handle SSE/SZSE column mapping differences:
- SSE orders: explicit ADD/CANCEL event kinds
- SZSE orders: always ADD, with separate cancel mechanism
- SZSE L2 snapshots: 10-level bid/ask arrays parsed from JSON strings

**Canonicalization** normalizes vendor-specific codes:
- Side: "1"/"B" → "BUY", "2"/"S" → "SELL"
- Prices/quantities: raw values × PRICE_SCALE/QTY_SCALE

### 9.3 Tushare (CN Metadata)

Pure-function module converting Tushare `stock_basic` API output to dim_symbol snapshots:

- `stock_basic_to_snapshot()` — full historical snapshot with listed/paused/delisted status
- `stock_basic_to_delistings()` — extract delistings for incremental SCD2 workflows
- Hardcoded: tick_size = 0.01 CNY, lot_size = 100 shares (200 for STAR Market)

---

## 10. Module Dependency Graph

```
pointline/
├── schemas/          ← no internal dependencies (leaf module)
│   ├── types.py      ← ColumnSpec, TableSpec, PRICE_SCALE, QTY_SCALE
│   ├── events.py     ← depends on types
│   ├── events_cn.py  ← depends on types
│   ├── events_tardis.py ← depends on types
│   ├── dimensions.py ← depends on types
│   ├── control.py    ← depends on types
│   └── registry.py   ← imports all specs
│
├── protocols.py      ← depends on schemas (for type hints only)
│
├── dim_symbol.py     ← depends on schemas/dimensions
│
├── ingestion/        ← depends on schemas, protocols, dim_symbol
│   ├── pipeline.py   ← orchestrator, depends on all ingestion submodules
│   ├── manifest.py   ← depends on protocols
│   ├── timezone.py   ← depends on exchange timezone map
│   ├── pit.py        ← depends on dim_symbol
│   └── lineage.py    ← pure function
│
├── storage/          ← depends on schemas, protocols
│   ├── contracts.py  ← protocol definitions
│   └── delta/        ← depends on contracts, schemas, deltalake
│
├── research/         ← depends on schemas, dim_symbol
│   ├── query.py      ← depends on schemas, deltalake
│   ├── discovery.py  ← depends on dim_symbol
│   ├── spine.py      ← depends on schemas, query
│   └── primitives.py ← depends on schemas, dim_symbol
│
└── vendors/          ← depends on schemas (for scale constants)
    ├── tardis/       ← self-contained
    ├── quant360/     ← self-contained
    └── tushare/      ← self-contained
```

Key constraint: **vendors never import from ingestion, storage, or research**. They produce DataFrames that conform to expected column contracts, but have no compile-time dependency on the rest of the system.

---

## 11. Key Architectural Decisions

### 11.1 Why Function-First (No Classes for Business Logic)

Core operations — `ingest_file()`, `dim_symbol.upsert()`, `check_pit_coverage()` — are pure functions operating on DataFrames. This makes them:

- **Testable**: pass in data, assert on output. No mocking needed.
- **Composable**: pipeline stages are independent functions chained in `ingest_file()`.
- **Debuggable**: intermediate DataFrames can be inspected at any stage.

Classes are reserved for storage implementations where stateful initialization (connection strings, file locks) is genuinely needed.

### 11.2 Why Fixed-Point Over Decimal/Float

- **Exact equality**: `price_a == price_b` works without epsilon comparison
- **Performance**: Int64 arithmetic is faster than Decimal in Polars
- **Storage efficiency**: Int64 is 8 bytes vs variable-length Decimal
- **Overflow safety**: 10^9 scale × typical prices fits comfortably in Int64. Dollar spine uses Python big-int for large intermediate products.

### 11.3 Why Quarantine Over Silent Drop

Dropping invalid rows creates invisible data gaps. Quarantining preserves auditability:
- You can query `validation_log` to find out *why* rows were excluded
- You can fix the validation rule and re-ingest if the rule was wrong
- Coverage reports accurately reflect what was received vs. what was kept

### 11.4 Why No Backward Compatibility

The cost/benefit is clear for an offline, single-machine system:
- **Cost of re-ingestion**: bounded, automated, deterministic
- **Cost of compatibility layers**: compounds over time, creates subtle bugs, slows development
- Bronze files are immutable, so a clean rebuild is always possible.

### 11.5 Why Delta Lake

- **Partition pruning**: critical for `(exchange, trading_date)` queries on large datasets
- **ACID transactions**: safe concurrent reads during ingestion
- **Time travel**: inspect historical table states for debugging
- **Schema enforcement**: catches column mismatches at write time
- **Compaction**: merge small files without rewriting the full table

---

## 12. Self-Review Checklist

Before merging changes that touch schema, ETL, or storage:

- [ ] **Deterministic?** Given the same inputs, will the output be byte-identical?
- [ ] **Idempotent?** Can the operation be safely re-run?
- [ ] **No lookahead bias?** Are all symbol resolutions and joins PIT-correct?
- [ ] **`ts_local_us` preserved?** Is replay fidelity maintained?
- [ ] **As-of joins, not exact?** Are temporal joins using `<=` / `<` bounds?
- [ ] **Fixed-point decoded only at output?** No mid-pipeline Float64 conversion?
- [ ] **Quarantine, not drop?** Are invalid rows written to validation_log?
- [ ] **Tie-break keys correct?** Does the table spec define full deterministic ordering?
