# Infra North Star & Architecture Review (2026-02-10)

## North Star Goal

Pointline is a **universal offline research data lake** for quantitative trading research across all frequencies (HFT/MFT/LFT), all asset classes (crypto, equities, futures, options, forex), and all data vendors.

### Design Principles

1. **One lake, one truth.** Delta Lake is the single authoritative store for all data tiers. No secondary databases. DuckDB serves as a query layer over the same physical files when SQL or cross-sectional access is needed.

2. **Asset-class agnostic core.** The ingestion pipeline, symbol management, and storage layer make no assumptions about crypto, equities, or futures. Asset-class-specific semantics live in dimension tables and vendor plugins, not in the core pipeline.

3. **Vendor as a plugin, not a dependency.** Every data vendor is a self-contained plugin. The core system never references a specific vendor. Adding a new vendor (Databento, Polygon, CME DataMine, Bloomberg) requires zero changes to the core pipeline.

4. **Frequency-neutral storage.** Tick data, bar data, and daily snapshots share the same storage format, partitioning strategy, and query API. The difference is volume and access pattern, not architecture.

5. **PIT correctness is non-negotiable.** Every table, every join, every feature computation respects point-in-time semantics. No lookahead bias, no silent data leaks, no "just this once" exceptions.

6. **Configuration is data, not code.** Exchange registries, timezone mappings, asset class taxonomies, and vendor-to-table routing are data artifacts (config files or dimension tables), not hardcoded Python dicts.

7. **Correctness first, performance second.** Validation and lineage guarantees are never relaxed by default. Fast-path modes are explicit and documented.

### Target Scope

**Asset Classes:**

| Asset Class | Status | Exchanges | Vendors |
|-------------|--------|-----------|---------|
| Crypto spot | Active | binance, coinbase, kraken, okx, +9 | tardis, binance_vision |
| Crypto derivatives | Active | binance-futures, deribit, bybit, +5 | tardis |
| Chinese stocks (L3) | Active | szse, sse | quant360 |
| US equities | Planned | NYSE, NASDAQ, BATS, IEX, ARCA | databento, polygon |
| Traditional futures | Planned | CME, ICE, EUREX | databento, CME DataMine |
| Options | Planned | CBOE, deribit | tardis, databento |
| Forex | Planned | EBS, various ECNs | TBD |

**Data Types (Silver Tables):**

| Table | Frequency | Status |
|-------|-----------|--------|
| trades | HFT | Active |
| quotes | HFT | Active |
| book_snapshot_25 | HFT | Active |
| derivative_ticker | MFT | Active |
| kline_1h | MFT | Active |
| szse_l3_orders | HFT | Active |
| szse_l3_ticks | HFT | Active |
| kline_1d | LFT | Planned |
| liquidations | MFT | Planned |
| options_chain | MFT | Planned |
| fundamentals | LFT | Planned |

**Dimension Tables:**

| Table | Status | Purpose |
|-------|--------|---------|
| dim_symbol | Active | SCD Type 2 instrument metadata |
| dim_asset_stats | Active | Daily asset-level stats (supply, market cap) |
| dim_trading_calendar | Planned | Exchange trading schedules and holidays |
| dim_exchange | Planned | Exchange metadata (replaces hardcoded EXCHANGE_MAP) |
| stock_basic_cn | Active | CN equity reference data |
| ingest_manifest | Active | ETL tracking ledger |

### Non-Goals

- Live trading data serving (sub-ms latency SLA)
- Distributed storage or cloud object stores
- Multi-user concurrent writes
- Real-time streaming ingestion
- Backward compatibility with legacy schemas

---

## Architecture Review (2026-02-10)

### Previous Review Status (2026-02-02)

The Feb 2 review identified four weaknesses. Current status:

| Issue | Status | Resolution |
|-------|--------|------------|
| Ingestion boilerplate | **Resolved** | `GenericIngestionService` consolidated all per-table services |
| Vendor coupling | **Resolved** | Vendor plugin system (`io/vendors/`) with 5 plugins |
| DQ observability | **Open** | `validation_log` table exists but not wired into ingestion pipeline |
| Schema management | **Partially resolved** | `docs/reference/schemas.md` is comprehensive; no runtime schema registry |

### Current State (2026-02-10)

**Codebase:** ~24K LoC Python, 12 silver tables, 5 vendor plugins, 27 registered exchanges.

**What works well:**

1. **Layered architecture.** `tables/` (domain logic, pure Polars) → `services/` (orchestration) → `io/` (Delta Lake storage). Clean separation with Protocol-based interfaces. Service layer is storage-agnostic and testable.

2. **Vendor plugin system.** Capability-based protocol (`supports_parsers`, `supports_download`, `supports_prehooks`, `supports_api_snapshots`) with auto-discovery. Adding a vendor requires zero core changes. The `read_and_parse()` contract is asset-agnostic.

3. **SCD Type 2 symbol management.** `dim_symbol` with validity windows, as-of joins via `join_asof()`, contiguous coverage checks. Hash-based `symbol_id` assignment gives deterministic surrogates. Quarantine logic prevents ingestion of data for unknown symbols.

4. **Fixed-point encoding.** `px_int = round(price / price_increment)` eliminates floating-point error. Dual `tick_size` (exchange rule) vs `price_increment` (storage encoding) is well-documented.

5. **Two-tier research API.** `research.query.*` (convenience, auto-resolution) and `research.core.*` (production, explicit symbol_id). `decoded=True` hides the fixed-point join. Good developer experience.

6. **Timezone-aware partitioning.** `date` partition derived from `ts_local_us` in exchange-local timezone. `EXCHANGE_TIMEZONES` registry ensures one trading day = one partition.

### Identified Issues

#### P0: Manifest Concurrency Model

**Location:** `io/delta_manifest_repo.py:86-164`

`resolve_file_id()` uses `fcntl.flock()` on a lock file inside the Delta table directory, with a full `pl.read_delta()` under the lock to compute `max(file_id) + 1`.

Problems:
- Platform-specific (`fcntl` is POSIX-only).
- Lock file sits alongside Delta's own `_delta_log/`, creating parallel coordination.
- Full-table read under exclusive lock becomes a bottleneck as manifest grows.

Recommendation: Use Delta native MERGE for atomic ID reservation, or maintain a monotonic counter file separate from the Delta table.

#### P1: Default Write Strategy is MERGE, Not Append

**Location:** `services/generic_ingestion_service.py:118-130`

`write()` prefers `repo.merge(result, keys=["file_id", "file_line_number"])` for every ingestion, paying full MERGE cost even on the happy path. The stated justification (crash between Silver write and manifest update) can be solved more cheaply with a three-phase commit: write manifest as "in_progress" → append Silver → update manifest to "success".

#### P1: No Schema Validation at Repository Boundary

**Location:** `io/base_repository.py:49-73`

`write_full()` and `append()` blindly convert DataFrame → Arrow → Delta. No check that the DataFrame schema matches the expected table schema. Schema mismatches produce cryptic Arrow errors or silent schema widening.

Recommendation: Add `validate_schema(df, expected_schema)` before Arrow conversion. Canonical schemas exist in `tables/*.py` — they're just not enforced at the write boundary.

#### P1: symbol_id Hash Collision Risk

**Location:** `dim_symbol.py:93-119`

`assign_symbol_id_hash()` uses `blake2b` with 4-byte digest (32-bit space). With crypto (~5K symbols) this is safe. With US equities + options (~1M+ contracts), birthday paradox gives ~50% collision probability.

Recommendation: Widen digest to 8 bytes. The column is already Int64 — the storage supports it.

#### P1: Configuration is Hardcoded

**Location:** `config.py` (736 lines)

`EXCHANGE_MAP`, `EXCHANGE_METADATA`, `EXCHANGE_TIMEZONES`, `ASSET_CLASS_TAXONOMY`, `TYPE_MAP`, `ASSET_TO_COINGECKO_MAP` are all Python dicts. Every new exchange requires a code change. This violates the north star principle that configuration is data.

Recommendation: Phase 1 — move exchange/asset metadata to `dim_exchange` Silver table or `config.toml`. Phase 2 — make the vendor plugin system self-register its supported exchanges.

#### P2: dim_symbol Assumes Crypto Semantics

**Location:** `dim_symbol.py:47-75`

`TRACKED_COLS` are crypto-centric: `base_asset`, `quote_asset`, `asset_type`, `tick_size`, `lot_size`, `price_increment`, `amount_increment`, `contract_size`. Options fields (`expiry_ts_us`, `underlying_symbol_id`, `strike`, `put_call`) have been added as nullable columns for crypto options support.

Remaining gaps for future asset classes:

| Asset Class | Missing Fields |
|-------------|---------------|
| Futures | `contract_month`, `settlement_type` |
| Equities | `isin`, `cusip`, `listing_exchange`, `sector` |

Recommendation: Add nullable columns as needed when onboarding each asset class. Reconsider satellite dimension tables if dim_symbol exceeds ~25 columns.

#### P2: No Trading Calendar

No concept of trading days, exchange holidays, or market hours exists in the codebase. Cross-asset queries ("last trading day for both BTC and SPY") cannot be answered correctly.

Recommendation: Add `dim_trading_calendar` table with `(exchange, date, is_trading_day, open_time_us, close_time_us, session_type)`. Source from `exchange_calendars` package or vendor APIs.

#### P2: Quarantine Check is O(n * m)

**Location:** `services/generic_ingestion_service.py:269-293`

Row-by-row Python loop over unique `(exchange_id, exchange_symbol, date)` pairs, each doing a `dim_symbol.filter(...)`. For Quant360 files with ~3000 symbols, this is a performance bottleneck.

Recommendation: Vectorize with a single anti-join between unique pairs and dim_symbol validity windows.

#### P2: Registry Re-reads dim_symbol on Every Call

**Location:** `registry.py:19-65`

`_read_dim_symbol()` calls `pl.scan_delta(...).collect()` on every `resolve_symbol()`, `find_symbol()`, or `resolve_symbols()` invocation. No caching. In notebook sessions, this re-reads the entire table from disk repeatedly.

Recommendation: Module-level LRU cache with TTL (5-minute staleness is acceptable for offline research).

#### P2: DQ Observability Gap

`validation_log` table module exists in `pointline/tables/` but is not wired into `GenericIngestionService`. Ingestion metrics (row counts, quarantined symbols, errors, duration) are only logged to stderr. No queryable audit trail.

Recommendation: Add a `_write_validation_log()` step at the end of `ingest_file()`.

#### P3: Vendor-to-Table Routing is Implicit

When multiple vendors provide the same data type under different names (e.g., Databento `mbp-1` vs Tardis `trades` → both target `silver.trades`), the routing is implicit via `data_type` string matching.

Recommendation: Add `get_table_mapping() -> dict[str, str]` to the `VendorPlugin` protocol.

#### P3: Reverse Exchange Lookup is O(n)

**Location:** `config.py:204-225`

`get_exchange_name()` iterates the entire `EXCHANGE_MAP` dict. Called during every SCD2 upsert. Harmless at 27 exchanges, incorrect as a pattern.

Recommendation: Pre-compute reverse map at module load time.

#### P3: Schema Docs List Unimplemented Tables

`docs/reference/schemas.md` defines `book_ticker`, `liquidations`, `options_chain`, `options_surface_grid` which have no corresponding `TABLE_PATHS` entries, table modules, or parsers.

Recommendation: Add status markers (Implemented / Planned) to the schema catalog.

### Improvement Roadmap

#### Phase 1: Foundation Hardening (Before Next Asset Class)

| Task | Priority | Effort | Impact |
|------|----------|--------|--------|
| Widen `symbol_id` hash to 8 bytes | P1 | Small | Prevents collision at scale |
| Add schema validation at repository write boundary | P1 | Small | Catches schema drift early |
| Switch ingestion default from MERGE to append with in-progress status | P1 | Medium | ~2-5x write performance |
| Pre-compute reverse exchange map | P3 | Trivial | Code correctness |
| Cache dim_symbol reads with TTL | P2 | Small | Research API performance |

#### Phase 2: Multi-Asset Readiness

| Task | Priority | Effort | Impact |
|------|----------|--------|--------|
| Extract exchange registry to `dim_exchange` table or config file | P1 | Medium | Enables data-driven exchange management |
| Add nullable columns to `dim_symbol` for futures/options/equities | P2 | Medium | Unblocks next asset class |
| Implement `dim_trading_calendar` | P2 | Medium | Enables cross-asset time alignment |
| Add `get_table_mapping()` to vendor plugin protocol | P3 | Small | Clean vendor-to-table routing |

#### Phase 3: Operational Maturity

| Task | Priority | Effort | Impact |
|------|----------|--------|--------|
| Wire `validation_log` into `GenericIngestionService` | P2 | Medium | Queryable DQ audit trail |
| Vectorize quarantine check | P2 | Medium | Ingestion performance for large universes |
| Fix manifest concurrency model | P0 | Medium | Correctness under concurrent ingestion |
| Add status markers to schema catalog | P3 | Trivial | Documentation accuracy |

#### Phase 4: Scale (When 3+ Asset Classes Active)

| Task | Priority | Effort | Impact |
|------|----------|--------|--------|
| Cross-asset query helpers in research API | P2 | Medium | Research ergonomics |
| Runtime schema registry (replace scattered schema dicts) | P2 | Large | Automated schema validation and docs |

### Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                     Research Interface                            │
│  research.query.*  (convenience, auto-resolution, decoded=True)  │
│  research.core.*   (production, explicit symbol_id)              │
│  research.pipeline / research.workflow  (feature engineering)    │
└──────────┬──────────────────────────────────┬────────────────────┘
           │                                  │
  ┌────────┴────────┐               ┌────────┴────────┐
  │  Polars engine  │               │  DuckDB engine  │
  │  (HFT: tick     │               │  (MFT/LFT:      │
  │   replay, scan) │               │   SQL, cross-    │
  │                 │               │   sectional)     │
  └────────┬────────┘               └────────┬────────┘
           │                                  │
           ▼                                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Delta Lake (Single Source of Truth)            │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Silver (Canonical Research Foundation)                          │
│  ├── trades, quotes, book_snapshot_25         (HFT, partitioned)│
│  ├── derivative_ticker, kline_1h, kline_1d    (MFT, partitioned)│
│  ├── szse_l3_orders, szse_l3_ticks            (HFT, partitioned)│
│  ├── liquidations, options_chain              (planned)          │
│  └── fundamentals                             (LFT, planned)    │
│                                                                  │
│  Dimension Tables (Reference)                                    │
│  ├── dim_symbol          (SCD2, universal core)                  │
│  ├── dim_exchange         (planned, replaces EXCHANGE_MAP)       │
│  ├── dim_trading_calendar (planned)                              │
│  ├── dim_asset_stats      (daily supply/market cap)              │
│  └── ingest_manifest      (ETL ledger)                           │
│                                                                  │
│  Gold (Derived Research Tables)                                  │
│  ├── daily_ohlcv          (cross-sectional, Z-order by date)     │
│  ├── reflexivity_bars     (dollar-volume bars)                   │
│  └── options_surface_grid (time-gridded surface)                 │
│                                                                  │
├──────────────────────────────────────────────────────────────────┤
│  Bronze (Immutable Vendor Truth)                                 │
│  └── <vendor>/exchange=<exch>/type=<type>/date=<date>/...       │
└──────────────────────────────────────────────────────────────────┘
           ▲
           │
┌──────────┴──────────────────────────────────────────────────────┐
│                    Vendor Plugin System                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐          │
│  │  tardis  │ │ quant360 │ │ binance  │ │ coingecko│  ...      │
│  │          │ │          │ │ _vision  │ │          │          │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘          │
│                                                                  │
│  Future: databento, polygon, CME DataMine, tushare, bloomberg   │
└──────────────────────────────────────────────────────────────────┘

  In-Process Cache Layer:
    dim_symbol       → LRU, 5min TTL
    dim_asset_stats  → LRU, 1hr TTL
    dim_exchange     → LRU, 24hr TTL
```

### Key Design Decisions

1. **Pure data lake over hybrid storage.** DuckDB as a query layer over Delta Lake gives SQL and indexed access without dual-write consistency problems. No separate database for klines or metadata.

2. **Wide dim_symbol with nullable columns.** Options fields (strike, put_call, expiry, underlying) live directly in dim_symbol. Add more nullable columns for future asset classes. Reconsider satellite tables only if dim_symbol exceeds ~25 columns.

3. **Config as data, phased migration.** Phase 1: move `EXCHANGE_MAP` to `dim_exchange` table. Phase 2: vendor plugins self-register exchange support. Phase 3: retire `config.py` registries entirely.

4. **Trading calendar as a first-class dimension.** Required before any non-24/7 asset class can be queried correctly in cross-asset research.

5. **Append-first ingestion with in-progress status tracking.** Replace MERGE-by-default with a three-phase commit to improve write performance while maintaining crash recovery guarantees.
