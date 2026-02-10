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
| liquidations | MFT | Active (Ingest) |
| options_chain | MFT | Active (Ingest) |
| fundamentals | LFT | Planned |

**Dimension Tables:**

| Table | Status | Purpose |
|-------|--------|---------|
| dim_symbol | Active | SCD Type 2 instrument metadata |
| dim_asset_stats | Active | Daily asset-level stats (supply, market cap) |
| dim_exchange | Schema defined | Exchange metadata (fallback chain with EXCHANGE_MAP) |
| dim_trading_calendar | Schema defined | Exchange trading schedules and holidays |
| stock_basic_cn | Active | CN equity reference data |
| ingest_manifest | Active | ETL tracking ledger |
| validation_log | Active | Ingestion DQ audit trail |
| dq_summary | Active | Per-run data quality summaries |

### Non-Goals

- Live trading data serving (sub-ms latency SLA)
- Distributed storage or cloud object stores
- Multi-user concurrent writes
- Real-time streaming ingestion
- Backward compatibility with legacy schemas

---

## Architecture Review (2026-02-10)

### Review History

**Feb 2, 2026 review** identified four weaknesses. **All four are now resolved:**

| Issue | Status | Resolution |
|-------|--------|------------|
| Ingestion boilerplate | **Resolved** | `GenericIngestionService` consolidated all per-table services |
| Vendor coupling | **Resolved** | Vendor plugin system (`io/vendors/`) with 5 plugins |
| DQ observability | **Resolved** | `_write_validation_log()` wired into `GenericIngestionService` at all exit points |
| Schema management | **Resolved** | `pointline/schema_registry.py` provides runtime registration, lookup, and validation |

### Current State (2026-02-10)

**Codebase:** ~24K LoC Python, 12 silver tables, 5 vendor plugins, 27 registered exchanges.

### What Works Well

1. **Layered architecture.** `tables/` (domain logic, pure Polars) -> `services/` (orchestration) -> `io/` (Delta Lake storage). Clean separation with Protocol-based interfaces (`TableRepository`, `AppendableTableRepository`, `BronzeSource`). Service layer is storage-agnostic and testable.

2. **Vendor plugin system.** Capability-based protocol (`supports_parsers`, `supports_download`, `supports_prehooks`, `supports_api_snapshots`) with auto-discovery. The `VendorPlugin` protocol includes `get_table_mapping()` for explicit vendor-to-table routing and `read_and_parse()` for asset-agnostic parsing. `resolve_table_name()` in the vendor registry handles parameterized table names (e.g., `kline_{interval}`). Adding a vendor requires zero core changes.

3. **SCD Type 2 symbol management.** `dim_symbol` with validity windows, as-of joins via `join_asof()`, contiguous coverage checks. Hash-based `symbol_id` assignment using BLAKE2b with 8-byte digest (full Int64 range) gives deterministic surrogates safe for >1M contracts. Quarantine logic prevents ingestion of data for unknown symbols.

4. **Fixed-point encoding.** `px_int = round(price / price_increment)` eliminates floating-point error. Dual `tick_size` (exchange rule) vs `price_increment` (storage encoding) is well-documented.

5. **Two-tier research API.** `research.query.*` (convenience, auto-resolution) and `research.core.*` (production, explicit symbol_id). `decoded=True` hides the fixed-point join. Discovery API (`list_exchanges`, `list_symbols`, `data_coverage`, `summarize_symbol`) provides exploration without prior knowledge.

6. **Timezone-aware partitioning.** `date` partition derived from `ts_local_us` in exchange-local timezone. `EXCHANGE_TIMEZONES` registry ensures one trading day = one partition.

7. **Idempotent ingestion.** The CLI runs real ingests with `idempotent_write=True`, using MERGE on lineage keys `(file_id, file_line_number)` so retries and crash recovery are safe. Dry-run walks the full pipeline but skips all writes and validation-log side effects. The `write()` method supports both append (for bulk loads) and MERGE (for production ingests).

8. **Schema validation at write boundary.** `validate_schema()` in `base_repository.py` checks column presence, absence of unexpected columns, and type matching before all write paths (`append`, `write_full`, `overwrite_partition`, `merge`). Canonical schemas registered via `schema_registry.py`.

9. **Runtime schema registry.** `pointline/schema_registry.py` provides `register_schema()`, `get_schema()`, `get_entry()`, `list_tables()`, and `validate_df()`. Each table module self-registers at import time.

10. **Cross-platform manifest.** `resolve_file_id()` uses `filelock.FileLock` (cross-platform) with a separate binary counter file (`_FileIdCounter`) for monotonic file_id assignment. Counter syncs with manifest max on startup for crash recovery.

11. **Caching.** TTL-based caches with double-checked locking: `dim_symbol` (5 min), `dim_exchange` (1 hour). Pre-computed `_REVERSE_EXCHANGE_MAP` for O(1) reverse exchange lookup.

12. **DQ observability.** `_write_validation_log()` wired into `GenericIngestionService` at all exit points (success, error, quarantine, empty file, missing metadata). Suppressed during dry-run to guarantee no side effects. Status classification uses structured `IngestionResult.failure_reason` (e.g., `all_symbols_quarantined`, `unknown_exchange`, `ingest_exception`) rather than error-message substring matching. Best-effort writes to avoid blocking ingestion on DQ failures.

13. **Vectorized quarantine.** `_check_quarantine_vectorized()` builds unique `(exchange_id, exchange_symbol, date)` pairs, evaluates coverage per pair, then filters via anti-join in one pass. Coverage check per unique pair is still a Python call to `check_coverage()`, but the final DataFrame filter is vectorized.

14. **Config-as-data migration in progress.** `get_exchange_id()` uses a fallback chain: `dim_exchange` table (1-hour TTL cache) -> hardcoded `EXCHANGE_MAP`. The hardcoded dicts remain as fallback for cold start and backward compatibility.

### Open Issues

#### P3: Manifest Full-Table Read Under Lock (Transitional — Acceptable at Current Scale)

**Location:** `io/delta_manifest_repo.py` — `resolve_file_id()`

`resolve_file_id()` holds an exclusive `FileLock` while performing `pl.read_delta()` on the entire manifest table to check for existing identity keys. The current approach is correct and simple to reason about — it prevents duplicate file_id assignment races and works well for single-host/local workloads where manifest size is manageable.

**Why it hurts later:** Every `resolve_file_id()` reads the full manifest under lock, so concurrent workers queue up. As the manifest grows (millions of rows), lock hold time rises and ingestion throughput drops. The issue is lock duration, not correctness.

**Incremental improvement path:**
1. **Current (operational):** Full read under lock. Safest, simplest. Justified at current scale.
2. **Next step:** Keyed lookup under lock — read only the `(vendor, data_type, bronze_file_name, sha256)` match instead of the full table. Keep the lock only around lookup + insert critical section.
3. **Later:** Manifest index/partition strategy + batched ID reservation to amortize lock overhead.

#### P1: Counter File Uses 4-Byte Integer

**Location:** `io/delta_manifest_repo.py` — `_FileIdCounter`

Counter is stored as `struct.pack("<i", next_val)` — a signed 32-bit integer capping `file_id` at 2,147,483,647. Sufficient for current scale but will silently overflow if the lake reaches billions of ingested files over its lifetime.

**Recommendation:** Widen to `struct.pack("<q", next_val)` (signed 64-bit) to match the `Int64` column type. This is a one-line change with a migration step to rewrite the counter file.

#### P1: Hardcoded Config Dicts Not Yet Retired

**Location:** `config.py` (787 lines)

`EXCHANGE_MAP`, `EXCHANGE_METADATA`, `EXCHANGE_TIMEZONES`, `ASSET_CLASS_TAXONOMY`, `TYPE_MAP`, and `ASSET_TO_COINGECKO_MAP` remain as hardcoded Python dicts. The `dim_exchange` fallback chain is implemented, but the hardcoded dicts are still the primary source in practice (most deployments don't have a populated `dim_exchange` table).

Every new exchange still requires a code change to `config.py`.

**Recommendation:** Phase 1 — populate `dim_exchange` table as part of standard setup (CLI command or bootstrap script). Phase 2 — make vendor plugins self-register supported exchanges. Phase 3 — remove hardcoded dicts, keep only as emergency fallback.

#### P2: dim_symbol Column Growth for Future Asset Classes

**Location:** `dim_symbol.py`

`TRACKED_COLS` are crypto-centric. Options fields (`expiry_ts_us`, `underlying_symbol_id`, `strike`, `put_call`) have been added as nullable columns. Remaining gaps:

| Asset Class | Missing Fields |
|-------------|---------------|
| Futures | `contract_month`, `settlement_type` |
| Equities | `isin`, `cusip`, `listing_exchange`, `sector` |

**Recommendation:** Add nullable columns as needed when onboarding each asset class. Reconsider satellite dimension tables if `dim_symbol` exceeds ~25 columns.

#### P2: dim_trading_calendar and dim_exchange Not Populated

Both table modules exist with schema definitions, but neither is populated with data as part of standard setup. Cross-asset queries requiring trading day alignment ("last trading day for both BTC and SPY") cannot be answered correctly.

**Recommendation:** Add CLI commands `pointline dim populate-exchange` and `pointline dim populate-calendar` that bootstrap these tables from existing config + `exchange_calendars` package.

#### P2: Gold Layer Is Aspirational

The architecture diagram lists `daily_ohlcv`, `reflexivity_bars`, and `options_surface_grid` in Gold, but there is no Gold-layer service, no Gold-layer write path, and no documented contract for how Gold tables relate to Silver tables. The research framework v2 (`research.pipeline` / `research.workflow`) will need to produce Gold-tier outputs.

**Recommendation:** Define the boundary between "research notebook output" and "Gold table" before the research framework v2 reaches M3 (bar_then_feature production-ready). At minimum, document: who writes Gold tables (pipeline output vs. dedicated Gold service), what guarantees Gold tables provide (schema, freshness, lineage), and whether Gold tables are append-only or overwrite.

#### P2: No Backfill / Re-ingestion Strategy

The design assumes append-only ingestion. No documented path exists for:
- Vendor corrections to historical data
- Re-ingestion after schema migrations (which the versioning policy explicitly allows)
- Selective partition rebuilds

The `ingest_manifest` tracks file-level status, but there is no "mark dirty and re-ingest" workflow.

**Recommendation:** Document a backfill protocol: (1) mark affected manifest rows as `superseded`, (2) delete or archive affected Silver partitions, (3) re-run ingestion from bronze. Add CLI support: `pointline ingest backfill --table trades --exchange binance-futures --date-range 2024-05-01:2024-05-31`.

#### P2: Cross-Table Join Semantics Undocumented

The most common research pattern — joining trades + quotes + book_snapshot_25 for a single symbol — has no formalized semantics. Questions without documented answers:
- Join key: always `ts_local_us`? What about vendor clock skew between data types?
- Join type: as-of join with backward tolerance? What tolerance?
- Ordering: when trades and quotes share the same `ts_local_us`, which comes first?

The research framework v2 will need to codify these choices.

**Recommendation:** Add a `docs/reference/join-semantics.md` documenting canonical join patterns. Implement helper functions in the research API (e.g., `query.trades_with_quotes()`) that encode these semantics.

#### P2: No Data Retention or Lifecycle Policy

Bronze is immutable, Silver is append-only. At HFT resolution, storage grows linearly without bound. For a single-machine target, this matters.

No documented policy for:
- When (if ever) old Bronze files can be archived or deleted
- Silver compaction cadence beyond ad-hoc `pointline delta optimize`
- Storage growth projections or monitoring

**Recommendation:** Document a retention policy. At minimum: (1) Bronze retention = forever (or until explicitly archived), (2) Silver compaction SLA (e.g., optimize partitions older than 7 days, vacuum after 168 hours), (3) Storage growth monitoring via a simple CLI command (`pointline lake stats`).

#### P2: No Golden-File Integration Tests

Unit tests exist for individual parsers and services, but no end-to-end integration test verifies the full Bronze -> Silver pipeline with deterministic output. The PIT correctness and deterministic ordering claims are not mechanically verified by CI.

**Recommendation:** Add a golden-file test: fixed bronze input files -> `ingest_file()` -> compare Silver output against a checked-in expected Parquet file (byte-level or row-level comparison). Cover at least `trades` and `quotes` for one vendor.

#### P3: Schema Docs List Unimplemented Tables

`docs/reference/schemas.md` defines `book_ticker`, `options_surface_grid` which have no corresponding `TABLE_PATHS` entries, table modules, or parsers.

**Recommendation:** Add status markers (Implemented / Planned) to the schema catalog.

#### P3: Quarantine Coverage Check Is Per-Pair Python Loop

Within `_check_quarantine_vectorized()`, the anti-join filter is vectorized, but `check_coverage()` is still called once per unique `(exchange_id, exchange_symbol, date)` pair in a Python list comprehension. For typical crypto files (~1-10 symbols) this is negligible. For Quant360 SZSE files (~3000 symbols) it adds measurable overhead.

**Recommendation:** Refactor `check_coverage()` to accept a batch of pairs and return a boolean Series in one pass, using a vectorized interval-overlap join against dim_symbol validity windows.

### Improvement Roadmap

#### Phase 1: Foundation Hardening (Before Next Asset Class)

| Task | Priority | Effort | Impact |
|------|----------|--------|--------|
| Widen counter file to 64-bit integer | P1 | Trivial | Future-proofs file_id space |
| Populate `dim_exchange` via CLI bootstrap command | P1 | Small | Enables config-as-data migration |
| Add golden-file integration test for trades pipeline | P2 | Medium | Mechanically verifies PIT correctness |

#### Phase 2: Multi-Asset Readiness

| Task | Priority | Effort | Impact |
|------|----------|--------|--------|
| Retire hardcoded config dicts (vendor self-registration) | P1 | Medium | Completes config-as-data migration |
| Add nullable columns to `dim_symbol` for futures/options/equities | P2 | Medium | Unblocks next asset class |
| Populate `dim_trading_calendar` via CLI | P2 | Medium | Enables cross-asset time alignment |
| Document cross-table join semantics | P2 | Small | Research correctness |

#### Phase 3: Operational Maturity

| Task | Priority | Effort | Impact |
|------|----------|--------|--------|
| Document backfill / re-ingestion protocol | P2 | Small | Operational completeness |
| Define Gold layer contract and write path | P2 | Medium | Unblocks research framework v2 M3 |
| Add data retention policy and `pointline lake stats` | P2 | Small | Storage management |
| Manifest keyed lookup under lock (replace full-table read) | P3 | Medium | Ingestion scalability at large manifest size |
| Batch-vectorize quarantine coverage check | P3 | Medium | Ingestion performance for large universes |
| Add status markers to schema catalog | P3 | Trivial | Documentation accuracy |

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
│  ├── liquidations                             (active ingest)    │
│  ├── options_chain                            (active ingest)    │
│  └── fundamentals                             (LFT, planned)    │
│                                                                  │
│  Dimension Tables (Reference)                                    │
│  ├── dim_symbol          (SCD2, active, 8-byte hash IDs)        │
│  ├── dim_exchange         (schema defined, fallback chain active)│
│  ├── dim_trading_calendar (schema defined, not yet populated)    │
│  ├── dim_asset_stats      (daily supply/market cap)              │
│  ├── ingest_manifest      (ETL ledger, cross-platform locking)  │
│  └── validation_log       (DQ audit trail, wired into pipeline) │
│                                                                  │
│  Gold (Derived Research Tables — not yet implemented)            │
│  ├── daily_ohlcv          (cross-sectional, Z-order by date)    │
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
│  Each plugin: get_table_mapping(), read_and_parse(),            │
│  get_bronze_layout_spec(), normalize_exchange/symbol()           │
│                                                                  │
│  Future: databento, polygon, CME DataMine, tushare, bloomberg   │
└──────────────────────────────────────────────────────────────────┘

  Schema Registry:  schema_registry.py (register, get, validate)
  In-Process Cache:
    dim_symbol       → TTL 5min,  thread-safe double-checked lock
    dim_exchange     → TTL 1hr,   thread-safe double-checked lock
    dim_asset_stats  → TTL 1hr
  Write Boundary:   validate_schema() before all Delta writes
```

### Key Design Decisions

1. **Pure data lake over hybrid storage.** DuckDB as a query layer over Delta Lake gives SQL and indexed access without dual-write consistency problems. No separate database for klines or metadata.

2. **Wide dim_symbol with nullable columns.** Options fields (strike, put_call, expiry, underlying) live directly in dim_symbol. Add more nullable columns for future asset classes. Reconsider satellite tables only if dim_symbol exceeds ~25 columns.

3. **Config as data, phased migration.** Phase 1 (in progress): `dim_exchange` table with fallback to hardcoded dicts. Phase 2: vendor plugins self-register exchange support. Phase 3: retire `config.py` registries entirely.

4. **Trading calendar as a first-class dimension.** Required before any non-24/7 asset class can be queried correctly in cross-asset research. Schema defined; population tooling needed.

5. **Idempotent ingestion by default.** Production ingests use MERGE on lineage keys `(file_id, file_line_number)` for crash-safe retries. `append()` remains available for bulk loads. Manifest writes `pending` on file_id mint, `success` after Silver write. Dry-run mode walks the full pipeline with zero side effects (no writes, no validation-log records).
