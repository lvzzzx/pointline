# Infra Roadmap: Universal Offline Research Data Lake

**Owner:** Infra Engineering
**Created:** 2026-02-10
**North Star:** [infra-north-star.md](infra-north-star.md)
**Previous Review:** [review_2026_02_02.md](review_2026_02_02.md)

---

## Current Baseline

| Metric | Value |
|--------|-------|
| Codebase | ~24K LoC Python, v0.1.0 |
| Active asset classes | 2 (crypto, CN stocks) |
| Active vendors | 5 (tardis, binance_vision, quant360, coingecko, tushare) |
| Silver tables (active) | 9 (trades, quotes, book_snapshot_25, derivative_ticker, kline_1h, szse_l3_orders, szse_l3_ticks, dim_symbol, ingest_manifest) |
| Registered exchanges | 27 |
| Test files | 55 |

---

## Phase 0: Foundation Hardening

**Goal:** Fix correctness and performance issues in the existing core before any expansion.
**Trigger:** Now. These are preconditions for safe multi-asset growth.
**Duration:** 1-2 weeks.

### 0.1 Widen symbol_id Hash to 8 Bytes

**Why:** Current 4-byte blake2b digest has ~50% collision probability at ~77K symbols. US equities + options push past 1M contracts.

**Scope:**
- `dim_symbol.py`: Change `assign_symbol_id_hash()` digest_size from 4 to 8
- Existing dim_symbol data must be rebuilt (symbol_ids change)
- All downstream joins use Int64 already — no schema change

**Acceptance:**
- `assign_symbol_id_hash()` produces 8-byte digests
- Zero collisions on a synthetic 1M-symbol test set
- Existing tests pass after rebuild

**Risk:** Breaking change for existing dim_symbol tables. Requires a one-time `scd2_bootstrap()` rebuild. Acceptable under the clean-break versioning policy.

---

### 0.2 Schema Validation at Repository Write Boundary

**Why:** `BaseDeltaRepository.append()` and `write_full()` silently accept schema mismatches. A parser bug can widen the Delta schema or produce cryptic Arrow errors.

**Scope:**
- Add `validate_schema(df: pl.DataFrame, expected: dict[str, pl.DataType])` to `io/base_repository.py`
- Call before Arrow conversion in `append()`, `write_full()`, `overwrite_partition()`
- Canonical schemas already exist in `tables/*.py` — wire them to the repository layer
- Raise `SchemaValidationError` on mismatch (column missing, type wrong, unexpected column)

**Acceptance:**
- Schema mismatch raises a clear error before any write occurs
- Existing ingestion pipeline passes (schemas match)
- Test: intentionally pass wrong-typed DataFrame, verify rejection

---

### 0.3 Append-First Ingestion (Three-Phase Commit)

**Why:** Current `write()` defaults to MERGE on `(file_id, file_line_number)` for every ingestion. Full MERGE cost on event tables with millions of rows is unnecessary on the happy path.

**Scope:**
- Change `GenericIngestionService.ingest_file()` to three-phase commit:
  1. Update manifest status to `in_progress`
  2. `repo.append(df)` (not merge)
  3. Update manifest status to `success`
- On retry (manifest shows `in_progress`): delete partial Silver data by `file_id`, then re-append
- Keep MERGE path available as explicit opt-in for tables that need upsert semantics

**Acceptance:**
- Happy-path ingestion uses `append()` only
- Crash between step 2 and 3 leaves manifest as `in_progress`
- Retry correctly cleans up partial writes and re-ingests
- Benchmark: measure write latency before/after on a representative trades partition

---

### 0.4 Cache dim_symbol Reads with TTL

**Why:** `registry.py` re-reads dim_symbol from disk on every call. Notebook sessions with 100 queries do 100 full Delta reads of the same table.

**Scope:**
- Add module-level cache in `registry.py` with 5-minute TTL
- `_read_dim_symbol()` returns cached DataFrame if TTL not expired
- Add `registry.invalidate_cache()` for explicit refresh
- Thread-safe (use `threading.Lock`)

**Acceptance:**
- Second call within TTL returns cached result (no disk I/O)
- Call after TTL expiry re-reads from disk
- `invalidate_cache()` forces immediate re-read
- Existing tests pass

---

### 0.5 Fix Manifest Concurrency

**Why:** `fcntl.flock()` inside Delta table directory creates parallel coordination outside Delta's own transaction log. Full-table read under exclusive lock is a scalability bottleneck.

**Scope:**
- Replace `resolve_file_id()` with Delta-native MERGE for atomic ID reservation
- Or: use a separate monotonic counter file (single integer, not inside `_delta_log/`)
- Remove `.file_id_lock` from Delta table directory
- Ensure cross-platform compatibility (no `fcntl`)

**Acceptance:**
- `resolve_file_id()` works without `fcntl`
- Concurrent calls from two processes produce unique, monotonic IDs
- No lock file inside Delta table directory

---

### 0.6 Pre-compute Reverse Exchange Map

**Why:** `get_exchange_name()` is O(n) linear scan. Trivial to fix.

**Scope:**
- Add `_REVERSE_EXCHANGE_MAP: dict[int, str]` computed at module load time in `config.py`
- Rewrite `get_exchange_name()` to use it

**Acceptance:**
- O(1) lookup
- Existing tests pass

---

### Phase 0 Deliverables

| Deliverable | Files Changed | Depends On |
|-------------|---------------|------------|
| 8-byte symbol_id | `dim_symbol.py`, rebuild script | — |
| Schema validation | `io/base_repository.py`, `tables/*.py` | — |
| Append-first ingestion | `generic_ingestion_service.py`, `delta_manifest_repo.py` | — |
| Registry cache | `registry.py` | — |
| Manifest concurrency | `delta_manifest_repo.py` | — |
| Reverse exchange map | `config.py` | — |

All items are independent and can be worked in parallel.

---

## Phase 1: Config-as-Data

**Goal:** Decouple exchange/asset metadata from Python code so adding a new exchange requires zero code changes.
**Trigger:** After Phase 0 complete.
**Duration:** 1-2 weeks.

### 1.1 Implement dim_exchange Table

**Why:** `EXCHANGE_MAP`, `EXCHANGE_METADATA`, `EXCHANGE_TIMEZONES` are 300+ lines of hardcoded Python dicts in `config.py`. Every new exchange requires a code change, PR, and redeploy.

**Scope:**
- New Silver table `dim_exchange` (unpartitioned, small):

```
exchange        string   PK  (e.g., "binance-futures")
exchange_id     i16      unique
asset_class     string   (e.g., "crypto-derivatives")
timezone        string   IANA timezone (e.g., "UTC", "Asia/Shanghai")
description     string
is_active       bool
supported_tables list<string>  (e.g., ["trades", "quotes", "book_snapshot_25"])
```

- New table module: `pointline/tables/dim_exchange.py`
- New service: `pointline/services/dim_exchange_service.py` (full overwrite from config seed)
- Bootstrap script to seed from current `EXCHANGE_MAP` + `EXCHANGE_METADATA`
- Modify `config.py` to read from dim_exchange at startup (with fallback to hardcoded for cold start)

**Migration path:**
1. Create `dim_exchange` table and seed from current hardcoded values
2. Modify `get_exchange_id()`, `get_exchange_name()`, `get_exchange_timezone()` to read from dim_exchange (cached)
3. Keep hardcoded dicts as fallback for first release
4. Remove hardcoded dicts in subsequent release

**Acceptance:**
- Adding a new exchange = one row insert to dim_exchange, zero code changes
- All existing code that calls `get_exchange_id()` works unchanged
- Discovery API `list_exchanges()` reads from dim_exchange

---

### 1.2 Extract Asset Class Taxonomy to Config

**Why:** `ASSET_CLASS_TAXONOMY` in `config.py` is a nested dict defining parent/child relationships. This should be data, not code.

**Scope:**
- Move to `dim_exchange.asset_class` column (the taxonomy is just a grouping of exchanges)
- Parent/child relationships derived from naming convention (`crypto` → children = all exchanges where `asset_class` starts with `crypto-`)
- Or: separate `asset_class_taxonomy.toml` config file
- Remove `ASSET_CLASS_TAXONOMY` dict from `config.py`

**Acceptance:**
- `get_asset_class_exchanges("crypto")` works by querying dim_exchange
- Adding a new asset class = adding exchanges with that asset_class value

---

### 1.3 Add get_table_mapping() to Vendor Plugin Protocol

**Why:** When multiple vendors provide the same data under different names (Databento `mbp-1` vs Tardis `trades`), routing to the target Silver table is implicit.

**Scope:**
- Add `get_table_mapping(self) -> dict[str, str]` to `VendorPlugin` protocol
- Returns mapping from vendor `data_type` to Silver table name
- Example: `{"trades": "trades", "book_snapshot_25": "book_snapshot_25", "mbp-1": "trades"}`
- Update `GenericIngestionService` to use this mapping for table resolution
- Implement for all 5 existing vendors

**Acceptance:**
- Each vendor declares its data_type → table mapping
- Ingestion service uses the mapping instead of assuming data_type == table_name
- Adding a new vendor data format that maps to an existing table requires no core changes

---

### Phase 1 Deliverables

| Deliverable | Files Changed | Depends On |
|-------------|---------------|------------|
| dim_exchange table | new `tables/dim_exchange.py`, `services/dim_exchange_service.py`, `config.py` | Phase 0 (schema validation) |
| Asset class from dim_exchange | `config.py`, `research/discovery.py` | 1.1 |
| Vendor table mapping | `io/vendors/base.py`, all vendor plugins, `generic_ingestion_service.py` | — |

---

## Phase 2: Multi-Asset Data Model

**Goal:** Extend the data model to support non-crypto asset classes without breaking the existing pipeline.
**Trigger:** When the next asset class is identified (US equities or traditional futures).
**Duration:** 2-3 weeks.

### 2.1 Extend dim_symbol with Nullable Asset-Class Columns

**Why:** Futures need `expiry_ts_us`, `underlying_symbol_id`, `settlement_type`. Options need `strike`, `put_call`. Equities need `isin`, `listing_exchange`. These don't apply to crypto, so they must be nullable.

**Scope:**
- Add nullable columns to `dim_symbol.SCHEMA`:

```python
# Futures/Options fields (nullable for crypto/equity)
"expiry_ts_us": pl.Int64,           # Contract expiry (nullable)
"underlying_symbol_id": pl.Int64,   # Underlying instrument (nullable)
"settlement_type": pl.Utf8,         # "cash" / "physical" (nullable)

# Options fields (nullable for non-options)
"strike": pl.Float64,               # Strike price (nullable)
"put_call": pl.Utf8,                # "put" / "call" (nullable)

# Equity fields (nullable for non-equity)
"isin": pl.Utf8,                    # ISIN identifier (nullable)
```

- Update `TRACKED_COLS` to include new fields (SCD2 change detection)
- Update `normalize_dim_symbol_schema()` to handle new columns
- Update `docs/reference/schemas.md` with new columns

**Decision gate:** If this push dim_symbol past ~25 columns, switch to satellite tables (Phase 4) instead.

**Acceptance:**
- Existing crypto dim_symbol data loads with new columns (all null)
- SCD2 upsert correctly tracks changes to new fields
- Schema docs updated

---

### 2.2 Implement dim_trading_calendar

**Why:** Non-24/7 markets (equities, futures) have holidays, early closes, and trading hours. Cross-asset queries require knowing "was this a trading day?"

**Scope:**
- New Silver table `dim_trading_calendar`:

```
exchange         string   (e.g., "nyse", "szse")
date             date
is_trading_day   bool
session_type     string   ("regular", "early_close", "holiday", "weekend")
open_time_us     i64      (nullable, market open in UTC µs)
close_time_us    i64      (nullable, market close in UTC µs)
```

- New table module: `pointline/tables/dim_trading_calendar.py`
- Bootstrap from `exchange_calendars` Python package (covers NYSE, SZSE, SSE, CME, etc.)
- CLI command: `pointline calendar sync --exchange szse --year 2024`
- Research API helper: `research.trading_days(exchange, start, end) -> list[date]`

**Acceptance:**
- `research.trading_days("szse", "2024-01-01", "2024-12-31")` returns ~244 trading days
- `research.trading_days("binance-futures", ...)` returns all days (24/7)
- Calendar data seeded for all active exchanges

---

### 2.3 Extend Silver Table Schemas for Multi-Asset

**Why:** US equity trades have condition codes and reporting venues. These don't exist in crypto. The schema must accommodate both without separate tables.

**Scope:**
- `trades` schema: add nullable columns:
  - `conditions` (i32, bitfield for sale conditions)
  - `venue_id` (i16, nullable, reporting venue for equities)
  - `sequence_number` (i64, nullable, SIP/exchange sequence)
- `quotes` schema: add nullable columns:
  - `conditions` (i32, bitfield)
  - `venue_id` (i16, nullable)
- Document condition code bitfield semantics per asset class in `docs/reference/schemas.md`
- Existing crypto ingestion fills new columns as null (zero change to parsers)

**Acceptance:**
- Existing crypto ingestion works unchanged (new columns null)
- New vendor parsers for equities can populate the new columns
- Schema docs define condition code semantics

---

### 2.4 Add kline_1d Silver Table

**Why:** LFT research needs daily bars. Currently only `kline_1h` exists. Daily bars are the fundamental unit for LFT cross-sectional analysis.

**Scope:**
- New table module: `pointline/tables/kline_1d.py` (same schema as `kline_1h`)
- Register in `TABLE_PATHS`
- Vendor support: binance_vision provides daily klines
- Research API: `query.kline_1d(exchange, symbol, start, end, decoded=True)`
- Gold table: `gold.daily_ohlcv` — cross-sectional view Z-ordered by `(date, symbol_id)` for "all symbols on one day" queries

**Acceptance:**
- `query.kline_1d(...)` works for Binance symbols
- Gold cross-sectional table enables efficient multi-symbol daily queries

---

### Phase 2 Deliverables

| Deliverable | Files Changed | Depends On |
|-------------|---------------|------------|
| Extended dim_symbol | `dim_symbol.py`, `registry.py`, schemas doc | Phase 0 (8-byte hash) |
| dim_trading_calendar | new table module, new service, CLI, research API | Phase 1 (dim_exchange) |
| Multi-asset Silver schemas | `tables/trades.py`, `tables/quotes.py`, schemas doc | Phase 0 (schema validation) |
| kline_1d + daily_ohlcv | new table modules, research API | — |

---

## Phase 3: Operational Maturity

**Goal:** Make the ingestion pipeline production-grade: observable, performant at scale, and operationally reliable.
**Trigger:** Can run in parallel with Phase 2.
**Duration:** 2-3 weeks.

### 3.1 Wire validation_log into GenericIngestionService

**Why:** Ingestion metrics (row counts, quarantined symbols, errors, duration) are only logged to stderr. No queryable audit trail for data quality.

**Scope:**
- `validation_log` table module already exists in `pointline/tables/validation_log.py`
- Add `_write_validation_log()` at the end of `GenericIngestionService.ingest_file()`
- Record: file_id, vendor, data_type, exchange, date, status, row_count, filtered_row_count, filtered_symbol_count, error_message, duration_ms, ingested_at_us
- CLI command: `pointline dq summary --exchange binance-futures --date 2024-05-01`
- Research API: `research.dq_summary(exchange, start, end)`

**Acceptance:**
- Every ingestion writes a row to validation_log
- `pointline dq summary` shows per-partition quality metrics
- Quarantined symbols are queryable ("which symbols were quarantined last week?")

---

### 3.2 Vectorize Quarantine Check

**Why:** Row-by-row Python loop over unique `(exchange_id, exchange_symbol, date)` pairs does O(n) `dim_symbol.filter()` per pair. Bottleneck for Quant360 files with ~3000 symbols.

**Scope:**
- Replace `_check_quarantine()` loop with a single vectorized anti-join:
  1. Build unique `(exchange_id, exchange_symbol, date)` from the DataFrame
  2. Compute day boundaries per exchange timezone (vectorized)
  3. Join against dim_symbol validity windows
  4. Anti-join to find uncovered pairs
  5. Filter DataFrame in one pass

**Acceptance:**
- Quarantine check for 3000-symbol Quant360 file completes in <1s (currently ~10-30s)
- Same quarantine decisions as the current row-by-row implementation
- Unit test with synthetic dim_symbol verifies edge cases

---

### 3.3 Post-Ingest Partition Optimization

**Why:** After ingesting many files into a single partition, Delta Lake has many small files. `OPTIMIZE` compacts them and Z-orders for query performance. Currently manual.

**Scope:**
- Add optional `auto_optimize` flag to `GenericIngestionService`
- After successful ingestion, if partition file count exceeds threshold (e.g., 10), auto-run `optimize_partition(z_order=["symbol_id", "ts_local_us"])`
- CLI flag: `pointline ingest run --auto-optimize`
- Default: off (explicit operator control, per north star principle)

**Acceptance:**
- `--auto-optimize` triggers compaction after ingestion
- Partition file count drops to 1-2 files after optimization
- No optimization runs if file count is below threshold

---

### 3.4 Ingestion Dry-Run Mode

**Why:** Before ingesting a large backfill, operators need to verify what will happen: how many files, which symbols, estimated row counts, any quarantine warnings.

**Scope:**
- Add `--dry-run` flag to `pointline ingest run`
- Walks the full pipeline (discover → filter_pending → read → quarantine check) but skips write
- Outputs: file count, estimated row count, quarantined symbols, schema validation results
- No side effects (no manifest updates, no Silver writes)

**Acceptance:**
- `pointline ingest run --dry-run --table trades --exchange binance-futures --date 2024-05-01` shows expected ingestion summary
- Zero side effects on dry run

---

### Phase 3 Deliverables

| Deliverable | Files Changed | Depends On |
|-------------|---------------|------------|
| DQ observability | `generic_ingestion_service.py`, `validation_log.py` | — |
| Vectorized quarantine | `generic_ingestion_service.py` | — |
| Auto-optimize | `generic_ingestion_service.py`, `cli/` | — |
| Dry-run mode | `cli/`, `generic_ingestion_service.py` | — |

All items are independent.

---

## Phase 4: First New Asset Class

**Goal:** Prove the multi-asset architecture by onboarding one non-crypto asset class end-to-end.
**Trigger:** After Phase 1 (config-as-data) and Phase 2 (data model) are complete.
**Duration:** 3-4 weeks.

**Candidate A: US Equities** (via Databento or Polygon)
**Candidate B: Traditional Futures** (via Databento or CME DataMine)

The choice depends on research priority. The architecture supports either.

### 4.1 New Vendor Plugin

**Scope:**
- Create `pointline/io/vendors/<new_vendor>/` with full plugin implementation
- Implement `VendorPlugin` protocol: `read_and_parse()`, `get_parsers()`, `get_bronze_layout_spec()`, `get_table_mapping()`
- Download client if vendor supports API access
- Prehook if vendor delivers archives

**Acceptance:**
- `list_vendors()` includes new vendor
- `get_parser(vendor, data_type)` returns parsers for all supported data types
- Bronze discovery works: `pointline bronze discover --vendor <new_vendor>`

---

### 4.2 Register New Exchanges

**Scope:**
- Insert new exchange rows into `dim_exchange` (no code changes, per Phase 1)
- Seed dim_symbol for new exchange symbols
- Seed dim_trading_calendar for new exchange (per Phase 2)

**Acceptance:**
- `research.list_exchanges(asset_class="<new_class>")` returns new exchanges
- `research.list_symbols(exchange="<new_exchange>")` returns symbols
- `research.data_coverage(exchange, symbol)` works

---

### 4.3 End-to-End Ingestion

**Scope:**
- Bronze: download or reorganize vendor data
- Silver: ingest trades, quotes for new exchange
- Validate: run DQ checks, verify quarantine behavior
- Research: `query.trades("<new_exchange>", "<symbol>", start, end, decoded=True)` works

**Acceptance:**
- Full pipeline works: bronze discover → ingest run → query
- DQ validation_log records ingestion metrics
- Research API returns decoded data
- Cross-asset query works: load both crypto and new-asset trades in same notebook

---

### Phase 4 Deliverables

| Deliverable | Files Changed | Depends On |
|-------------|---------------|------------|
| New vendor plugin | new `io/vendors/<vendor>/` | Phase 1 (table mapping) |
| Exchange registration | dim_exchange data | Phase 1 (dim_exchange) |
| Symbol seeding | dim_symbol data | Phase 2 (extended schema) |
| Calendar seeding | dim_trading_calendar data | Phase 2 (calendar) |
| End-to-end validation | tests, DQ checks | Phase 3 (observability) |

---

## Phase 5: Scale & Maturity

**Goal:** Optimize the data model and tooling for 3+ active asset classes.
**Trigger:** When the third asset class is onboarded, or when dim_symbol exceeds ~25 columns.
**Duration:** Ongoing.

### 5.1 Satellite Dimension Tables

**Why:** When three asset classes have distinct metadata (crypto, equities, futures), a wide dim_symbol with 30+ nullable columns becomes unwieldy. Satellite tables keep the core slim.

**Scope:**
- `dim_futures_contract(symbol_id, expiry_ts_us, underlying_symbol_id, settlement_type, contract_month, multiplier)`
- `dim_options_contract(symbol_id, strike, put_call, exercise_style, expiry_ts_us, underlying_symbol_id)`
- `dim_equity_listing(symbol_id, isin, cusip, figi, listing_exchange, sector, industry)`
- Each is a small unpartitioned Delta table joined by `symbol_id`
- Move asset-class-specific nullable columns from dim_symbol to satellites
- Research API: `research.symbol_metadata(symbol_id)` auto-joins relevant satellite

---

### 5.2 Runtime Schema Registry

**Why:** Canonical schemas are scattered across `tables/*.py` modules as Python dicts. No single place to enumerate all schemas, validate consistency, or auto-generate docs.

**Scope:**
- Central registry: `pointline/schema_registry.py`
- Each table module registers its schema at import time
- API: `get_schema(table_name)`, `list_tables()`, `validate_df(table_name, df)`
- Auto-generate `docs/reference/schemas.md` from registry
- Wire into `BaseDeltaRepository` for write-time validation (replaces Phase 0.2 ad-hoc approach)

---

### 5.3 Cross-Asset Research Helpers

**Why:** Researchers need to load and align data across asset classes (e.g., BTC perp + SPY equity in one analysis). Different trading hours, different calendars, different metadata.

**Scope:**
- `research.align(datasets: list[LazyFrame], on="ts_local_us", method="asof")` — time-align multiple datasets
- `research.universe(asset_class, date)` — return all active symbols for an asset class on a date
- `research.cross_sectional(table, date, columns)` — return one row per symbol for a date (using Gold cross-sectional tables)
- Calendar-aware resampling: skip non-trading days when computing rolling windows

---

### 5.4 DuckDB Query Layer Integration

**Why:** MFT/LFT cross-sectional queries ("today's close for all 500 symbols") benefit from SQL and indexed access. DuckDB reads Delta tables natively.

**Scope:**
- Add `pointline.research.sql` module with DuckDB connection management
- Auto-register all Silver/Gold tables as Delta scans
- Helper: `research.sql("SELECT ... FROM trades WHERE ...")` — returns Polars DataFrame
- Useful for ad-hoc exploration, cross-table joins, and LFT workflows

---

## Dependency Graph

```
Phase 0 (Foundation)
├── 0.1 symbol_id 8-byte ──────────────────────┐
├── 0.2 schema validation ─────────────────────┐│
├── 0.3 append-first ingestion                 ││
├── 0.4 registry cache                         ││
├── 0.5 manifest concurrency                   ││
└── 0.6 reverse exchange map                   ││
                                               ││
Phase 1 (Config-as-Data)                       ││
├── 1.1 dim_exchange ──────────────────────┐   ││
├── 1.2 asset class from dim_exchange ◄────┘   ││
└── 1.3 vendor table mapping                   ││
                                               ││
Phase 2 (Multi-Asset Data Model)               ││
├── 2.1 extend dim_symbol ◄────────────────────┘│
├── 2.2 dim_trading_calendar ◄── 1.1            │
├── 2.3 multi-asset Silver schemas ◄────────────┘
└── 2.4 kline_1d + daily_ohlcv

Phase 3 (Operational Maturity)       [parallel with Phase 2]
├── 3.1 validation_log integration
├── 3.2 vectorized quarantine
├── 3.3 auto-optimize
└── 3.4 dry-run mode

Phase 4 (First New Asset Class)      [after Phase 1 + 2]
├── 4.1 new vendor plugin
├── 4.2 register exchanges ◄── 1.1
├── 4.3 end-to-end ingestion ◄── 4.1, 4.2
└── validation ◄── 3.1

Phase 5 (Scale & Maturity)           [when 3+ asset classes]
├── 5.1 satellite dimensions
├── 5.2 schema registry
├── 5.3 cross-asset research helpers
└── 5.4 DuckDB query layer
```

## Summary

| Phase | Goal | Duration | Key Deliverable |
|-------|------|----------|-----------------|
| **0** | Fix correctness/perf bugs | 1-2 weeks | Safe foundation for growth |
| **1** | Config as data | 1-2 weeks | Adding exchanges requires zero code changes |
| **2** | Multi-asset data model | 2-3 weeks | dim_symbol, calendar, schema extensions |
| **3** | Operational maturity | 2-3 weeks | DQ observability, performance, dry-run |
| **4** | First new asset class | 3-4 weeks | End-to-end proof of multi-asset architecture |
| **5** | Scale | Ongoing | Satellite dims, schema registry, DuckDB, cross-asset |

**Total to first new asset class (Phases 0-4):** ~8-12 weeks.
Phases 0, 1, 3 can overlap. Phase 2 depends on Phase 0. Phase 4 depends on Phases 1+2.

**Guiding constraint:** Every phase must leave the system shippable. No phase introduces a half-migrated state. Each phase has its own acceptance criteria and can be released independently.
