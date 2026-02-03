# Offline Data Lake Design for Multi-Vendor Market Data (HFT Research)

This document describes a high-performance, point-in-time (PIT) accurate offline data lake for
**multi-vendor** datasets (e.g., Tardis, Binance Public Data).

Primary goals:
1. **PIT correctness**: backtests reproduce what could have been known at the time.
2. **Compression**: integer encoding + Parquet best practices.
3. **Query speed**: optimized for Python researchers using Pandas/Polars + DuckDB (and optionally ClickHouse/kdb+).
4. **Determinism**: stable ordering and reproducible ETL outputs.

---

## 1) Timeline semantics: use `local_timestamp` as the default

Tardis provides both:
- `timestamp` (exchange-provided when available)
- `local_timestamp` (provider arrival / observation time)

**Default replay timeline:** `local_timestamp`

Why:
- Live trading reacts to *arrival time*, not omniscient exchange time.
- Many market microstructure analyses break if you accidentally use exchange time for state and local time for decisions.

**Store both**:
- `ts_exch_us` (int64 µs)
- `ts_local_us` (int64 µs)

**Stable ordering key for every table:**
- `(exchange_id, symbol_id, date, ts_local_us, file_id, file_line_number)`

Where:
- `file_line_number` is deterministic within the source file; combined with `file_id` it provides a stable tie-break.
- **Lineage tracking**: In Silver tables, store `file_id` (i32) and `file_line_number`. Join with `silver.ingest_manifest` to resolve the original `bronze_file_name`.

---

## 2) Lake layers: Bronze → Silver → Gold

### Bronze (immutable vendor truth)
Store raw vendor files exactly as downloaded. Use a vendor-first layout so multiple raw sources
can coexist for the same exchange and data type.

**Recommended layout (vendor → exchange → type → date → symbol):**
`/lake/bronze/{vendor}/exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/...`

**Example (Tardis):**
`/lake/bronze/tardis/exchange=deribit/type=book_snapshot_25/date=2025-12-28/`
`symbol=BTC-PERPETUAL/deribit_book_snapshot_25_2025-12-28_BTC-PERPETUAL.csv.gz`

**Example (Binance Public Data):**
`/lake/bronze/binance_vision/spot/exchange=binance/type=klines/date=2025-01-01/`
`symbol=ADABKRW/interval=1h/ADABKRW-1h-2025-01-01.zip`

**Vendor-specific templates:**
- **Tardis:** `exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/`
  `{exchange}_{data_type}_{date}_{symbol}.{format}`
- **Binance Public Data:** `{market}/exchange={exchange}/type={data_type}/`
  `date={date}/symbol={symbol}/...` (use `interval={interval}` for klines;
  monthly files map to `date=YYYY-MM-01`)

Notes:
- For Binance Public Data, prefer explicit `market` folders (`spot`, `usd_m`, `coin_m`).
- Keep `.CHECKSUM` files adjacent to the zips; verify and record status in
  `silver.ingest_manifest`.
- If a vendor republishes a file, treat it as a new bronze version and preserve both.

No transformations besides checksums/manifests.

### Silver (typed, normalized Parquet)
Convert to Parquet with:
- integer timestamps
- integer IDs (dictionary encoding)
- stable lineage ordering (`file_id` + `file_line_number`)
- normalized numeric types (fixed-point ints where possible)

Silver is the canonical research foundation.

### Gold (research-optimized derived tables)
Precomputed “fast paths”:
- top-of-book quotes (`tob_quotes`)
- merged “event tape” for replay (optional)
- options surface snapshots on a time grid (optional)

Gold tables are reproducible from Silver (and versioned).

Current scope decision:
- Keep **Bronze (vendor raw)** + **Silver (canonical)** as the foundation.
- Defer Gold adoption until a concrete need is identified.

---

## 3) Storage format & performance defaults

**Format:** Delta Lake (via `delta-rs`)
**Compression:** ZSTD (best compression)
**Row group sizing:** target ~128–512MB row groups
**File sizing:** 256MB–1GB files (controlled by Delta writer)

**Integer Type Limitations:**
Delta Lake (via Parquet) does not support unsigned integer types `UInt16` and `UInt32`.
These are automatically converted to signed types (`Int16` and `Int32`) when written.
- Use `Int16` instead of `UInt16` for `exchange_id`
- Use `Int32` instead of `UInt32` for `file_id`, `file_line_number`, `flags`
- Use `Int64` for `symbol_id` to match `dim_symbol`
- `UInt8` is supported and maps to TINYINT (use for `side`, `asset_type`)

**Data Organization inside partitions**:
- **Z-Order / Cluster by:** `(symbol_id, ts_local_us)`
- or **Sort by:** `(symbol_id, ts_local_us, file_id, file_line_number)`
This boosts pruning for specific symbols without creating thousands of tiny partition directories.

---

## 4) Reference data: instrument metadata & fixed-point encoding

For compression + exact comparisons:
- Encode prices and sizes as integers.

### 4.1 Symbol Master (SCD Type 2)
Symbols change over time (renames, delistings, tick size changes). Maintain a **Slowly Changing Dimension (SCD) Type 2** table `dim_symbol`.

**Implementation:** Delta Table (`silver.dim_symbol`)
- **Storage:** Single versioned table (not partitioned, as it's small).
- **Surrogate Key (`symbol_id`):** `i64` integer.
  - *Strategy:* Use a deterministic hash of `(exchange_id, exchange_symbol, valid_from_ts)` or a managed autoincrementing sequence during registry updates.
- **Natural Key:** `(exchange_id, exchange_symbol)`.

**Schema:** See [Schema Reference](../schemas.md#11-silverdim_symbol) for complete column definitions.

**Update Logic:**
1. If a symbol's metadata (e.g., `tick_size`) changes:
   - Update the existing record: set `valid_until_ts = change_ts` and `is_current = false`.
   - Insert a new record: set `valid_from_ts = change_ts`, `valid_until_ts = infinity`, and `is_current = true`.
2. This ensures that Silver data ingested on `date=X` joins against the metadata that was **active on `date=X`**, preserving historical accuracy.

**Ingestion Join:**
```sql
SELECT
    b.*,
    s.symbol_id
FROM bronze_data b
JOIN silver.dim_symbol s
  ON  b.exchange_id = s.exchange_id
  AND b.exchange_symbol = s.exchange_symbol
  AND b.ts_local_us >= s.valid_from_ts
  AND b.ts_local_us <  s.valid_until_ts
```


### 4.2 Fixed-point Logic
Recommended approach:
- `px_int = round(price_px / price_increment)`
- `qty_int   = round(qty   / amount_increment)`

**Alternative (Robustness):** Standardize on a fixed multiplier (e.g., `1e8` or `1e9`) if metadata is flaky. "Tick-based" is better for compression; "Fixed Multiplier" is safer for operations.

Store both:
- `px_int`, `qty_int` (required)
- optionally `price_f64`, `qty_f64` for convenience (derived)

**Detailed encoding explanation:** See [Schema Reference - Fixed-Point Encoding](../schemas.md#fixed-point-encoding).

---

## 5) Silver schemas per Tardis dataset

**Schema Reference:** For complete column definitions, see [Schema Reference - Silver Tables](../schemas.md#silver-tables).

### 5.1 `book_snapshot_25` (from Tardis snapshots)
Snapshots are full top-N book states (e.g., 25 levels).

**Source schema (Tardis `book_snapshot_25`)**
- `exchange` (string), `symbol` (string, uppercase)
- `timestamp` (exchange timestamp in µs; falls back to `local_timestamp` if exchange does not provide)
- `local_timestamp` (arrival timestamp in µs, UTC)
- `asks[0..24].price`, `asks[0..24].amount` (ascending by price)
- `bids[0..24].price`, `bids[0..24].amount` (descending by price)
- Missing levels may be empty if fewer than 25 price levels exist.

**Normalization + mapping**
- `exchange` is normalized (lowercase, trimmed) and used for partitioning.
- `exchange_id` is derived via `EXCHANGE_MAP` and stored for joins.
- `symbol` is mapped to `symbol_id` via `dim_symbol`.
- `ts_exch_us` maps from `timestamp`; `ts_local_us` maps from `local_timestamp`.
- `date` is derived from `ts_local_us` in UTC.
- `file_line_number` provides deterministic ordering within the source file.

**List encoding**
- Convert `asks[0..24].price/amount` and `bids[0..24].price/amount` into lists of length 25.
- Use nulls for missing levels (retain list length for positional consistency).
- Convert prices/sizes into fixed-point `i64` using the symbol's `price_increment` / `amount_increment`.

**Recommended storage: list columns (Silver)**
**Verdict:** Stick to `list<i64>`. DuckDB/Polars handle lists natively and efficiently. Wide columns explode schema metadata.

**Table:** `silver.book_snapshot_25`
**Partitioned by:** `["exchange", "date"]` (same strategy as trades/quotes tables)
**Schema:** See [Schema Reference - book_snapshot_25](../schemas.md#22-silverbook_snapshot_25)

---

### 5.3 `trades`
**Table:** `silver.trades`
**Schema:** See [Schema Reference - trades](../schemas.md#23-silvertrades)

---

### 5.4 `tob_quotes` (from `quotes` or derived from L2)
**Table:** `silver.quotes` (or `gold.tob_quotes` if you treat it as a fast path)
**Schema:** See [Schema Reference - quotes](../schemas.md#24-silverquotes)

---

### 5.5 `book_ticker`
Use if you subscribe to exchange-native `bookTicker`/similar.

**Table:** `silver.book_ticker`
**Schema:** See [Schema Reference - book_ticker](../schemas.md#25-silverbook_ticker)

Same schema as top-of-book quotes, plus any venue-specific fields (update ids, etc.).

---

### 5.6 `derivative_ticker`
Includes mark/index, funding, OI, etc.

**Table:** `silver.derivative_ticker`
**Schema:** See [Schema Reference - derivative_ticker](../schemas.md#26-silverderivative_ticker)

---

### 5.7 `liquidations`
**Table:** `silver.liquidations`
**Schema:** See [Schema Reference - liquidations](../schemas.md#27-silverliquidations)

---

### 5.8 `options_chain`
Options chain is typically cross-sectional and heavy. Store updates per contract.

**Table:** `silver.options_chain`
**Schema:** See [Schema Reference - options_chain](../schemas.md#28-silveroptions_chain)

**Gold recommendation:** `gold.options_surface_grid`
- Choose a time grid (e.g., 1s or 100ms).
- For each grid timestamp, take last-known as-of values per option contract.
- This makes "entire surface at time t" queries cheap and deterministic.
- See [Schema Reference - Gold Tables](../schemas.md#options_surface_grid) for details.

---

## 6) Partitioning strategy

**Default Partitioning (most Silver tables):**
- `exchange`
- `date` (daily partitions, derived from `ts_local_us` in UTC)

**Handling Massive Universes (e.g., Options):
For datasets like `options_chain` where a single day is massive:
- The `exchange/date` strategy still holds.
- Delta Lake will automatically split the data into multiple files (e.g., 1GB each) within that folder.
- **Critical:** Run `OPTIMIZE ... ZORDER BY (symbol_id)` (or underlying/strike) after writing. This ensures that all rows for a specific contract are co-located in the same file(s), allowing the reader to skip 99% of the data.

---

## 7) Book reconstruction rules (PIT correctness)

### 7.1 Snapshots as anchors
Use snapshots (top-25) to accelerate random access:
- Find the snapshot with max `ts_local_us <= t`
- Use as the starting point for book state at time `t`
- Note: Full-depth book state beyond 25 levels requires alternative data sources.

---

## 8) Cross-stream PIT alignment (trades, liquidations, ticker, quotes)

When joining streams:
- Use **as-of join** on `ts_local_us` by default:
  - join each event with the last-known book snapshot/quote/ticker at or before that time
- Use `file_id` + `file_line_number` to break ties within the same microsecond.

This prevents lookahead bias from “future” book states.

### 8.1 The "Clock" Stream
For true PIT replay, include a `clock` stream (e.g., 1s or 1m ticks).
- **Why:** Market data events only exist when market moves. A backtest iterating only on data updates will skip quiet periods, missing scheduled timer events (e.g., "close position at 16:00:00").
- **Implementation:** Union the data stream with a `clock` stream, sorted by `ts_local_us`.

---

## 9) Backtest ↔ live execution matching

Define a contract:

### 9.1 Decision timeline
- Market state at time `t_decide` is `book_state(asof=ts_local_us=t_decide)`
- Order is sent at:
  - `t_send = t_decide + strategy_latency + network_latency`
- Order hits the venue at:
  - `t_arrive = t_send + extra_venue_latency_model`

All latencies are explicit and versioned.

### 9.2 Fill modeling by data depth
- If you have top-25 snapshots: fills must acknowledge truncated depth.
- For more accurate queue-position modeling, detailed order book data or execution feed is required.

Version the fill model and store `fill_model_version` in results.

---

## 10) ETL pipeline (deterministic & reproducible)

Stages:
1. **Ingest (Bronze)**
   - download + store raw files
   - compute checksums
   - write manifest (ingestion ledger): row counts, min/max timestamps, file metadata
2. **Parse & Normalize (Silver)**
   - read CSV.gz
   - cast types
   - preserve `file_line_number`
   - encode instrument IDs
   - convert price/qty to fixed-point ints (using metadata)
   - **Write to Delta Table**:
     - `mode="append"` (or `overwrite` for full reloads)
     - `partition_by=["exchange", "date"]`
3. **Maintenance (Delta)**
   - **Optimize / Z-Order:** Reorganize files to cluster by `symbol_id` for fast reads.
   - **Vacuum:** Clean up old file versions to save space (after retention period).
4. **Derive (Gold)**
   - create wide snapshot tables (optional)
   - create `tob_quotes` (if desired)
   - create options surface grid (optional)
4. **QA metrics (always)**
   - snapshot resets count
   - crossed-book incidents (bid >= ask)
   - duplicate rows by `(ts_local_us, side, price)` frequency
   - gap stats around daily boundaries

All outputs are **idempotent**:
- re-running ETL yields identical Parquet (same schema, ordering, checksums), given same inputs + metadata versions.

### 10.1 Ingestion ledger (silver.ingest_manifest)
Use a dedicated Delta table to track ingestion status per Bronze file. This makes re-runs
idempotent, provides auditability, and enables fast skip logic.

**Table:** `silver.ingest_manifest` (small, unpartitioned Delta table)

**Primary key (logical):**
`(vendor, exchange, data_type, symbol, date, bronze_file_name)`
*Note: `file_id` is the surrogate key for joins from Silver tables.*

**Schema:** See [Schema Reference - ingest_manifest](../schemas.md#12-silveringest_manifest) for complete column definitions.

**Ingestion decision (skip logic):**
1. For each Bronze file, compute `file_size_bytes` and `last_modified_ts`.
2. Lookup by `(vendor, exchange, data_type, symbol, date, bronze_file_name)`.
3. If a row exists with `status=success` **and** matching `file_size_bytes` + `last_modified_ts`
   (or `sha256` if used), **skip** ingestion.
4. Otherwise, ingest and write/overwrite a manifest row with updated stats.

**Lineage guarantees:**
- Silver tables **must** include `file_id` and `file_line_number`.
- `file_line_number` provides deterministic ordering within each file.
- Full lineage (source file path) is retrieved by joining Silver tables with `silver.ingest_manifest` on `file_id`.

---

## 11) Researcher workflows (Polars/DuckDB)

Recommended patterns:
- Use DuckDB for ad-hoc SQL over Delta Tables.
- Use Polars LazyFrame scans (`pl.read_delta`) for feature pipelines.

Core "safe" APIs (suggested):
- `load_snapshots_top25(exchange, symbols, start, end)`
- `load_trades(symbol_id, start_ts_us, end_ts_us)`
- `asof_join(left, right, on="ts_local_us", by=["exchange_id","symbol_id"])`
- `book_asof(symbol, t_local)` (anchor snapshot + replay updates)

**DuckDB Example:**
```sql
SELECT * FROM delta_scan('/lake/silver/trades')
WHERE date = '2025-12-28' AND symbol_id = 123
```

**Polars Example:**
```python
pl.read_delta("/lake/silver/trades", version=None) \
  .filter(pl.col("date") == ... )
```

---

## 12) Appendix: recommended naming & conventions

### Dataset naming
- Bronze mirrors Tardis dataset names.
- Silver uses domain names:
  - `trades`, `quotes`, `book_snapshot_25`, `book_ticker`, `derivative_ticker`, `liquidations`, `options_chain`

### Timestamp units
- Store `*_us` (microseconds) as int64 consistently
- Convert to ns only if you truly need it (crypto feeds are often µs anyway)

### ID dictionaries
- `exchange_id`: i16
- `symbol_id`: i64
- See [Schema Reference - dim_symbol](../schemas.md#11-silverdim_symbol) for `dim_symbol` (SCD Type 2) structure.
