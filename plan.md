# Offline Data Lake Design for Tardis Market Data (HFT Research)

This document describes a high-performance, point-in-time (PIT) accurate offline data lake for **Tardis** datasets:
- `incremental_book_L2`
- order book `snapshots` (e.g., top-25)
- `trades`
- `quotes`
- `book_ticker`
- `derivative_ticker`
- `liquidations`
- `options_chain`

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
- `(exchange_id, symbol_id, trading_date, ts_local_us, ingest_seq)`

Where:
- `ingest_seq` is a deterministic sequence within the source file (e.g., line number).
d- **Lineage tracking**: In Silver metadata, store `bronze_file_name` and `file_line_number` to ensure `ingest_seq` is robust and debuggable.

---

## 2) Lake layers: Bronze → Silver → Gold

### Bronze (immutable vendor truth)
Store raw Tardis files exactly as downloaded.

Example:

/lake/bronze/tardis/exchange=deribit/type=incremental_book_L2/date=2025-12-28/symbol=BTC-PERPETUAL.csv.gz

No transformations besides checksums/manifests.

### Silver (typed, normalized Parquet)
Convert to Parquet with:
- integer timestamps
- integer IDs (dictionary encoding)
- stable `ingest_seq`
- normalized numeric types (fixed-point ints where possible)

Silver is the canonical research foundation.

### Gold (research-optimized derived tables)
Precomputed “fast paths”:
- top-of-book quotes (`tob_quotes`)
- top-N snapshots in wide format (`book_snapshots_top25_wide`)
- merged “event tape” for replay (optional)
- options surface snapshots on a time grid (optional)

Gold tables are reproducible from Silver (and versioned).

---

## 3) Storage format & performance defaults

**Format:** Parquet  
**Compression:** ZSTD (best compression) or LZ4 (fastest decode)  
**Row group sizing:** target ~128–512MB row groups (tune by workload)  
**File sizing:** 256MB–2GB files (avoid tiny-file overhead)

**Sort inside each Parquet file**:
- `(symbol_id, ts_local_us, ingest_seq)`  
This boosts row-group pruning and fast as-of joins.

---

## 4) Reference data: instrument metadata & fixed-point encoding

For compression + exact comparisons:
- Encode prices and sizes as integers.

### 4.1 Symbol Master (SCD Type 2)
Symbols change over time (renames, delistings). Maintain a **Slowly Changing Dimension (SCD) Type 2** table `dim_symbol`:
- `symbol_id` (pk, surrogate key)
- `exchange_symbol` (raw string)
- `exchange_id`
- `valid_from_ts`, `valid_until_ts`
- `tick_size`, `contract_size`, `price_increment`, `amount_increment`

When ingesting Silver, join against `dim_symbol` using:
- `exchange_id` (match context)
- `exchange_symbol` (match raw string)
- `trading_date` (filter where `valid_from_ts <= date < valid_until_ts`)

This resolves the stable `symbol_id` even if tickers are reused or ambiguous across exchanges.

### 4.2 Fixed-point Logic
Recommended approach:
- `price_int = round(price / price_increment)`
- `qty_int   = round(qty   / amount_increment)`

**Alternative (Robustness):** Standardize on a fixed multiplier (e.g., `1e8` or `1e9`) if metadata is flaky. "Tick-based" is better for compression; "Fixed Multiplier" is safer for operations.

Store both:
- `price_int`, `qty_int` (required)
- optionally `price_f64`, `qty_f64` for convenience (derived)

---

## 5) Silver schemas per Tardis dataset

### 5.1 `l2_updates` (from `incremental_book_L2`)
Tardis incremental L2 updates are **absolute sizes** at a price level (not deltas).
- `size == 0` means delete the level.
- `is_snapshot` marks snapshot rows.
- If a new snapshot appears after incremental updates, **reset book state** before applying it.

**Table:** `silver.l2_updates`

| Column | Type | Notes |
|---|---:|---|
| trading_date | date | derived from `ts_local_us` in UTC |
| exchange_id | u16 | dictionary-encoded |
| symbol_id | u32 | dictionary-encoded |
| ts_local_us | i64 | primary replay timeline |
| ts_exch_us | i64 | exchange time if available |
| ingest_seq | u32 | stable ordering within file |
| is_snapshot | bool | snapshot row marker |
| side | u8 | 0=bid, 1=ask |
| price_int | i64 | fixed-point ticks |
| size_int | i64 | fixed-point units |

**Optional convenience columns:**
- `msg_id` (group rows belonging to the same source message)
- `event_group_id` (if vendor provides transaction IDs spanning multiple updates)
- `source_file` / `source_offset` (lineage)

---

### 5.2 `book_snapshots_top25` (from Tardis snapshots)
Snapshots are full top-N book states (e.g., 25 levels).

**Recommended storage: list columns (Silver)**
**Verdict:** Stick to `list<i64>`. DuckDB/Polars handle lists natively and efficiently. Wide columns explode schema metadata.

**Table:** `silver.book_snapshots_top25`

| Column | Type | Notes |
|---|---:|---|
| trading_date | date | |
| exchange_id | u16 | |
| symbol_id | u32 | |
| ts_local_us | i64 | |
| ts_exch_us | i64 | |
| ingest_seq | u32 | |
| bids_px | list<i64> | length N |
| bids_sz | list<i64> | length N |
| asks_px | list<i64> | length N |
| asks_sz | list<i64> | length N |

**Gold option: wide columns (Legacy Support)**
`gold.book_snapshots_top25_wide` with columns:
- `bid_px_01..bid_px_25`, `bid_sz_01..bid_sz_25`, ...
Use this strictly for Gold if legacy tools (Pandas without explode) require it.

---

### 5.3 `trades`
**Table:** `silver.trades`

| Column | Type | Notes |
|---|---:|---|
| trading_date | date | |
| exchange_id | u16 | |
| symbol_id | u32 | |
| ts_local_us | i64 | |
| ts_exch_us | i64 | |
| ingest_seq | u32 | |
| trade_id | string | optional depending on venue |
| side | u8 | 0=buy,1=sell,2=unknown |
| price_int | i64 | |
| qty_int | i64 | |
| flags | u32 | optional packed conditions |

---

### 5.4 `tob_quotes` (from `quotes` or derived from L2)
**Table:** `silver.quotes` (or `gold.tob_quotes` if you treat it as a fast path)

| Column | Type |
|---|---:|
| trading_date | date |
| exchange_id | u16 |
| symbol_id | u32 |
| ts_local_us | i64 |
| ts_exch_us | i64 |
| ingest_seq | u32 |
| bid_px_int | i64 |
| bid_sz_int | i64 |
| ask_px_int | i64 |
| ask_sz_int | i64 |

---

### 5.5 `book_ticker`
Use if you subscribe to exchange-native `bookTicker`/similar.

**Table:** `silver.book_ticker`

Same schema as top-of-book quotes:
- plus any venue-specific fields (update ids, etc.)

---

### 5.6 `derivative_ticker`
Includes mark/index, funding, OI, etc.

**Table:** `silver.derivative_ticker`

| Column | Type | Notes |
|---|---:|---|
| trading_date | date | |
| exchange_id | u16 | |
| symbol_id | u32 | |
| ts_local_us | i64 | |
| ts_exch_us | i64 | |
| ingest_seq | u32 | |
| mark_px_int | i64 | optional fixed-point |
| index_px_int | i64 | optional fixed-point |
| last_px_int | i64 | optional fixed-point |
| funding_rate | f64 | funding often fine as float |
| funding_ts_us | i64 | next/last funding time |
| open_interest | f64/i64 | depends on venue |
| volume_24h | f64/i64 | depends on venue |

---

### 5.7 `liquidations`
**Table:** `silver.liquidations`

| Column | Type |
|---|---:|
| trading_date | date |
| exchange_id | u16 |
| symbol_id | u32 |
| ts_local_us | i64 |
| ts_exch_us | i64 |
| ingest_seq | u32 |
| liq_id | string |
| side | u8 |
| price_int | i64 |
| qty_int | i64 |

---

### 5.8 `options_chain`
Options chain is typically cross-sectional and heavy. Store updates per contract.

**Table:** `silver.options_chain`

| Column | Type | Notes |
|---|---:|---|
| trading_date | date | |
| exchange_id | u16 | |
| underlying_symbol_id | u32 | underlying |
| option_symbol_id | u32 | contract |
| ts_local_us | i64 | |
| ts_exch_us | i64 | |
| ingest_seq | u32 | |
| option_type | u8 | call/put |
| strike_int | i64 | fixed-point |
| expiry_ts_us | i64 | |
| bid_px_int | i64 | |
| ask_px_int | i64 | |
| iv | f32/f64 | |
| delta/gamma/vega/theta | f32/f64 | |
| open_interest | f64/i64 | |

**Gold recommendation:** `gold.options_surface_grid`
- Choose a time grid (e.g., 1s or 100ms).
- For each grid timestamp, take last-known as-of values per option contract.
- This makes “entire surface at time t” queries cheap and deterministic.

---

### 5.9 `gold.bars_1m` (OHLCV)
Derived from `silver.trades`. Standard time bars for signal generation.
- **Interval:** 1 minute (or 1s/1h)
- **Time labeling:** `ts_open` (inclusive) or `ts_close` (exclusive) - define convention clearly.

**Table:** `gold.bars_1m`

| Column | Type |
|---|---:|
| trading_date | date |
| exchange_id | u16 |
| symbol_id | u32 |
| ts_bucket_start_us | i64 |
| open_px_int | i64 |
| high_px_int | i64 |
| low_px_int | i64 |
| close_px_int | i64 |
| volume_qty_int | i64 |
| volume_notional | f64 |
| trade_count | u32 |

---

## 6) Partitioning strategy

Default directory partitioning:
- `exchange` and `date` as partitions.
- `symbol` partition only if universe is small enough to avoid tiny files.

Examples:

/lake/silver/l2_updates/exchange=deribit/date=2025-12-28/part-000.parquet
/lake/silver/trades/exchange=deribit/date=2025-12-28/part-000.parquet
/lake/silver/book_snapshots_top25/exchange=deribit/date=2025-12-28/part-000.parquet

Inside files:
- sort by `(symbol_id, ts_local_us, ingest_seq)`

For large universes:
- cluster by `symbol_id` within each date file (sorting already helps).

**Options Refinement:**
For massive universes (e.g., Deribit Options), single daily files may be too large.
- **Strategy A:** rely on sorting + row groups (if row groups are ~512MB, readers skip efficiently).
- **Strategy B:** Secondary partitioning: `exchange=deribit/date=2025-12-28/expiry_month=DEC25/part-000.parquet`.

---

## 7) Book reconstruction rules (PIT correctness)

### 7.1 Reconstructing L2 state from `l2_updates`
Maintain:
- `book_bids: map(price_int -> size_int)`
- `book_asks: map(price_int -> size_int)`

Apply in strict order:
- `(ts_local_us ASC, ingest_seq ASC)`

Rules:
1. If `is_snapshot = true` and you are currently in incremental mode (or you see a snapshot after updates):
   - **reset** bids/asks maps.
2. For each update row:
   - if `size_int == 0`: delete that price level
   - else: set `size_int` at that price level for that side

### 7.2 Snapshots as anchors
Use snapshots (top-25) to accelerate random access:
- Find the snapshot with max `ts_local_us <= t`
- Apply only subsequent L2 updates after that snapshot time to get exact book at `t`
- If you need deeper-than-25, you must use full incremental L2 state (if available) or accept top-25 limitations.

---

## 8) Cross-stream PIT alignment (trades, liquidations, ticker, quotes)

When joining streams:
- Use **as-of join** on `ts_local_us` by default:
  - join each event with the last-known book snapshot/quote/ticker at or before that time
- Keep `ingest_seq` to break ties within the same microsecond.

This prevents lookahead bias from “future” book states.

### 8.1 The "Clock" Stream
For true PIT replay, include a `clock` stream (e.g., 1s or 1m ticks).
- **Why:** `l2_updates` only exist when market moves. A backtest iterating only on data updates will skip quiet periods, missing scheduled timer events (e.g., "close position at 16:00:00").
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
- If you only have L2: you cannot reproduce FIFO queue exactly.
  - Use a documented approximation:
    - queue-position model based on observed size at price
    - or trade-through / volume-at-price heuristics
- If you have only top-25 snapshots: fills must acknowledge truncated depth.

Version the fill model and store `fill_model_version` in results.

---

## 10) ETL pipeline (deterministic & reproducible)

Stages:
1. **Ingest (Bronze)**
   - download + store raw files
   - compute checksums
   - write manifest: row counts, min/max timestamps
2. **Parse & Normalize (Silver)**
   - read CSV.gz
   - cast types
   - compute `ingest_seq`
   - encode instrument IDs
   - convert price/qty to fixed-point ints (using metadata)
   - write sorted Parquet
3. **Derive (Gold)**
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

---

## 11) Researcher workflows (Polars/DuckDB)

Recommended patterns:
- Use DuckDB for ad-hoc SQL over Parquet.
- Use Polars LazyFrame scans for feature pipelines (predicate pushdown + streaming).

Core “safe” APIs (suggested):
- `load_l2_updates(exchange, symbols, start, end)`
- `load_snapshots_top25(exchange, symbols, start, end)`
- `load_trades(exchange, symbols, start, end)`
- `asof_join(left, right, on="ts_local_us", by=["exchange_id","symbol_id"])`
- `book_asof(symbol, t_local)` (anchor snapshot + replay updates)

---

## 12) Appendix: recommended naming & conventions

### Dataset naming
- Bronze mirrors Tardis dataset names.
- Silver uses domain names:
  - `l2_updates`, `book_snapshots_top25`, `trades`, `quotes`, `book_ticker`, `derivative_ticker`, `liquidations`, `options_chain`

### Timestamp units
- Store `*_us` (microseconds) as int64 consistently
- Convert to ns only if you truly need it (crypto feeds are often µs anyway)

### ID dictionaries
- `exchange_id`: u16
- `symbol_id`: u32
- See **Section 4.1** for `dim_symbol` (SCD Type 2) structure.

---

## Next steps (to finalize the design)
To lock in partitioning and file sizing, decide:
1. Exchanges / venues (Deribit, Binance, OKX, Bybit, etc.)
2. Universe size (#symbols, #options contracts)
3. Daily volume (rows/day per dataset)
4. Typical research queries (single-symbol intraday vs cross-sectional at time t)

With those, we can tune:
- symbol partitioning vs clustering
- snapshot cadence for derived gold anchors
- row group sizing and file batching strategy
- which gold tables materially reduce researcher compute costs