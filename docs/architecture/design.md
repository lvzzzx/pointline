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
- `(exchange_id, symbol_id, date, ts_local_us, ingest_seq)`

Where:
- `ingest_seq` is a deterministic sequence within the source file (e.g., line number).
- **Lineage tracking**: In Silver tables, store `file_id` (i32) and `file_line_number` to ensure `ingest_seq` is robust and debuggable. Join with `silver.ingest_manifest` to resolve the original `bronze_file_name`.

---

## 2) Lake layers: Bronze → Silver → Gold

### Bronze (immutable vendor truth)
Store raw Tardis files exactly as downloaded.

Example:

/lake/bronze/tardis/exchange=deribit/type=incremental_book_L2/date=2025-12-28/symbol=BTC-PERPETUAL/deribit_incremental_book_L2_2025-12-28_BTC-PERPETUAL.csv.gz

**Tardis downloader filename template (recommended):**
`tardis/exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/{exchange}_{data_type}_{date}_{symbol}.{format}`

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
- top-N snapshots in wide format (`book_snapshot_25_wide`)
- merged “event tape” for replay (optional)
- options surface snapshots on a time grid (optional)

Gold tables are reproducible from Silver (and versioned).

Additional replay accelerator (derived from `silver.l2_updates`):
- `gold.l2_state_checkpoint` for full-depth replay checkpoints

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
- Use `Int32` instead of `UInt32` for `ingest_seq`, `file_id`, `flags`
- Use `Int64` for `symbol_id` to match `dim_symbol`
- `UInt8` is supported and maps to TINYINT (use for `side`, `asset_type`)

**Data Organization inside partitions**:
- **Z-Order / Cluster by:** `(symbol_id, ingest_seq)`
- or **Sort by:** `(symbol_id, ts_local_us, ingest_seq)`
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
- `price_int = round(price / price_increment)`
- `qty_int   = round(qty   / amount_increment)`

**Alternative (Robustness):** Standardize on a fixed multiplier (e.g., `1e8` or `1e9`) if metadata is flaky. "Tick-based" is better for compression; "Fixed Multiplier" is safer for operations.

Store both:
- `price_int`, `qty_int` (required)
- optionally `price_f64`, `qty_f64` for convenience (derived)

**Detailed encoding explanation:** See [Schema Reference - Fixed-Point Encoding](../schemas.md#fixed-point-encoding).

---

## 5) Silver schemas per Tardis dataset

**Schema Reference:** For complete column definitions, see [Schema Reference - Silver Tables](../schemas.md#silver-tables).

**Related design:** For the Rust + PyO3 replay engine and build interfaces, see
[`docs/architecture/l2_replay_engine.md`](l2_replay_engine.md).

### 5.1 `l2_updates` (from `incremental_book_L2`)
Tardis incremental L2 updates are **absolute sizes** at a price level (not deltas).
- `size == 0` means delete the level.
- `is_snapshot` marks snapshot rows.
- If a new snapshot appears after incremental updates, **reset book state** before applying it.

**Table:** `silver.l2_updates`  
**Schema:** See [Schema Reference - l2_updates](../schemas.md#21-silverl2_updates)

**Optional convenience columns:**
- `file_id` (lineage tracking)
- `file_line_number` (lineage tracking)

**Replay accelerator (Gold):**
- `gold.l2_state_checkpoint` to jump close to a target time and replay forward
- Partition by `exchange` + `date`, cluster/Z-order by `symbol_id` + `ts_local_us`; rebuilds should upsert scoped to `(exchange, date, symbol_id)`.

#### 5.1.1 Build Recipes (Data Infra)

These tables are owned by **data infra** and should be built as scheduled jobs.

**Polars + Python: full-depth checkpoints (skeleton)**
```python
import os
from datetime import datetime, timezone
import polars as pl
from pointline import research

lake_root = os.getenv("LAKE_ROOT", "./data/lake")

checkpoint_every_us = 10_000_000
checkpoint_every_updates = 10_000

updates = (
    research.scan_table(
        "l2_updates",
        exchange_id=21,
        symbol_id=1234,
        start_date="2025-12-01",
        end_date="2025-12-01",
        columns=[
            "ts_local_us",
            "ingest_seq",
            "file_id",
            "file_line_number",
            "is_snapshot",
            "side",
            "price_int",
            "size_int",
        ],
    )
    .sort(["ts_local_us", "ingest_seq", "file_line_number"])
    .collect()
)

bids: dict[int, int] = {}
asks: dict[int, int] = {}
checkpoints: list[dict] = []
last_checkpoint_us = None
updates_since = 0

for row in updates.iter_rows(named=True):
    if row["is_snapshot"]:
        bids.clear()
        asks.clear()

    side_map = bids if row["side"] == 0 else asks
    if row["size_int"] == 0:
        side_map.pop(row["price_int"], None)
    else:
        side_map[row["price_int"]] = row["size_int"]

    now_us = row["ts_local_us"]
    if last_checkpoint_us is None:
        last_checkpoint_us = now_us

    updates_since += 1
    if (now_us - last_checkpoint_us) >= checkpoint_every_us or updates_since >= checkpoint_every_updates:
        date = datetime.fromtimestamp(now_us / 1_000_000, tz=timezone.utc).date().isoformat()
        checkpoints.append(
            {
                "exchange_id": 21,
                "symbol_id": 1234,
                "date": date,
                "ts_local_us": now_us,
                "bids": [{"price_int": p, "size_int": s} for p, s in sorted(bids.items(), reverse=True)],
                "asks": [{"price_int": p, "size_int": s} for p, s in sorted(asks.items())],
                "file_id": row["file_id"],
                "ingest_seq": row["ingest_seq"],
                "file_line_number": row["file_line_number"],
                "checkpoint_kind": "periodic",
            }
        )
        last_checkpoint_us = now_us
        updates_since = 0

pl.DataFrame(checkpoints).write_delta(f"{lake_root}/gold/l2_state_checkpoint", mode="append")
```

This checkpoint loop is intentionally simple. In production, stream by partition and write
incrementally to avoid collecting large ranges in memory.

---

### 5.2 `book_snapshot_25` (from Tardis snapshots)
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
- `ingest_seq` provides deterministic ordering within the source file.

**List encoding**
- Convert `asks[0..24].price/amount` and `bids[0..24].price/amount` into lists of length 25.
- Use nulls for missing levels (retain list length for positional consistency).
- Convert prices/sizes into fixed-point `i64` using the symbol's `price_increment` / `amount_increment`.

**Recommended storage: list columns (Silver)**
**Verdict:** Stick to `list<i64>`. DuckDB/Polars handle lists natively and efficiently. Wide columns explode schema metadata.

**Table:** `silver.book_snapshot_25`  
**Partitioned by:** `["exchange", "date"]` (same strategy as trades/quotes tables)  
**Schema:** See [Schema Reference - book_snapshot_25](../schemas.md#22-silverbook_snapshot_25)

**Gold option: wide columns (Legacy Support)**
`gold.book_snapshot_25_wide` with columns:
- `bid_px_01..bid_px_25`, `bid_sz_01..bid_sz_25`, ...
Use this strictly for Gold if legacy tools (Pandas without explode) require it. See [Schema Reference - Gold Tables](../schemas.md#gold-tables) for details.

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

### 5.9 `gold.bars_1m` (OHLCV)
Derived from `silver.trades`. Standard time bars for signal generation.
- **Interval:** 1 minute (or 1s/1h)
- **Time labeling:** `ts_open` (inclusive) or `ts_close` (exclusive) - define convention clearly.

**Table:** `gold.bars_1m`  
**Schema:** See [Schema Reference - Gold Tables - bars_1m](../schemas.md#31-goldbars_1m)

---

## 6) Partitioning strategy

**Default Partitioning (most Silver tables):**
- `exchange`
- `date` (daily partitions, derived from `ts_local_us` in UTC)

**`silver.l2_updates` (ingest-ordered for replay):**
- `exchange`
- `date`
- `symbol_id`

This extra `symbol_id` partition makes replay ordering guarantees enforceable without a global
sort, at the cost of more partitions (acceptable in development).

**Path structure (l2_updates):**
`/lake/silver/l2_updates/exchange=deribit/date=2025-12-28/symbol_id=1234/part-000-uuid.parquet`

**Handling Massive Universes (e.g., Options):**
For datasets like `options_chain` where a single day is massive:
- The `exchange/date` strategy still holds.
- Delta Lake will automatically split the data into multiple files (e.g., 1GB each) within that folder.
- **Critical:** Run `OPTIMIZE ... ZORDER BY (symbol_id)` (or underlying/strike) after writing. This ensures that all rows for a specific contract are co-located in the same file(s), allowing the reader to skip 99% of the data.

---

## 7) Book reconstruction rules (PIT correctness)

### 7.1 Reconstructing L2 state from `l2_updates`
Maintain:
- `book_bids: map(price_int -> size_int)`
- `book_asks: map(price_int -> size_int)`

Apply in strict order (per symbol, per day):
- `(ts_local_us ASC, ingest_seq ASC, file_id ASC, file_line_number ASC)`
- Ingest must write **sorted** files within each `exchange/date/symbol_id` partition.
  Readers may skip global sort only if this invariant is guaranteed.

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
   - write manifest (ingestion ledger): row counts, min/max timestamps, file metadata
2. **Parse & Normalize (Silver)**
   - read CSV.gz
   - cast types
   - compute `ingest_seq`
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
`(exchange, data_type, symbol, date, bronze_file_name)`
*Note: `file_id` is the surrogate key for joins from Silver tables.*

**Schema:** See [Schema Reference - ingest_manifest](../schemas.md#12-silveringest_manifest) for complete column definitions.

**Ingestion decision (skip logic):**
1. For each Bronze file, compute `file_size_bytes` and `last_modified_ts`.
2. Lookup by `(exchange, data_type, symbol, date, bronze_file_name)`.
3. If a row exists with `status=success` **and** matching `file_size_bytes` + `last_modified_ts`
   (or `sha256` if used), **skip** ingestion.
4. Otherwise, ingest and write/overwrite a manifest row with updated stats.

**Lineage guarantees:**
- Silver tables **must** include `file_id` and `file_line_number`.
- `ingest_seq` is derived from `file_line_number` to ensure deterministic ordering.
- Full lineage (source file path) is retrieved by joining Silver tables with `silver.ingest_manifest` on `file_id`.

---

## 11) Researcher workflows (Polars/DuckDB)

Recommended patterns:
- Use DuckDB for ad-hoc SQL over Delta Tables.
- Use Polars LazyFrame scans (`pl.read_delta`) for feature pipelines.

Core “safe” APIs (suggested):
- `load_l2_updates(exchange, symbols, start, end)` -> wraps `deltalake` reader
- `load_snapshots_top25(exchange, symbols, start, end)`
- `load_trades(exchange, symbols, start, end)`
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
  - `l2_updates`, `book_snapshot_25`, `trades`, `quotes`, `book_ticker`, `derivative_ticker`, `liquidations`, `options_chain`

### Timestamp units
- Store `*_us` (microseconds) as int64 consistently
- Convert to ns only if you truly need it (crypto feeds are often µs anyway)

### ID dictionaries
- `exchange_id`: i16
- `symbol_id`: i64
- See [Schema Reference - dim_symbol](../schemas.md#11-silverdim_symbol) for `dim_symbol` (SCD Type 2) structure.
