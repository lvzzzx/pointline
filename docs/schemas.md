# Data Lake Schema Reference

This document is the **single source of truth** for all table schemas in the Pointline data lake. It provides complete column definitions, partitioning information, and data semantics for researchers and LLM agents.

**Organization:**
- **Reference Tables**: Metadata and tracking tables (`dim_symbol`, `ingest_manifest`)
- **Silver Tables**: Canonical research foundation tables (normalized, typed Parquet)
- **Gold Tables**: Derived tables optimized for specific research workflows
- **Common Patterns**: Shared conventions (fixed-point encoding, common columns, partitioning)

For design rationale and architecture decisions, see [Architecture Design](architecture/design.md).

---

## 1. Reference Tables

### 1.1 `silver.dim_symbol`

Slowly Changing Dimension (SCD) Type 2 table tracking instrument metadata over time. Symbols may change (renames, delistings, tick size changes), so this table maintains a validity window for each version.

**Storage:** Single unpartitioned Delta table (small size).

**Surrogate Key:** `symbol_id` (i32)  
**Natural Key:** `(exchange_id, exchange_symbol)`

| Column | Type | Description |
|---|---|---|
| **symbol_id** | i32 | Surrogate Key (Primary Key) |
| **exchange_id** | i16 | Dictionary ID (e.g., 1=binance, 2=binance-futures) |
| **exchange** | string | Normalized exchange name (e.g., `binance-futures`) for consistency with silver tables |
| **exchange_symbol** | string | Raw vendor ticker (e.g., `BTC-PERPETUAL`) |
| **base_asset** | string | e.g., `BTC` |
| **quote_asset** | string | e.g., `USD` or `USDT` |
| **asset_type** | u8 | 0=spot, 1=perp, 2=future, 3=option |
| **tick_size** | f64 | Minimum price step allowed by the exchange |
| **lot_size** | f64 | Minimum tradable quantity allowed by the exchange |
| **price_increment** | f64 | The value of "1" in `price_int` (Storage Encoding) |
| **amount_increment** | f64 | The value of "1" in `qty_int` (Storage Encoding) |
| **contract_size** | f64 | Value of 1 contract in base/quote units |
| **valid_from_ts** | i64 | Inclusive µs timestamp |
| **valid_until_ts** | i64 | Exclusive µs timestamp (default: `2^63 - 1`) |
| **is_current** | bool | Helper for latest version queries |

**Notes:**
- In **Tick-based encoding** (recommended for compression), `price_increment == tick_size`.
- In **Fixed Multiplier encoding** (recommended for cross-symbol math consistency), `price_increment` is a constant like `1e-8`, regardless of the current `tick_size`.
- We store both to allow the backtester to know the *exchange rules* (`tick_size`) while the database driver uses `price_increment` for decoding.

**Update Logic:**
1. If a symbol's metadata (e.g., `tick_size`) changes:
   - Update the existing record: set `valid_until_ts = change_ts` and `is_current = false`.
   - Insert a new record: set `valid_from_ts = change_ts`, `valid_until_ts = infinity`, and `is_current = true`.
2. This ensures that Silver data ingested on `date=X` joins against the metadata that was **active on `date=X`**, preserving historical accuracy.

**Join Pattern:**
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

---

### 1.2 `silver.ingest_manifest`

Tracks ingestion status per Bronze file. Enables idempotent re-runs, provides auditability, and enables fast skip logic.

**Storage:** Small unpartitioned Delta table.

**Primary Key (logical):** `(exchange, data_type, symbol, date, bronze_file_name)`  
**Surrogate Key:** `file_id` (i32) - used for joins from Silver tables

| Column | Type | Description |
|---|---|---|
| **file_id** | i32 | Surrogate Key (Primary Key) |
| **exchange** | string | e.g., `binance` |
| **data_type** | string | e.g., `trades`, `quotes` |
| **symbol** | string | upper-case, `BTCUSDT` |
| **date** | date | UTC date from file path |
| **bronze_file_name** | string | full relative path to the CSV.GZ |
| **file_size_bytes** | i64 | size at ingest time |
| **last_modified_ts** | i64 | mtime in µs (UTC) |
| **sha256** | string | optional hash for immutability checks |
| **row_count** | i64 | number of rows ingested |
| **ts_local_min_us** | i64 | min `ts_local_us` in file |
| **ts_local_max_us** | i64 | max `ts_local_us` in file |
| **ts_exch_min_us** | i64 | optional min exchange ts |
| **ts_exch_max_us** | i64 | optional max exchange ts |
| **ingested_at** | i64 | ingest time in µs |
| **status** | string | `success`, `failed`, `pending`, `quarantined` |
| **error_message** | string | error text for failed ingests |

**Ingestion Decision (Skip Logic):**
1. For each Bronze file, compute `file_size_bytes` and `last_modified_ts`.
2. Lookup by `(exchange, data_type, symbol, date, bronze_file_name)`.
3. If a row exists with `status=success` **and** matching `file_size_bytes` + `last_modified_ts` (or `sha256` if used), **skip** ingestion.
4. Otherwise, ingest and write/overwrite a manifest row with updated stats.

**Lineage Guarantees:**
- Silver tables **must** include `file_id` and `file_line_number`.
- `ingest_seq` is derived from `file_line_number` to ensure deterministic ordering.
- Full lineage (source file path) is retrieved by joining Silver tables with `silver.ingest_manifest` on `file_id`.

---

## 2. Silver Tables

Silver tables are the canonical research foundation. They are normalized, typed Parquet files with fixed-point integer encoding for prices and quantities.

**Common Partitioning:** All Silver tables (except reference tables) are partitioned by `["exchange", "date"]`.

**Common Columns:** Most Silver tables include:
- `date` (date): Partition key, derived from `ts_local_us` in UTC
- `exchange` (string): Partition key (not stored in Parquet, reconstructed from directory)
- `exchange_id` (i16): For joins and compression
- `symbol_id` (i32/i64): Stable identifier from `dim_symbol`
- `ts_local_us` (i64): Primary replay timeline (arrival time)
- `ts_exch_us` (i64): Exchange time if available
- `ingest_seq` (i32): Stable ordering within file
- `file_id` (i32): Lineage tracking (join with `ingest_manifest`)
- `file_line_number` (i32): Lineage tracking (deterministic ordering)

---

### 2.1 `silver.l2_updates`

Incremental Level 2 order book updates from Tardis `incremental_book_L2`. Updates are **absolute sizes** at a price level (not deltas).

**Source:** Tardis `incremental_book_L2`  
**Partitioned by:** `["exchange", "date"]`

| Column | Type | Notes |
|---|---:|---|
| date | date | derived from `ts_local_us` in UTC |
| exchange | string | partitioned by (not stored in Parquet files) |
| exchange_id | i16 | dictionary-encoded |
| symbol_id | i32 | dictionary-encoded |
| ts_local_us | i64 | primary replay timeline |
| ts_exch_us | i64 | exchange time if available |
| ingest_seq | i32 | stable ordering within file |
| is_snapshot | bool | snapshot row marker |
| side | u8 | 0=bid, 1=ask |
| price_int | i64 | fixed-point ticks |
| size_int | i64 | fixed-point units |
| file_id | i32 | lineage tracking |
| file_line_number | i32 | lineage tracking |

**Optional convenience columns:**
- `msg_id` (group rows belonging to the same source message)
- `event_group_id` (if vendor provides transaction IDs spanning multiple updates)

**Data Semantics:**
- `size == 0` means delete the level.
- `is_snapshot` marks snapshot rows.
- If a new snapshot appears after incremental updates, **reset book state** before applying it.

**Reconstruction Order:** Apply updates in strict order: `(ts_local_us ASC, ingest_seq ASC)`

---

### 2.2 `silver.book_snapshot_25`

Full top-25 order book snapshots from Tardis `book_snapshot_25`.

**Source:** Tardis `book_snapshot_25`  
**Partitioned by:** `["exchange", "date"]`

| Column | Type | Notes |
|---|---:|---|
| date | date | partitioned by (derived from `ts_local_us` in UTC) |
| exchange | string | partitioned by (not stored in Parquet files, reconstructed from directory) |
| exchange_id | i16 | for joins and compression |
| symbol_id | i64 | match dim_symbol type |
| ts_local_us | i64 | primary replay timeline |
| ts_exch_us | i64 | exchange time if available |
| ingest_seq | i32 | stable ordering within file |
| bids_px | list<i64> | length 25 (nulls for missing levels) |
| bids_sz | list<i64> | length 25 (nulls for missing levels) |
| asks_px | list<i64> | length 25 (nulls for missing levels) |
| asks_sz | list<i64> | length 25 (nulls for missing levels) |
| file_id | i32 | lineage tracking (join with `silver.ingest_manifest`) |
| file_line_number | i32 | lineage tracking (deterministic ordering within file) |

**Source Schema (Tardis):**
- `exchange` (string), `symbol` (string, uppercase)
- `timestamp` (exchange timestamp in µs; falls back to `local_timestamp` if exchange does not provide)
- `local_timestamp` (arrival timestamp in µs, UTC)
- `asks[0..24].price`, `asks[0..24].amount` (ascending by price)
- `bids[0..24].price`, `bids[0..24].amount` (descending by price)
- Missing levels may be empty if fewer than 25 price levels exist.

**Normalization:**
- `exchange` is normalized (lowercase, trimmed) and used for partitioning.
- `exchange_id` is derived via `EXCHANGE_MAP` and stored for joins.
- `symbol` is mapped to `symbol_id` via `dim_symbol`.
- `ts_exch_us` maps from `timestamp`; `ts_local_us` maps from `local_timestamp`.
- `date` is derived from `ts_local_us` in UTC.

**List Encoding:**
- Convert `asks[0..24].price/amount` and `bids[0..24].price/amount` into lists of length 25.
- Use nulls for missing levels (retain list length for positional consistency).
- Convert prices/sizes into fixed-point `i64` using the symbol's `price_increment` / `amount_increment`.

**Storage:** Uses `list<i64>` columns. DuckDB/Polars handle lists natively and efficiently.

---

### 2.3 `silver.trades`

Trade executions from Tardis `trades` dataset.

**Source:** Tardis `trades`  
**Partitioned by:** `["exchange", "date"]`

| Column | Type | Notes |
|---|---:|---|
| date | date | |
| exchange | string | partitioned by (not stored in Parquet files) |
| exchange_id | i16 | |
| symbol_id | i32 | |
| ts_local_us | i64 | |
| ts_exch_us | i64 | |
| ingest_seq | i32 | |
| trade_id | string | optional depending on venue |
| side | u8 | 0=buy,1=sell,2=unknown |
| price_int | i64 | fixed-point |
| qty_int | i64 | fixed-point |
| flags | i32 | optional packed conditions |
| file_id | i32 | lineage tracking |
| file_line_number | i32 | lineage tracking |

---

### 2.4 `silver.quotes`

Top-of-book quotes from Tardis `quotes` dataset or derived from L2.

**Source:** Tardis `quotes` or derived from L2  
**Partitioned by:** `["exchange", "date"]`

| Column | Type | Notes |
|---|---:|---|
| date | date | |
| exchange | string | partitioned by (not stored in Parquet files) |
| exchange_id | i16 | |
| symbol_id | i32 | |
| ts_local_us | i64 | |
| ts_exch_us | i64 | |
| ingest_seq | i32 | |
| bid_px_int | i64 | fixed-point |
| bid_sz_int | i64 | fixed-point |
| ask_px_int | i64 | fixed-point |
| ask_sz_int | i64 | fixed-point |
| file_id | i32 | lineage tracking |
| file_line_number | i32 | lineage tracking |

**Note:** This table may also be implemented as `gold.tob_quotes` if treated as a fast path derived table.

---

### 2.5 `silver.book_ticker`

Exchange-native `bookTicker` messages (if subscribed directly).

**Source:** Exchange-native `bookTicker`  
**Partitioned by:** `["exchange", "date"]`

Same schema as top-of-book quotes (`silver.quotes`), plus any venue-specific fields (update ids, etc.).

---

### 2.6 `silver.derivative_ticker`

Derivative market data including mark/index, funding, open interest, etc.

**Source:** Tardis `derivative_ticker`  
**Partitioned by:** `["exchange", "date"]`

| Column | Type | Notes |
|---|---:|---|
| date | date | |
| exchange | string | partitioned by (not stored in Parquet files) |
| exchange_id | i16 | |
| symbol_id | i32 | |
| ts_local_us | i64 | |
| ts_exch_us | i64 | |
| ingest_seq | i32 | |
| mark_px_int | i64 | optional fixed-point |
| index_px_int | i64 | optional fixed-point |
| last_px_int | i64 | optional fixed-point |
| funding_rate | f64 | funding often fine as float |
| funding_ts_us | i64 | next/last funding time |
| open_interest | f64/i64 | depends on venue |
| volume_24h | f64/i64 | depends on venue |
| file_id | i32 | lineage tracking |
| file_line_number | i32 | lineage tracking |

---

### 2.7 `silver.liquidations`

Liquidation events from Tardis `liquidations` dataset.

**Source:** Tardis `liquidations`  
**Partitioned by:** `["exchange", "date"]`

| Column | Type | Notes |
|---|---:|---|
| date | date | |
| exchange | string | partitioned by (not stored in Parquet files) |
| exchange_id | i16 | |
| symbol_id | i32 | |
| ts_local_us | i64 | |
| ts_exch_us | i64 | |
| ingest_seq | i32 | |
| liq_id | string | |
| side | u8 | |
| price_int | i64 | fixed-point |
| qty_int | i64 | fixed-point |
| file_id | i32 | lineage tracking |
| file_line_number | i32 | lineage tracking |

---

### 2.8 `silver.options_chain`

Options chain data, typically cross-sectional and heavy. Store updates per contract.

**Source:** Tardis `options_chain`  
**Partitioned by:** `["exchange", "date"]`

| Column | Type | Notes |
|---|---:|---|
| date | date | |
| exchange | string | partitioned by (not stored in Parquet files) |
| exchange_id | i16 | |
| underlying_symbol_id | i32 | underlying |
| option_symbol_id | i32 | contract |
| ts_local_us | i64 | |
| ts_exch_us | i64 | |
| ingest_seq | i32 | |
| option_type | u8 | call/put |
| strike_int | i64 | fixed-point |
| expiry_ts_us | i64 | |
| bid_px_int | i64 | fixed-point |
| ask_px_int | i64 | fixed-point |
| iv | f32/f64 | implied volatility |
| delta | f32/f64 | Greeks |
| gamma | f32/f64 | Greeks |
| vega | f32/f64 | Greeks |
| theta | f32/f64 | Greeks |
| open_interest | f64/i64 | |
| file_id | i32 | lineage tracking |
| file_line_number | i32 | lineage tracking |

---

## 3. Gold Tables

Gold tables are derived from Silver tables and optimized for specific research workflows. They are reproducible from Silver and versioned.

---

### 3.1 `gold.bars_1m`

OHLCV (Open-High-Low-Close-Volume) time bars derived from `silver.trades`.

**Source:** Derived from `silver.trades`  
**Partitioned by:** `["exchange", "date"]`

| Column | Type | Notes |
|---|---:|---|
| date | date | |
| exchange | string | partitioned by (not stored in Parquet files) |
| exchange_id | i16 | |
| symbol_id | i32 | |
| ts_bucket_start_us | i64 | bucket start time |
| open_px_int | i64 | fixed-point |
| high_px_int | i64 | fixed-point |
| low_px_int | i64 | fixed-point |
| close_px_int | i64 | fixed-point |
| volume_qty_int | i64 | fixed-point |
| volume_notional | f64 | |
| trade_count | i32 | |

**Interval:** 1 minute (or 1s/1h, configurable)  
**Time Labeling:** `ts_bucket_start_us` (inclusive) or `ts_close` (exclusive) - convention must be clearly defined.

---

### 3.2 `gold.book_snapshot_25_wide`

Wide-format version of `silver.book_snapshot_25` for legacy tools.

**Source:** Derived from `silver.book_snapshot_25`  
**Partitioned by:** `["exchange", "date"]`

Columns: `bid_px_01..bid_px_25`, `bid_sz_01..bid_sz_25`, `ask_px_01..ask_px_25`, `ask_sz_01..ask_sz_25`, plus common columns.

**Use Case:** Strictly for Gold if legacy tools (Pandas without explode) require it.

---

### 3.3 `gold.tob_quotes`

Top-of-book quotes fast path (if treated as derived table).

**Source:** Derived from `silver.quotes` or `silver.l2_updates`  
**Partitioned by:** `["exchange", "date"]`

Same schema as `silver.quotes`.

---

### 3.4 `gold.l2_snapshot_index`

Snapshot anchor index for fast lookup of the latest L2 snapshot at or before a timestamp.

**Source:** Derived from `silver.l2_updates`  
**Partitioned by:** `["exchange", "date"]`

| Column | Type | Notes |
|---|---:|---|
| date | date | derived from `ts_local_us` in UTC |
| exchange | string | partitioned by (not stored in Parquet files) |
| exchange_id | i16 | |
| symbol_id | i32 | |
| ts_local_us | i64 | snapshot timestamp (replay timeline) |
| ingest_seq | i32 | stable ordering within file |
| file_id | i32 | lineage tracking |
| file_line_number | i32 | first row for the snapshot group |
| msg_id | i64 | optional, if provided by vendor |

**Semantics:**
- One row per snapshot group (use `msg_id` when available, otherwise group by `(ts_local_us, file_id, ingest_seq)`).
- Used to anchor incremental replay without scanning all `l2_updates`.

---

### 3.5 `gold.l2_state_checkpoint`

Full-depth book checkpoints to accelerate incremental replay over long ranges.

**Source:** Derived from `silver.l2_updates`  
**Partitioned by:** `["exchange", "date"]`

| Column | Type | Notes |
|---|---:|---|
| date | date | derived from `ts_local_us` in UTC |
| exchange | string | partitioned by (not stored in Parquet files) |
| exchange_id | i16 | |
| symbol_id | i32 | |
| ts_local_us | i64 | checkpoint timestamp (replay timeline) |
| bids | list<struct<price_int: i64, size_int: i64>> | descending by price |
| asks | list<struct<price_int: i64, size_int: i64>> | ascending by price |
| file_id | i32 | lineage tracking |
| ingest_seq | i32 | stable ordering within file |
| file_line_number | i32 | deterministic ordering |
| checkpoint_kind | string | optional (`periodic` or `snapshot`) |

**Semantics:**
- Checkpoints store **full depth** state plus the exact stream position used to create them.
- Safe replay start point for long-range queries (replay forward from checkpoint).

---

### 3.6 `gold.options_surface_grid`

Options surface snapshots on a time grid for efficient "entire surface at time t" queries.

**Source:** Derived from `silver.options_chain`  
**Partitioned by:** `["exchange", "date"]`

**Time Grid:** Choose a time grid (e.g., 1s or 100ms). For each grid timestamp, take last-known as-of values per option contract.

This makes "entire surface at time t" queries cheap and deterministic.

---

## 4. Common Patterns

### 4.1 Fixed-Point Encoding

Prices and quantities are stored as integers (`i64`) for compression and exact precision.

**Encoding Formula:**
- `price_int = round(price / price_increment)`
- `qty_int = round(qty / amount_increment)`

**Decoding Formula:**
- `real_price = price_int * price_increment`
- `real_qty = qty_int * amount_increment`

**Increment Values:**
- `price_increment` and `amount_increment` are stored in `silver.dim_symbol`
- In **Tick-based encoding** (recommended for compression), `price_increment == tick_size`
- In **Fixed Multiplier encoding** (recommended for cross-symbol math consistency), `price_increment` is a constant like `1e-8`, regardless of the current `tick_size`

**Storage:**
- `price_int`, `qty_int` (required, stored as `i64`)
- Optionally `price_f64`, `qty_f64` for convenience (derived, not stored)

**Alternative (Robustness):** Standardize on a fixed multiplier (e.g., `1e8` or `1e9`) if metadata is flaky. "Tick-based" is better for compression; "Fixed Multiplier" is safer for operations.

---

### 4.2 Common Columns

Most Silver tables include these standard columns:

| Column | Type | Description |
|---|---|---|
| `date` | date | Partition key, derived from `ts_local_us` in UTC |
| `exchange` | string | Partition key (not stored in Parquet, reconstructed from directory) |
| `exchange_id` | i16 | Dictionary-encoded exchange ID for joins |
| `symbol_id` | i32/i64 | Stable identifier from `dim_symbol` |
| `ts_local_us` | i64 | Primary replay timeline (arrival time) |
| `ts_exch_us` | i64 | Exchange time if available |
| `ingest_seq` | i32 | Stable ordering within file |
| `file_id` | i32 | Lineage tracking (join with `ingest_manifest`) |
| `file_line_number` | i32 | Lineage tracking (deterministic ordering) |

**Stable Ordering Key:** `(exchange_id, symbol_id, date, ts_local_us, ingest_seq)`

---

### 4.3 Partitioning Strategy

**Primary Partitioning:**
- `exchange` (string)
- `date` (date, daily partitions, derived from `ts_local_us` in UTC)

**Path Structure:**
```
/lake/silver/<table_name>/exchange=<exchange>/date=<date>/part-*.parquet
```

**Do NOT partition by `symbol`:**
- High-cardinality partitioning creates too many tiny files and metadata overhead in Delta Lake.
- Instead, rely on **Z-Ordering** (or local sorting) within the daily partition to cluster data by `symbol_id`.

**Data Organization inside partitions:**
- **Z-Order / Cluster by:** `(symbol_id, ingest_seq)`
- or **Sort by:** `(symbol_id, ts_local_us, ingest_seq)`

This boosts pruning for specific symbols without creating thousands of tiny partition directories.

**Handling Massive Universes (e.g., Options):**
For datasets like `options_chain` where a single day is massive:
- The `exchange/date` strategy still holds.
- Delta Lake will automatically split the data into multiple files (e.g., 1GB each) within that folder.
- **Critical:** Run `OPTIMIZE ... ZORDER BY (symbol_id)` (or underlying/strike) after writing. This ensures that all rows for a specific contract are co-located in the same file(s), allowing the reader to skip 99% of the data.

---

### 4.4 Type Conventions

**Integer Type Limitations:**
Delta Lake (via Parquet) does not support unsigned integer types `UInt16` and `UInt32`. These are automatically converted to signed types (`Int16` and `Int32`) when written.

- Use `Int16` instead of `UInt16` for `exchange_id`
- Use `Int32` instead of `UInt32` for `symbol_id`, `ingest_seq`, `file_id`, `flags`
- `UInt8` is supported and maps to TINYINT (use for `side`, `asset_type`)

**Timestamp Units:**
- Store `*_us` (microseconds) as `i64` consistently
- Convert to ns only if you truly need it (crypto feeds are often µs anyway)

**ID Dictionaries:**
- `exchange_id`: i16
- `symbol_id`: i32 (or i64 for `book_snapshot_25` to match `dim_symbol`)

---

## 5. Quick Reference

### Table Catalog

| Table | Layer | Partitions | Key Columns |
|---|---|---|---|
| `dim_symbol` | Silver (Reference) | none | `symbol_id`, `exchange_id`, `exchange_symbol`, validity range |
| `ingest_manifest` | Silver (Reference) | none | `exchange`, `data_type`, `date`, `status` |
| `l2_updates` | Silver | `exchange`, `date` | `ts_local_us`, `symbol_id`, `price_int`, `size_int` |
| `book_snapshot_25` | Silver | `exchange`, `date` | `ts_local_us`, `symbol_id`, `bids_px`, `asks_px` |
| `trades` | Silver | `exchange`, `date` | `ts_local_us`, `symbol_id`, `price_int`, `qty_int` |
| `quotes` | Silver | `exchange`, `date` | `ts_local_us`, `symbol_id`, `bid_px_int`, `ask_px_int` |
| `book_ticker` | Silver | `exchange`, `date` | `ts_local_us`, `symbol_id`, `bid_px_int`, `ask_px_int` |
| `derivative_ticker` | Silver | `exchange`, `date` | `ts_local_us`, `symbol_id`, `mark_px_int`, `funding_rate` |
| `liquidations` | Silver | `exchange`, `date` | `ts_local_us`, `symbol_id`, `price_int`, `qty_int` |
| `options_chain` | Silver | `exchange`, `date` | `ts_local_us`, `underlying_symbol_id`, `option_symbol_id` |
| `bars_1m` | Gold | `exchange`, `date` | `ts_bucket_start_us`, `symbol_id`, OHLCV |
| `book_snapshot_25_wide` | Gold | `exchange`, `date` | Wide format for legacy tools |
| `tob_quotes` | Gold | `exchange`, `date` | Fast path for top-of-book |
| `options_surface_grid` | Gold | `exchange`, `date` | Time-gridded options surface |

---

For design rationale, ETL pipeline details, and architecture decisions, see [Architecture Design](architecture/design.md).
