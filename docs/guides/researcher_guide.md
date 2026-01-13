# Researcher's Guide: High-Performance HFT Data Lake

This guide is for **Quantitative Researchers** and **LLM Agents** working with the Tardis market data lake.

## 1. Introduction

This data lake is designed for **Point-in-Time (PIT) accuracy** and **query speed**.
-   **PIT Correctness:** All data is indexed by `ts_local_us` (arrival time). Backtests replaying this timeline will see data exactly as it was known to the trading system, avoiding lookahead bias.
-   **Storage:** Data is stored in Delta Lake (Parquet) with Z-Ordering, optimized for fast retrieval by `(exchange, date, symbol)`.

## 2. Quick Start

**Lake root:** Resolved in this order (see `pointline/config.py`):
1. `LAKE_ROOT` environment variable
2. User config file at `~/.config/pointline/config.toml`
3. Default `~/data/lake`

**Config file example:**
```toml
lake_root = "/mnt/pointline/lake"
```

**CLI:**
```bash
pointline config show
pointline config set --lake-root /mnt/pointline/lake
```

### 2.1 Discover Symbols
Before querying data, find the correct `symbol_id`. This ID is the **primary key** for all research operations.

**CLI:**
```bash
pointline symbol search "BTC-PERPETUAL" --exchange deribit
```

**Python:**
```python
from pointline import registry
import polars as pl

# Find symbols matching criteria
df = registry.find_symbol("BTC-PERPETUAL", exchange="deribit")
print(df)

# Pick the ID that covers your time range (check valid_from_ts/valid_until_ts)
symbol_id = df["symbol_id"][0]
```

**Time-range selection (SCD-safe):**
```python
from datetime import datetime, timezone
import polars as pl
from pointline import registry

as_of = datetime(2025, 12, 28, tzinfo=timezone.utc)
as_of_us = int(as_of.timestamp() * 1_000_000)

df = registry.find_symbol("BTC-PERPETUAL", exchange="deribit")
active = df.filter(
    (pl.col("valid_from_ts") <= as_of_us) & (pl.col("valid_until_ts") > as_of_us)
)
print(active)
```

### 2.2 DuckDB (Ad-hoc SQL)
Once you have the `symbol_id` (e.g., `101`), query the tables directly.

```sql
SELECT
    ts_local_us,
    side,
    price_int,
    qty_int
FROM delta_scan('${LAKE_ROOT}/silver/trades')
WHERE date = '2025-12-28'
  AND symbol_id = 101 -- Use the ID found in step 2.1
ORDER BY ts_local_us;
```

### 2.3 Polars (Python Pipelines)
Use the `pointline.research` helpers for streamlined access. They automatically handle partition pruning.

```python
from pointline import research

# Load trades for a specific symbol ID
# The system automatically resolves the exchange partition for optimization
trades = research.load_trades(
    symbol_id=101,
    start_date="2025-12-28",
    end_date="2025-12-29"
)

print(trades)
```

## 3. Data Lake Layout

### 3.1 Directory structure
- **Bronze:** raw vendor files + ingestion manifest
- **Silver:** canonical tables (research foundation)
- **Gold:** derived tables for convenience (optional)

**Partitioning:** Most Silver tables are partitioned by `exchange` and `date` (with `date`
derived from `ts_local_us` in UTC). `silver.l2_updates` additionally partitions by
`symbol_id` to preserve replay ordering without a global sort.
Example:
`${LAKE_ROOT}/silver/trades/exchange=binance/date=2025-12-28/part-*.parquet`

### 3.2 Table catalog (Silver)
| Table | Path | Partitions | Key columns |
|---|---|---|---|
| trades | `${LAKE_ROOT}/silver/trades` | `exchange`, `date` | `ts_local_us`, `symbol_id`, `price_int`, `qty_int` |
| quotes | `${LAKE_ROOT}/silver/quotes` | `exchange`, `date` | `ts_local_us`, `symbol_id`, `bid_px_int`, `ask_px_int` |
| book_snapshot_25 | `${LAKE_ROOT}/silver/book_snapshot_25` | `exchange`, `date` | `ts_local_us`, `symbol_id`, `bids_px`, `asks_px` |
| l2_updates | `${LAKE_ROOT}/silver/l2_updates` | `exchange`, `date`, `symbol_id` | `ts_local_us`, `symbol_id`, `side`, `price_int`, `size_int` |
| dim_symbol | `${LAKE_ROOT}/silver/dim_symbol` | none | `symbol_id`, `exchange_id`, `exchange_symbol`, validity range |
| ingest_manifest | `${LAKE_ROOT}/silver/ingest_manifest` | none | `exchange`, `data_type`, `date`, `status` |

**For complete schema definitions, see [Schema Reference](../schemas.md).**

### 3.3 Table catalog (Gold)
- `gold.book_snapshot_25_wide` (legacy wide format)
- `gold.tob_quotes` (top-of-book)
- Other derived tables are reproducible from Silver.

## 4. Access Patterns

DuckDB is the recommended tool for interactive exploration. Polars is ideal for building pipelines.

**Partition-first rule:** always filter by `date` to avoid full scans (for tables that include a `date` column). When using `pointline.research` or `l2_replay` APIs with `symbol_id`, exchange partitioning is handled automatically.

### 4.1 Check Data Availability (Manifest)
Before running a heavy query, check which dates are present:
```python
import polars as pl
from pointline.config import get_table_path

manifest = pl.read_delta(str(get_table_path("ingest_manifest")))
available = (
    manifest
    .filter((pl.col("exchange") == "deribit") & (pl.col("data_type") == "trades"))
    .select("date")
    .unique()
    .sort("date")
)
print(available)
```

## 5. Core Concepts

### 5.1 Time: `ts_local_us` vs `ts_exch_us`
-   **`ts_local_us` (Local Time):** The timestamp when the data arrived at the recording server. **ALWAYS use this for backtesting replay.**
-   **`ts_exch_us` (Exchange Time):** The matching engine timestamp. Use this only for latency analysis (e.g., `ts_local - ts_exch`).

### 5.2 Symbol Resolution (SCD Type 2)
Symbols change (e.g., renames). We use a stable `symbol_id`.
-   **Table:** `silver.dim_symbol`
-   **Logic:** A symbol is valid for a specific time range `[valid_from_ts, valid_until_ts)`.
-   **Resolution:** To find the correct `symbol_id` for "BTC-PERPETUAL" at time `T`, use `pointline.registry.find_symbol()` or query `dim_symbol` directly.

### 5.2.1 Exchange ID Selection
`exchange_id` is the stable numeric ID used for joins and filters. It is defined in `pointline/config.py`.
While `symbol_id` is now the primary access key, `exchange_id` is still useful for aggregate analysis (e.g., "all volume on Binance").

**Example (derive `exchange_id` from a resolved symbol):**
```python
from pointline import registry

df = registry.find_symbol("BTC-PERPETUAL", exchange="deribit")
exchange_id = df["exchange_id"][0]
```

### 5.3 Fixed-Point Math
To save space and ensure precision, prices and quantities are stored as integers (`i64`).
-   **Storage:** `price_int`, `qty_int`
-   **Metadata:** `price_increment`, `amount_increment` (from `dim_symbol`)
-   **Conversion:** `real_price = price_int * price_increment`

**For detailed encoding explanation, see [Schema Reference - Fixed-Point Encoding](../schemas.md#fixed-point-encoding).**

#### 5.3.1 Decoding to Floats (Explicit)
Decoding is **explicit** to avoid silently changing semantics. Use the domain helpers:

```python
from pointline import research
from pointline.trades import decode_fixed_point as decode_trades
from pointline.quotes import decode_fixed_point as decode_quotes
from pointline.book_snapshots import decode_fixed_point as decode_books
from pointline.config import get_table_path
import polars as pl

dim_symbol = pl.read_delta(str(get_table_path("dim_symbol"))).select(
    ["symbol_id", "price_increment", "amount_increment"]
)

trades = research.load_trades(symbol_id=101, start_date="2025-12-28")
trades = decode_trades(trades, dim_symbol)  # drops *_int, outputs Float64
```

**Minimal end-to-end example (real prices and sizes):**
```python
from pointline import research
from pointline.trades import decode_fixed_point as decode_trades
from pointline.config import get_table_path
import polars as pl

dim_symbol = pl.read_delta(str(get_table_path("dim_symbol"))).select(
    ["symbol_id", "price_increment", "amount_increment"]
)

trades = research.load_trades(symbol_id=101, start_date="2025-12-28", end_date="2025-12-28")
trades = decode_trades(trades, dim_symbol)
print(trades.select(["ts_local_us", "price", "qty"]).head(5))
```

## 6. Common Workflows

### 6.1 Join Trades with Quotes (As-Of Join)
To get the effective spread at the time of a trade:

```python
# Load trades and quotes
trades = research.load_trades(symbol_id=101, start_date="2025-01-01").sort("ts_local_us")
quotes = research.load_quotes(symbol_id=101, start_date="2025-01-01").sort("ts_local_us")

# Join
pit_data = trades.join_asof(
    quotes,
    on="ts_local_us",
    by=["exchange_id", "symbol_id"],
    strategy="backward" # Get the last quote known BEFORE or AT trade time
)
```

### 6.2 Reconstruct Order Book
To get the book state at time `T`:
1.  Find the snapshot with max `ts_local_us <= T`.
2.  Apply all `l2_updates` where `snapshot_ts < ts_local_us <= T`.

### 6.3 L2 Replay (Researcher Usage)
Use the high-level L2 replay APIs. They automatically resolve metadata from the `symbol_id`.

```python
from pointline import l2_replay

# Get a full depth snapshot at a specific time
snapshot = l2_replay.snapshot_at(
    symbol_id=101,
    ts_local_us=1700000000000000,
)

# Replay updates over a window
df = l2_replay.replay_between(
    symbol_id=101,
    start_ts_local_us=1700000000000000,
    end_ts_local_us=1700003600000000,
    every_us=1_000_000,
)
# df is a polars.DataFrame containing PIT snapshots
```

## 7. Researcher Interface (Conventions)

### 7.1 Exchange and Symbol Identity
- **symbol_id (i64):** The **Primary Key** for research. It uniquely identifies a specific version of a symbol (with specific tick size, etc.) valid for a specific time range. Use `pointline.registry` or CLI `symbol search` to find it.
- **exchange (string):** vendor name, used for partitioning. Auto-resolved by APIs when `symbol_id` is provided.
- **exchange_id (i16):** stable numeric mapping used for joins.

**Symbol name convenience:** you can provide `exchange` + `symbol` to `pointline.research` loaders.  
If the symbol changed over time, the loader returns the **union of all matching `symbol_id` values** in the date range.
If `exchange` is omitted, the lookup spans all exchanges and may return multiple IDs.

### 7.2 Safe query template (DuckDB)
```sql
-- 1. Find your ID first
-- 2. Query with ID
SELECT *
FROM delta_scan('${LAKE_ROOT}/silver/<table_name>')
WHERE date >= '<start_date>' AND date <= '<end_date>'
  AND symbol_id = <your_id>
LIMIT 100;
```

**DuckDB setup note:**
If `delta_scan` is unavailable, install and load the Delta extension in DuckDB:
```sql
INSTALL delta;
LOAD delta;
```

### 7.3 Polars best practices
- Use `pointline.research` helpers (`load_trades`, `scan_table`) instead of raw `pl.read_delta` where possible.
- Provide `symbol_id` to ensure optimal partition pruning.
- Keep joins on `exchange_id` + `symbol_id`.
- Only pass `start_date`/`end_date` to tables that include a `date` column (e.g., trades/quotes/l2_updates); others will raise.

### 7.4 Common Mistakes
- Using `ts_exch_us` for backtesting instead of `ts_local_us`.
- Forgetting to filter by `date` and scanning full tables.
- Treating `price_int` / `qty_int` as real values without decoding.

## 8. Agent Interface

**Instructions for LLMs:**
When writing code for this data lake, follow these rules:

1.  **Symbol Discovery:** Always start by finding the correct `symbol_id` using `pointline.registry.find_symbol()`.
2.  **Python Access:** Use `pointline.research` helpers (`scan_table`, `load_trades`, etc.) passing `symbol_id`.
3.  **L2 Replay:** Use `pointline.l2_replay.snapshot_at(symbol_id=...)` for book reconstruction.
4.  **Schema Awareness:** See [Schema Reference](../schemas.md) for complete table schemas.
5.  **Joins:** Always use `join_asof` on `ts_local_us`.

**Safe Query Template (DuckDB):**
```sql
SELECT * 
FROM delta_scan('/lake/silver/<table_name>')
WHERE date >= '<start_date>' AND date <= '<end_date>'
  AND symbol_id = <id>
LIMIT 100;
```
