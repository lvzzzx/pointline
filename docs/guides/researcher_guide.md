# Researcher's Guide: High-Performance HFT Data Lake

This guide is for **Quantitative Researchers** and **LLM Agents** working with the Tardis market data lake.

## 1. Introduction

This data lake is designed for **Point-in-Time (PIT) accuracy** and **query speed**.
-   **PIT Correctness:** All data is indexed by `ts_local_us` (arrival time). Backtests replaying this timeline will see data exactly as it was known to the trading system, avoiding lookahead bias.
-   **Storage:** Data is stored in Delta Lake (Parquet) with Z-Ordering, optimized for fast retrieval by `(exchange, date, symbol)`.

## 2. Quick Start

**Lake root:** The base path is controlled by `LAKE_ROOT` (see `pointline/config.py`). If unset, it defaults to `./data/lake`.

### 2.1 DuckDB (Ad-hoc SQL)
```sql
WITH resolved AS (
  SELECT symbol_id
  FROM delta_scan('${LAKE_ROOT}/silver/dim_symbol')
  WHERE exchange_id = 21
    AND exchange_symbol = 'BTC-PERPETUAL'
    AND valid_from_ts <= 1766923200000000
    AND valid_until_ts > 1766923200000000
)
SELECT
    ts_local_us,
    side,
    price_int,
    qty_int
FROM delta_scan('${LAKE_ROOT}/silver/trades')
WHERE date = '2025-12-28'
  AND exchange_id = 21
  AND symbol_id = (SELECT symbol_id FROM resolved)
ORDER BY ts_local_us;
```

### 2.2 Polars (Python Pipelines)
```python
import os
from datetime import datetime, timezone
import polars as pl

lake_root = os.getenv("LAKE_ROOT", "./data/lake")
asof = datetime(2025, 12, 28, 12, 0, 0, tzinfo=timezone.utc)
asof_us = int(asof.timestamp() * 1_000_000)
symbol = "BTC-PERPETUAL"

dim_symbol = pl.read_delta(f"{lake_root}/silver/dim_symbol", version=None)
symbol_id = (
    dim_symbol.filter(
        (pl.col("exchange_id") == 21)
        & (pl.col("exchange_symbol") == symbol)
        & (pl.col("valid_from_ts") <= asof_us)
        & (pl.col("valid_until_ts") > asof_us)
    )
    .select("symbol_id")
    .collect()
    .item()
)

trades_lf = pl.read_delta(f"{lake_root}/silver/trades", version=None)
daily_vol = (
    trades_lf
    .filter((pl.col("date") == "2025-12-28") & (pl.col("symbol_id") == symbol_id))
    .group_by("symbol_id")
    .agg([
        pl.len().alias("trade_count"),
        pl.col("qty_int").sum().alias("total_volume"),
    ])
    .collect()
)
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
`/lake/silver/trades/exchange=binance/date=2025-12-28/part-*.parquet`

### 3.2 Table catalog (Silver)
| Table | Path | Partitions | Key columns |
|---|---|---|---|
| trades | `/lake/silver/trades` | `exchange`, `date` | `ts_local_us`, `symbol_id`, `price_int`, `qty_int` |
| quotes | `/lake/silver/quotes` | `exchange`, `date` | `ts_local_us`, `symbol_id`, `bid_px_int`, `ask_px_int` |
| book_snapshot_25 | `/lake/silver/book_snapshot_25` | `exchange`, `date` | `ts_local_us`, `symbol_id`, `bids_px`, `asks_px` |
| l2_updates | `/lake/silver/l2_updates` | `exchange`, `date`, `symbol_id` | `ts_local_us`, `symbol_id`, `side`, `price_int`, `size_int` |
| dim_symbol | `/lake/silver/dim_symbol` | none | `symbol_id`, `exchange_id`, `exchange_symbol`, validity range |
| ingest_manifest | `/lake/silver/ingest_manifest` | none | `exchange`, `data_type`, `date`, `status` |

**For complete schema definitions, see [Schema Reference](../schemas.md).**

### 3.3 Table catalog (Gold)
- `gold.book_snapshot_25_wide` (legacy wide format)
- `gold.tob_quotes` (top-of-book)
- Other derived tables are reproducible from Silver.

## 4. Access Patterns

DuckDB is the recommended tool for interactive exploration. Polars is ideal for building pipelines.

**Partition-first rule:** always filter by `date` (and `exchange_id` if possible) to avoid full scans.

## 5. Core Concepts

### 5.1 Time: `ts_local_us` vs `ts_exch_us`
-   **`ts_local_us` (Local Time):** The timestamp when the data arrived at the recording server. **ALWAYS use this for backtesting replay.**
-   **`ts_exch_us` (Exchange Time):** The matching engine timestamp. Use this only for latency analysis (e.g., `ts_local - ts_exch`).

### 5.2 Symbol Resolution (SCD Type 2)
Symbols change (e.g., renames). We use a stable `symbol_id`.
-   **Table:** `silver.dim_symbol`
-   **Logic:** A symbol is valid for a specific time range `[valid_from_ts, valid_until_ts)`.
-   **Resolution:** To find the correct `symbol_id` for "BTC-PERPETUAL" at time `T`, query `dim_symbol` where `exchange_symbol = 'BTC-PERPETUAL'` AND `valid_from_ts <= T < valid_until_ts`.

### 5.2.1 Exchange ID Selection
`exchange_id` is the stable numeric ID used for joins and filters. It is defined in `pointline/config.py`:
-   **Source of truth:** `EXCHANGE_MAP` and `get_exchange_id()`.
-   **Normalization:** exchange names are lowercased and trimmed before lookup.
-   **If missing:** add a new entry to `EXCHANGE_MAP` with a new stable ID.

```python
from pointline.config import get_exchange_id

exchange_id = get_exchange_id("deribit")  # 21
```

### 5.3 Fixed-Point Math
To save space and ensure precision, prices and quantities are stored as integers (`i64`).
-   **Storage:** `price_int`, `qty_int`
-   **Metadata:** `price_increment`, `amount_increment` (from `dim_symbol`)
-   **Conversion:** `real_price = price_int * price_increment`

**For detailed encoding explanation, see [Schema Reference - Fixed-Point Encoding](../schemas.md#fixed-point-encoding).**

## 6. Common Workflows

### 6.1 Join Trades with Quotes (As-Of Join)
To get the effective spread at the time of a trade:

```python
# Load trades and quotes
trades = pl.read_delta("/lake/silver/trades").sort("ts_local_us")
quotes = pl.read_delta("/lake/silver/quotes").sort("ts_local_us")

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
Use the high-level L2 replay APIs; they hide batch management and input scans. The Rust
engine reads Delta directly; Python is a thin wrapper.

```python
import l2_replay

snapshot = l2_replay.snapshot_at(
    exchange_id=21,
    symbol_id=1234,
    ts_local_us=1700000000000000,
)

for snap in l2_replay.replay_between(
    exchange_id=21,
    symbol_id=1234,
    start_ts_local_us=1700000000000000,
    end_ts_local_us=1700003600000000,
    every_us=1_000_000,
):
    ...
```

## 7. Researcher Interface (Conventions)

### 7.1 Exchange and Symbol Identity
- **exchange (string):** vendor name from Tardis, used for partitioning.
- **exchange_id (i16):** stable numeric mapping (`EXCHANGE_MAP` in `pointline/config.py`) used for joins.
- **symbol_id (i64):** stable identifier from `silver.dim_symbol` (SCD Type 2).

### 7.2 Safe query template (DuckDB)
```sql
SELECT *
FROM delta_scan('/lake/silver/<table_name>')
WHERE date >= '<start_date>' AND date <= '<end_date>'
  AND exchange_id = <id>
  AND symbol_id = <id>
LIMIT 100;
```

### 7.3 Polars best practices
- Use `pl.read_delta` or `pl.scan_delta` with filters on `date`.
- Keep joins on `exchange_id` + `symbol_id`.
- Convert fixed-point to real values only at the end of a pipeline.

## 8. Agent Interface

**Instructions for LLMs:**
When writing code for this data lake, follow these rules:

1.  **Python Access:** Prefer using `pointline.research` helpers (`scan_table`, `load_trades`, etc.) instead of raw `pl.read_delta`.
2.  **Schema Awareness:** See [Schema Reference](../schemas.md) for complete table schemas.
3.  **Column Names:**
    -   Timestamp: `ts_local_us` (int64)
    -   Price: `price_int` (int64)
    -   Quantity: `qty_int` (int64)
    -   IDs: `exchange_id` (u16), `symbol_id` (u32)
4.  **Joins:** Always use `join_asof` on `ts_local_us` for merging asynchronous streams.
5.  **Partitioning:** Always filter by `date` partition first to avoid scanning the entire lake.

**Safe Query Template (DuckDB):**
```sql
SELECT * 
FROM delta_scan('/lake/silver/<table_name>')
WHERE date >= '<start_date>' AND date <= '<end_date>'
  AND symbol_id = <id>
LIMIT 100;
```
