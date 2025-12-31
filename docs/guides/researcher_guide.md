# Researcher's Guide: High-Performance HFT Data Lake

This guide is for **Quantitative Researchers** and **LLM Agents** working with the Tardis market data lake.

## 1. Introduction

This data lake is designed for **Point-in-Time (PIT) accuracy** and **query speed**.
-   **PIT Correctness:** All data is indexed by `ts_local_us` (arrival time). Backtests replaying this timeline will see data exactly as it was known to the trading system, avoiding lookahead bias.
-   **Storage:** Data is stored in Delta Lake (Parquet) with Z-Ordering, optimized for fast retrieval by `(exchange, date, symbol)`.

## 2. Access Patterns

### 2.1 DuckDB (Ad-hoc SQL)
DuckDB is the recommended tool for interactive exploration.

```sql
-- Query trades for a specific symbol on a specific day
SELECT 
    ts_local_us, 
    side, 
    price_int, 
    qty_int 
FROM delta_scan('/lake/silver/trades')
WHERE date = '2025-12-28' 
  AND exchange_id = 1 
  AND symbol_id = 3019004731 -- Use resolved symbol_id
ORDER BY ts_local_us;
```

### 2.2 Polars (Python Pipelines)
Polars is ideal for building high-performance feature pipelines.

```python
import polars as pl

# Lazy load trades
trades_lf = pl.read_delta("/lake/silver/trades", version=None)

# Filter and aggregate
daily_vol = (
    trades_lf
    .filter(pl.col("date") == "2025-12-28")
    .group_by("symbol_id")
    .agg([
        pl.count().alias("trade_count"),
        pl.col("qty_int").sum().alias("total_volume")
    ])
    .collect()
)
```

## 3. Core Concepts

### 3.1 Time: `ts_local_us` vs `ts_exch_us`
-   **`ts_local_us` (Local Time):** The timestamp when the data arrived at the recording server. **ALWAYS use this for backtesting replay.**
-   **`ts_exch_us` (Exchange Time):** The matching engine timestamp. Use this only for latency analysis (e.g., `ts_local - ts_exch`).

### 3.2 Symbol Resolution (SCD Type 2)
Symbols change (e.g., renames). We use a stable `symbol_id`.
-   **Table:** `silver.dim_symbol`
-   **Logic:** A symbol is valid for a specific time range `[valid_from_ts, valid_until_ts)`.
-   **Resolution:** To find the correct `symbol_id` for "BTC-PERPETUAL" at time `T`, query `dim_symbol` where `exchange_symbol = 'BTC-PERPETUAL'` AND `valid_from_ts <= T < valid_until_ts`.

### 3.3 Fixed-Point Math
To save space and ensure precision, prices and quantities are stored as integers (`i64`).
-   **Storage:** `price_int`, `qty_int`
-   **Metadata:** `price_increment`, `amount_increment` (from `dim_symbol`)
-   **Conversion:** `real_price = price_int * price_increment`

## 4. Common Workflows

### 4.1 Join Trades with Quotes (As-Of Join)
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

### 4.2 Reconstruct Order Book
To get the book state at time `T`:
1.  Find the snapshot with max `ts_local_us <= T`.
2.  Apply all `l2_updates` where `snapshot_ts < ts_local_us <= T`.

## 5. Agent Interface

**Instructions for LLMs:**
When writing code for this data lake, follow these rules:

1.  **Schema Awareness:** Assume standard Silver schemas (see `docs/architecture/design.md`).
2.  **Column Names:**
    -   Timestamp: `ts_local_us` (int64)
    -   Price: `price_int` (int64)
    -   Quantity: `qty_int` (int64)
    -   IDs: `exchange_id` (u16), `symbol_id` (u32)
3.  **Joins:** Always use `join_asof` on `ts_local_us` for merging asynchronous streams.
4.  **Partitioning:** Always filter by `date` partition first to avoid scanning the entire lake.

**Safe Query Template (DuckDB):**
```sql
SELECT * 
FROM delta_scan('/lake/silver/<table_name>')
WHERE date >= '<start_date>' AND date <= '<end_date>'
  AND symbol_id = <id>
LIMIT 100;
```
