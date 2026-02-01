# Researcher's Guide: High-Performance HFT Data Lake

This guide is for **Quantitative Researchers** and **LLM Agents** working with the Pointline market data lake.

---

## 1. Introduction

This data lake is designed for **Point-in-Time (PIT) accuracy** and **query speed**.
- **PIT Correctness:** All data is indexed by `ts_local_us` (arrival time). Backtests replaying this timeline will see data exactly as it was known to the trading system, avoiding lookahead bias.
- **Storage:** Data is stored in Delta Lake (Parquet) with Z-Ordering, optimized for fast retrieval by `(exchange, date, symbol)`.

---

## 2. Quick Start (5 Minutes)

### 2.1 Installation

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

### 2.2 Discover and Load Data (Recommended Workflow)

**Step 1: Discover available data**
```python
from pointline import research

# What exchanges have data?
exchanges = research.list_exchanges(asset_class="crypto-derivatives")

# What symbols are available?
symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")

# Check data coverage
coverage = research.data_coverage("binance-futures", "BTCUSDT")
print(f"Trades available: {coverage['trades']['available']}")
```

**Step 2: Load data with query API**
```python
from pointline.research import query

# Load trades (automatic symbol resolution + decoding)
trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,  # Human-readable float prices
)

print(f"Loaded {trades.height:,} trades")
```

**Step 3: Explore and analyze**
```python
import polars as pl

# Calculate VWAP
vwap = trades.select([
    (pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()
]).item()

print(f"VWAP: ${vwap:.2f}")
```

**That's it!** For 90% of research tasks, this is all you need.

**For production research:** See [Section 7: Advanced Topics](#7-advanced-topics-core-api) for the core API with explicit symbol_id control.

---

## 3. Data Lake Layout

### 3.1 Directory structure
- **Bronze:** raw vendor files (vendor-first layout)
- **Silver:** canonical tables (research foundation)
- **Gold:** derived tables for convenience (optional)

**Partitioning:** Most Silver tables are partitioned by `exchange` and `date` (with `date`
derived from `ts_local_us` in the exchange's local timezone). The planned `silver.l2_updates` table would additionally partition by
`symbol_id` to preserve replay ordering without a global sort.

Example:
`${LAKE_ROOT}/silver/trades/exchange=binance-futures/date=2024-05-01/part-*.parquet`

### 3.2 Table catalog (Silver)
| Table | Path | Partitions | Key columns |
|---|---|---|---|
| trades | `${LAKE_ROOT}/silver/trades` | `exchange`, `date` | `ts_local_us`, `symbol_id`, `price_int`, `qty_int` |
| quotes | `${LAKE_ROOT}/silver/quotes` | `exchange`, `date` | `ts_local_us`, `symbol_id`, `bid_px_int`, `ask_px_int` |
| book_snapshot_25 | `${LAKE_ROOT}/silver/book_snapshot_25` | `exchange`, `date` | `ts_local_us`, `symbol_id`, `bids_px`, `asks_px` |
| l2_updates ⚠️ | `${LAKE_ROOT}/silver/l2_updates` | `exchange`, `date`, `symbol_id` | ⚠️ **Planned** - Not yet implemented |
| dim_symbol | `${LAKE_ROOT}/silver/dim_symbol` | none | `symbol_id`, `exchange_id`, `exchange_symbol`, validity range |
| ingest_manifest | `${LAKE_ROOT}/silver/ingest_manifest` | none | `vendor`, `exchange`, `data_type`, `date`, `status` |

**For complete schema definitions, see [Schema Reference](../schemas.md).**

### 3.3 Table catalog (Gold)
- `gold.book_snapshot_25_wide` (legacy wide format)
- `gold.tob_quotes` (top-of-book)
- Other derived tables are reproducible from Silver.

---

## 4. Query API (Recommended)

### 4.1 Overview

The **query API** is designed for exploration, prototyping, and interactive analysis. It provides:
- Automatic symbol resolution
- Human-friendly timestamps (ISO strings, datetime objects)
- Automatic decoding to float prices (with `decoded=True`)
- One-liner data loading

**Use this for:**
- ✅ Exploration and prototyping
- ✅ Jupyter notebooks
- ✅ Quick data checks
- ✅ Teaching and examples
- ✅ LLM agent workflows

### 4.2 Loading Data

#### Trades
```python
from pointline.research import query

trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,
)
```

#### Quotes
```python
quotes = query.quotes(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,
)
```

#### Book Snapshots
```python
book = query.book_snapshot_25(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,
)
```

### 4.3 Timestamp Flexibility

The query API accepts multiple timestamp formats:

```python
# ISO strings (simplest)
trades = query.trades(..., start="2024-05-01", end="2024-05-02")

# Datetime objects
from datetime import datetime, timezone
trades = query.trades(
    ...,
    start=datetime(2024, 5, 1, tzinfo=timezone.utc),
    end=datetime(2024, 5, 2, tzinfo=timezone.utc),
)

# Microsecond timestamps (if you prefer)
trades = query.trades(..., start=1714521600000000, end=1714608000000000)
```

### 4.4 Lazy Evaluation for Large Datasets

```python
# Load as LazyFrame (default)
trades_lf = query.trades(
    "binance-futures",
    "BTCUSDT",
    "2024-05-01",
    "2024-09-30",
    lazy=True,  # Default
    decoded=True,
)

# Filter while still lazy
large_trades = trades_lf.filter(pl.col("qty") > 1.0)

# Aggregate before collecting
hourly = large_trades.group_by_dynamic("ts_local_us", every="1h").agg([
    pl.col("price").mean(),
    pl.col("qty").sum(),
])

# Only materialize aggregated results
df = hourly.collect()
```

---

## 5. Core Concepts

### 5.1 Time: `ts_local_us` vs `ts_exch_us`
- **`ts_local_us` (Local Time):** The timestamp when the data arrived at the recording server. **ALWAYS use this for backtesting replay.**
- **`ts_exch_us` (Exchange Time):** The matching engine timestamp. Use this only for latency analysis (e.g., `ts_local - ts_exch`).
- **Time range filters:** `start`/`end` parameters use **[start, end)** semantics and default to `ts_local_us` (set `ts_col="ts_exch_us"` for latency work).

### 5.2 Symbol Resolution (SCD Type 2)
Symbols change (e.g., renames, tick size updates). We use a stable `symbol_id`.
- **Table:** `silver.dim_symbol`
- **Logic:** A symbol is valid for a specific time range `[valid_from_ts, valid_until_ts)`.
- **Query API:** Automatically handles symbol resolution and warns on metadata changes
- **Core API:** Explicit resolution via `pointline.registry.find_symbol()`

### 5.3 Fixed-Point Math
To save space and ensure precision, prices and quantities are stored as integers (`i64`).
- **Storage:** `price_int`, `qty_int`
- **Metadata:** `price_increment`, `amount_increment` (from `dim_symbol`)
- **Conversion:** `real_price = price_int * price_increment`

**Query API:** Use `decoded=True` to get float columns automatically

**Core API:** Manual decoding required (see [Section 7.3](#73-fixed-point-decoding))

---

## 6. Common Workflows

### 6.1 Join Trades with Quotes (As-Of Join)
To get the effective spread at the time of a trade:

```python
from pointline.research import query
import polars as pl

# Load trades and quotes
trades = query.trades(
    "binance-futures",
    "BTCUSDT",
    "2024-05-01",
    "2024-05-02",
    decoded=True,
).sort("ts_local_us")

quotes = query.quotes(
    "binance-futures",
    "BTCUSDT",
    "2024-05-01",
    "2024-05-02",
    decoded=True,
).sort("ts_local_us")

# Join
pit_data = trades.join_asof(
    quotes,
    on="ts_local_us",
    by=["exchange_id", "symbol_id"],
    strategy="backward"  # Get the last quote known BEFORE or AT trade time
)
```

### 6.2 Calculate VWAP

```python
import polars as pl

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

vwap = trades.select([
    (pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()
]).item()

print(f"VWAP: ${vwap:.2f}")
```

### 6.3 Aggregate to Bars

```python
# 1-minute OHLCV bars
bars = trades.group_by_dynamic("ts_local_us", every="1m").agg([
    pl.col("price").first().alias("open"),
    pl.col("price").max().alias("high"),
    pl.col("price").min().alias("low"),
    pl.col("price").last().alias("close"),
    pl.col("qty").sum().alias("volume"),
])
```

### 6.4 Compute Spread

```python
book = query.book_snapshot_25("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Extract best bid/ask
features = book.select([
    "ts_local_us",
    pl.col("bids_px").list.get(0).alias("best_bid"),
    pl.col("asks_px").list.get(0).alias("best_ask"),
]).with_columns([
    (pl.col("best_ask") - pl.col("best_bid")).alias("spread")
])
```

### 6.5 Multi-Table Analysis

```python
# Load multiple data types
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
book = query.book_snapshot_25("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Combine and analyze
# ... (your analysis code)
```

---

## 7. Advanced Topics (Core API)

### 7.1 When to Use Core API

The **core API** provides explicit control over symbol resolution and is designed for production research.

**Use core API when:**
- ✅ Production research requiring reproducibility
- ✅ You need to log exact symbol_ids used
- ✅ Performance-critical queries needing optimization
- ✅ Handling SCD Type 2 symbol changes explicitly
- ✅ Multi-symbol queries with complex filtering

**For most research, use the query API instead (see [Section 4](#4-query-api-recommended)).**

### 7.2 Core API: Symbol Resolution

**Step 1: Find symbol_id**
```python
from pointline import registry
import polars as pl

# Find symbols matching criteria
df = registry.find_symbol("BTC-PERPETUAL", exchange="deribit")
print(df)

# Pick the ID that covers your time range
symbol_id = df["symbol_id"][0]
```

**Step 2: Time-range selection (SCD-safe)**
```python
from datetime import datetime, timezone

as_of = datetime(2025, 12, 28, tzinfo=timezone.utc)
as_of_us = int(as_of.timestamp() * 1_000_000)

df = registry.find_symbol("BTC-PERPETUAL", exchange="deribit")
active = df.filter(
    (pl.col("valid_from_ts") <= as_of_us) & (pl.col("valid_until_ts") > as_of_us)
)
print(active)
```

**Step 3: Load with explicit symbol_ids**
```python
from pointline import research

start_ts_us = 1700000000000000
end_ts_us = 1700003600000000

df = registry.find_symbol("BTC-PERPETUAL", exchange="deribit")
active = df.filter(
    (pl.col("valid_from_ts") < end_ts_us) & (pl.col("valid_until_ts") > start_ts_us)
)
symbol_ids = active["symbol_id"].to_list()

trades = research.load_trades(
    symbol_id=symbol_ids,
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
)
```

### 7.3 Fixed-Point Decoding

Decoding is **explicit** in the core API to avoid silently changing semantics. Use the domain helpers:

```python
from pointline import research
from pointline.tables.trades import decode_fixed_point as decode_trades
from pointline.config import get_table_path
import polars as pl

dim_symbol = pl.read_delta(str(get_table_path("dim_symbol"))).select(
    ["symbol_id", "price_increment", "amount_increment"]
)

trades = research.load_trades(
    symbol_id=101,
    start_ts_us=1700000000000000,
    end_ts_us=1700003600000000,
)
trades = decode_trades(trades, dim_symbol)  # drops *_int, outputs Float64
```

**Convenience loaders (decoded by default):**
```python
from pointline import research

trades = research.load_trades_decoded(
    symbol_id=101,
    start_ts_us=1700000000000000,
    end_ts_us=1700003600000000,
)
```

### 7.4 DuckDB (Ad-hoc SQL)

Once you have the `symbol_id` (e.g., `101`), query the tables directly.

```sql
SELECT
    ts_local_us,
    side,
    price_int,
    qty_int
FROM delta_scan('${LAKE_ROOT}/silver/trades')
WHERE date >= '2024-05-01' AND date <= '2024-05-02'
  AND symbol_id = 101  -- Use the ID found via registry
  AND ts_local_us >= <start_ts_us>  -- [start, end)
  AND ts_local_us <  <end_ts_us>
ORDER BY ts_local_us;
```

**DuckDB setup note:**
If `delta_scan` is unavailable, install and load the Delta extension in DuckDB:
```sql
INSTALL delta;
LOAD delta;
```

### 7.5 Researcher Interface Conventions

#### Exchange and Symbol Identity
- **symbol_id (i64):** The **Primary Key** for research. It uniquely identifies a specific version of a symbol (with specific tick size, etc.) valid for a specific time range.
- **exchange (string):** vendor name, used for partitioning. Auto-resolved by APIs when `symbol_id` is provided.
- **exchange_id (i16):** stable numeric mapping used for joins.

#### Safe query template (DuckDB)
```sql
-- 1. Find your ID first
-- 2. Query with ID
SELECT *
FROM delta_scan('${LAKE_ROOT}/silver/<table_name>')
WHERE date >= '<start_date>' AND date <= '<end_date>'
  AND symbol_id = <your_id>
  AND ts_local_us >= <start_ts_us>
  AND ts_local_us <  <end_ts_us>
LIMIT 100;
```

#### Polars best practices
- Use `pointline.research` helpers (`load_trades`, `scan_table`) instead of raw `pl.read_delta` where possible.
- Provide `symbol_id` to ensure optimal partition pruning.
- Keep joins on `exchange_id` + `symbol_id`.
- `start_ts_us`/`end_ts_us` are required; date partitions are derived implicitly.

### 7.6 Common Mistakes
- Using `ts_exch_us` for backtesting instead of `ts_local_us`.
- Forgetting to filter by time (or date) and scanning full tables.
- Treating `price_int` / `qty_int` as real values without decoding.

---

## 8. LLM Agent Interface

**Instructions for LLMs:**
When writing code for this data lake, follow these rules:

1. **Default to Query API:** Use `pointline.research.query` for exploration and analysis
2. **Discovery First:** Always start by checking data availability with discovery API
3. **Simple Timestamps:** Use ISO strings ("2024-05-01") or datetime objects
4. **Decoded Prices:** Always use `decoded=True` for human-readable prices
5. **Schema Awareness:** See [Schema Reference](../schemas.md) for complete table schemas
6. **Joins:** Always use `join_asof` on `ts_local_us` for point-in-time correctness

**Example workflow:**
```python
from pointline import research
from pointline.research import query

# 1. Discover
coverage = research.data_coverage("binance-futures", "BTCUSDT")

# 2. Load
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# 3. Analyze
import polars as pl
vwap = trades.select([
    (pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()
]).item()
```

**Core API (only when explicitly needed):**
```python
from pointline import research, registry

# Explicit symbol resolution
symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]

# Load with explicit control
trades = research.load_trades(symbol_id=symbol_id, start_ts_us=..., end_ts_us=...)
```

---

## 9. Choosing the Right API

### Quick Decision

| Use Case | API |
|----------|-----|
| Exploration, prototyping | **Query API** |
| Jupyter notebooks | **Query API** |
| Quick checks | **Query API** |
| LLM agent workflows | **Query API** |
| Production research | **Core API** |
| Explicit reproducibility | **Core API** |
| SCD Type 2 handling | **Core API** |

**For detailed guidance, see [Choosing an API](choosing-an-api.md).**

---

## 10. Further Reading

- **[Quickstart Guide](../quickstart.md)** - 5-minute tutorial
- **[Choosing an API](choosing-an-api.md)** - Detailed API selection guide
- **[Research API Reference](../research_api_guide.md)** - Complete API documentation
- **[Schema Reference](../schemas.md)** - Table schemas and data types
- **[Architecture Design](../architecture/design.md)** - Data lake design principles
