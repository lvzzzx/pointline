# Research API: Two-Layer Design

## Overview

The Pointline research API provides two layers for querying data:

### Layer 1: Core API (Explicit Symbol Resolution)
**Module:** `pointline.research`
**Best for:** Production research, reproducibility, when you need control

### Layer 2: Query API (Automatic Symbol Resolution)
**Module:** `pointline.research.query`
**Best for:** Exploration, prototyping, quick checks, LLM agents

---

## Quick Start Examples

### Example 1: Exploration with Query API

```python
from pointline.research import query
from datetime import datetime, timezone
import polars as pl

# Quick exploration - automatic symbol resolution
book = query.book_snapshot_25(
    exchange="binance-futures",
    symbol="SOLUSDT",
    start=datetime(2024, 5, 1, tzinfo=timezone.utc),
    end=datetime(2024, 9, 30, tzinfo=timezone.utc),
    lazy=True,  # Don't load all data into memory
)

# Feature engineering on LazyFrame
features = book.select([
    "ts_local_us",
    pl.col("bids_px").list.get(0).alias("best_bid"),
    pl.col("asks_px").list.get(0).alias("best_ask"),
]).with_columns([
    (pl.col("best_ask") - pl.col("best_bid")).alias("spread"),
])

# Aggregate before collecting
hourly = features.group_by_dynamic("ts_local_us", every="1h").agg([
    pl.col("spread").mean().alias("avg_spread"),
])

df = hourly.collect()  # Only load aggregated results
```

### Example 2: Production with Core API

```python
from pointline import research, registry
from datetime import datetime, timezone
import polars as pl

# Explicit symbol resolution with SCD Type 2 filtering
start = datetime(2024, 5, 1, tzinfo=timezone.utc)
end = datetime(2024, 9, 30, tzinfo=timezone.utc)
start_ts_us = int(start.timestamp() * 1_000_000)
end_ts_us = int(end.timestamp() * 1_000_000)

# Find and filter symbols
symbols_df = registry.find_symbol("SOLUSDT", exchange="binance-futures")
active_symbols = symbols_df.filter(
    (pl.col("valid_from_ts") < end_ts_us) &
    (pl.col("valid_until_ts") > start_ts_us)
)
symbol_ids = active_symbols["symbol_id"].to_list()

# Check if metadata changed
if len(symbol_ids) > 1:
    print(f"⚠️  Warning: {len(symbol_ids)} symbol versions found")
    print(active_symbols[["symbol_id", "valid_from_ts", "tick_size"]])

# Load with explicit symbol_ids
book = research.load_book_snapshot_25(
    symbol_id=symbol_ids,
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
    lazy=True,
)
```

---

## When to Use Each Layer

### Use Query API When:
✅ Prototyping and exploring
✅ You trust automatic symbol resolution
✅ Speed matters more than control
✅ Working in Jupyter notebooks
✅ Building LLM agents

### Use Core API When:
✅ Writing production research code
✅ You need to inspect/validate symbol_ids
✅ Performance tuning is important
✅ Code needs to be auditable/reproducible
✅ Symbol metadata changes matter to your analysis

---

## Handling Symbol Metadata Changes

### What Are Symbol Metadata Changes?

When exchanges update contract parameters (tick_size, lot_size, etc.), the system creates a new `symbol_id` to maintain data integrity. This is called SCD Type 2 (Slowly Changing Dimension).

**Example:**
- May 1 - Aug 15: `symbol_id=12345` (tick_size=0.001)
- Aug 16 - Sep 30: `symbol_id=12346` (tick_size=0.0001)

### Query API Behavior

The query API **automatically handles this** and **warns you**:

```python
book = query.book_snapshot_25(
    "binance-futures",
    "SOLUSDT",
    "2024-05-01",
    "2024-09-30",
)

# Output:
# ⚠️  Warning: Symbol metadata changed during query period:
#     Exchange: binance-futures
#     Symbol: SOLUSDT
#     Symbol IDs: [12345, 12346]
#
#     Found 2 versions:
#       - symbol_id=12345: valid_from_ts=..., tick_size=0.001
#       - symbol_id=12346: valid_from_ts=..., tick_size=0.0001
```

### Core API Behavior

You **explicitly** decide how to handle it:

```python
# You see the symbol_ids before querying
symbols = registry.find_symbol("SOLUSDT", exchange="binance-futures")
active = symbols.filter(...)  # Filter by time range
symbol_ids = active["symbol_id"].to_list()

if len(symbol_ids) > 1:
    # Option 1: Use all versions (continuous time series)
    book = research.load_book_snapshot_25(symbol_id=symbol_ids, ...)

    # Option 2: Use only latest version
    latest_symbol_id = active.sort("valid_from_ts", descending=True)["symbol_id"][0]
    book = research.load_book_snapshot_25(symbol_id=latest_symbol_id, ...)

    # Option 3: Analyze each version separately
    for sid in symbol_ids:
        book = research.load_book_snapshot_25(symbol_id=sid, ...)
        # ...
```

---

## Complete Real-World Example

### Scenario: 5-Month Book Snapshot Analysis

```python
from pointline.research import query
from datetime import datetime, timezone
import polars as pl

# 1. Load data with lazy evaluation (query API)
book_lf = query.book_snapshot_25(
    exchange="binance-futures",
    symbol="SOLUSDT",
    start=datetime(2024, 5, 1, tzinfo=timezone.utc),
    end=datetime(2024, 9, 30, tzinfo=timezone.utc),
    lazy=True,  # Critical for large datasets
)

# 2. Feature engineering (all lazy - no data loaded yet)
features_lf = book_lf.select([
    "ts_local_us",
    "symbol_id",
    # Extract best bid/ask
    pl.col("bids_px").list.get(0).alias("best_bid_px"),
    pl.col("asks_px").list.get(0).alias("best_ask_px"),
    pl.col("bids_sz").list.get(0).alias("best_bid_sz"),
    pl.col("asks_sz").list.get(0).alias("best_ask_sz"),
    # Book imbalance (top 5 levels)
    (
        pl.col("bids_sz").list.head(5).list.sum() /
        (pl.col("bids_sz").list.head(5).list.sum() +
         pl.col("asks_sz").list.head(5).list.sum())
    ).alias("imbalance_top5"),
]).with_columns([
    # Spread
    (pl.col("best_ask_px") - pl.col("best_bid_px")).alias("spread"),
    # Mid price
    ((pl.col("best_bid_px") + pl.col("best_ask_px")) / 2).alias("mid_px"),
])

# 3. Aggregate to reduce data volume (still lazy)
hourly_lf = features_lf.group_by_dynamic(
    "ts_local_us",
    every="1h",
    by="symbol_id",
).agg([
    pl.col("spread").mean().alias("avg_spread"),
    pl.col("spread").std().alias("std_spread"),
    pl.col("mid_px").mean().alias("avg_mid_px"),
    pl.col("imbalance_top5").mean().alias("avg_imbalance"),
    pl.col("ts_local_us").count().alias("snapshot_count"),
])

# 4. NOW collect (query plan executes)
hourly_df = hourly_lf.collect()

print(f"Processed 5 months → {hourly_df.height} hourly rows")
# Output: "Processed 5 months → 3,600 hourly rows"
# Instead of millions of raw snapshots!
```

---

## API Reference

### Query API Functions

All functions support:
- **exchange** (str): Exchange name (e.g., "binance-futures")
- **symbol** (str): Exchange symbol (e.g., "SOLUSDT")
- **start** (datetime | int | str): Start time (inclusive)
  - `datetime`: timezone-aware datetime object
  - `int`: microseconds since Unix epoch
  - `str`: ISO 8601 date/datetime ("2024-05-01", "2024-05-01T12:30:45Z", "2024-05-01T12:30:45+00:00")
- **end** (datetime | int | str): End time (exclusive)
  - Same formats as `start`
  - Time range is [start, end) - includes start, excludes end
- **ts_col** (str): Timestamp column to filter on (default: "ts_local_us")
- **columns** (list[str]): Columns to select (default: all)
- **lazy** (bool): Return LazyFrame (True) or DataFrame (False)

**Important Notes:**
- **Time range semantics**: [start, end) is half-open (includes start, excludes end)
- **Timestamp units**: Integer timestamps are in **microseconds** (not seconds or milliseconds)
- **Timezone handling**: Date-only strings ("2024-05-01") are interpreted as midnight UTC with a warning
- **ISO format support**: Accepts standard ISO 8601 formats including Z suffix for UTC

#### `query.trades(...)`
Load trades with automatic symbol resolution.

#### `query.quotes(...)`
Load quotes with automatic symbol resolution.

#### `query.book_snapshot_25(...)`
Load book snapshots with automatic symbol resolution.

### Core API Functions

See existing documentation for:
- `research.load_trades(symbol_id=..., start_ts_us=..., end_ts_us=...)`
- `research.load_quotes(symbol_id=..., start_ts_us=..., end_ts_us=...)`
- `research.load_book_snapshot_25(symbol_id=..., start_ts_us=..., end_ts_us=...)`

---

## Backward Compatibility

All existing code continues to work unchanged:

```python
# Old code (still works)
from pointline import research

trades = research.load_trades(
    symbol_id=101,
    start_ts_us=1700000000000000,
    end_ts_us=1700003600000000,
)
```

The new query API is an **additional** convenience layer, not a replacement.
