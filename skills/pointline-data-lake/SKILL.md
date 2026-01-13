---
name: pointline-data-lake
description: Use when working with the Pointline/Tardis data lake to answer research questions, write DuckDB or Polars queries, resolve symbols, interpret schemas, or build PIT-correct pipelines (trades, quotes, L2, books, options, gold tables) from Delta Lake tables.
---

# Pointline Data Lake

## Overview

Provide PIT-correct data access guidance and query patterns for the Pointline/Tardis data lake, including symbol resolution, partition pruning, and fixed-point decoding.

## Workflow Decision Tree

1. **Clarify the research task** (exploration vs pipeline vs backtest).
2. **Find the `symbol_id`** using `pointline.registry` or CLI `symbol search`. This is your primary key.
3. **Identify the table(s)** and required fields.
4. **Choose access method**:
   - Polars via `pointline.research` helpers (uses `symbol_id` for auto-partitioning).
   - DuckDB `delta_scan` for ad-hoc SQL (filter by `symbol_id`).
5. **Handle time correctness**:
   - Use `ts_local_us` for replay.
   - Use `ts_exch_us` only for latency analysis.
6. **Convert fixed-point values late** using `price_increment`/`amount_increment`.

## Core Rules

- **Source of Truth:** The `symbol_id` is the canonical identifier. Resolve it once from `dim_symbol` (Registry).
- **PIT correctness:** Use `ts_local_us` as the primary timeline for any backtest or replay.
- **Partition-first:** Use `pointline.research` helpers; they automatically resolve partitions from `symbol_id`.
- **Joins:** Use `join_asof` on `ts_local_us` for asynchronous streams.
- **Fixed-point:** Keep `price_int`/`qty_int` integers until the final step.
- **Lake root:** Resolve paths relative to `LAKE_ROOT` or user config
  (`~/.config/pointline/config.toml`), default `~/data/lake` in `pointline/config.py`.
- **Table helpers:** Use `pointline.tables.*` modules for table-specific decoding/validation.

## Quick Start Patterns

### Polars (preferred for pipelines)

```python
import polars as pl
from pointline import registry, research

# 1. Find your symbol ID
# Check registry.find_symbol() output for valid_from/until to match your time range
match = registry.find_symbol("BTC-PERPETUAL", exchange="deribit")
symbol_id = match["symbol_id"][0]

# 2. Load data (Partition pruning is handled automatically)
trades = research.load_trades(
    symbol_id=symbol_id,
    start_date="2025-12-28",
    end_date="2025-12-28",
    lazy=True,
)

daily = (
    trades.group_by("symbol_id")
    .agg([pl.len().alias("trade_count"), pl.col("qty_int").sum().alias("total_qty")])
    .collect()
)
```

### DuckDB (ad-hoc)

```sql
-- 1. Find ID in CLI: pointline symbol search "BTC-PERPETUAL" --exchange deribit
-- 2. Use ID in query
SELECT ts_local_us, side, price_int, qty_int
FROM delta_scan('${LAKE_ROOT}/silver/trades')
WHERE date = '2025-12-28'
  AND symbol_id = 101 -- ID found in step 1
ORDER BY ts_local_us;
```

## Reference Files

- **`references/researcher_guide.md`**: Data lake layout, core concepts, and workflow patterns.
- **`references/schemas.md`**: Full table schemas, column types, and partitioning rules.

Load these references when you need complete schema details, table catalogs, or encoding rules.
