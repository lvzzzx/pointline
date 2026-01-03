---
name: pointline-data-lake
description: Use when working with the Pointline/Tardis data lake to answer research questions, write DuckDB or Polars queries, resolve symbols, interpret schemas, or build PIT-correct pipelines (trades, quotes, L2, books, options, gold tables) from Delta Lake tables.
---

# Pointline Data Lake

## Overview

Provide PIT-correct data access guidance and query patterns for the Pointline/Tardis data lake, including symbol resolution, partition pruning, and fixed-point decoding.

## Workflow Decision Tree

1. **Clarify the research task** (exploration vs pipeline vs backtest).
2. **Identify the table(s)** and required fields.
3. **Resolve `exchange_id` + `symbol_id`** (SCD Type 2 rules) if filtering by symbol.
4. **Apply partition-first filters** (`date`, then `exchange`/`exchange_id`, then `symbol_id`).
5. **Choose access method**:
   - Polars via `pointline.research` helpers for pipelines.
   - DuckDB `delta_scan` for ad-hoc SQL.
6. **Handle time correctness**:
   - Use `ts_local_us` for replay.
   - Use `ts_exch_us` only for latency analysis.
7. **Convert fixed-point values late** using `price_increment`/`amount_increment`.

## Core Rules

- **PIT correctness:** Use `ts_local_us` as the primary timeline for any backtest or replay.
- **Partition-first:** Always filter by `date`; add `exchange`/`exchange_id` and `symbol_id` ASAP.
- **Symbol changes:** Use `silver.dim_symbol` with validity ranges to resolve `symbol_id`.
- **Exchange IDs:** Use `get_exchange_id()` (from `pointline.config`) as the source of truth.
- **Joins:** Use `join_asof` on `ts_local_us` for asynchronous streams.
- **Fixed-point:** Keep `price_int`/`qty_int` integers until the final step.
- **Lake root:** Resolve paths relative to `LAKE_ROOT` (default `./data/lake` in `pointline/config.py`).

## Quick Start Patterns

### Polars (preferred for pipelines)

```python
from datetime import datetime, timezone
import polars as pl
from pointline import research
from pointline.config import get_exchange_id

asof = datetime(2025, 12, 28, 12, 0, 0, tzinfo=timezone.utc)
asof_us = int(asof.timestamp() * 1_000_000)
exchange_id = get_exchange_id("deribit")
symbol = "BTC-PERPETUAL"

symbol_id = (
    research.scan_table(
        "dim_symbol",
        exchange_id=exchange_id,
        columns=["symbol_id", "exchange_symbol", "valid_from_ts", "valid_until_ts"],
    )
    .filter(
        (pl.col("exchange_symbol") == symbol)
        & (pl.col("valid_from_ts") <= asof_us)
        & (pl.col("valid_until_ts") > asof_us)
    )
    .select("symbol_id")
    .collect()
    .item()
)

trades = research.load_trades(
    exchange="deribit",
    symbol_id=symbol_id,
    start_date="2025-12-28",
    end_date="2025-12-28",
    lazy=True,
)

daily = (
    trades.group_by("symbol_id")
    .agg([pl.count().alias("trade_count"), pl.col("qty_int").sum().alias("total_qty")])
    .collect()
)
```

### DuckDB (ad-hoc)

```sql
WITH resolved AS (
  SELECT symbol_id
  FROM delta_scan('${LAKE_ROOT}/silver/dim_symbol')
  WHERE exchange_id = 21
    AND exchange_symbol = 'BTC-PERPETUAL'
    AND valid_from_ts <= 1766923200000000
    AND valid_until_ts > 1766923200000000
)
SELECT ts_local_us, side, price_int, qty_int
FROM delta_scan('${LAKE_ROOT}/silver/trades')
WHERE date = '2025-12-28'
  AND exchange_id = 21
  AND symbol_id = (SELECT symbol_id FROM resolved)
ORDER BY ts_local_us;
```

## Reference Files

- **`references/researcher_guide.md`**: Data lake layout, core concepts, and workflow patterns.
- **`references/schemas.md`**: Full table schemas, column types, and partitioning rules.

Load these references when you need complete schema details, table catalogs, or encoding rules.
