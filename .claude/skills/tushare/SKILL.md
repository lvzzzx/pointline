---
name: tushare
description: >-
  Tushare Pro API reference and Pointline integration patterns for Chinese A-share
  (SSE/SZSE) market data. Use when: (1) writing or modifying Tushare API queries,
  (2) building or updating CN stock symbol ingestion (dim_symbol), (3) adding new
  Tushare data types to the Pointline pipeline, (4) debugging Tushare field mappings
  or SCD2 symbol lifecycle, (5) working with CN A-share reference data, valuation
  metrics, financials, or money flow APIs.
---

# Tushare Pro API — Pointline Integration

## Quick Reference

- **SDK:** `import tushare as ts; pro = ts.pro_api(token)`
- **API docs:** See [references/tushare_cn.md](references/tushare_cn.md) for full API specs (parameters, return fields, rate limits)
- **Pointline adapter:** `pointline/vendors/tushare/symbols.py` — pure-function transforms
- **Sync script:** `scripts/sync_tushare_dim_symbol.py`
- **Tests:** `tests/tushare/test_symbol_snapshot.py`

## Pointline Integration Architecture

Tushare is used **only for reference/dimension data** (dim_symbol), not event data.

```
Tushare API (pro.stock_basic)
  → pandas DataFrame
  → pl.from_pandas()
  → stock_basic_to_snapshot(raw)     # Pure function, no I/O
  → dim_symbol.upsert() / bootstrap()
  → dim_symbol.assign_symbol_ids()
  → DeltaDimensionStore.save_dim_symbol()
  → Silver: /silver/dim_symbol/
```

## Schema Mapping: Tushare → dim_symbol

| Tushare Field | Pointline Field | Notes |
|---|---|---|
| `exchange` | `exchange` | Normalized to lowercase (szse/sse) |
| `symbol` | `exchange_symbol` | Exchange-native (000001) |
| `ts_code` | `canonical_symbol` | Tushare format (000001.SZ) |
| `market` | `market_type` | 主板, 创业板, 科创板 |
| `name` | `base_asset` | Company name |
| — | `quote_asset` | Hard-coded "CNY" |
| — | `tick_size` | CN_TICK_SIZE = 10_000_000 (0.01 CNY * 1e9) |
| `market` | `lot_size` | 科创板: 200e9, else: 100e9 (shares * QTY_SCALE) |
| `list_date` | `valid_from_ts_us` | YYYYMMDD → UTC midnight microseconds |
| `delist_date` | `valid_until_ts_us` | D: delist_date, L/P: MAX (2^63-1) |
| `list_status` | `is_current` | L/P → True, D → False |

## Key Constants

```python
CN_TICK_SIZE = 10_000_000          # 0.01 CNY × PRICE_SCALE (1e9)
CN_LOT_SIZE = 100_000_000_000      # 100 shares × QTY_SCALE (1e9)
STAR_MARKET_LOT_SIZE = 200_000_000_000  # 200 shares (科创板)
```

## Critical Rules

1. **BSE filtered out.** Only SSE/SZSE are in scope; Beijing Stock Exchange rows are dropped.
2. **Delisted stocks included.** SCD2 requires explicit close dates for PIT correctness.
3. **Pure functions only.** No API calls inside `pointline/vendors/tushare/`. I/O lives in scripts.
4. **Preserve other vendors.** Sync script must not delete crypto/other-vendor symbols from dim_symbol.
5. **Timestamps as Int64 microseconds.** Parse YYYYMMDD to UTC midnight via `_parse_yyyymmdd_us()`.
6. **Fixed-point integers.** Prices/quantities are scaled Int64; never use floats mid-pipeline.

## Workflow Patterns

### Full-Historical Load (First Run)

```python
raw = pl.concat([
    pl.from_pandas(pro.stock_basic(exchange=ex, list_status="L")),
    pl.from_pandas(pro.stock_basic(exchange=ex, list_status="D")),
])
snapshot = stock_basic_to_snapshot(raw, effective_ts_us=now_us)
result = assign_symbol_ids(snapshot)
store.save_dim_symbol(result)
```

### Incremental Daily Sync

```python
current = pl.from_pandas(pro.stock_basic(list_status="L"))
delisted = pl.from_pandas(pro.stock_basic(list_status="D"))
snap = stock_basic_to_snapshot(current)
dl = stock_basic_to_delistings(delisted)
dim = upsert(existing_dim, snap, effective_ts_us, delistings=dl)
result = assign_symbol_ids(dim)
store.save_dim_symbol(result, expected_version=old_v)
```

## Adding New Tushare Data Types

When extending Pointline to ingest new Tushare APIs (daily prices, financials, money flow, etc.):

1. Read the API spec in [references/tushare_cn.md](references/tushare_cn.md) for parameters, return fields, and rate limits
2. Create a new module under `pointline/vendors/tushare/` (pure functions, no I/O)
3. Define the canonical schema in `pointline/schemas/` following existing patterns
4. Use `ann_date` (not `end_date`) for PIT-correct financial data filtering
5. Handle rate limits: check points requirements (2000+ for most, 6000+ for minute bars)
6. Write tests in `tests/tushare/` covering field mapping and edge cases

### PIT Gotcha for Financial Data

```python
# WRONG: uses data before it was publicly available
df = pro.fina_indicator(ts_code='000001.SZ', period='20231231')

# CORRECT: filter by announcement date for PIT
df = pro.fina_indicator(ts_code='000001.SZ')
df = df[df['ann_date'] <= query_date]  # Only data known at query_date
```

## Symbol Code Format

| Suffix | Exchange | Code Range | Market |
|---|---|---|---|
| `.SH` | SSE | 600000-699999 | Main Board |
| `.SH` | SSE | 680000-689999 | STAR Market (科创板) |
| `.SZ` | SZSE | 000001-009999 | Main Board |
| `.SZ` | SZSE | 300000-309999 | ChiNext (创业板) |

## API Reference

For full API specifications (all parameters, return fields, rate limits, error codes), read [references/tushare_cn.md](references/tushare_cn.md). Key sections:

- **Section 3:** Reference data (stock_basic, trade_cal)
- **Section 4:** Market data (daily, adj_factor, daily_basic, pro_bar)
- **Section 5:** Financial data (fina_indicator, income, balance_sheet, cashflow)
- **Section 6:** Market behavior (moneyflow, limit_list)
- **Section 8:** Rate limits and points requirements
- **Section 10:** PIT considerations for backtesting
