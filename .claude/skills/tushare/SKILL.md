---
name: tushare
description: >-
  Tushare Pro API reference for Chinese A-share (SSE/SZSE) market data. Use when:
  (1) writing Tushare API queries for stock prices, financials, or valuation data,
  (2) looking up Tushare API parameters, return fields, or rate limits,
  (3) working with CN A-share reference data, daily bars, adjustment factors,
  money flow, or limit up/down lists, (4) handling PIT-correct financial data
  queries with Tushare, (5) any task involving tushare Python SDK.
---

# Tushare Pro API

Tushare is a Python SDK-based data platform for Chinese A-share market data (SSE, SZSE, BSE). It provides reference data, OHLCV prices, valuation metrics, financial statements, and market behavior data via a simple Python API.

## Setup

```python
import tushare as ts

ts.set_token('YOUR_TOKEN')
pro = ts.pro_api()

# Query data — all APIs return pandas DataFrames
df = pro.stock_basic(exchange='SZSE', list_status='L')
```

## API Quick Reference

| API | Description | Points | Rate |
|---|---|---|---|
| `stock_basic` | Listed stock info (name, industry, status) | 2000+ | 500/min |
| `trade_cal` | Exchange trading calendar | 2000+ | 500/min |
| `daily` | Daily OHLCV (unadjusted) | 2000+ | 200/min |
| `adj_factor` | Split/dividend adjustment factors | 2000+ | 200/min |
| `daily_basic` | Valuation (PE/PB/market cap) | 2000+ | 200/min |
| `pro_bar` | Unified bars (daily/weekly/minute, with adj) | 600+ | 200/min |
| `fina_indicator` | Quarterly financial indicators | 3000+ | 200/min |
| `income` / `balance_sheet` / `cashflow` | Financial statements | 3000+ | 100/min |
| `moneyflow` | Capital flow by order size | 2000+ | 200/min |
| `limit_list` | Limit up/down stocks | 2000+ | 200/min |

## Common Patterns

### Daily prices with forward adjustment

```python
df = ts.pro_bar(ts_code='000001.SZ', adj='qfq',
                start_date='20240101', end_date='20240131')
```

### All stocks for a single date

```python
df = pro.daily(trade_date='20240102')
```

### Valuation metrics

```python
df = pro.daily_basic(ts_code='000001.SZ',
                     start_date='20240101', end_date='20240131')
# Returns: pe, pb, ps, dv_ratio, total_mv, circ_mv, turnover_rate, etc.
```

### Financial indicators

```python
df = pro.fina_indicator(ts_code='000001.SZ', period='20231231')
# Returns: eps, roe, roa, gross_margin, revenue_yoy, netprofit_yoy, etc.
```

### Manual price adjustment

```python
daily = pro.daily(ts_code='000001.SZ', start_date='20240101', end_date='20240131')
adj = pro.adj_factor(ts_code='000001.SZ', start_date='20240101', end_date='20240131')
df = daily.merge(adj, on=['ts_code', 'trade_date'])
df['close_adj'] = df['close'] * df['adj_factor']
```

## Symbol Codes

| Suffix | Exchange | Code Range | Market |
|---|---|---|---|
| `.SH` | SSE | 600000-699999 | Main Board |
| `.SH` | SSE | 680000-689999 | STAR Market (科创板) |
| `.SZ` | SZSE | 000001-009999 | Main Board |
| `.SZ` | SZSE | 300000-309999 | ChiNext (创业板) |
| `.BJ` | BSE | 430000-899999 | Beijing Stock Exchange |

## PIT (Point-in-Time) for Financial Data

Financial data has two dates — `end_date` (reporting period) and `ann_date` (public announcement). For backtesting, always filter by `ann_date` to avoid lookahead bias:

```python
# WRONG: Q4 data may not be public until April
df = pro.fina_indicator(ts_code='000001.SZ', period='20231231')

# CORRECT: only use data announced by your query date
df = pro.fina_indicator(ts_code='000001.SZ')
df = df[df['ann_date'] <= '20240315']
```

## Key Gotchas

1. **`daily` returns unadjusted prices.** Use `pro_bar` with `adj='qfq'` for forward-adjusted, or `adj='hfq'` for backward-adjusted.
2. **Must provide either `trade_date` or `start_date`+`end_date`** for most market data APIs.
3. **Minute bars require 6000+ points** (account level). Daily only needs 600+.
4. **`vol` is in shares, `amount` is in CNY** (not lots or 万元).
5. **Rate limits are per-minute** and vary by API and account level.

## Full API Reference

For complete parameter lists, return field definitions, rate limits, and error codes, read [references/tushare.md](references/tushare.md):

- **Section 3:** Reference data (stock_basic, trade_cal)
- **Section 4:** Market data (daily, adj_factor, daily_basic, pro_bar)
- **Section 5:** Financial data (fina_indicator, income, balance_sheet, cashflow)
- **Section 6:** Market behavior (moneyflow, limit_list)
- **Section 8:** Rate limits and points requirements
- **Section 10:** PIT considerations
