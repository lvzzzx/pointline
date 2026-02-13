# Data Source: Tushare Pro API (China A-Share)

This document describes the **Tushare Pro API** for Chinese A-share market data. Tushare is a Python SDK-based data platform providing reference data, market data, and fundamentals.

> **Official Documentation:** https://tushare.pro/document/2
>
> This document is based on the official Tushare Pro documentation. For the most up-to-date information, please refer to the official source.

---

## Document Reference

| Resource | URL |
| :--- | :--- |
| **Main Documentation** | https://tushare.pro/document/2 |
| **Stock Basic** | https://tushare.pro/document/2?doc_id=25 |
| **Trade Calendar** | https://tushare.pro/document/2?doc_id=26 |
| **Daily Prices** | https://tushare.pro/document/2?doc_id=27 |
| **Adj Factor** | https://tushare.pro/document/2?doc_id=28 |
| **Daily Basic** | https://tushare.pro/document/2?doc_id=32 |
| **Pro Bar** | https://tushare.pro/document/2?doc_id=109 |
| **Financial Indicators** | https://tushare.pro/document/2?doc_id=79 |
| **Income Statement** | https://tushare.pro/document/2?doc_id=33 |
| **Balance Sheet** | https://tushare.pro/document/2?doc_id=36 |
| **Cash Flow** | https://tushare.pro/document/2?doc_id=44 |
| **Money Flow** | https://tushare.pro/document/2?doc_id=170 |
| **Limit List** | https://tushare.pro/document/2?doc_id=183 |

---

## 1. Source Overview

| Attribute | Value |
| :--- | :--- |
| **Vendor** | Tushare Pro (tushare.pro) |
| **Access Method** | Python SDK (`pip install tushare`) |
| **Authentication** | API Token (registered user) |
| **Markets** | SSE, SZSE, BSE (A-shares) |
| **Return Format** | pandas DataFrame (default) or JSON |
| **Base URL** | `https://api.tushare.pro` |
| **Rate Limit** | Based on integration points (积分), 20-500+ calls/minute |

---

## 2. SDK Setup

```python
import tushare as ts

# Set token (obtain from tushare.pro)
ts.set_token('YOUR_TOKEN_HERE')

# Initialize API
pro = ts.pro_api()

# Query data
df = pro.stock_basic(exchange='SZSE')
```

---

## 3. Reference Data API

### 3.1 Stock Basic Info

**Reference:** https://tushare.pro/document/2?doc_id=25

**API:** `stock_basic`

**Description:** Basic information for all listed stocks including company profile, industry classification, and listing status.

**Input Parameters**

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `ts_code` | String | No | - | Stock code with exchange suffix (e.g., `000001.SZ`) |
| `name` | String | No | - | Stock name (fuzzy search) |
| `exchange` | String | No | - | Exchange: `SSE`, `SZSE`, `BSE` |
| `market` | String | No | - | Market segment |
| `is_hs` | String | No | - | HK/SZ connect: `N` (No), `H` (Yes), `S` (Yes) |
| `list_status` | String | No | `L` | Status: `L` (Listed), `D` (Delisted), `P` (Paused) |
| `fields` | String | No | - | Specific fields to return |

**Return Fields**

| Field | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Tushare code (e.g., `000001.SZ`, `600000.SH`) |
| `symbol` | String | Exchange symbol without suffix (e.g., `000001`) |
| `name` | String | Stock name (Chinese) |
| `area` | String | Geographic region/province |
| `industry` | String | Industry classification |
| `fullname` | String | Full company name |
| `enname` | String | English name |
| `cnspell` | String | Pinyin abbreviation |
| `market` | String | Market type: `主板`, `创业板`, `科创板`, `北交所` |
| `exchange` | String | Exchange: `SZSE`, `SSE`, `BSE` |
| `curr_type` | String | Currency: `CNY` |
| `list_status` | String | `L`=Listed, `D`=Delisted, `P`=Paused |
| `list_date` | String | Listing date (`YYYYMMDD`) |
| `delist_date` | String | Delisting date (`YYYYMMDD`, null if active) |
| `is_hs` | String | Connect eligibility: `N`=No, `H`=HK, `S`=Stock Connect |
| `act_name` | String | Actual controller name |
| `act_ent_type` | String | Controller type |

**Example**
```python
# Get all SZSE listed stocks
df = pro.stock_basic(exchange='SZSE', list_status='L')

# Get specific stock
 df = pro.stock_basic(ts_code='000001.SZ')
```

---

### 3.2 Trade Calendar

**Reference:** https://tushare.pro/document/2?doc_id=26

**API:** `trade_cal`

**Description:** Exchange trading calendar.

**Input Parameters**

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `exchange` | String | No | `SSE` | Exchange: `SSE`, `SZSE` |
| `start_date` | String | No | - | Start date (`YYYYMMDD`) |
| `end_date` | String | No | - | End date (`YYYYMMDD`) |
| `is_open` | String | No | - | `1`=Trading day, `0`=Holiday |

**Return Fields**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange |
| `cal_date` | String | Calendar date (`YYYYMMDD`) |
| `is_open` | String | `1`=Open, `0`=Closed |
| `pretrade_date` | String | Previous trading date |

**Example**
```python
# Get all trading days in January 2024
df = pro.trade_cal(exchange='SSE', start_date='20240101', end_date='20240131', is_open='1')
```

---

## 4. Market Data API

### 4.1 Daily Prices

**Reference:** https://tushare.pro/document/2?doc_id=27

**API:** `daily`

**Description:** End-of-day OHLCV prices (不复权/unadjusted).

**Points Required:** 2000+

**Input Parameters**

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `ts_code` | String | No | - | Stock code (e.g., `000001.SZ`). If empty, returns all stocks for date |
| `trade_date` | String | No | - | Trade date (`YYYYMMDD`). Priority over start/end dates |
| `start_date` | String | No | - | Start date for range query |
| `end_date` | String | No | - | End date for range query |

**Note:** Must provide either `trade_date` OR (`start_date` + `end_date`).

**Return Fields**

| Field | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Stock code |
| `trade_date` | String | Trade date (`YYYYMMDD`) |
| `open` | Float | Opening price (CNY) |
| `high` | Float | Highest price (CNY) |
| `low` | Float | Lowest price (CNY) |
| `close` | Float | Closing price (CNY) |
| `pre_close` | Float | Previous closing price (CNY) |
| `change` | Float | Price change (CNY) |
| `pct_chg` | Float | Percentage change (%) |
| `vol` | Float | Volume (shares) |
| `amount` | Float | Turnover (CNY) |

**Example**
```python
# Single stock history
df = pro.daily(ts_code='000001.SZ', start_date='20240101', end_date='20240131')

# All stocks for single date
df = pro.daily(trade_date='20240102')
```

---

### 4.2 Adjustment Factor

**Reference:** https://tushare.pro/document/2?doc_id=28

**API:** `adj_factor`

**Description:** Cumulative adjustment factors for split/dividend adjustments.

**Points Required:** 2000+

**Input Parameters**

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `ts_code` | String | No | Stock code |
| `trade_date` | String | No | Trade date (`YYYYMMDD`) |
| `start_date` | String | No | Start date |
| `end_date` | String | No | End date |

**Return Fields**

| Field | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Stock code |
| `trade_date` | String | Trade date |
| `adj_factor` | Float | Cumulative adjustment factor |

**Usage for Forward Adjustment (前复权)**
```python
# Fetch daily prices and adj_factor
daily = pro.daily(ts_code='000001.SZ', start_date='20240101', end_date='20240131')
adj = pro.adj_factor(ts_code='000001.SZ', start_date='20240101', end_date='20240131')

# Merge and calculate adjusted price
df = daily.merge(adj, on=['ts_code', 'trade_date'])
df['close_adj'] = df['close'] * df['adj_factor']
```

---

### 4.3 Daily Valuation Metrics

**Reference:** https://tushare.pro/document/2?doc_id=32

**API:** `daily_basic`

**Description:** Daily valuation metrics (PE/PB/market cap).

**Points Required:** 2000+

**Input Parameters**

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `ts_code` | String | No | Stock code |
| `trade_date` | String | No | Trade date |
| `start_date` | String | No | Start date |
| `end_date` | String | No | End date |

**Return Fields**

| Field | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Stock code |
| `trade_date` | String | Trade date |
| `close` | Float | Closing price |
| `turnover_rate` | Float | Turnover rate (%) - total shares |
| `turnover_rate_f` | Float | Turnover rate (%) - free float |
| `volume_ratio` | Float | Volume ratio |
| `pe` | Float | PE ratio (TTM) |
| `pe_ttm` | Float | PE TTM |
| `pb` | Float | PB ratio |
| `ps` | Float | PS ratio |
| `ps_ttm` | Float | PS TTM |
| `dv_ratio` | Float | Dividend yield (%) |
| `dv_ttm` | Float | Dividend yield TTM (%) |
| `total_share` | Float | Total shares outstanding |
| `float_share` | Float | Float shares (tradable A-shares) |
| `free_share` | Float | Free float shares |
| `total_mv` | Float | Total market value (CNY) |
| `circ_mv` | Float | Circulating market value (CNY) |

---

### 4.4 Pro Bar (Unified Interface)

**Reference:** https://tushare.pro/document/2?doc_id=109

**API:** `pro_bar`

**Description:** Unified interface supporting stocks, indexes, futures with multiple frequencies and adjustments.

**Points Required:** 600+ (minute data requires 6000+)

**Input Parameters**

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `ts_code` | String | Yes | - | Asset code (e.g., `000001.SZ`, `000001.SH`) |
| `pro_api` | Object | No | - | Tushare API instance |
| `start_date` | String | No | - | Start date (`YYYYMMDD`) |
| `end_date` | String | No | - | End date (`YYYYMMDD`) |
| `asset` | String | No | `E` | Asset type: `E`=Stock, `I`=Index, `FT`=Futures, `O`=Option, `C`=Crypto |
| `freq` | String | No | `D` | Frequency: `D`=Daily, `W`=Weekly, `M`=Monthly, `1min`, `5min`, `15min`, `30min`, `60min` |
| `adj` | String | No | - | Adjustment: `qfq`=Forward, `hfq`=Backward, null=Unadjusted |
| `ma` | List | No | - | Moving averages, e.g., `[5, 20, 50]` |
| `factors` | List | No | - | Additional factors (for minute data) |

**Return Fields**

| Field | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Stock code |
| `trade_date` | String | Trade date/time |
| `open` | Float | Opening price |
| `high` | Float | Highest price |
| `low` | Float | Lowest price |
| `close` | Float | Closing price |
| `vol` | Float | Volume |
| `amount` | Float | Turnover |
| `ma5`, `ma20`, etc. | Float | Moving averages (if requested) |

**Example**
```python
# Daily with forward adjustment
df = ts.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20240101', end_date='20240131')

# 5-minute bars (requires 6000+ points)
df = ts.pro_bar(ts_code='000001.SZ', freq='5min', start_date='20240102', end_date='20240102')

# With moving averages
df = ts.pro_bar(ts_code='000001.SZ', adj='qfq', ma=[5, 20, 50])
```

---

## 5. Financial Data API

### 5.1 Financial Indicators

**Reference:** https://tushare.pro/document/2?doc_id=79

**API:** `fina_indicator`

**Description:** Quarterly financial indicators covering profitability, solvency, and growth.

**Points Required:** 3000+

**Input Parameters**

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `ts_code` | String | No | Stock code |
| `ann_date` | String | No | Announcement date (`YYYYMMDD`) |
| `start_date` | String | No | Start date (reporting period) |
| `end_date` | String | No | End date (reporting period) |
| `period` | String | No | Reporting period (e.g., `20231231` for Q4 2023) |

**Return Fields (Selected)**

| Field | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Stock code |
| `ann_date` | String | **Announcement date** - critical for PIT |
| `end_date` | String | Reporting period end |
| `eps` | Float | Basic EPS |
| `dt_eps` | Float | Diluted EPS |
| `total_revenue_ps` | Float | Revenue per share |
| `revenue_ps` | Float | Operating revenue per share |
| `capital_rese_ps` | Float | Capital reserve per share |
| `surplus_rese_ps` | Float | Surplus reserve per share |
| `undist_profit_ps` | Float | Undistributed profit per share |
| `extra_item` | Float | Extraordinary items |
| `profit_dedt` | Float | Profit after non-recurring items |
| `gross_margin` | Float | Gross margin |
| `current_ratio` | Float | Current ratio |
| `quick_ratio` | Float | Quick ratio |
| `cash_ratio` | Float | Cash ratio |
| `invturn_days` | Float | Inventory turnover days |
| `arturn_days` | Float | AR turnover days |
| `inv_turn` | Float | Inventory turnover |
| `ar_turn` | Float | AR turnover |
| `ca_turn` | Float | Current assets turnover |
| `fa_turn` | Float | Fixed assets turnover |
| `assets_turn` | Float | Total assets turnover |
| `op_income` | Float | Operating income |
| `ebit` | Float | EBIT |
| `ebitda` | Float | EBITDA |
| `fcff` | Float | Free cash flow to firm |
| `fcfe` | Float | Free cash flow to equity |
| `current_exint` | Float | Current interest expense |
| `noncurrent_exint` | Float | Non-current interest expense |
| `interestdebt` | Float | Interest-bearing debt |
| `netdebt` | Float | Net debt |
| `tangible_asset` | Float | Tangible assets |
| `working_capital` | Float | Working capital |
| `networking_capital` | Float | Net working capital |
| `invest_capital` | Float | Invested capital |
| `retained_earnings` | Float | Retained earnings |
| `eps_yoy` | Float | EPS YoY growth (%) |
| `dt_eps_yoy` | Float | Diluted EPS YoY growth (%) |
| `revenue_yoy` | Float | Revenue YoY growth (%) |
| `operate_profit_yoy` | Float | Operating profit YoY growth (%) |
| `netprofit_yoy` | Float | Net profit YoY growth (%) |
| `roe` | Float | Return on equity (%) |
| `roe_dt` | Float | ROE after non-recurring items (%) |
| `roa` | Float | Return on assets (%) |
| `npta` | Float | Net profit / total assets (%) |
| `roic` | Float | Return on invested capital (%) |
| `roe_yearly` | Float | Annualized ROE |
| `roa2_yearly` | Float | Annualized ROA |
| `debt_to_assets` | Float | Debt-to-assets ratio (%) |
| `debt_to_eqt` | Float | Debt-to-equity ratio (%) |
| `gdays` | Float | Gross profit days |
| `turn_days` | Float | Operating cycle days |
| `grossprofit_margin` | Float | Gross profit margin (%) |
| `operate_profit_margin` | Float | Operating profit margin (%) |
| `netprofit_margin` | Float | Net profit margin (%) |
| `ocf_to_revenue` | Float | OCF to revenue ratio |
| `ocf_to_operate_profit` | Float | OCF to operating profit |

**Example**
```python
# Get all quarterly financials for a stock
df = pro.fina_indicator(ts_code='000001.SZ')

# Get specific reporting period
df = pro.fina_indicator(ts_code='000001.SZ', period='20231231')
```

---

### 5.2 Financial Statements (Three Reports)

**References:**
- Income: https://tushare.pro/document/2?doc_id=33
- Balance Sheet: https://tushare.pro/document/2?doc_id=36
- Cash Flow: https://tushare.pro/document/2?doc_id=44

| API | Description | Key Fields |
| :--- | :--- | :--- |
| `income` | Income Statement | Revenue, COGS, operating profit, net profit |
| `balance_sheet` | Balance Sheet | Assets, liabilities, equity |
| `cashflow` | Cash Flow Statement | Operating, investing, financing cash flows |

**Common Parameters:** `ts_code`, `ann_date`, `f_ann_date`, `start_date`, `end_date`, `period`

---

## 6. Market Behavior API

### 6.1 Money Flow

**Reference:** https://tushare.pro/document/2?doc_id=170

**API:** `moneyflow`

**Description:** Capital flow data categorized by order size.

**Points Required:** 2000+

**Input Parameters**

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `ts_code` | String | No | Stock code |
| `trade_date` | String | No | Trade date |
| `start_date` | String | No | Start date |
| `end_date` | String | No | End date |

**Return Fields**

| Field | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Stock code |
| `trade_date` | String | Trade date |
| `buy_sm_amount` | Float | Small buy amount (< 4 CNY/share) |
| `buy_sm_vol` | Float | Small buy volume |
| `sell_sm_amount` | Float | Small sell amount |
| `sell_sm_vol` | Float | Small sell volume |
| `buy_md_amount` | Float | Medium buy amount (4-20 CNY/share) |
| `buy_md_vol` | Float | Medium buy volume |
| `sell_md_amount` | Float | Medium sell amount |
| `sell_md_vol` | Float | Medium sell volume |
| `buy_lg_amount` | Float | Large buy amount (20-100 CNY/share) |
| `buy_lg_vol` | Float | Large buy volume |
| `sell_lg_amount` | Float | Large sell amount |
| `sell_lg_vol` | Float | Large sell volume |
| `buy_elg_amount` | Float | Extra-large buy amount (> 100 CNY/share) |
| `buy_elg_vol` | Float | Extra-large buy volume |
| `sell_elg_amount` | Float | Extra-large sell amount |
| `sell_elg_vol` | Float | Extra-large sell volume |
| `net_mf_amount` | Float | Net main force amount (institutional) |
| `net_mf_vol` | Float | Net main force volume |

---

### 6.2 Limit Up/Down List

**Reference:** https://tushare.pro/document/2?doc_id=183

**API:** `limit_list`

**Description:** Stocks hitting price limits (涨停/跌停).

**Points Required:** 2000+

**Input Parameters**

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `trade_date` | String | No | Trade date |
| `ts_code` | String | No | Stock code |
| `limit_type` | String | No | Limit type: `U` (Up), `D` (Down) |

**Return Fields**

| Field | Type | Description |
| :--- | :--- | :--- |
| `trade_date` | String | Trade date |
| `ts_code` | String | Stock code |
| `name` | String | Stock name |
| `close` | Float | Closing price |
| `pct_chg` | Float | Price change (%) |
| `amp` | Float | Amplitude (%) |
| `fc_ratio` | Float | Float capitalization ratio |
| `fl_ratio` | Float | Float ratio |
| `fd_amount` | Float | Float amount |
| `first_time` | String | First limit time (`HHMMSS`) |
| `last_time` | String | Last limit time (`HHMMSS`) |
| `open_times` | Integer | Times limit was broken |
| `strth` | Float | Strength indicator |
| `limit` | String | `U`=Limit Up, `D`=Limit Down |

---

## 7. Symbol Code Reference

### Exchange Suffixes

| Suffix | Exchange | Examples |
| :--- | :--- | :--- |
| `.SH` | SSE | `600000.SH`, `688001.SH` |
| `.SZ` | SZSE | `000001.SZ`, `300001.SZ` |
| `.BJ` | BSE | `430001.BJ`, `835001.BJ` |

### Code Ranges

| Exchange | Code Range | Market Segment |
| :--- | :--- | :--- |
| **SSE** | 600000-699999 | Main Board |
| **SSE** | 680000-689999 | STAR Market (科创板) |
| **SSE** | 510000-519999 | ETFs |
| **SZSE** | 000001-009999 | Main Board |
| **SZSE** | 300000-309999 | ChiNext (创业板) |
| **SZSE** | 159000-159999 | ETFs |
| **BSE** | 430000-899999 | Beijing Stock Exchange |

---

## 8. Rate Limits and Points

### Points Requirements by API

| API | Points Required | Rate Limit |
| :--- | :--- | :--- |
| `stock_basic` | 2000+ | 500/minute |
| `trade_cal` | 2000+ | 500/minute |
| `daily` | 2000+ | 200/minute |
| `adj_factor` | 2000+ | 200/minute |
| `daily_basic` | 2000+ | 200/minute |
| `pro_bar` (daily) | 600+ | 200/minute |
| `pro_bar` (minute) | 6000+ | 100/minute |
| `fina_indicator` | 3000+ | 200/minute |
| `income`/`balance_sheet`/`cashflow` | 3000+ | 100/minute |
| `moneyflow` | 2000+ | 200/minute |
| `limit_list` | 2000+ | 200/minute |

### Account Levels

| Points | Level | Description |
| :--- | :--- | :--- |
| < 2000 | Basic | Limited access to basic data |
| 2000-5000 | Standard | Most daily data available |
| 5000+ | Advanced | High-frequency data, historical backfill |
| 6000+ | Professional | Minute-level bars |

---

## 9. Update Schedule

| Data Type | Update Time (CST) | Frequency |
| :--- | :--- | :--- |
| Reference data | 08:00-09:00 | Daily |
| Daily prices | 17:00-19:00 | Trading days |
| Valuation metrics | 19:00-21:00 | Trading days |
| Adjustment factors | 17:00-19:00 | Trading days (as needed) |
| Financial indicators | Within 30 days of quarter end | Quarterly |
| Money flow | 19:00-21:00 | Trading days |
| Limit list | 15:30-17:00 | Trading days |

---

## 10. PIT (Point-in-Time) Considerations

### Financial Data Timing

| Field | Meaning | Usage |
| :--- | :--- | :--- |
| `end_date` | End of reporting period | Identifies quarter/year |
| `ann_date` | Date data was announced | **Use for PIT filtering** |

**Example:**
- `end_date` = 2023-12-31 (Q4 2023 data)
- `ann_date` = 2024-04-30 (publicly available April 2024)

**For backtest as of 2024-03-15:** This Q4 data should **NOT** be used (lookahead bias).

```python
# PIT-correct query: only use data announced by query_date
def get_fina_pit(pro, ts_code, query_date):
    df = pro.fina_indicator(ts_code=ts_code)
    # Filter: ann_date must be <= query_date
    return df[df['ann_date'] <= query_date]
```

---

## Appendix: Error Codes

| Code | Meaning | Solution |
| :--- | :--- | :--- |
| `1001` | Invalid token | Check token validity |
| `1002` | Insufficient points | Upgrade account or reduce requests |
| `1003` | Rate limit exceeded | Wait and retry |
| `1004` | Invalid parameter | Check parameter format |
| `1005` | No data | Verify symbol/date exists |
| `1006` | Server error | Retry later |
