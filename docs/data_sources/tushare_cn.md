# Data Source: Tushare Pro (China A-Share)

This document describes the raw data format for **Tushare Pro**, a financial data platform providing Chinese A-share market data. Tushare serves as a **secondary/derived data source** complementary to primary exchange feeds (Quant360 Level 2).

---

## 1. Source Overview

| Attribute | Value |
| :--- | :--- |
| **Vendor** | Tushare Pro (tushare.pro) |
| **Access** | API token-based (REST API via Python SDK) |
| **Markets** | SSE, SZSE, BSE (A-shares), HKEX, US equities |
| **Data Types** | Reference data, EOD prices, fundamentals, macro |
| **Format** | JSON via API (ingested as JSONL.GZ in Bronze) |
| **Update Frequency** | Daily (after market close), quarterly (financials) |
| **Timezone** | China Standard Time (CST, UTC+8) for dates |

---

## 2. Data Access

### 2.1 Authentication

Tushare uses token-based authentication. Users must register and obtain a personal token:

```python
import tushare as ts
pro = ts.pro_api("YOUR_TOKEN_HERE")
```

### 2.2 API Rate Limits

Rate limits are determined by user **integration points** (积分):

| Points Level | Approximate Requests/Minute | Typical Data Access |
| :--- | :--- | :--- |
| < 2000 | 20-50 | Basic reference data |
| 2000-5000 | 100-200 | Daily prices, fundamentals |
| 5000+ | 500+ | High-frequency data, historical backfill |

**Note:** Higher-privilege data (minute bars, tick data) requires 6000+ points.

---

## 3. File Naming Conventions (Bronze)

### 3.1 Pattern

```
tushare/<data_type>/date=<YYYY-MM-DD>/<filename>.jsonl.gz
```

### 3.2 Components

| Component | Values | Description |
| :--- | :--- | :--- |
| `<data_type>` | `stock_basic_cn`, `stock_daily_cn`, `fina_indicator_cn` | Data category |
| `<YYYY-MM-DD>` | e.g., `2024-01-02` | Snapshot or trade date |
| `<filename>` | `<interface>_<timestamp>.jsonl.gz` | Source interface name |

### 3.3 Type Definitions

| Type | Tushare Interface | Content |
| :--- | :--- | :--- |
| `stock_basic_cn` | `stock_basic` | Stock reference data (listings, industry) |
| `trade_cal` | `trade_cal` | Trading calendar |
| `stock_daily_cn` | `daily` | End-of-day OHLCV prices |
| `stock_daily_basic_cn` | `daily_basic` | Daily valuation metrics (PE/PB) |
| `fina_indicator_cn` | `fina_indicator` | Financial indicators (quarterly) |
| `moneyflow_cn` | `moneyflow` | Money flow data |
| `limit_list_cn` | `limit_list` | Limit up/down stocks |

---

## 4. Reference Data

### 4.1 Stock Basic (`stock_basic_cn`)

**Tushare Interface:** `stock_basic`

**File Pattern:** `tushare/stock_basic_cn/date=<YYYY-MM-DD>/stock_basic_<timestamp>.jsonl.gz`

**Schema**

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Tushare code with exchange suffix (e.g., `000001.SZ`) |
| `symbol` | String | Exchange symbol without suffix (e.g., `000001`) |
| `name` | String | Security name (Chinese) |
| `area` | String | Geographic region/area (province) |
| `industry` | String | Industry classification |
| `fullname` | String | Full company name |
| `enname` | String | English name |
| `cnspell` | String | Chinese pinyin abbreviation |
| `market` | String | Market segment (主板/创业板/科创板) |
| `exchange` | String | Exchange: `SZSE`, `SSE`, `BSE` |
| `curr_type` | String | Currency type (typically `CNY`) |
| `list_status` | String | Listing status: `L`=Listed, `D`=Delisted, `P`=Paused |
| `list_date` | String | Listing date (`YYYYMMDD` format) |
| `delist_date` | String | Delisting date (`YYYYMMDD`, null if active) |
| `is_hs` | String | HK/SZ connect eligibility: `N`=No, `H`=HK, `S`=Stock Connect |
| `act_name` | String | Actual controller name |
| `act_ent_type` | String | Actual controller entity type |

**Exchange Suffix Mapping**

| Suffix | Exchange | Market |
| :--- | :--- | :--- |
| `.SH` | SSE | Main board, STAR Market |
| `.SZ` | SZSE | Main board, ChiNext |
| `.BJ` | BSE | Beijing Stock Exchange |

---

## 5. Market Data

### 5.1 Daily Prices (`stock_daily_cn`)

**Tushare Interface:** `daily`

**File Pattern:** `tushare/stock_daily_cn/date=<YYYY-MM-DD>/daily_<timestamp>.jsonl.gz`

**Input Parameters**

| Parameter | Required | Description |
| :--- | :--- | :--- |
| `ts_code` | No | Stock code (e.g., `000001.SZ`); if empty, returns all stocks |
| `trade_date` | No | Trade date (`YYYYMMDD`); if empty, requires date range |
| `start_date` | No | Start date for range query (`YYYYMMDD`) |
| `end_date` | No | End date for range query (`YYYYMMDD`) |

**Schema**

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Tushare code (e.g., `000001.SZ`) |
| `trade_date` | String | Trade date (`YYYYMMDD`) |
| `open` | Float | Opening price (CNY) |
| `high` | Float | Highest price (CNY) |
| `low` | Float | Lowest price (CNY) |
| `close` | Float | Closing price (CNY) |
| `pre_close` | Float | Previous closing price (CNY) |
| `change` | Float | Price change (CNY) |
| `pct_chg` | Float | Percentage change (%) |
| `vol` | Float | Volume (in **shares**) |
| `amount` | Float | Turnover (in **CNY**) |

**Note:** Tushare daily data is **unadjusted** (不复权). Adjusted prices require separate `adj_factor` data.

---

### 5.2 Daily Valuation Metrics (`stock_daily_basic_cn`)

**Tushare Interface:** `daily_basic`

**File Pattern:** `tushare/stock_daily_basic_cn/date=<YYYY-MM-DD>/daily_basic_<timestamp>.jsonl.gz`

**Schema**

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Tushare code |
| `trade_date` | String | Trade date (`YYYYMMDD`) |
| `close` | Float | Closing price |
| `turnover_rate` | Float | Turnover rate (%) - total shares |
| `turnover_rate_f` | Float | Turnover rate (%) - free float |
| `volume_ratio` | Float | Volume ratio |
| `pe` | Float | Price-to-earnings ratio (TTM) |
| `pe_ttm` | Float | PE ratio (trailing twelve months) |
| `pb` | Float | Price-to-book ratio |
| `ps` | Float | Price-to-sales ratio |
| `ps_ttm` | Float | PS ratio (TTM) |
| `dv_ratio` | Float | Dividend yield (%) |
| `dv_ttm` | Float | Dividend yield TTM (%) |
| `total_share` | Float | Total shares outstanding |
| `float_share` | Float | Float shares (A-share tradable) |
| `free_share` | Float | Free float shares |
| `total_mv` | Float | Total market value (CNY) |
| `circ_mv` | Float | Circulating market value (CNY) |

---

### 5.3 Adjustment Factors (`adj_factor`)

**Tushare Interface:** `adj_factor`

**Purpose:** Provides adjustment factors for split/dividend adjustments.

**Schema**

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Tushare code |
| `trade_date` | String | Trade date |
| `adj_factor` | Float | Cumulative adjustment factor |

**Usage for Forward Adjustment (前复权):**
```
adjusted_price = raw_price * adj_factor
```

---

## 6. Financial Data

### 6.1 Financial Indicators (`fina_indicator_cn`)

**Tushare Interface:** `fina_indicator`

**File Pattern:** `tushare/fina_indicator_cn/end_date=<YYYY-MM-DD>/fina_indicator_<timestamp>.jsonl.gz`

**Update Frequency:** Quarterly (after earnings release)

**Schema (Selected Fields)**

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Tushare code |
| `ann_date` | String | Announcement date (`YYYYMMDD`) |
| `end_date` | String | Reporting period end (`YYYYMMDD`) |
| `eps` | Float | Basic earnings per share |
| `dt_eps` | Float | Diluted EPS |
| `total_revenue_ps` | Float | Total revenue per share |
| `revenue_ps` | Float | Operating revenue per share |
| `capital_rese_ps` | Float | Capital reserve per share |
| `surplus_rese_ps` | Float | Surplus reserve per share |
| `undist_profit_ps` | Float | Undistributed profit per share |
| `extra_item` | Float | Extraordinary items |
| `profit_dedt` | Float | Profit after deducting non-recurring items |
| `gross_margin` | Float | Gross margin |
| `current_ratio` | Float | Current ratio |
| `quick_ratio` | Float | Quick ratio |
| `cash_ratio` | Float | Cash ratio |
| `invturn_days` | Float | Inventory turnover days |
| `arturn_days` | Float | Accounts receivable turnover days |
| `inv_turn` | Float | Inventory turnover |
| `ar_turn` | Float | Accounts receivable turnover |
| `ca_turn` | Float | Current assets turnover |
| `fa_turn` | Float | Fixed assets turnover |
| `assets_turn` | Float | Total assets turnover |
| `op_income` | Float | Operating income |
| `ebit` | Float | Earnings before interest and tax |
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
| `eps_yoy` | Float | EPS year-over-year growth (%) |
| `dt_eps_yoy` | Float | Diluted EPS YoY growth (%) |
| `revenue_yoy` | Float | Revenue YoY growth (%) |
| `operate_profit_yoy` | Float | Operating profit YoY growth (%) |
| `netprofit_yoy` | Float | Net profit YoY growth (%) |
| `roe` | Float | Return on equity (%) |
| `roe_dt` | Float | ROE after deducting non-recurring items (%) |
| `roa` | Float | Return on assets (%) |
| `npta` | Float | Net profit/total assets (%) |
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
| `ocf_to_revenue` | Float | Operating cash flow to revenue |
| `ocf_to_operate_profit` | Float | OCF to operating profit |

---

### 6.2 PIT (Point-in-Time) Considerations

**Critical distinction for financial data:**

| Date Field | Meaning | Usage |
| :--- | :--- | :--- |
| `end_date` | End of reporting period (e.g., 2023-12-31) | Identifies which quarter/year |
| `ann_date` | Date when data was publicly announced | **Use for PIT correctness** |

**Example:**
- `end_date` = 2023-12-31 (Q4 2023 data)
- `ann_date` = 2024-04-30 (announced in April 2024)
- For backtests as of 2024-03-15, **do not use** this Q4 data (not yet announced)

---

## 7. Market Behavior Data

### 7.1 Money Flow (`moneyflow_cn`)

**Tushare Interface:** `moneyflow`

**Schema**

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `ts_code` | String | Tushare code |
| `trade_date` | String | Trade date |
| `buy_sm_amount` | Float | Small buy amount (< 4 CNY) |
| `buy_sm_vol` | Float | Small buy volume |
| `sell_sm_amount` | Float | Small sell amount |
| `sell_sm_vol` | Float | Small sell volume |
| `buy_md_amount` | Float | Medium buy amount (4-20 CNY) |
| `buy_md_vol` | Float | Medium buy volume |
| `sell_md_amount` | Float | Medium sell amount |
| `sell_md_vol` | Float | Medium sell volume |
| `buy_lg_amount` | Float | Large buy amount (20-100 CNY) |
| `buy_lg_vol` | Float | Large buy volume |
| `sell_lg_amount` | Float | Large sell amount |
| `sell_lg_vol` | Float | Large sell volume |
| `buy_elg_amount` | Float | Extra-large buy amount (> 100 CNY) |
| `buy_elg_vol` | Float | Extra-large buy volume |
| `sell_elg_amount` | Float | Extra-large sell amount |
| `sell_elg_vol` | Float | Extra-large sell volume |
| `net_mf_amount` | Float | Net main force amount |
| `net_mf_vol` | Float | Net main force volume |

---

### 7.2 Limit List (`limit_list_cn`)

**Tushare Interface:** `limit_list`

**Schema**

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `trade_date` | String | Trade date |
| `ts_code` | String | Tushare code |
| `name` | String | Stock name |
| `close` | Float | Closing price |
| `pct_chg` | Float | Price change (%) |
| `amp` | Float | Amplitude (%) |
| `fc_ratio` | Float | Float capitalization ratio |
| `fl_ratio` | Float | Float ratio |
| `fd_amount` | Float | Float amount |
| `first_time` | String | First limit-up/down time (`HHMMSS`) |
| `last_time` | String | Last limit-up/down time (`HHMMSS`) |
| `open_times` | Integer | Number of times limit was broken |
| `strth` | Float | Strength indicator |
| `limit` | String | Limit type: `U`=Up, `D`=Down |

---

## 8. Exchange-Specific Differences

| Feature | SSE | SZSE | BSE |
| :--- | :--- | :--- | :--- |
| **Tushare Suffix** | `.SH` | `.SZ` | `.BJ` |
| **Code Range** | 600000-699999, 680000-689999 | 000001-099999, 300000-309999 | 430000-899999 |
| **Main Board** | ✅ | ✅ | ✅ |
| **Growth Board** | 科创板 (STAR) | 创业板 (ChiNext) | - |
| **Price Limits** | ±10% (±20% for STAR) | ±10% (±20% for ChiNext) | ±30% |
| **Lot Size** | 100 shares | 100 shares | 100 shares |

---

## 9. Timestamp and Date Reference

### 9.1 Timezone

All dates use **China Standard Time (CST, UTC+8)**.

### 9.2 Date Formats

| Context | Format | Example |
| :--- | :--- | :--- |
| API parameters | `YYYYMMDD` | `20240102` |
| File partitioning | `YYYY-MM-DD` | `2024-01-02` |
| Bronze storage | `YYYY-MM-DD` | `date=2024-01-02` |

### 9.3 Update Schedule

| Data Type | Typical Update Time | Notes |
| :--- | :--- | :--- |
| Daily prices | 17:00-19:00 CST | After market close |
| Daily metrics | 19:00-21:00 CST | Depends on exchange publication |
| Financial data | Quarterly (within 1 month of quarter end) | Varies by company |
| Reference data | Daily early morning | Company actions, listings |

---

## 10. Data Comparison with Primary Feeds

| Aspect | Tushare (Secondary) | Quant360 (Primary) |
| :--- | :--- | :--- |
| **Source** | Aggregated, processed | Exchange-native feed |
| **Latency** | T+0 (EOD) | Real-time/Intraday |
| **Granularity** | Daily bars, fundamentals | Tick-by-tick, order book |
| **Adjustment** | Requires adj_factor | Unadjusted native |
| **PIT correctness** | Requires ann_date handling | Native timestamps |
| **Use case** | Fundamental research, screening | HFT, microstructure |

---

## Appendix: Symbol Code Reference

### SSE Code Ranges

| Range | Description |
| :--- | :--- |
| 600000-699999 | Main board A-shares |
| 680000-689999 | STAR Market (科创板) |
| 510000-519999 | ETFs |
| 500000-509999 | Closed-end funds |

### SZSE Code Ranges

| Range | Description |
| :--- | :--- |
| 000001-009999 | Main board |
| 300000-309999 | ChiNext (创业板) |
| 159000-159999 | ETFs |

### BSE Code Ranges

| Range | Description |
| :--- | :--- |
| 430000-899999 | BSE-listed stocks |
| 820000-899999 | BSE Innovation Layer |
| 435000-489999 | BSE Base Layer |
