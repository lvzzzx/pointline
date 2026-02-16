# T+1 Rotation Strategy: Dataset Requirements

> Comprehensive data requirements for implementing a T+1 rotation strategy in CN A-shares, organized by component and implementation tier.

---

## Executive Summary

| Implementation Tier | Data Sources | Cost Estimate | Sharpe Potential |
|---------------------|--------------|---------------|------------------|
| **Minimum Viable** | Daily bars + Tushare fundamentals | ¥0-3k/year | 0.8-1.2 |
| **Standard** | L2 snapshots + aggregated features | ¥50-150k/year | 1.2-1.8 |
| **Professional** | L3 order-by-order + real-time execution | ¥200-500k/year | 1.5-2.5 |

---

## 1. Stock Selection Model (Slow-Decay Features)

### 1.1 Daily Price/Volume Data (Required - All Tiers)

| Field | Description | Frequency | Source |
|-------|-------------|-----------|--------|
| `open`, `high`, `low`, `close` | OHLC prices | Daily | Tushare, AkShare, vendor |
| `volume` | Trading volume (shares) | Daily | Tushare, AkShare |
| `amount` | Trading amount (RMB) | Daily | Tushare, AkShare |
| `turnover_ratio` | Turnover percentage | Daily | Tushare |
| `adj_factor` | Adjustment factor (splits/dividends) | Daily | Tushare |

**Derived Features:**
```python
# Return-based features
features['return_1d'] = close_t / close_t_1 - 1
features['return_5d'] = close_t / close_t_5 - 1
features['return_20d'] = close_t / close_t_20 - 1
features['volatility_20d'] = std(returns, 20) * sqrt(252)

# Volume-based features
features['volume_zscore'] = (volume - volume_ma20) / volume_std20
features['turnover_anomaly'] = turnover / turnover_ma20
```

### 1.2 Aggregated Microstructure Features (Standard+ Tier)

Requires **L2 snapshot data** (3-second or 1-second intervals) to compute:

| Feature | Calculation | Decay Horizon |
|---------|-------------|---------------|
| `session_imbalance` | Cumulative (buy_volume - sell_volume) / total_volume | 1-3 days |
| `late_session_imbalance` | Imbalance during 14:30-15:00 | <1 day |
| `closing_auction_pressure_szse` | Order imbalance at 14:57-15:00 (SZSE only) | <1 day |
| `spread_regime` | Average bid-ask spread as % of mid | 1-2 days |
| `vpin_daily` | Volume-synchronized probability of informed trading | 1-3 days |
| `kyle_lambda_daily` | Price impact coefficient (daily regression) | 1-5 days |
| `large_trade_ratio` | Volume from trades > ¥100k / total volume | 1-3 days |
| `order_to_cancel_ratio` | Order submissions / cancellations | 1-2 days |

**Data Source:**
- Quant360 L2 CSV files (historical)
- Exchange L2 feeds via vendor (real-time)
- Self-aggregated from L2 snapshots

### 1.3 Fundamental Data (Required - All Tiers)

| Category | Fields | Frequency | Source |
|----------|--------|-----------|--------|
| **Earnings** | EPS, net_profit, revenue, SUE | Quarterly | Tushare `fina_indicator` |
| **Analyst** | Rating changes, target price revisions | Event | Tushare `report_rc`, `report_fp` |
| **Valuation** | PE, PB, PS, PCF | Daily | Tushare `daily_basic` |
| **Growth** | YoY growth rates, QoQ changes | Quarterly | Tushare `fina_indicator` |
| **Ownership** | Float shares, total shares, free float | Daily | Tushare `stk_holdernumber` |

**Derived Features:**
```python
# Earnings surprise
features['sue'] = (actual_eps - expected_eps) / std_eps_estimates

# Analyst revision
features['revision_ratio'] = upgrades / (upgrades + downgrades)
features['target_price_gap'] = (target_price - close) / close

# Earnings momentum
features['earnings_accel'] = sue_t - sue_t_1
```

### 1.4 Cross-Sectional / Alternative Data (Standard+ Tier)

| Feature | Description | Frequency | Source |
|---------|-------------|-----------|--------|
| `sector_return` | Industry/sector index return | Daily | Tushare `index_daily` |
| `sector_rank` | Stock's return rank within sector | Daily | Computed |
| `market_cap` | Free float market cap | Daily | Tushare `daily_basic` |
| `northbound_holdings` | HKEX connect holdings | Daily | Tushare `hk_hold` |
| `northbound_flow` | Daily net buy/sell (pre-2024) | Daily | Tushare `moneyflow_hsgt` |
| `margin_balance` | 融资余额 | Daily | Tushare `margin` |
| `short_balance` | 融券余额 | Daily | Tushare `margin` |
| `abnormal_volume_alert` | Exchange unusual trading flag | Event | Exchange announcements |

**Note:** Real-time northbound flow data was discontinued in 2024; only aggregated holdings available now.

---

## 2. Entry Timing Model (Fast-Decay Features)

### 2.1 L2 Order Book Data (Standard+ Tier)

**Required for each stock, intraday:**

| Field | Description | Frequency |
|-------|-------------|-----------|
| `timestamp` | Exchange timestamp | 3s or 1s snapshots |
| `bid_price_1..5` | Best 5 bid prices | Per snapshot |
| `ask_price_1..5` | Best 5 ask prices | Per snapshot |
| `bid_volume_1..5` | Best 5 bid volumes | Per snapshot |
| `ask_volume_1..5` | Best 5 ask volumes | Per snapshot |
| `total_volume` | Cumulative volume | Per snapshot |
| `total_amount` | Cumulative amount | Per snapshot |
| `weighted_avg_price` | VWAP so far | Per snapshot |

**Derived Features:**
```python
# Spread and liquidity
features['spread_bps'] = (ask_1 - bid_1) / mid * 10000
features['spread_5level_bps'] = (ask_5 - bid_5) / mid * 10000
features['depth_imbalance'] = (bid_vol_1_5 - ask_vol_1_5) / (bid_vol_1_5 + ask_vol_1_5)

# Book pressure
features['book_slope_bid'] = (bid_1 - bid_5) / bid_vol_2_5
features['book_slope_ask'] = (ask_5 - ask_1) / ask_vol_2_5

# Volume profile
features['volume_rate'] = volume_last_5min / volume_same_time_avg
features['trade_intensity'] = trade_count_last_5min / time_window
```

### 2.2 Trade/Tick Data (Professional Tier - L3)

**Order-by-order execution data:**

| Field | Description |
|-------|-------------|
| `trade_time` | Execution timestamp (ms precision) |
| `trade_price` | Execution price |
| `trade_volume` | Volume (shares) |
| `trade_type` | Buy initiative / Sell initiative / Unknown |
| `order_ref` | Reference to resting order (if available) |

**Derived Features:**
```python
# Trade flow
features['buy_sell_imbalance'] = (buy_volume - sell_volume) / total_volume
features['large_trade_ratio'] = large_trade_volume / total_volume
features['trade_size_trend'] = slope(avg_trade_size, last_30min)

# Price impact
features['lambda_kyle'] = delta_price / signed_volume
features['vpin_intraday'] = compute_vpin(trades, volume_buckets=50)
```

### 2.3 Intraday Volume Profile (Standard+ Tier)

| Feature | Description |
|---------|-------------|
| `intraday_volume_curve` | Historical average volume by time-of-day |
| `volume_participation_rate` | Current volume / historical average |
| `liquidity_score` | Composite of spread, depth, and recent volume |

---

## 3. Exit Timing Model (Gap Management)

### 3.1 Opening Auction Data (Standard+ Tier)

| Field | Description | Source |
|-------|-------------|--------|
| `auction_start_price` | 09:15 opening reference | L2 feed |
| `auction_volumes` | Cumulative auction volume by 09:25 | L2 feed |
| `auction_imbalance` | Buy/sell imbalance at 09:25 | Computed from L2 |
| `overnight_gap` | (open - prev_close) / prev_close | Computed |

### 3.2 Pre-Market / Overnight Data (Standard+ Tier)

| Feature | Description | Frequency |
|---------|-------------|-----------|
| `overnight_news_sentiment` | News/announcements overnight | Event |
| `adr_premium` | ADR premium (for dual-listed) | Pre-market |
| `futures_premarket` | CSI300/500 futures 09:15-09:30 | Real-time |
| `global_markets` | US markets, Asian markets overnight | Daily |

### 3.3 Morning Session L2 (Standard+ Tier)

Same as Entry Timing L2 data, but for 09:30-10:30 window:
- Opening spread dynamics
- Initial volume surge
- Order flow in first 30 minutes
- Gap-fill detection

---

## 4. Risk Management & Execution

### 4.1 Reference Data (Required - All Tiers)

| Data | Description | Source |
|------|-------------|--------|
| `universe_constituents` | CSI 300/500/1000 constituents | Tushare `index_weight` |
| `sector_classifications` | SW industry, Shenwan sectors | Tushare `stock_basic` |
| `price_limits` | Daily limit up/down prices | Computed (±10%, ±20% for STAR) |
| `float_shares` | Float for ADV calculation | Tushare `daily_basic` |
| `adv_20d` | 20-day average daily volume | Computed |
| `st_status` | ST/*ST flag | Tushare `stock_basic` |

### 4.2 Index Futures Data (Hedging)

| Instrument | Code | Data Needed |
|------------|------|-------------|
| CSI 300 Futures | IF | OHLCV, basis, open interest |
| SSE 50 Futures | IH | OHLCV, basis, open interest |
| CSI 500 Futures | IC | OHLCV, basis, open interest |
| CSI 1000 Futures | IM | OHLCV, basis, open interest |

**Source:** Tushare `fut_daily`, real-time via CTP/ vendor

### 4.3 融券 (Securities Lending) Data (If Available)

| Field | Description | Source |
|-------|-------------|--------|
| `borrowable_shares` | Available short inventory | Broker API |
| `lending_rate` | Annualized borrow cost | Broker API |
| `utilization_rate` | Shares borrowed / total available | Exchange stats |

---

## 5. Data Requirements by Implementation Tier

### Tier 1: Minimum Viable Product (MVP)

**Goal:** Validate stock selection alpha with minimal cost

**Required Data:**
```yaml
Daily_Bars:
  - OHLCV for CSI 500 constituents
  - 3 years history minimum

Fundamentals:
  - Quarterly earnings (EPS, revenue, net_profit)
  - Daily valuation metrics (PE, PB)
  - Source: Tushare (free tier or basic subscription)

Reference:
  - Sector classifications
  - Index constituents
  - Price limit rules

Cost: ¥0 - 3,000/year
Storage: < 10 GB
Update: Daily batch (after 15:00)
```

**Limitations:**
- No intraday timing optimization
- No microstructure features
- Higher slippage assumption required (+20-30bps)

**Expected Performance:** Sharpe 0.8-1.2 (if genuine alpha exists)

---

### Tier 2: Standard Implementation

**Goal:** Full T+1 rotation with entry/exit optimization

**Required Data (in addition to Tier 1):**
```yaml
L2_Snapshots:
  - 3-second snapshots for CSI 500
  - 10am-11:30am, 1pm-3pm (trading hours)
  - 2 years history for feature engineering
  - Source: Quant360, Wind, or exchange vendor

Aggregated_Features:
  - Daily aggregated order flow
  - Closing auction imbalance
  - Session-level spread statistics
  - VPIN estimates

Intraday_Profile:
  - Historical volume curves
  - Liquidity scores by time-of-day

Cost: ¥50,000 - 150,000/year
Storage: 100-500 GB
Update: Daily batch + real-time L2
```

**Capabilities:**
- Entry timing optimization (5-15bps improvement)
- Exit timing based on morning L2 (3-10bps improvement)
- Daily microstructure aggregates as features

**Expected Performance:** Sharpe 1.2-1.8

---

### Tier 3: Professional Implementation

**Goal:** Full-featured with L3 precision and real-time execution

**Required Data (in addition to Tier 2):**
```yaml
L3_Order_By_Order:
  - Full order book updates
  - Trade-by-trade execution data
  - Order type and aggressor side
  - Source: Exchange direct feed (expensive)

Real_Time_Feeds:
  - Level 2 streaming (WebSocket/TCP)
  - Index futures tick data
  - News/event feed

Advanced_Features:
  - Real-time VPIN
  - Order flow toxicity
  - Hidden order detection
  - Latency-optimized features

Cost: ¥200,000 - 500,000/year
Storage: 1-5 TB
Update: Real-time streaming
```

**Capabilities:**
- Sub-second signal computation
- Microsecond-aware execution
- True order flow alpha
- HFT-level entry timing

**Expected Performance:** Sharpe 1.5-2.5+ (but capacity limited)

---

## 6. Data Schema Recommendations

### 6.1 Feature Store Structure

```
features/
├── daily/                    # Updated once daily
│   ├── stock_features.parquet    # Tidy format: (date, stock_id, feature_name, value)
│   ├── sector_features.parquet
│   └── market_features.parquet
│
├── intraday/                 # Updated intraday (for live trading)
│   ├── l2_snapshots.parquet      # Raw L2 data
│   ├── derived_features.parquet  # Real-time computed features
│   └── signals.parquet           # Model outputs
│
└── reference/                # Static/Slow-changing
    ├── stock_info.parquet
    ├── sector_mapping.parquet
    └── calendar.parquet
```

### 6.2 Recommended Storage Format

| Data Type | Format | Reason |
|-----------|--------|--------|
| Time series (L2/tick) | Parquet + partitioning | Compression, query speed |
| Daily features | Parquet or Feather | Fast I/O |
| Real-time streaming | Arrow/Redis | Low latency |
| Historical backtest | Zarr or HDF5 | Large arrays |

---

## 7. Data Vendor Comparison

| Vendor | Coverage | Cost | Best For |
|--------|----------|------|----------|
| **Tushare** | Daily bars, fundamentals, some L2 | Free-¥5k/year | MVP, research |
| **AkShare** | Similar to Tushare, open source | Free | Prototyping |
| **Wind** | Full L2, fundamentals, news | ¥100k+/year | Professional |
| **Quant360** | L2/L3 historical CSVs | ¥30-100k/year | Backtesting L2 strategies |
| **Exchange Direct** | Real-time L2/L3 | ¥200k+/year | HFT, market making |
| **Bloomberg/Reuters** | Global, fundamentals | $$$ | Institutional |

---

## 8. Key Data Quality Checks

### 8.1 Daily Bars
```python
# Survivorship bias check
assert len(delisted_stocks) > 0, "Must include delisted stocks"

# Corporate actions
dividends = get_dividends(stock)
splits = get_splits(stock)
prices = adjust_prices(prices, dividends, splits)

# Price limits
check_price_within_limits(open, high, low, close, limit_up, limit_down)
```

### 8.2 L2 Data
```python
# Quote consistency
assert all(bid_price < ask_price), "Crossed spread detected"
assert all(bid_price > 0), "Invalid bid price"

# Volume monotonicity
assert volume[t] >= volume[t-1], "Volume decreased"

# Timestamp continuity
assert no_missing_timestamps(timestamps, expected_interval='3s')
```

### 8.3 Fundamentals
```python
# Point-in-time correctness
earnings_date = get_announcement_date(stock, quarter)
feature_value = get_value_as_of(date, earnings_date)

# Avoid lookahead bias
assert feature_date >= announcement_date
```

---

## Summary: Minimum Dataset to Start

If you're starting from scratch, prioritize in this order:

1. **Daily OHLCV** (Tushare free) — absolute minimum
2. **Sector classifications** — for risk management
3. **Quarterly earnings** — SUE is strong T+1 feature
4. **L2 snapshots (3s)** — entry/exit timing optimization
5. **Intraday volume profile** — execution planning
6. **L3 order flow** — only if pursuing professional tier

**Start with Tier 1 (MVP)**, validate stock selection alpha exists, then upgrade to Tier 2 for timing optimization.

---

*Document created for T+1 rotation strategy implementation planning.*
