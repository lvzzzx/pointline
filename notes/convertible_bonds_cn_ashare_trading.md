# Chinese Convertible Bonds (可转债) Trading Reference

> Date: 2026-02-18
> Source: Research from project skills (quant360) + market analysis

---

## Executive Summary

**Key Advantage: T+0 Trading** ✅ — 可转债 is one of the few true T+0 instruments in Chinese A-share markets.

| Feature | Stocks | 可转债 |
|---------|--------|--------|
| **Settlement** | T+1 | **T+0** |
| **Price Limits** | ±10% / ±20% | **First day: +57.3% / -43.3%**<br>**After: ±20%** |
| **Short Selling** | Limited (融券) | **Not available** |
| **Intraday Round-trip** | ❌ Not possible | ✅ **Possible** |
| **Data Availability** | L2/L3 | **L2/L3 available** (Quant360 ConFI) |

---

## Trading Rules

### Basic Specifications

| Rule | Detail |
|------|--------|
| **Trading Hours** | 09:30-11:30, 13:00-15:00 (same as stocks) |
| **Settlement** | **T+0** — same-day buy and sell allowed |
| **Tick Size** | 0.01 CNY |
| **Lot Size** | 10 bonds (1 lot = 10 units) |
| **Face Value** | 100 CNY per bond |

### Price Limit Rules (涨跌幅限制)

| Period | Upper Limit | Lower Limit | Circuit Breaker |
|--------|-------------|-------------|-----------------|
| **First Trading Day** | +57.3% | -43.3% | Halt at +20% (30 min)<br>Halt at +30% (to 14:57) |
| **Day 2+** | +20% | -20% | Same as ChiNext |

### Circuit Breaker Details (Day 1)

| Trigger | Action |
|---------|--------|
| Price reaches ±20% | Trading halt for 30 minutes |
| Price reaches ±30% | Trading halt until 14:57 |
| After 14:57 | Resume trading until close |

---

## Key Formulas

### Conversion Metrics

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| **转股价值 (Conversion Value)** | `(100 / 转股价) × 正股价` | Value if converted to stock now |
| **转股溢价率 (Conversion Premium)** | `(可转债价格 - 转股价值) / 转股价值 × 100%` | Premium over conversion value |
| **纯债价值 (Bond Floor)** | `PV(coupons + principal)` | Value as straight bond |
| **纯债溢价率 (Bond Premium)** | `(可转债价格 - 纯债价值) / 纯债价值 × 100%` | Premium over bond floor |
| **双低得分 (Double-Low Score)** | `可转债价格 + 10 × 转股溢价率` | Lower = better value |

### Example Calculation

```
Bond Price: 120 CNY
转股价: 25 CNY
正股价: 30 CNY

转股价值 = (100 / 25) × 30 = 120 CNY
转股溢价率 = (120 - 120) / 120 = 0%

→ Bond priced at par to conversion value
```

---

## Statistical Arbitrage Strategies

### Strategy 1: Bond-Stock Pair Trading (债券-正股配对交易)

**Mechanism:**
```
Long 可转债 + Short 正股 (via 融券)
→ Capture conversion premium convergence
```

**Logic:**
- 可转债 has embedded call option on underlying stock
- Premium expands/contracts based on volatility expectations
- When premium is extreme → convergence trade opportunity

**Trading Signals:**

| Premium Level | Signal | Action |
|---------------|--------|--------|
| > 30% | Bond expensive | Short bond / Long stock |
| 10-30% | Normal range | No trade |
| < 0% (negative) | Bond cheap | Long bond / Short stock |
| < -5% | Deep discount | Strong buy signal |

**Constraints:**
- Short stock leg requires 融券 access (severely limited as of 2024-2025)
- Alternative: Trade directionally on bond only (long cheap bonds)

---

### Strategy 2: 双低轮动 (Double-Low Rotation)

**Mechanism:**
```
Select bonds with:
  1. Low bond price (< 115 CNY)
  2. Low conversion premium (< 30%)
→ Rank by: Price + 10 × Premium
→ Hold 10-20 bonds, rotate monthly/quarterly
```

**Why it works:**
- Low price = downside protection (bond floor at ~100 CNY)
- Low premium = equity-like upside participation
- Historically generated 15-25% annual returns with lower volatility than stocks

**Selection Criteria:**

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| Bond Price | < 115 CNY | Bond floor protection |
| Premium | < 30% | Equity participation |
| Double-Low Score | < 150 | Combined metric |
| Time to Maturity | > 1 year | Avoid redemption pressure |
| Credit Rating | AA or above | Default risk management |
| Daily Turnover | > 10M CNY | Liquidity filter |

**T+0 Enhancement:**
- Use intraday signals to time entries/exits within rotation
- Exit bonds hitting price limits or showing abnormal volume
- Rebalance same day if signal changes

---

### Strategy 3: Intraday Momentum / Mean Reversion

**Mechanism:**
```
T+0 intraday trading on single bonds:
  - Momentum: Follow strong directional moves
  - Mean reversion: Fade extreme moves
  - Volume breakouts: Trade on unusual volume spikes
```

**Advantages over stocks:**
- ✅ **No overnight risk** — can flatten before close
- ✅ **Higher volatility** — ±20% limits create bigger moves
- ✅ **T+0 flexibility** — true intraday round-trips
- ✅ **Retail participation** — similar behavioral patterns as stocks

**Target Universe:**

| Criteria | Threshold | Purpose |
|----------|-----------|---------|
| Issuance Size | > 1B CNY | Liquidity |
| Daily Turnover | > 50M CNY | Tradability |
| Sector Alignment | Hot themes (AI, chips, EV) | Momentum potential |
| Premium | 10-40% | Active trading range |

**Signal Framework:**

```python
# 5-min momentum signal
momentum_score = rank(bond_5min_return)  # within universe

# Volume confirmation
volume_spike = current_volume / avg_volume_20d

# Proximity to limits (mean reversion)
distance_to_upper = (upper_limit_price - current_price) / current_price
distance_to_lower = (current_price - lower_limit_price) / current_price

# Combined signal
signal = momentum_score * volume_spike * (1 - min(distance_to_limit))
```

---

### Strategy 4: New Listing Arbitrage (打新)

**Mechanism:**
```
Participate in primary market issuance
→ Sell on first day of trading
→ Capture first-day premium
```

**Requirements (Retail):**
- Hold 正股市值 for 20-day average (市值配售)
- Winning rate varies by issuance popularity

**Status (2025):**
- **Winning rate:** Low (< 5% for popular issues)
- **First-day performance:** Variable, some bonds drop below par recently
- **Risk:** No longer "risk-free" — market efficiency has reduced edge

**Verdict:** Not recommended as primary strategy; treat as incidental bonus.

---

### Strategy 5: Event-Driven Arbitrage

**Trigger Events:**

| Event | Strategy | Expected Move |
|-------|----------|---------------|
| **下修转股价** | Buy before announcement | Bond price typically rises |
| **强赎公告** | Sell or convert | Bond converges to par |
| **回售期临近** | Buy below回售价 | Price floor protection |
| **正股涨停** | Buy bond for exposure | Bond follows with lag |

**Key Dates to Monitor:**
- 下修转股价股东大会日期
- 强赎触发条件满足日
- 回售起始日
- 到期兑付日

---

## Practical Strategy Template: 可转债 T+0 Momentum

**For traders without 融券 access:**

```python
# Universe Selection
universe = bonds where:
    - daily_turnover > 50M CNY
    - bond_price between 100 and 130
    - premium between 5% and 40%
    - underlying_stock not limit-locked

# Entry Signal (every 5 minutes)
for bond in universe:
    momentum = return_5min(bond)
    volume_spike = volume_current / avg_volume_20d
    rank_score = rank(momentum) * rank(volume_spike)
    
entry_candidates = top 3 by rank_score

# Execution
for candidate in entry_candidates:
    if rank_score > threshold:
        buy(candidate, position_size=portfolio/3)

# Exit Rules (T+0 mandatory)
for position in positions:
    if unrealized_pnl < -2%:
        sell(position)  # Stop loss
    elif unrealized_pnl > +3%:
        sell(position)  # Profit take
    elif time > 14:50:
        sell(position)  # Time stop (flatten EOD)
    elif bond_price >= upper_limit * 0.99:
        sell(position)  # Near limit-lock

# Risk Controls
- Max 3 positions simultaneously
- No overnight holding (mandatory T+0)
- Skip if 正股 is limit-locked
- Skip if bond premium > 50% (bubble risk)
```

**Expected Characteristics:**

| Metric | Expected Range |
|--------|----------------|
| Turnover | 100% daily |
| Win rate | 45-55% |
| Reward/Risk | 1.5:1 |
| Cost | 20-30 bps round-trip |
| Capacity | Medium (limited by bond liquidity) |

---

## Data Availability

可转债 L2/L3 data is available via **Quant360**:

| Data Type | File Pattern | Exchange | Notes |
|-----------|--------------|----------|-------|
| Order stream | `order_new_ConFI_SH/SZ_<date>.7z` | SSE & SZSE | Same schema as stocks |
| Trade stream | `tick_new_ConFI_SH/SZ_<date>.7z` | SSE & SZSE | Same schema as stocks |
| L2 snapshots | `L2_new_ConFI_SZ_<date>.7z` | SZSE only | 10-level book |

**Key Fields:**
- Same as stock data (see quant360 skill)
- Symbol codes: 6-digit numeric (e.g., `113001`, `128001`)
- Exchange suffix in filename determines SH vs SZ

---

## Comparison: 可转债 vs Other T+0 Instruments

| Factor | 可转债 | ETFs | Futures | Options |
|--------|--------|------|---------|---------|
| **Universe size** | ~500 bonds | ~50 liquid | 4 main contracts | Multiple |
| **Volatility** | High (±20%) | Medium | High | Very High |
| **Liquidity** | Variable | Consistent | Deep | Moderate |
| **Leverage** | Implicit | None | 5-8x | High |
| **Downside protection** | Bond floor (~100) | None | None | None |
| **Complexity** | High (conversion math) | Low | Medium | High |
| **Short selling** | ❌ No | Via 融券 | ✅ Yes | ✅ Yes |
| **Retail access** | ✅ Full | ✅ Full | ✅ Qualified | ✅ Qualified |

**Best use cases:**
- **可转债:** T+0 with downside protection, volatility harvesting
- **ETFs:** Index-level exposure, lowest complexity
- **Futures:** Hedging, directional leverage
- **Options:** Volatility trading, defined risk strategies

---

## Key Metrics Dashboard

### Market-Level Indicators

| Metric | Normal Range | Extreme | Signal |
|--------|--------------|---------|--------|
| **Average Premium** | 25-40% | > 50% | Market overvalued |
| **Average Price** | 110-125 CNY | > 135 CNY | Bubble risk |
| **Turnover Ratio** | 5-15% | > 25% | High speculation |
| **New Issuance** | 20-50/month | < 10/month | Supply constraint |

### Bond-Level Indicators

| Metric | Formula | Trading Signal |
|--------|---------|----------------|
| **转股溢价率** | `(Price - ConvValue) / ConvValue` | Mean reversion when >40% or <0% |
| **纯债溢价率** | `(Price - BondFloor) / BondFloor` | Overvaluation when >30% |
| **双低得分** | `Price + 10 × Premium` | Primary selection criteria |
| **隐含波动率** | From BSM model | Rich when >50%, cheap when <20% |
| **正股相关性** | Rolling 20-day correlation | Breakdown = opportunity |
| **Delta** | `∂Price/∂Stock` | Position sizing |
| **Gamma** | `∂²Price/∂Stock²` | Risk management |

---

## Risk Management

### Specific Risks to 可转债

| Risk | Description | Mitigation |
|------|-------------|------------|
| **强赎风险** | Issuer calls bond, forces conversion | Monitor强赎触发条件 |
| **下修失败** | 转股价下修 proposal rejected | Check shareholder meeting dates |
| **信用风险** | Issuer default (rare but increasing) | Filter AA+ and above |
| **流动性风险** | Wide spreads in small bonds | ADV > 10M CNY filter |
| **正股暴跌** | Bond follows stock down | Bond floor limits downside |
| **条款博弈** | Complex issuer behavior | Stay informed on proposals |

### Position Management

```python
# Pre-trade checks
if bond_price > 140:
    skip("Too far above par")
if premium > 60:
    skip("Extreme premium")
if underlying_stock near_limit_down:
    skip(" contagion risk")
if days_to_maturity < 90:
    skip("Redemption pressure")

# Position sizing
max_position = min(
    portfolio_value * 0.05,  # 5% max per bond
    daily_volume * 0.01       # 1% of ADV max
)
```

---

## Summary: Strategy Feasibility

| Strategy | Feasibility | Holding Period | Complexity | Key Requirement |
|----------|-------------|----------------|------------|-----------------|
| **T+0 intraday momentum** | ⭐⭐⭐⭐⭐ | Intraday | Medium | L2 data, fast execution |
| **双低轮动** | ⭐⭐⭐⭐ | 1-4 weeks | Low | Screening system |
| **Bond-stock pairs** | ⭐⭐⭐ | 1-3 days | High | 融券 access |
| **Event-driven** | ⭐⭐⭐⭐ | Days to weeks | High | Information edge |
| **打新套利** | ⭐⭐ | 1 day | Low | 市值配售资格 |
| **Volatility arb** | ⭐⭐⭐ | Intraday | Very High | Options pricing model |

---

## Bottom Line

**可转债 is one of the best T+0 vehicles in Chinese markets** because:

1. **True T+0** — same-day round-trips without restrictions
2. **Higher volatility** — ±20% limits vs ±10% for main board stocks
3. **Downside protection** — bond floor (~100 CNY) limits worst-case loss
4. **Rich data** — L2/L3 available via Quant360
5. **No 融券 required** — for directional strategies

**Best for:**
- Traders wanting T+0 flexibility without 融券 access
- Those comfortable with conversion math and optionality
- Volatility harvesting strategies

**Not ideal for:**
- Pure short selling (not available)
- Long-term buy-and-hold (better in stock form)
- Risk-averse investors (volatility can be extreme)

---

## References

- Project skills: `quant360` (ConFI data format)
- Exchange rules: SSE/SZSE 可转债交易实施细则 (2025)
- Market data: Quant360 ConFI data feed

---

## Appendix: Common 可转债 Codes

| Code Range | Exchange | Notes |
|------------|----------|-------|
| 110xxx | SSE | Main board convertible bonds |
| 113xxx | SSE | Newer issuance |
| 127xxx | SZSE | Main board convertible bonds |
| 128xxx | SZSE | ChiNext convertible bonds |
| 123xxx | SZSE | Newer issuance |

---

*Last updated: 2026-02-18*
