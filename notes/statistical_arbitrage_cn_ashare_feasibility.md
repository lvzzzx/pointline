# Statistical Arbitrage Feasibility in Chinese A-Share Markets

> Date: 2026-02-18
> Source: Research from project skills (cn-ashare-microstructure-researcher, quant360) + market analysis

---

## Executive Summary

**TL;DR:** Traditional pairs trading? ❌ Hard. Modified Stat Arb? ✅ Yes.

| Stat Arb Type | Feasibility | Key Constraint |
|--------------|-------------|----------------|
| Classic pairs trading (long A / short B) | ⚠️ Very Limited | 融券 access + T+1 |
| ETF-statistical arbitrage | ✅ Feasible | T+0 ETFs available |
| Futures-spot basis arbitrage | ✅ Feasible | Requires futures access |
| Cross-sectional mean reversion | ✅ Feasible | Long-only or hedged |
| Intraday pairs (with 融券) | ⚠️ Regulatory risk | 融券 heavily restricted |

---

## 1. Why Classic Pairs Trading is Difficult

### The Core Problem: Short Selling Constraints

**Traditional pairs trading requires:**
```
Long Stock A + Short Stock B → Profit when spread reverts
```

### Chinese A-Share Constraints

| Constraint | Impact on Pairs Trading |
|------------|------------------------|
| **融券 (securities lending) limited** | Most stocks unavailable for short selling |
| **融券 costs: 8-18% annual** | ~3-7 bps/day just for lending fee |
| **T+1 settlement** | Even with 融券, intraday round-trips are operationally complex |
| **券源 instability** | Borrowed shares can be recalled intraday |

**Result:** Pure pairs trading is only viable for large institutional players with confirmed 融券 access — and even they face regulatory tightening (2023-2025).

---

## 2. Feasible Statistical Arbitrage Strategies

### Strategy 1: ETF Statistical Arbitrage ✅

**Mechanism:**
```
Long 510300 (300ETF) + Short 510050 (50ETF)
→ Trade relative value between CSI 300 and SSE 50
```

**Why it works:**
- ETFs are **T+0 eligible** — true intraday round-trips possible
- No 融券 required for short ETF leg (can sell ETF short via 融券 or use inverse ETF)
- Lower costs: ~15-30 bps round-trip (vs 20-50 bps for 融券T+0 stocks)

**Common ETF Pairs:**

| Long | Short | Relationship |
|------|-------|--------------|
| 510300 (300ETF) | 510050 (50ETF) | Large vs mega-cap |
| 510500 (500ETF) | 510300 (300ETF) | Mid vs large-cap |
| 159919 (深300ETF) | 510300 (沪300ETF) | SZSE vs SSE 300 |
| 510500 (500ETF) | 510050 (50ETF) | Mid vs mega-cap |
| 159922 (500ETF) | 510500 (500ETF) | Cross-market 500 |

**Implementation Notes:**
- **Liquidity check:** Ensure both ETFs trade >100M CNY daily
- **Cointegration testing:** Use Engle-Granger or Johansen tests on log prices
- **Half-life estimation:** Typical ETF pair half-life = 2-5 days
- **Entry threshold:** Z-score > 1.5 for signal, > 2.0 for strong signal
- **Exit threshold:** Z-score mean-reversion to 0.5 or time-based (max 2 days)

---

### Strategy 2: Futures-Spot Basis Arbitrage ✅

**Mechanism:**
```
Long CSI 300 stocks + Short IF futures
→ Capture basis when futures trade at premium/discount to spot
```

**Advantages:**
- **T+0 on futures leg** — can adjust hedge intraday
- **Deep liquidity** — IF/IH/IC/IM futures are actively traded
- **No 融券 needed** for the short exposure

**Available Futures Contracts:**

| Contract | Underlying | Margin | Typical Spread |
|----------|------------|--------|----------------|
| IF | CSI 300 | ~12% | 2-10 bps |
| IH | SSE 50 | ~12% | 2-8 bps |
| IC | CSI 500 | ~14% | 3-15 bps |
| IM | CSI 1000 | ~14% | 5-20 bps |

**Costs:**

| Component | Rate |
|-----------|------|
| Stock round-trip | ~15-30 bps |
| Futures commission | ~1-2 bps |
| Margin cost | ~3-5% annual on notional |
| Basis risk | Variable |

**Net cost:** ~20-40 bps round-trip

**Implementation Notes:**
- **Beta calculation:** Use rolling 60-day beta between stock and index
- **Hedge ratio:** Adjust for beta: `hedge_shares = stock_value * beta / futures_price`
- **Basis monitoring:** Track `futures_price - spot_price` for entry timing
- **Rebalancing:** Adjust hedge intraday as beta drifts

---

### Strategy 3: Cross-Sectional Mean Reversion (Long-Only) ✅

**Mechanism:**
```
Rank stocks by intraday return deviation from sector
→ Buy underperformers (reversion candidates)
→ Hold overnight (T+1 constraint)
→ Sell next day
```

**Why it's feasible:**
- No short selling required
- Exploits retail herding behavior (60-70% of turnover)
- T+1 rotation structure accommodates overnight holding

**Key Features:**

| Feature | Formula | Expected IC |
|---------|---------|-------------|
| Intraday relative strength | `return(stock) - return(sector_index)` | 0.02-0.04 |
| Beta-adjusted return | `return(stock) - beta * return(CSI300)` | 0.02-0.04 |
| Rank return | Percentile rank within sector | 0.02-0.03 |
| Correlated pair spread | Deviation from correlated peer | 0.02-0.05 |

**Expected Performance:**
- IC: 0.02-0.04 for next-day returns
- Turnover: 100% daily
- Cost-adjusted Sharpe: 0.8-1.5 (depending on execution)

**Implementation Notes:**
- **Z-score normalization:** Cross-sectional at each timestamp essential
- **Sector neutralization:** Residualize against sector returns
- **Cost filter:** Only trade if expected alpha > 30 bps
- **Liquidity filter:** Only trade stocks with ADV > 100M CNY

---

### Strategy 4: Intraday Stock Pairs (融券T+0) ⚠️

**Mechanism:**
```
09:30  融券卖出 Stock B (borrow and sell)
10:30  Buy Stock A + Buy Stock B (cover short)
       → Net position flat EOD
```

**Current Access Status (2025):**

| Participant | Access Level |
|-------------|--------------|
| Top-tier quant funds (>10B AUM) | Limited access retained |
| Mid-tier funds | Severely reduced or lost |
| Retail / Small institutional | **Effectively unavailable** |

**Regulatory Timeline:**
- **Oct 2023:** Banned lock-up shares from 融券 lending
- **Jan-Mar 2024:** Restricted quant 融券 volumes
- **Jul 2024:** Enhanced scrutiny on "market fairness"
- **2025+:** Continued tightening trajectory

**Regulatory Direction:**
> Eliminate the T+0 advantage quant has over retail

**Recommendation:** Do NOT build strategies that depend on 融券T+0. Treat as enhancement only if you have confirmed access.

---

## 3. Comparative Cost Analysis

| Strategy | Round-Trip Cost | Signal Required | Capacity |
|----------|----------------|-----------------|----------|
| Classic pairs (融券) | ~40-70 bps | Very strong (>50 bps alpha) | Limited by 券源 |
| ETF pairs (T+0) | ~15-30 bps | Moderate (>30 bps alpha) | Medium |
| Futures-spot basis | ~20-40 bps | Moderate (>40 bps alpha) | Large |
| Cross-sectional mean rev | ~15-30 bps | Moderate (>30 bps alpha) | Large |

**Cost Breakdown (Typical):**

| Component | Rate | Notes |
|-----------|------|-------|
| Commission (buy) | 0.02-0.03% | ~2.5 bps |
| Commission (sell) | 0.02-0.03% | ~2.5 bps |
| Stamp duty (sell only) | 0.05% | 5 bps |
| 融券 lending fee | 8-18% annual | ~3-7 bps/day |
| Spread + slippage | 3-15 bps each way | Varies by liquidity |

---

## 4. Key Implementation Considerations

### General Principles

1. **Strategy structure first:** Match signal decay to holding period
   - Fast signals (half-life < 1hr) → ETF pairs or 融券T+0
   - Slow signals (half-life > 1 day) → Cross-sectional or futures-hedged

2. **Cost-awareness:** CN round-trip costs are ~15-50 bps depending on structure
   - Signal must clear cost hurdle + risk premium

3. **Session-awareness:** 
   - AM (09:30-11:30) and PM (13:00-15:00) are different regimes
   - Best execution: 10:00-11:00 for passive entry

4. **T+1 realism:** 
   - Any stock purchase locks capital overnight
   - Include overnight gap risk in backtests

### Risk Management for Stat Arb

| Risk Type | Mitigation |
|-----------|------------|
| **Spread blow-up** | Stop-loss at 2-sigma deviation from historical |
| **Cointegration break** | Rolling ADF test; exit if p-value > 0.10 |
| **Overnight gap** | Hedge with futures (50-100% of exposure) |
| **Liquidity shock** | Liquidity filter: ADV > 100M CNY, spread < 20 bps |
| **Correlation spike** | Monitor portfolio correlation; reduce exposure if > 0.7 |

---

## 5. Summary: What Works Now

| Strategy | Feasibility | Holding Period | Complexity | Key Requirement |
|----------|-------------|----------------|------------|-----------------|
| ETF pairs | ⭐⭐⭐⭐⭐ | Intraday (T+0) | Low | ETF market data |
| Futures-spot basis | ⭐⭐⭐⭐⭐ | 1-2 days | Medium | Futures account |
| Cross-sectional mean rev | ⭐⭐⭐⭐ | Overnight (T+1) | Medium | Stock selection model |
| Stock pairs (融券) | ⭐⭐ | Intraday | High | 融券 access |
| Multi-leg options | ⭐⭐⭐ | Intraday | High | Options market data |

---

## 6. Recommended Starting Points

### For Beginners (No 融券 Access)
1. **ETF pairs trading** — Start with 300ETF vs 50ETF
   - Universe: 510300, 510050, 510500, 159919
   - Signal: Z-score of price ratio
   - Entry: |z| > 1.5
   - Exit: z reverts to 0.5 or 2-day max hold

2. **Cross-sectional mean reversion** — CSI 500 universe
   - Signal: Intraday return rank within sector
   - Entry: Bottom quintile (underperformers)
   - Exit: Next day VWAP
   - Hedge: Short IC futures at 75% of long notional

### For Institutional (With 融券 Access)
1. **Dual-mode design:**
   - Mode A: T+1 rotation (slow signals, always available)
   - Mode B: 融券T+0 overlay (fast signals, 券源-dependent)
   - Fallback to Mode A when 券源 unavailable

2. **Risk monitoring:**
   - Track 融券 balance daily
   - Set 券源 utilization limit at 80%
   - Regulatory policy monitoring for 融券 changes

---

## 7. Backtest Requirements

### Must-Have Realism

| Check | Description |
|-------|-------------|
| ✅ T+1 settlement | Buy today, sell tomorrow earliest |
| ✅ Cost model | Commission + stamp duty + spread + slippage |
| ✅ Liquidity filter | Skip illiquid stocks (ADV < threshold) |
| ✅ 融券 constraints | Only short if 券源 available (if applicable) |
| ✅ Overnight gap | Include gap PnL in returns |
| ✅ Lunch break | Don't compute signals across 11:30-13:00 |

### Comparison Backtests

Run three versions to understand strategy viability:

| Backtest | Description | Purpose |
|----------|-------------|---------|
| Idealized T+0 | Buy/sell same day | Upper bound (unrealistic for stocks) |
| T+1 Realistic | Buy today, sell T+1 | Realistic for long-only |
| 融券T+0 | 融券 round-trip with lending cost | Realistic for short-enabled |

**Decision rules:**
- If Sharpe(T+1) > 0.7 * Sharpe(T+0): Signal survives overnight → T+1 rotation viable
- If Sharpe(T+1) < 0.5 * Sharpe(T+0): Signal needs 融券T+0 → High regulatory risk

---

## 8. References

- Project skills: `cn-ashare-microstructure-researcher`
- Strategy structure: `.claude/skills/cn-ashare-microstructure-researcher/references/strategy-structure.md`
- Features: `.claude/skills/cn-ashare-microstructure-researcher/references/features.md`
- Trading rules: `@notes/cn_ashare_trading_rules_and_t0_strategies.md`

---

## Bottom Line

**Statistical arbitrage in A-shares is feasible**, but requires adapting to market structure:

1. **Use T+0 instruments** (ETFs, futures) for the short leg instead of stocks
2. **Accept overnight holding** for stock legs and hedge with futures
3. **Focus on cross-sectional signals** rather than pure pair spreads
4. **Avoid 融券-dependent strategies** unless you have confirmed institutional access

**The most practical starting point for most practitioners:**
- **ETF pairs trading** (intraday, T+0)
- **Cross-sectional mean reversion with futures hedging** (overnight, T+1)
