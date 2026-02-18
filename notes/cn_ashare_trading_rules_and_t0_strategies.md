# Chinese A-Share Trading Rules & T+0 Strategies Reference

> Date: 2026-02-18
> Source: Research from project skills (cn-ashare-microstructure-researcher, quant360)

---

## 1. Core Trading Rules

### T+1 Settlement (T+1 交易制度)
- **Rule**: Shares purchased on day T cannot be sold until day T+1
- **Implication**: Buy today → must hold overnight → sell tomorrow earliest
- **Asymmetry**: Selling existing holdings is unrestricted; new purchases are locked

### Trading Sessions (CST / UTC+8)

| Phase | Time | Description |
|-------|------|-------------|
| Pre-Open Auction | 09:15 - 09:25 | Call auction, orders accepted |
| Cancel Window | 09:15 - 09:20 | Orders can be cancelled |
| No-Cancel Window | 09:20 - 09:25 | Orders locked |
| Pre-Open (no trade) | 09:25 - 09:30 | Order accumulation |
| Morning Session | 09:30 - 11:30 | Continuous trading |
| **Lunch Break** | **11:30 - 13:00** | **90 min gap - treat AM/PM separately** |
| Afternoon Session | 13:00 - 14:57 | Continuous trading |
| Closing Auction (SZSE only) | 14:57 - 15:00 | Closing price determination |
| SSE Close | 14:59 - 15:00 | VWAP of last minute |

### Price Limits (涨跌停)

| Board | Daily Limit | Notes |
|-------|-------------|-------|
| Main Board (主板) | ±10% | Based on previous close |
| ChiNext (创业板) | ±20% | Since 2020 reform |
| STAR Market (科创板) | ±20% | 200 shares lot size |
| BSE | ±30% | Smaller exchange |
| New listings (ChiNext/STAR) | No limit for first 5 days | Then normal limits apply |
| ST Stocks | ±5% | Special treatment stocks |

### Order Specifications

| Parameter | Value |
|-----------|-------|
| Tick size | 0.01 CNY (universal) |
| Main Board lot size | 100 shares |
| ChiNext lot size | 100 shares |
| STAR Market lot size | 200 shares |
| Order types | Limit, Market, Cancel |

### Trading Costs

| Component | Rate | Notes |
|-----------|------|-------|
| Commission (buy) | 0.02-0.03% | ~2.5 bps |
| Commission (sell) | 0.02-0.03% | ~2.5 bps |
| Stamp duty (sell only) | 0.05% | 5 bps (reduced from 0.1% in Aug 2023) |
| Spread + slippage | 3-50 bps | Varies by liquidity |
| **Round-trip (T+1)** | **~15-30 bps** | |
| **融券 lending fee** | **8-18% annual** | **~3-7 bps/day** |
| **Round-trip (融券T+0)** | **~20-50 bps** | Higher due to lending cost |

---

## 2. Exchange Differences (SSE vs SZSE)

| Feature | SSE (上海证券交易所) | SZSE (深圳证券交易所) |
|---------|---------------------|---------------------|
| Main boards | Main Board, STAR Market | Main Board, ChiNext |
| Closing auction | No (closes at 15:00) | Yes (14:57-15:00) |
| L2 snapshots | Not available | Available |
| Order ID field | `OrderNo` | `ApplSeqNum` |
| Side encoding | `B`/`S` (strings) | `1`/`2` (integers) |
| Trade direction | `TradeBSFlag` explicit | Inferred from order refs |

---

## 3. T+0 Strategies Available Now

### 3.1 T+0 ETFs (Most Accessible)
**Rule**: Certain ETF categories allow same-day buy+sell

| Category | Examples |
|----------|----------|
| Equity ETFs | 510050 (50ETF), 510300/159919 (300ETF), 510500/159922 (500ETF) |
| Cross-border ETFs | QDII ETFs (HK, US, global indices) |
| Bond ETFs | Government/corporate bond ETFs |
| Commodity ETFs | Gold ETFs |

**Application**: Intraday momentum, mean-reversion, basis arbitrage

### 3.2 Sell-Side Overlay
**Requirement**: Pre-existing stock holdings (purchased T-1 or earlier)

- Can sell existing holdings anytime (no T+1 restriction on sells)
- Use fast-decay microstructure signals for optimal exit timing
- Cannot act on buy signals intraday (new buys locked until T+1)

**Best for**: Adding intraday alpha to existing long-term portfolios

### 3.3 Index Futures (T+0)
CFFEX futures allow true intraday round-trips:

| Contract | Underlying | Margin | Trading Hours |
|----------|------------|--------|---------------|
| IF | CSI 300 | ~12% | 09:30-11:30, 13:00-15:00 |
| IH | SSE 50 | ~12% | Same |
| IC | CSI 500 | ~14% | Same |
| IM | CSI 1000 | ~14% | Same |

**Strategies**:
- Directional intraday trading
- Hedge stock positions (short futures vs long stocks)
- Basis arbitrage (futures vs spot/ETF)

### 3.4 ETF Options (T+0)
Available on major ETFs - allow same-day buy/sell

| Options | Underlying |
|---------|------------|
| 50ETF Options | 510050 |
| 300ETF Options | 510300/159919 |
| 500ETF Options | 510500/159922 |

**Strategies**: Intraday volatility plays, gamma scalping, delta hedging

### 3.5 融券T+0 (Securities Lending) — ⚠️ Regulatory Constrained

**Mechanism**:
```
Long round-trip: Buy → 融券卖出 → 现券还券 (flat EOD)
Short round-trip: 融券卖出 → 买券还券 (flat EOD)
```

**Current Access (2025)**:
| Participant | Access Level |
|-------------|--------------|
| Top-tier quant funds (>10B AUM) | Limited access retained |
| Mid-tier funds | Severely reduced or lost |
| Retail / Small institutional | **Effectively unavailable** |

**Regulatory Timeline**:
- Oct 2023: Banned lock-up shares from 融券 lending
- Jan-Mar 2024: Restricted quant 融券 volumes
- Jul 2024: "Market fairness" enhanced scrutiny
- 2025+: Continued tightening trajectory

**Verdict**: Do NOT build strategies that depend on 融券T+0. Treat as enhancement only.

---

## 4. Strategy Architecture Comparison

| Architecture | Effective Holding | Overnight Risk | 融券 Required | Signal Decay Usable | Practicality |
|--------------|-------------------|----------------|---------------|---------------------|--------------|
| **T+1 rotation** | Overnight + next day | Yes | No | Slow (>overnight) | ✅ Most accessible |
| **Sell-side overlay** | 1min-2hr | Reduced | No | Fast (1min-2hr) | ✅ Requires existing holdings |
| **T+0 ETF intraday** | 5min-2hr | None | No | Fast (1min-2hr) | ✅ ETFs only |
| **Hedged stock + futures** | Overnight | Market-hedged | No | Slow (>overnight) | ✅ Requires futures access |
| **融券T+0 intraday** | 5min-2hr | None | Yes | Fast (1min-2hr) | ⚠️ Regulatory risk |

---

## 5. Market Structure Notes

### Participant Structure
| Participant | ~% Turnover | Behavior |
|-------------|-------------|----------|
| Retail investors | 60-70% | Short-term, momentum-chasing |
| Mutual funds | 10-15% | Longer-term, fundamentals |
| Hedge funds/quant | 5-10% | Intraday alpha, systematic |
| Foreign (QFII/北向) | 2-5% | Mix of fundamental and quant |

### Session Dynamics
- **09:30-10:00**: Highest volatility, widest spreads (avoid unless urgent)
- **10:00-11:00**: Best for passive entry (deep book, stable spreads)
- **11:00-11:30**: Pre-lunch position reduction
- **13:00-13:30**: Mini-open effect (post-lunch volatility)
- **13:30-14:30**: Lowest volume, thin liquidity
- **14:30-15:00**: Rising volume, institutional rebalancing

### Critical Research Rules
1. **Never compute features across lunch break** (11:30-13:00)
2. **Handle price limit regimes separately** (features degenerate at limits)
3. **Treat AM and PM as quasi-independent sessions**
4. **Overnight gap std**: ~1.5-3.0% for typical stocks

---

## 6. Recommended Dual-Mode Design

```
Strategy operates in two modes:
├── Mode A: T+1 rotation (baseline, always available)
│   └── Uses slow-decay signals (overnight-surviving)
│
└── Mode B: T+0 overlay (when available, enhancement)
    ├── T+0 ETFs (when ETF signal strong)
    ├── Index futures (for market direction)
    └── 融券T+0 (only if confirmed access)
    
Daily decision:
  For each instrument:
    If T+0 eligible and fast signal strong:
      → Mode B (intraday round-trip)
    Else:
      → Mode A (T+1 rotation) or skip
```

This ensures the strategy works regardless of 融券 access or regulatory changes.

---

## 7. Quick Reference: Symbol Code Ranges

| Exchange | Code Range | Market |
|----------|------------|--------|
| SSE | 600000-699999 | Main Board |
| SSE | 680000-689999 | STAR Market |
| SSE | 510000-519999 | ETFs |
| SZSE | 000001-009999 | Main Board |
| SZSE | 300000-309999 | ChiNext |
| SZSE | 159000-159999 | ETFs |

---

## References
- Project skills: `cn-ashare-microstructure-researcher`
- Project skills: `quant360`
- Strategy structure: `.claude/skills/cn-ashare-microstructure-researcher/references/strategy-structure.md`
- Market structure: `.claude/skills/cn-ashare-microstructure-researcher/references/market-structure.md`
