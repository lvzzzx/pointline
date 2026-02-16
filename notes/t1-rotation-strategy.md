# T+1 Rotation Strategy: Complete Implementation Guide

> **Context**: This document describes the T+1 rotation strategy structure for CN A-share markets, where the T+1 settlement constraint (buy today, can only sell tomorrow) creates unique challenges and opportunities for quantitative trading.

---

## Table of Contents

1. [The Core Constraint](#the-core-constraint)
2. [Three-Model Architecture](#three-model-architecture)
3. [Detailed Daily Workflow](#detailed-daily-workflow)
4. [Position Management Mechanics](#position-management-mechanics)
5. [Cost Structure & Breakeven Analysis](#cost-structure--breakeven-analysis)
6. [Risk Management Framework](#risk-management-framework)
7. [Label Design](#label-design)
8. [Signal Requirements](#signal-requirements)
9. [Common Failure Modes](#common-failure-modes)
10. [Implementation Checklist](#implementation-checklist)

---

## The Core Constraint

### The T+1 Settlement Rule

```
Buy at time T → Locked until T+1 morning (minimum 15.5 hours)
                   ↓
Your signal must survive: overnight gap + next morning volatility
```

This structural constraint is the defining feature of CN A-share intraday strategies. Unlike T+0 markets (crypto, US equities) where you can flatten positions EOD, T+1 forces you to hold overnight—exposing you to gap risk you cannot hedge away completely.

### The Decay-Structure Mismatch

| Signal Type | Half-Life | T+1 Holding | Fit |
|-------------|-----------|-------------|-----|
| Raw LOB imbalance | 1-10 min | ~18-20 hr | ❌ Mismatch |
| Trade flow (instant) | Minutes | ~18-20 hr | ❌ Mismatch |
| Daily aggregates | 1-3 days | ~18-20 hr | ✅ Good fit |
| Earnings momentum | Days-weeks | ~18-20 hr | ✅ Good fit |

**Gap ratio**: ~100x (signal decays 100x faster than holding period)

This is why raw microstructure signals don't work for T+1 rotation—they're dead before you can exit.

---

## Three-Model Architecture

Unlike a single "prediction model," T+1 rotation requires **three distinct models** working together:

```
┌─────────────────────────────────────────────────────────────────┐
│                    STOCK SELECTION MODEL                         │
│  Input:  Daily features (aggregated microstructure, cross-       │
│          sectional, fundamental, alternative data)               │
│  Output: Ranked list of stocks (top-N to consider)               │
│  Horizon: Close-to-sell-execution (~18-20 hours)                 │
│  Requirement: IC > 0.02-0.03 at overnight horizon                │
│  Alpha Contribution: ~70-80% of gross alpha                      │
└──────────────────────────┬──────────────────────────────────────┘
                           ↓ ranked universe (e.g., top 50)
┌─────────────────────────────────────────────────────────────────┐
│                    ENTRY TIMING MODEL                            │
│  Input:  Intraday features (LOB, flow, spread, volume profile)   │
│  Output: Entry signal + execution urgency level                  │
│  Horizon: Within-day (now → end of session)                      │
│  Purpose: Minimize execution shortfall vs VWAP                   │
│  Alpha Contribution: ~10-15% of gross alpha                      │
└──────────────────────────┬──────────────────────────────────────┘
                           ↓ entry orders
┌─────────────────────────────────────────────────────────────────┐
│                    EXIT TIMING MODEL                             │
│  Input:  Overnight gap, opening auction data, morning flow       │
│  Output: Sell timing (immediate vs wait for recovery)            │
│  Horizon: First 30-90 minutes of T+1 session                     │
│  Purpose: Maximize exit price vs VWAP                            │
│  Alpha Contribution: ~5-10% of gross alpha                       │
└─────────────────────────────────────────────────────────────────┘
```

### Model Interactions

1. **Stock Selection** runs first—identifies which stocks have overnight-viable alpha
2. **Entry Timing** runs second—optimizes execution for selected stocks
3. **Exit Timing** runs on T+1—manages gap risk and exit optimization

**Key Insight**: Stock Selection is the *fundamental* model—without edge here, timing optimization cannot save the strategy.

---

## Detailed Daily Workflow

### Day T (Entry Day)

| Time | Activity | Details |
|------|----------|---------|
| 09:15-09:25 | Opening Auction | Monitor auction for held positions (from T-1). Compute overnight gap features. No trading yet (can't sell until 09:30). |
| 09:30-10:30 | Morning Session - Exit Window | **SELL yesterday's positions**. Exit timing model decides: immediate market order, delay 10-30 min for recovery, or stop-loss. Target: Complete >80% of exits by 10:30. |
| 10:30-11:30 | Pre-Lunch - Entry Scouting | Stock selection model refreshes rankings. Entry timing identifies candidates showing selling exhaustion or dislocation. Place passive limit orders. |
| 11:30-13:00 | Lunch Break | No trading. Review morning fills, adjust afternoon plan. |
| 13:00-13:30 | Post-Lunch - Mini Open | Higher volatility, wider spreads. Good for momentum entries; bad for large orders. |
| 13:30-14:30 | Mid-Afternoon - Primary Entry | Deepest liquidity of afternoon. Execute TWAP for position building. Target: Complete 60-70% of intended positions. |
| 14:30-14:50 | Pre-Close - Final Entry | Stock selection model final ranking. Buy top-N stocks passing all filters. TWAP or market orders for remaining fills. |
| 14:50-15:00 | Closing | SZSE: auction orders possible. SSE: last regular trades. Record final positions for T+1 tracking. |

### Day T+1 (Exit Day)

- Positions from Day T are now sellable
- Gap risk realized at 09:30 open
- Execute exits per exit-timing model
- Proceeds available for Day T+1 afternoon entries
- Cycle repeats

---

## Position Management Mechanics

### Portfolio Turnover

```
Daily Turnover = 100% (sell entire portfolio, buy new one)
Effective Holding Period = 0.5 days (on average)

Position lifecycle example:
  Day T 14:30  Buy Stock A at ¥10.00
  Day T 15:00  Close at ¥10.05  → Unrealized +0.5%
  Day T 19:00  Overnight news breaks (market risk)
  Day T+1 09:30  Open at ¥9.90  → Gap -1.5%
  Day T+1 10:00  Sell at ¥9.95  → Realized -0.5%

Outcome: Stock selection was right (up 0.5%), but overnight gap erased it
```

### Position Sizing

**Equal-Weight (Simplest, Robust)**
```python
n_positions = 20
target_weight = 1.0 / n_positions  # 5% each
```

**Signal-Weighted (Higher Conviction = Larger Position)**
```python
z_scores = cross_sectional_z_score(predictions)
weights = softmax(z_scores * temperature)  # temperature controls concentration
weights = clip(weights, min=0.01, max=0.10)  # 1-10% bounds
weights = weights / weights.sum()  # renormalize
```

### Cash Management

| Time Period | Cash Position | Notes |
|-------------|---------------|-------|
| 09:30-10:30 | High | Just sold, haven't bought yet |
| 14:30-15:00 | Low | Fully invested |
| Overnight | 0% | 100% in positions |

**Implications**:
- Cannot use leverage intraday (no T+0 to free up cash)
- Must hold cash buffer for next day's purchases
- Market impact of large entry/exit is unavoidable

---

## Cost Structure & Breakeven Analysis

### Transaction Costs (Round-Trip)

| Component | Rate | Notes |
|-----------|------|-------|
| Commission (buy) | 2-3 bps | Negotiable with broker |
| Commission (sell) | 2-3 bps | |
| Stamp duty (sell) | 5 bps | Government tax (reduced from 10bps in Aug 2023) |
| Spread crossing | 4-20 bps | Half-spread × 2 (large cap: 2-10bps, small cap: 10-50bps) |
| Market impact | 5-15 bps | Depends on ADV, order size |
| **Total round-trip** | **~18-45 bps** | |

### Overnight Risk Cost

```
Overnight gap volatility: σ_gap ≈ 1.5-2.5% per stock
Diversified portfolio (20 stocks, ρ≈0.3): VaR_95 ≈ 1.0-1.5%

Risk-adjusted cost:
  Expected risk premium = ~10 bps (compensation for gap risk)
```

### Breakeven Math

```
Required gross alpha per trade:
  = Round-trip cost + Risk premium
  = ~30 bps + ~10 bps
  = ~40 bps per day

Annualized requirement:
  = 40 bps × 242 trading days
  = 96.8% gross annual alpha

But cross-sectionally:
  - Top vs bottom decile spread: typically 2-3%
  - Capturing 2% of spread = 40 bps per stock per day
  - With 20 stocks, need ~2 bps per stock average
```

---

## Risk Management Framework

### Pre-Trade Filters

```python
def can_enter_stock(stock, signal, portfolio):
    # 1. Signal strength check
    if signal.z_score < 1.0:  # Must be >1σ to justify cost
        return False

    # 2. Liquidity check
    position_value = portfolio.nav * target_weight
    if position_value > 0.01 * stock.adv_20d:  # <1% of ADV
        return False

    # 3. Price limit check (avoid limit-lock)
    if stock.price > stock.limit_up * 0.98:  # Within 2% of up-limit
        return False

    # 4. Volatility check
    if stock.realized_vol_20d > 0.50:  # >50% annualized
        return False

    # 5. Sector concentration
    sector_weight = portfolio.sector_exposure[stock.sector] + target_weight
    if sector_weight > 0.25:
        return False

    return True
```

### Intraday Risk Controls

| Risk | Threshold | Action |
|------|-----------|--------|
| Single stock stop-loss | -2% from entry | Immediate exit (if sellable) |
| Portfolio stop-loss | -1% NAV | Reduce all positions by 50% |
| Gap-down response | >-3% gap at open | Override models, sell immediately |
| Limit-lock monitoring | Approaching limit-down | Priority override, attempt exit |

### Overnight Risk Controls

- **Hedging**: Short index futures (IF/IH/IC/IM) at 60-80% of long exposure
- **Concentration**: Max 20 positions, max 5% per stock, max 25% per sector
- **Blacklist**: Earnings in next 48h, ST/*ST stocks, exchange watch list

---

## Label Design

### The Critical Choice: What Are You Predicting?

| Label Type | Formula | Use Case |
|------------|---------|----------|
| **A: Open-to-Close** | `(close_T+1 - open_T+1) / open_T+1` | ❌ **Wrong** for T+1 (not executable) |
| **B: Close-to-Sell-Execution** | `(sell_fill_T+1 - close_T) / close_T` | ✅ **Correct** (matches actual strategy) |
| **C: Close-to-Open** | `(open_T+1 - close_T) / close_T` | ✅ **Simplified** (good for research) |

### Why Label A Fails

```
Example:
Day T 15:00: Buy at ¥10.00 (your actual entry)
       ↓ (overnight)
Day T+1 09:30: Open at ¥10.20 (gap +2%)
Day T+1 15:00: Close at ¥10.30

Label A (open-to-close): +0.98%  ← Only captures 10:30→10:20
Actual return (close-to-sell): +3.0%  ← Includes the +2% gap

Problem: You CANNOT buy at the open on T+1—you're already holding!
Label A is mismatched with T+1 constraints.
```

### Recommended Label Progression

```python
# Phase 1: Research (simplified)
label = (open_t1 - close_t) / close_t  # close-to-open

# Phase 2: Refinement
label = (vwap_10am_t1 - close_t) / close_t  # close-to-morning-vwap

# Phase 3: Full simulation (requires built exit-timing model)
label = (exit_timing_model_output - close_t) / close_t
```

**Bottom line**: Your model must predict returns from **your actual entry** (Day T close) to **your actual exit** (Day T+1 morning execution). The overnight gap is 50-80% of return variance in T+1—it cannot be ignored.

---

## Signal Requirements

### IC Decay Requirements

| Signal Type | 1-Hour IC | 20-Hour IC | T+1 Viability |
|-------------|-----------|------------|---------------|
| Raw LOB imbalance | 0.05 | 0.005 | ❌ Too fast |
| Daily agg. order flow | 0.04 | 0.03 | ✅ Good |
| Earnings surprise (SUE) | 0.03 | 0.025 | ✅ Good |
| Return reversal (1-5d) | 0.035 | 0.025 | ✅ Good |

### The Daily Aggregation Bridge

```python
# Don't use (fast decay):
features['lob_imbalance_at_1030'] = get_imbalance(timestamp='10:30')

# Do use (slow decay):
features['session_avg_imbalance'] = mean(imbalance, session='full')
features['late_session_imbalance'] = mean(imbalance, session='14:30-15:00')
features['imbalance_trend'] = slope(imbalance, session='full')
features['imbalance_persistence'] = autocorr(imbalance, lag=30)
```

### Feature Categories by Source

| Category | Examples | Weight |
|----------|----------|--------|
| Microstructure (aggregated) | Daily order flow, closing auction imbalance, VPIN | 20-40% |
| Cross-sectional | Return reversal, sector momentum, volume profile | 20-30% |
| Fundamental | SUE, analyst revisions, growth | 20-30% |
| Alternative | Northbound flow (pre-2024), news sentiment | 10-20% |

---

## Common Failure Modes

### 1. Signal Decay > Holding Period

```
Backtest (T+0): Sharpe 3.0
Live (T+1): Sharpe 0.8

Diagnosis: Signal IC drops from 0.05 (1hr) to 0.01 (20hr)
Fix: Find slower-decay signals or get 融券 access
```

### 2. Overfitting to Entry Timing

```
Backtest: Perfect dip-buying, Sharpe 2.5
Live: Slippage erodes 30bps, Sharpe 0.5

Diagnosis: Assumed unrealistic execution
Fix: Add 10-20bps slippage buffer; use passive limits
```

### 3. Overnight Gap Correlation

```
Portfolio: 20 stocks, different sectors
Normal correlation: ρ = 0.3
Stress correlation: ρ = 0.8
Result: VaR underestimated by 3x

Fix: Stress test with ρ=0.7; size for tail risk
```

### 4. Capacity Constraints

```
Works at ¥10M, fails at ¥100M
Impact cost dominates

Fix: Reduce max position to 0.5% of ADV; increase positions
```

---

## Implementation Checklist

### Phase 1: Validation

- [ ] Backtest with realistic T+1 constraints (not T+0)
- [ ] Verify signal IC at 20-hour horizon (not 1-hour)
- [ ] Add 30bps round-trip cost to backtest
- [ ] Test with randomized entry/exit timing (±30 min) for robustness
- [ ] Confirm label matches actual holding period (close-to-execution)

### Phase 2: Paper Trading

- [ ] Run 1-2 months with paper orders
- [ ] Compare fills to VWAP benchmark
- [ ] Verify turnover matches target (100% daily)
- [ ] Track overnight gap distribution
- [ ] Validate correlation assumptions under stress

### Phase 3: Live Trading

- [ ] Start with 10-20% of target size
- [ ] Monitor execution shortfall vs backtest
- [ ] Track overnight gap correlation live
- [ ] Build position gradually over 2-4 weeks
- [ ] Maintain dual-mode capability (T+1 baseline, 融券 overlay if available)

---

## Key Takeaways

1. **T+1 is the constraint**: Signal must survive 18-20 hours; raw microstructure doesn't work
2. **Stock Selection is fundamental**: ~70-80% of alpha; timing models are optimizers
3. **Label matters**: Use close-to-sell-execution, not open-to-close
4. **Overnight gap is dominant**: 50-80% of return variance; cannot be ignored
5. **Costs are high**: 30-45bps round-trip + gap risk; need genuine edge
6. **Start simple**: Equal-weight, CSI 500 universe, close-to-open label for research

---

*Document created based on T+1 rotation strategy discussion.*
