# Strategy Bridge: ML Signal → Tradable CN A-Share Intraday Strategy

## Table of Contents
1. [The Bridge Problem](#the-bridge-problem)
2. [Strategy Architectures](#strategy-architectures)
3. [Signal → Position Translation](#signal--position-translation)
4. [T+1 Rotation Strategy](#t1-rotation-strategy)
5. [融券T+0 Intraday Strategy](#融券t0-intraday-strategy)
6. [Execution Layer](#execution-layer)
7. [Risk Management](#risk-management)
8. [Performance Attribution](#performance-attribution)
9. [Practical Starting Points](#practical-starting-points)

---

## The Bridge Problem

An ML model outputs `prediction(stock_i, time_t) → expected_return_{t+h}`. This is not a strategy. The bridge must solve:

```
ML signal → position intent → T+1/T+0-aware execution plan → orders → risk management → PnL
```

The CN A-share structural constraints (T+1 settlement, price limits, lunch break, no effective short selling) make this bridge harder than in crypto or US equities. Every design decision must account for these constraints.

### What The Model Provides vs What Strategy Needs

| Model Output | Strategy Needs |
|---|---|
| Expected return (continuous) | Buy/sell/hold decision (discrete) |
| Point-in-time prediction | Entry AND exit timing |
| Per-stock signal | Portfolio-level position targets |
| Gross alpha estimate | Net-of-cost, net-of-risk alpha |
| Unconditional prediction | T+1-aware actionability check |

## Strategy Architectures

Five viable architectures, ordered by practical accessibility:

### Architecture Summary

| Architecture | T+1 Constraint | Overnight Risk | 融券 Required | Signal Decay Usable | Capacity |
|---|---|---|---|---|---|
| **T+1 rotation** | Full (buy today, sell T+1) | Yes, unavoidable | No | Slow (>overnight) | Large |
| **Sell-side overlay** | None (sell existing) | Reduced | No | Fast (1min-2hr) | Portfolio-limited |
| **T+0 ETF intraday** | None (ETF T+0) | None | No | Fast (1min-2hr) | Medium (ETF universe) |
| **Hedged stock + futures** | Full + futures hedge | Market-hedged | No | Slow (>overnight) | Large |
| **融券T+0 intraday** | Bypassed via 融券 | None (flatten EOD) | Yes | Fast (1min-2hr) | 券源-limited |

### Architecture 1: T+1 Rotation (Most Accessible)

```
Day T afternoon:  Buy top-N signal stocks
Day T overnight:  Hold (unavoidable)
Day T+1 morning:  Sell yesterday's buys at optimal time
Day T+1 afternoon: Buy today's new top-N signals
Repeat daily.
```

- **Requires:** Stock selection model + entry/exit timing model
- **Signal requirement:** Must survive overnight noise. Slow-decay signals only.
- **Cost:** ~20bps round-trip. Daily turnover = 100%.
- **Capacity:** Large (full stock universe).
- **Risk:** Full overnight gap exposure.

### Architecture 2: Sell-Side Overlay (Simplest)

```
Pre-existing: Hold a portfolio (from fundamental/slower alpha strategy)
Intraday: ML model signals "sell stock X now" → immediately actionable
```

- **Requires:** Existing portfolio + sell-timing model
- **Signal requirement:** Fast-decay signals directly usable (no T+1 constraint on sells).
- **Limitation:** Universe = current holdings only. Can't act on buy signals intraday.
- **Best for:** Adding intraday alpha to existing longer-horizon portfolio.

### Architecture 3: T+0 ETF Intraday

```
Intraday: Buy and sell T+0-eligible ETFs within same day
          50ETF, 300ETF, 500ETF, etc.
```

- **Requires:** ETF-level signal model (index/sector direction).
- **Signal requirement:** Fast-decay directly usable.
- **Limitation:** ~10 liquid ETFs. No single-stock alpha.
- **Best for:** Index-level microstructure signals, basis arbitrage.

**T+0 eligible ETFs (partial list):**
- 510050 (50ETF), 510300/159919 (300ETF), 510500/159922 (500ETF)
- 510310/159845 (300 value), sector ETFs (varies, check exchange rules)
- Cross-border ETFs (QDII), bond ETFs, commodity ETFs

### Architecture 4: Hedged Stock + Futures

```
Day T:    Buy signal stocks + short IF/IH/IC/IM futures
Day T+1:  Close stock positions + cover futures hedge
```

- **Requires:** Stock selection model + futures hedging layer.
- **Signal requirement:** Stock-specific alpha that survives overnight (market risk hedged).
- **Residual risk:** Idiosyncratic overnight gap (stock-specific, not market).
- **Cost:** Stock round-trip + futures commission + basis risk + margin cost.
- **Capacity:** Large, but constrained by futures margin and position limits.

### Architecture 5: 融券T+0 Intraday (Regulatory Risk)

```
Intraday: True long+short round-trips via securities lending
          Flatten all 融券 positions before close
```

- **Requires:** 融券 access + 券源 availability + margin account.
- **Signal requirement:** Fast-decay signals directly usable. Full intraday alpha extraction.
- **Limitation:** 券源 availability, high lending costs, regulatory uncertainty.
- **Details:** See [融券T+0 section](#融券t0-intraday-strategy) below.

## Signal → Position Translation

### Step 1: Raw Signal → Z-Score

```python
raw_signal = model.predict(features_t)                    # per-stock, continuous
z_signal = (raw_signal - cross_sectional_mean) / cross_sectional_std  # normalize
```

Cross-sectional z-scoring at each timestamp is essential: model predictions drift over time, but relative ranking is stable.

### Step 2: Z-Score → Position Intent

```python
# Threshold: only act when expected alpha > cost
cost_threshold = round_trip_cost / std_forward_return     # typically z ~ 1.0-1.5

long_candidates  = stocks where z_signal > +cost_threshold
short_candidates = stocks where z_signal < -cost_threshold  # only if 融券 or existing holding
neutral          = everything else

# Sizing: proportional to signal strength
target_weight = clip(z_signal * scale_factor, -max_weight, +max_weight)
```

**Key parameters:**
- `cost_threshold`: Minimum signal strength to trade. Higher for expensive stocks (small cap, illiquid).
- `max_weight`: Maximum position per stock. Typically 3-5% of portfolio.
- `scale_factor`: Maps z-score to weight. Calibrate so that portfolio turnover stays within cost budget.

### Step 3: Position Intent → T+1-Aware Action

```python
for stock in universe:
    target = target_weight[stock]
    current = current_weight[stock]
    delta = target - current

    if delta > 0:  # want to increase
        if architecture == 'T+1_rotation':
            # Buy commits to overnight. Signal must justify overnight risk.
            if expected_alpha > overnight_risk_premium + cost:
                queue_buy(stock, delta)
        elif architecture == '融券T+0':
            # Check 券源 availability for exit
            if borrowable_shares(stock) >= delta * portfolio_value / price:
                queue_buy(stock, delta)
            else:
                skip  # can't guarantee intraday exit

    elif delta < 0:  # want to decrease
        sellable = holdings_bought_before_today(stock)
        if abs(delta) <= sellable:
            queue_sell(stock, abs(delta))  # immediately actionable
        elif has_融券_access(stock):
            queue_融券_sell(stock, abs(delta))  # borrow and sell
        else:
            skip  # T+1 locked, can't reduce
```

### Step 4: Portfolio Optimization (Optional)

For cross-sectional models with many signals, optimize portfolio:

```python
# Minimize: -alpha'w + lambda * w'Sigma*w + penalty * turnover_cost
# Subject to:
#   sum(w) = 0 (market-neutral) or sum(w) = 1 (long-only)
#   |w_i| <= max_weight
#   sector_exposure <= max_sector
#   sum(|w_new - w_old|) * cost <= cost_budget
#   w_buy only for stocks where T+1/融券 exit is feasible
```

Use convex optimizer (cvxpy, scipy). Rebalance every N minutes or on signal threshold crossing.

## T+1 Rotation Strategy

### Detailed Daily Cycle

```
09:15-09:25  Opening auction monitoring
             → Compute overnight gap features for held stocks
             → Update exit-timing model for stocks to sell today

09:30-10:00  Execute sells (yesterday's buys)
             → Exit-timing model determines: sell at open, or wait?
             → For gap-up stocks: consider immediate sell
             → For gap-down stocks: wait for partial recovery or stop-loss

10:00-14:00  Monitor for entry signals
             → Entry-timing model identifies optimal buy window
             → Best entry typically 10:00-11:00 (deep book, settled vol)
             → Post-lunch 13:00-13:30 also viable (mini-open effect)

14:00-14:50  Execute buys for next-day rotation
             → Stock selection model ranks universe
             → Buy top-N stocks that pass cost + overnight risk filter
             → Passive execution (limit orders, TWAP) preferred

14:50-15:00  End-of-day reconciliation
             → Verify all intended buys executed
             → If partial fills, adjust for next day
             → Record positions for T+1 tracking
```

### Two Sub-Models Required

**Entry-timing model:** Predicts optimal buy time within day T.
- Features: intraday volume profile, spread dynamics, book imbalance trend
- Label: execution shortfall vs Day T VWAP
- Horizon: optimizes within-day entry, not next-day return

**Exit-timing model:** Predicts optimal sell time within day T+1.
- Features: overnight gap, morning order flow, relative strength
- Label: execution shortfall vs Day T+1 VWAP
- Horizon: 09:30-11:30 (AM session) or 13:00-15:00 (PM session)

**Stock selection model:** Predicts which stocks to hold overnight.
- Features: full feature catalog from features.md
- Label: overnight + next-day return (close-to-sell-execution)
- Must be strong enough to survive overnight noise (IC at overnight horizon > 0.02)

### Overnight Risk Management

```
overnight_gap_std ≈ 1.5-3.0% for typical A-share stocks
overnight_gap_std ≈ 0.5-1.0% for hedged (stock - index futures)

Position sizing rule:
  max_position = max_loss_tolerance / (overnight_gap_std * confidence_interval)

Example: 1% max loss tolerance, 2% gap std, 2-sigma
  max_position = 0.01 / (0.02 * 2) = 25% of portfolio per stock
  → diversify across 10-20 stocks to reduce concentration
```

## 融券T+0 Intraday Strategy

### Mechanism: How 融券 Enables T+0

**Long round-trip (做多回转):**
```
10:00  Buy 10,000 shares (regular buy, locked by T+1)
14:00  融券卖出 10,000 shares (sell borrowed shares)
Close  现券还券: deliver today's bought shares to cover borrow
Result: Intraday long from 10:00-14:00. Flat at EOD.
```

**Short round-trip (做空回转):**
```
10:00  融券卖出 10,000 shares (sell borrowed shares)
14:00  买券还券: buy 10,000 shares to return borrow
Result: Intraday short from 10:00-14:00. Flat at EOD.
```

**Existing holdings rotation (持仓回转):**
```
Pre-existing: Hold 10,000 shares from T-1
10:00  融券卖出 10,000 shares (sell borrowed, keep existing)
14:00  现券还券: deliver existing holdings to cover borrow
       Buy 10,000 shares (new position, locked T+1)
Result: Effectively sold at 10:00, rebought at 14:00. Position maintained.
```

### Cost Structure

| Component | Rate | Per Round-Trip |
|---|---|---|
| Commission (buy) | 0.02-0.03% | ~2.5bps |
| Commission (sell) | 0.02-0.03% | ~2.5bps |
| Stamp duty (sell only) | 0.05% | 5bps |
| **Lending fee (融券费率)** | **8-18% annualized** | **~3-7bps per day** |
| Spread + slippage | 3-15bps each way | 6-30bps |
| **Total round-trip** | | **~20-50bps** |

The lending fee is the incremental cost vs T+1 rotation. Signal must generate enough alpha to cover ~5-10bps additional daily cost.

### 券源 (Borrowable Shares) Management

券源 availability is the binding constraint:

```
Daily workflow:
06:00-09:00  Check 券源 allocation from broker
             → Which stocks available? How many shares?
             → Update universe filter: only trade stocks with 券源
09:15        Lock 券源 for planned trades (some brokers require pre-reservation)
09:30-14:50  Trade within 券源 limits
14:50-15:00  Flatten all 融券 positions (mandatory before close)
15:00        Return unused 券源 to broker pool
```

**券源 risk factors:**
- Broker can recall 券源 intraday (forced cover at bad time)
- Popular stocks have 券源 competition — may not get allocation
- 券源 quantity may be insufficient for desired position size
- 券源 availability varies day-to-day — universe is unstable

### Regulatory Environment (2023-2025)

| Date | Regulation | Impact on Quant 融券T+0 |
|---|---|---|
| Oct 2023 | Banned lock-up shares (限售股) from 融券 lending | Reduced 券源 pool ~30-40% |
| Jan 2024 | Restricted quant fund 融券 volumes | Direct capacity reduction |
| Mar 2024 | Enhanced 融券T+0 monitoring and restrictions | Some brokers suspended quant 融券 |
| Jul 2024 | "Market fairness" (公平性) rules | Tighter scrutiny on quant T+0 advantage |
| 2025 | Trajectory: continued tightening | Treat as regulatory risk factor |

**Practical reality (2025):**
- Top-tier quant funds (>10B AUM) retain some 融券T+0 access
- Mid-tier funds have severely reduced or lost access
- Retail/small institutional: effectively no 融券T+0
- Regulatory direction is to eliminate the T+0 advantage quant has over retail

### Dual-Mode Design (Recommended)

Do not build a strategy that DEPENDS on 融券T+0. Design for both modes:

```
Strategy operates in two modes:
├── Mode A: T+1 rotation (always available, baseline)
│   └── Uses slow-decay signals (overnight-surviving)
│
└── Mode B: 融券T+0 overlay (when 券源 available, enhancement)
    └── Uses fast-decay signals (1min-2hr) on 券源-available stocks
    └── Falls back to Mode A if 券源 recalled or unavailable

Daily decision:
  for each stock in universe:
      if 券源_available(stock) and fast_signal(stock) strong:
          → Mode B (融券T+0 intraday)
      else:
          → Mode A (T+1 rotation) or skip
```

## Execution Layer

### Execution Timing by Session

| Window | For Buys (Entry) | For Sells (Exit) | Notes |
|---|---|---|---|
| 09:30-10:00 | Avoid unless gap-fill | Good for momentum exits | Widest spreads, highest impact |
| 10:00-11:00 | **Best for passive entry** | Signal-dependent | Deepest book, most stable spreads |
| 11:00-11:30 | Acceptable | Pre-lunch reduction | Spreads widen slightly |
| 13:00-13:30 | Post-lunch dip entries | Avoid (thin book) | Mini-open effect, volatile |
| 13:30-14:30 | Acceptable | Acceptable | Lowest volume period |
| 14:30-14:57 | **VWAP/TWAP entries** | Pre-close positioning | Rising volume |
| 14:57-15:00 | SZSE closing auction | SZSE closing auction | Large blocks, index rebalance |

### Execution Algorithms

**Passive limit orders (preferred for MFT):**
- Place limit at or inside NBBO. Wait for fill.
- Target >50% passive fill rate to reduce spread cost.
- Cancel and replace if book moves away.
- Risk: non-fill → miss the trade. Use urgency scaling.

**TWAP (Time-Weighted Average Price):**
- Split order into equal slices over N-minute window.
- Good for T+1 rotation buys in afternoon (14:00-14:50).
- Reduces timing risk at cost of average execution.

**VWAP (Volume-Weighted Average Price):**
- Benchmark-aware. Follow historical intraday volume profile.
- Good for larger orders where impact matters.
- Profile: heavier at open/close, lighter mid-day.

**Implementation Shortfall (IS):**
- Urgency-aware. Front-load execution when signal is strong, back-load when weak.
- Best for 融券T+0 where signal decay matters.
- Trade-off: higher urgency = more impact but less decay loss.

### Slippage Model

```python
expected_slippage_bps = (
    half_spread_bps                                    # quoted half-spread
    + impact_coefficient * sqrt(order_size / ADV)      # market impact
    + timing_cost * signal_decay_rate * execution_time # signal decay during execution
)

# Typical values:
# half_spread: 2-10bps (large cap) to 10-50bps (small cap)
# impact_coefficient: 5-20 (calibrate from L2/L3 data)
# signal_decay_rate: from decay curve analysis (evaluation.md)
```

## Risk Management

### Pre-Trade Checks

```
Before each trade:
├── Position limit: |weight_i| < 5% of portfolio
├── Sector limit: |sector_exposure| < 20%
├── Liquidity check: order_size < 1% of stock's ADV
├── Price limit check: skip if stock within 2% of limit price
│   (risk of limit-lock → can't exit)
├── Cost check: expected_alpha > total_cost_estimate
├── 融券 check (if Mode B): 券源 confirmed and sufficient
└── Regulatory check: no trading halt, no unusual trading alert (异常交易)
```

### Intraday Risk Controls

```
Running checks during session:
├── Portfolio drawdown: if DD > X% → reduce all positions by 50%
├── Per-stock loss: if single stock loss > Y% → close position
├── Volatility scaling: if realized_vol > 2x normal → halve position sizes
├── Limit-lock monitoring: if held stock approaches limit-down
│   → attempt to exit before lock (priority override)
├── Correlation spike: if portfolio stocks start moving together
│   → reduce concentration, increase hedge
└── 融券 recall: if broker recalls 券源 → immediate cover
```

### Overnight Risk Controls (T+1 Rotation)

```
Before holding overnight:
├── Hedge ratio: short futures notional >= X% of long stock notional
│   (X = 50-100%, depending on target net exposure)
├── Gap risk budget: portfolio overnight gap VaR < Z% of NAV
├── Concentration: max N stocks, max weight per stock
├── Earnings filter: exclude stocks with imminent earnings release
├── Halt risk: exclude stocks with regulatory risk indicators
└── Margin check: sufficient margin for futures hedge overnight
```

### 融券-Specific Risk Controls

```
├── Flatten deadline: all 融券 positions closed by 14:50 (10min buffer)
├── 券源 utilization: never use >80% of allocated 券源 (recall buffer)
├── Forced cover plan: if 券源 recalled, cover at market immediately
├── Daily 券源 check: if 券源 drops below minimum → switch to Mode A
└── Regulatory monitoring: track CSRC/exchange 融券 policy announcements
```

## Performance Attribution

### PnL Decomposition

Split total PnL into components to diagnose what's working:

```
total_pnl = stock_selection_alpha       # which stocks (cross-sectional ranking)
          + entry_timing_alpha          # when you bought vs VWAP
          + exit_timing_alpha           # when you sold vs VWAP
          + overnight_gap_pnl           # overnight market move (T+1 only)
          + hedge_pnl                   # futures hedge P&L
          - commission_cost             # buy + sell commissions
          - stamp_duty_cost             # sell-side stamp duty
          - spread_cost                 # bid-ask spread crossing
          - impact_cost                 # market impact from order size
          - lending_cost                # 融券 fee (融券T+0 only)
          - funding_cost                # margin cost for futures/融券
```

### Attribution by Dimension

Report separately across all these dimensions to identify fragile vs robust alpha:

| Dimension | Segments | What It Reveals |
|---|---|---|
| Session | AM (09:30-11:30) vs PM (13:00-15:00) | Session-dependent signal fragility |
| Trading phase | Open / mid-morning / pre-lunch / post-lunch / pre-close | Phase-specific alpha source |
| Market regime | Bull / bear / range-bound (from CSI300) | Regime robustness |
| Volatility regime | Low / medium / high realized vol | Vol-dependency |
| Board | Main Board vs ChiNext/STAR | Board-specific dynamics |
| Market cap | Large / mid / small | Capacity indication |
| Strategy mode | T+1 rotation vs 融券T+0 (if dual-mode) | Mode-specific alpha |
| Signal source | Which feature categories drive PnL | Feature robustness |

### T+1 vs T+0 Comparison

Always run both backtests and compare:

```
Backtest A (Idealized T+0): Buy and sell within day. No overnight.
Backtest B (T+1 Realistic): Buy today, sell T+1 at exit-model time.
Backtest C (融券T+0):       融券 round-trip, deduct lending cost.

Key metrics to compare:
├── Sharpe: A > C > B (typical ordering)
├── Max DD: A < C < B (overnight adds drawdown)
├── Turnover cost: C > A ≈ B (lending fee adds cost)
└── Capacity: B > A > C (券源 limits C)

If Sharpe(B) < 0.5 * Sharpe(A):
  → Signal doesn't survive overnight. Only viable with 融券T+0.
  → High regulatory risk. Consider different signal family.

If Sharpe(B) > 0.7 * Sharpe(A):
  → Signal is overnight-robust. T+1 rotation is viable.
  → 融券T+0 is pure enhancement, not dependency.
```

## Practical Starting Points

### Minimum Viable Strategy (T+1 Rotation)

For someone with a working ML signal and no 融券 access:

1. **Universe:** CSI 500 constituents (liquid, mid-cap alpha richer than large-cap)
2. **Signal:** Cross-sectional z-scored LightGBM prediction, ranked
3. **Buy:** Top 20 stocks by signal at 14:30-14:50 (TWAP, passive)
4. **Sell:** Yesterday's 20 stocks at 10:00-10:30 (post-open settlement)
5. **Hedge:** Short IC futures (CSI 500) at 50-100% of long notional
6. **Cost filter:** Only trade if `expected_alpha > 25bps` (conservative)
7. **Position:** Equal weight (~5% each), 20 stocks = 100% long, ~75% hedged
8. **Evaluate:** Sharpe, IC, max DD, PnL by session, regime, and board

### Minimum Viable Strategy (融券T+0)

For someone with 融券 access and 券源:

1. **Universe:** 券源-available stocks within CSI 300 (~50-100 stocks/day)
2. **Signal:** Fast-decay features (LOB imbalance, trade flow, VPIN)
3. **Entry:** Signal threshold crossing (|z| > 1.5)
4. **Exit:** Signal decay below threshold, or 30min max hold, or 14:50 hard stop
5. **Direction:** Both long and short (融券 enables true short)
6. **Cost filter:** `expected_alpha > 40bps` (higher bar due to lending fee)
7. **Position:** Max 3% per stock, max 30% total 融券 exposure
8. **Flatten:** All 融券 positions closed by 14:50 (10min buffer)
9. **Fallback:** Stocks without 券源 → skip or T+1 rotation mode

### Scaling Path

```
Phase 1: T+1 rotation, 20 stocks, CSI 500 universe
         → Validate stock selection alpha survives overnight
         → Target: Sharpe > 1.5 (hedged, net of costs)

Phase 2: Add entry/exit timing models
         → Reduce execution slippage by 5-10bps
         → Target: 10-20% Sharpe improvement from timing alone

Phase 3: Add 融券T+0 overlay (if access available)
         → Fast-decay signals on 券源-available stocks
         → Target: 20-40% incremental Sharpe from T+0 trades

Phase 4: Multi-horizon blending
         → Blend slow (overnight) and fast (intraday) signals
         → Different models for different horizons, combined at portfolio level

Phase 5: Cross-instrument
         → Add ETF T+0, index futures directional, ETF options overlay
         → Diversify alpha sources beyond single-stock signals
```

### 融券 as Market Feature

Even if you don't USE 融券 for trading, 融券 market data is a valuable feature source:

- **融券余额 (outstanding short balance):** High = crowded short, squeeze risk. Rising = bearish sentiment.
- **融券卖出额 (daily short selling volume):** Spike = sudden bearish pressure.
- **融资余额 (outstanding margin long balance):** High = leveraged long, liquidation risk.
- **融资融券比 (margin long/short ratio):** Extreme values are contrarian signals.
- **个股融券余量变化 (per-stock short balance change):** Stock-level short interest change. Increasing = bearish flow, but crowded shorts can reverse sharply.
