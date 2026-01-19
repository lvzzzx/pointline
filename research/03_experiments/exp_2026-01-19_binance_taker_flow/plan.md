# Research Notes & Plan: Binance Tick-by-Tick Trades — Large vs Small Taker Flow Factors (3H Horizon)

## 0) Objective
Build and validate predictive factors derived from Binance tick-by-tick trade prints (with explicit taker buy/sell) by segmenting trades into large vs small buckets and aggregating signed notional flow to forecast 3-hour forward returns.

Primary question: Does size-segmented taker flow contain incremental predictive power for 3-hour returns beyond standard controls (momentum, volatility, activity)?

---

## 1) Data & Assumptions

### 1.1 Input data (per trade)
- Timestamp (ms preferred)
- Price p
- Base quantity q
- Side side ∈ {taker_buy, taker_sell} (explicit aggressor side)
- Symbol (e.g., BTCUSDT)
- (Optional) trade_id (for de-dup)
- (Optional) venue flags: block/liquidation/internal (if available)

### 1.2 Derived per-trade fields
- Quote notional: a = p * q (in quote currency)
- Sign: s = +1 if taker_buy else -1
- Signed notional: x = s * a

### 1.3 Output / target
- Bar close price P_t (last trade in bar)
- 3-hour forward log return:
  y_t = log(P_{t+3h}) - log(P_t)

Note: At 3h, last-trade close is typically adequate, but if L1 mid is available later, re-run using mid to reduce microstructure noise.

---

## 2) Bar Construction

### 2.1 Primary bar choice
- 5-minute time bars (baseline)

Rationale: smooths tick noise but keeps enough resolution to form stable 30m/1h/3h rolling aggregates.

### 2.2 Bar primitives (computed per bar t)
- Total notional: A_t = Σ a_i
- Net taker flow: N_t = Σ x_i
- Buy/Sell notionals: B_t = Σ_{s=+1} a_i, S_t = Σ_{s=-1} a_i
- Trade count: K_t = number of trades in bar

---

## 3) Large vs Small Trade Definition (Crypto-appropriate)

### 3.1 Rolling quantile thresholds (per symbol)
Absolute thresholds do not generalize across symbols/time; use rolling quantiles on notional.

For each trade, define bucket using thresholds from a trailing window W_q (strictly past data):
- Window: W_q = 72 hours (baseline), also test 24 hours
- Large trades: a_i >= Q_{0.85}(a) (Top 15%)
- Small trades: a_i <= Q_{0.50}(a) (Bottom 50%)
  (Alternative small: Bottom 30% if activity is high)

### 3.2 Implementation notes
- Thresholds must be computed without lookahead (only trades earlier than the current trade time).
- If using bar-level thresholds instead of per-trade rolling, ensure thresholds use only prior bars.
- For illiquid symbols: enforce minimum trade count per quantile window; otherwise exclude.

---

## 4) Factor Library (Bar-Level)

Let:
- A^L_t: notional from large trades in bar t
- N^L_t: signed notional from large trades in bar t
- B^L_t, S^L_t: buy/sell notional from large trades
- Analogous A^S_t, N^S_t for small trades

Use small epsilon ε to prevent divide-by-zero (e.g., 1e-12 or 1e-6 * median(A_t)).

### 4.1 Core flow imbalance factors
1) Large net share
   F_L1(t) = N^L_t / (A_t + ε)

2) Large imbalance (scale-free)
   F_L2(t) = (B^L_t - S^L_t) / (B^L_t + S^L_t + ε)

3) Large–small disagreement
   F_D(t) = (N^L_t - N^S_t) / (A_t + ε)

4) Total flow toxicity proxy
   F_T(t) = |N_t| / (A_t + ε)

### 4.2 Participation / dominance
5) Large participation
   F_P(t) = A^L_t / (A_t + ε)

6) Concentration proxy (top-k share)
- Sort trades in bar by notional a_i.
- F_Ck(t) = (sum of top-k a_i) / (A_t + ε) for k ∈ {1,3,5}.

---

## 5) Multi-Scale Feature Engineering (Aligned to 3H)

Compute rolling aggregates on 5m bars:
- 30m = 6 bars
- 1h  = 12 bars
- 3h  = 36 bars

For each base factor f_t:
- Rolling sums: Sum_30m(f), Sum_1h(f), Sum_3h(f)
- Rolling means: Mean_1h(f), Mean_3h(f)
- Acceleration proxy:
  Acc(f) = Sum_30m(f) - (1/6) * Sum_3h(f)

### 5.1 Controls (must-have)
- Past returns: r_1h, r_3h
- Realized volatility:
  RV_1h = sqrt(sum_{bars in 1h} (bar_return^2))
  RV_3h similarly
- Activity:
  log(A_t), Mean_1h(log(A))
  K_t (trades-per-bar)
  Optional: log(Mean trade notional)

---

## 6) Regime Conditioning (Crypto-specific)

### 6.1 Hypothesis: sign may flip by regime
- In trend regimes: large net flow may continue
- In stress/blow-off regimes: large flow may become exhaustion, leading to mean reversion

### 6.2 Regime features
- Volatility z-score:
  z_RV = zscore(RV_1h) (rolling z within symbol)
- Flow magnitude z-score:
  z_F = zscore(|Sum_30m(F_L1)|) or zscore(|N_t|/A_t)

### 6.3 Regime flags (example)
- Calm/Trend: z_RV < 0.6
- Stress: z_RV > 1.3 AND z_F > 1.0

### 6.4 Regime-interacted signals to test
- Trend signal:
  Sig_trend = zscore(Sum_1h(F_L1)) * I[Calm]
- Stress contrarian:
  Sig_stress = -zscore(Sum_30m(F_L1)) * I[Stress]

Do not assume outcomes—validate empirically.

---

## 7) Evaluation Design (Avoid Leakage)

### 7.1 Key leakage risks
- Rolling quantiles for trade size must not use future trades.
- Overlapping labels (3h forward) cause autocorrelation in errors.

### 7.2 Backtest / validation approach
- Use time-based split (train → validation → test) per symbol.
- Apply purged windows around split boundaries:
  - Purge at least the label horizon (3h) and the maximum feature lookback.

### 7.3 Metrics
- RankIC / IC of each factor vs y_t
- Quintile (or decile) portfolio:
  - Long top quantile, short bottom quantile
  - Evaluate mean return, Sharpe, hit rate
- Regression with controls:
  y_t = α + β f_t + γ^T controls_t + ε_t
  Use HAC/Newey–West errors (or block bootstrap).

### 7.4 Robustness checks
- Different bar sizes: 1m vs 5m vs 15m
- Different thresholds: Large top 10/15/20%
- Different threshold windows: 24h vs 72h
- Subperiod stability: month-by-month / regime-by-regime

---

## 8) Implementation Steps (Execution Plan)

### Phase 1 — Data QA (must complete)
1. De-duplicate trades by trade_id (if present)
2. Validate monotonic timestamps per symbol stream
3. Sanity check sign:
   - Compute E[ΔP | taker_buy] on short horizon; should be ≥ 0 if sign is correct
4. Outlier filtering:
   - Remove clearly erroneous prints (extreme price spikes not consistent with nearby trades)

Deliverable: cleaned trade stream + QA report.

### Phase 2 — Bar + Threshold Pipeline
1. Build 5m bars with primitives A_t, N_t, B_t, S_t, K_t
2. Online rolling quantiles for notional thresholds (per symbol)
3. Tag trades as large/small; aggregate A^L_t, N^L_t, ...

Deliverable: bar-level dataset with large/small aggregates.

### Phase 3 — Factor Computation
1. Compute base factors: F_L1, F_L2, F_D, F_T, F_P, F_Ck
2. Build multi-scale rollups: 30m/1h/3h sums and acceleration
3. Build control variables (returns, RV, activity)

Deliverable: feature matrix + target vector.

### Phase 4 — Modeling & Testing
1. Univariate IC and quintile tests for each feature
2. Multivariate regression with controls to test incremental value
3. Regime-conditioned variants (Calm vs Stress)
4. Document parameter sensitivity

Deliverable: research notebook/report with results tables and plots.

### Phase 5 — Optional Extensions
- Add L1 mid/spread (reduce measurement noise)
- Add futures data: funding rate, OI, liquidation flags
- Cross-venue confirmation (if multi-exchange later)

---

## 9) Initial Default Parameters (Baseline Run)
- Symbol: start with BTCUSDT and ETHUSDT
- Bars: 5m
- Large threshold: rolling notional Top 15%
- Small threshold: rolling notional Bottom 50%
- Threshold window W_q: 72h
- Features: multi-scale (30m/1h/3h) sums + acceleration
- Target: 3h forward log return

---

## 10) Expected Outcomes & Decision Criteria
Go/No-Go for size-flow alpha after baseline:
- Positive, stable top-minus-bottom spread across time splits
- Factor retains significance after controls
- Performance not confined to a single month/regime

If signals only work in specific regimes, proceed with regime model; otherwise consider reframing.

---

## 11) Open Questions
- Spot vs Perps? (If perps: add funding/OI/liquidations)
- Universe: single symbol vs basket; stablecoins vs alts
- Transaction cost model: for a 3h horizon, slippage assumptions should be realistic
