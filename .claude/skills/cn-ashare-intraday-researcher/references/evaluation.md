# Evaluation Methodology for CN A-Share Intraday

## Table of Contents
1. [Signal Quality Metrics](#signal-quality-metrics)
2. [Strategy Performance Metrics](#strategy-performance-metrics)
3. [Overfitting Detection](#overfitting-detection)
4. [Decay Analysis](#decay-analysis)
5. [Transaction Cost Model](#transaction-cost-model)
6. [CN-Specific Evaluation](#cn-specific-evaluation)
7. [Common Pitfalls](#common-pitfalls)
8. [Research Workflow Checklist](#research-workflow-checklist)

---

## Signal Quality Metrics

### Information Coefficient (IC)
- **Rank IC (Spearman):** `spearmanr(signal_t, return_{t+h})`. Preferred over Pearson — robust to outliers.
- **Typical values (CN intraday):** IC > 0.02 is meaningful, > 0.05 is strong, > 0.08 is exceptional (check for bugs).
- **IC by session:** Compute IC separately for AM (09:30-11:30) and PM (13:00-15:00). Signals that only work in one session are fragile.
- **IC by board:** Compute IC for Main Board vs ChiNext/STAR separately. Different microstructure may yield different IC.

### ICIR (IC Information Ratio)
- `ICIR = mean(IC) / std(IC)`. Measures IC stability.
- **Target:** ICIR > 0.5 acceptable, > 1.0 strong.
- **Interpretation:** High IC but low ICIR = inconsistent signal, likely overfit or regime-dependent.

### Hit Rate
- `hit_rate = P(sign(signal) == sign(return))`. Only meaningful after cost filter.
- **Threshold:** Hit rate > 51% net of CN transaction costs is meaningful.
- **Conditional hit rate:** Compute for high-conviction predictions only (|signal| > threshold).

### Turnover
- `turnover = mean(|position_t - position_{t-1}|)`. How often signal changes.
- **CN intraday typical:** 50-300% daily turnover depending on horizon.
- **Cost-adjusted IC:** `IC_net = IC - turnover * cost_per_unit`. True signal after costs.
- **T+1 impact on turnover:** Long-side turnover is constrained by T+1 (can only sell next-day purchases). Short-side (selling existing holdings) has no such constraint.

## Strategy Performance Metrics

### Return-Based
- **Sharpe ratio:** `mean(r) / std(r) * sqrt(N)`. Annualize: `sqrt(242 * bars_per_day)` for CN (242 trading days/year, ~240min/day).
- **Sortino ratio:** Penalizes only downside vol.
- **Calmar ratio:** `annualized_return / max_drawdown`.

### Drawdown-Based
- **Max drawdown:** Worst peak-to-trough decline.
- **Max drawdown duration:** Trading days to recover.
- **Underwater curve:** Continuous drawdown from running peak. Always visualize.

### Risk-Adjusted
- **PnL per trade:** `total_pnl / num_trades`. Must exceed total round-trip cost.
- **Profit factor:** `gross_profit / gross_loss`. Must exceed 1.0 after costs.
- **Tail ratio:** `|P95(returns)| / |P05(returns)|`. Right-skewed preferred.

### Intraday-Specific
- **PnL by session:** AM vs PM breakdown. Uniform = robust.
- **PnL by trading phase:** Open auction, first 30min, mid-day, last 30min, closing auction.
- **PnL by market regime:** Bull/bear/neutral (from CSI300).
- **PnL by volatility regime:** Backtest in top/bottom vol quintiles.
- **Overnight risk:** For positions held overnight (T+1 constraint), measure overnight gap PnL separately.

## Overfitting Detection

### Deflated Sharpe Ratio (DSR)
- Adjusts Sharpe for number of trials tested.
- **Rule of thumb:** N trials tested → need Sharpe > `sqrt(2 * ln(N))`.
- **CN intraday:** 100 features tested → need Sharpe > ~3.0 for 5% significance.

### Probability of Backtest Overfitting (PBO)
- CPCV-based. Fraction of combinations where IS-best strategy underperforms OOS.
- **Target:** PBO < 0.3.

### Practical Overfit Checks
- **IS vs OOS Sharpe ratio:** `OOS_Sharpe / IS_Sharpe > 0.5`. Below 0.5 = likely overfit.
- **Parameter stability:** Perturb hyperparameters +/-20%. Sharpe drop > 50% = overfit.
- **Temporal stability:** Sharpe positive in > 60% of monthly windows.
- **Random feature test:** Add 5 random features. If model uses them, it's memorizing noise.
- **Shuffled label test:** Shuffle labels, retrain. Performance should drop to ~0.
- **Walk-forward consistency:** OOS IC should be relatively stable across walk-forward windows.

## Decay Analysis

### Signal Decay Curve
- Compute IC at horizons `h = [1m, 2m, 5m, 10m, 15m, 30m, 1hr, 2hr]`.
- Plot IC(h) vs h. Shape reveals signal nature:
  - **Monotonic decay:** Classic alpha. Trade at shortest viable horizon.
  - **Peak then decay:** Optimal horizon at peak (momentum capture).
  - **U-shape:** Short-term reversal + medium-term momentum — two effects.

### Half-Life Estimation
- Fit `IC(h) = IC_0 * exp(-h / tau)`. Half-life = `tau * ln(2)`.
- **CN intraday typical:** 1-5min for LOB features, 5-30min for trade flow, 30min-2hr for cross-sectional.
- **Implication:** Target holding period ~ half-life. Shorter = higher Sharpe but more turnover/costs.

### Optimal Holding Period
- Grid search over holding periods, maximize `Sharpe_net = Sharpe_gross - cost * turnover(h)`.
- In CN, higher costs (stamp duty + commission) push optimal holding toward longer horizons vs US/crypto.

### Lunch Break Decay
- Measure IC decay across lunch break. Some signals survive lunch (fundamental), others don't (microstructure).
- Features with good pre-lunch IC but zero post-lunch IC are session-specific — use separate models.

## Transaction Cost Model

### Cost Components (CN A-Share, 2024-2025)

| Component | Rate | Notes |
|---|---|---|
| **Commission** | 0.02-0.03% (each way) | Negotiable. Minimum 5 CNY per trade for retail. Institutional can be lower. |
| **Stamp duty** | 0.05% (sell-side only) | Reduced from 0.1% in Aug 2023. Only on sells. |
| **Transfer fee** | 0.001% | SSE only. Negligible. |
| **Spread cost** | 1-10 bps per side | Depends on stock liquidity. Use median quoted half-spread. |
| **Market impact** | Variable | `impact = lambda * sqrt(order_size / ADV)`. Estimate from L2/L3 data. |

### Total Round-Trip Cost Estimate

| Stock Type | Conservative | Optimistic |
|---|---|---|
| Large cap (CSI300) | ~15-20 bps | ~10-12 bps |
| Mid cap (CSI500) | ~20-30 bps | ~15-20 bps |
| Small cap | ~30-50+ bps | ~20-30 bps |

**Note:** Includes commission (both sides) + stamp duty (sell only) + spread (both sides) + slippage.

### Cost-Adjusted Backtest
- Deduct `commission_buy + commission_sell + stamp_duty_sell + spread_buy + spread_sell + slippage` per round-trip.
- **Conservative:** Use median spread, taker execution, full stamp duty.
- **Break-even IC:** `IC_min = total_roundtrip_cost / std(return) / sqrt(N_trades)`.

### Maker vs Taker Considerations
- CN exchanges do not have explicit maker/taker fee differentiation for equities.
- However, passive execution (limit orders) avoids crossing the spread.
- MFT strategy should target >50% passive fills for viability.

### Capacity Estimation
- Strategy volume < 1% of ADV for target stocks.
- Impact test: scale position size, measure Sharpe degradation.
- **CN specific:** Main Board large caps: $10M-$100M capacity. Mid caps: $1M-$10M. Small caps: <$1M.

## CN-Specific Evaluation

### T+1 Settlement Impact
- **Long signals:** Buy today → earliest sell is tomorrow. Minimum holding = overnight + next day open.
- **Short signals:** Only actionable if you already hold the stock. Reduces effective universe.
- **Intraday-only backtest vs T+1 realistic backtest:** Intraday-only (no overnight) overstates strategy performance. Always compare:
  - Idealized: instant buy and sell within day.
  - Realistic: buy today, sell tomorrow (minimum 1-day hold).
  - Hybrid: intraday signals drive execution timing, but positions carry overnight.
- **ETF exception:** Some ETFs allow T+0 trading (buy and sell same day). These are legitimate intraday vehicles.

### Session Decomposition
- Always report AM (09:30-11:30) and PM (13:00-15:00) results separately.
- Many signals work in AM but not PM (opening volatility effect).
- Lunch break creates an information discontinuity — treat as two quasi-independent sessions.

### Board-Specific Results
- Report separately for Main Board (+/-10% limit) vs ChiNext/STAR (+/-20% limit).
- Different limit widths create different return distributions and feature dynamics.

### Market Regime Decomposition
- **Bull market (2020-H1, 2024-Q4):** Momentum works, short signals fail, high turnover.
- **Bear market (2022):** Reversal works, long signals fail, liquidity dries up.
- **Range-bound (2023-H1):** Mean-reversion works, signal IC typically highest.
- Signal must show positive IC in at least 2/3 regime types to be robust.

## Common Pitfalls

### Data Issues
- **Lookahead bias:** Using future data. Most common: global normalization (min/max from full dataset), using closing auction price for continuous-session features.
- **Survivorship bias:** Only testing on currently listed stocks. Must include delisted and suspended stocks.
- **Suspension handling:** CN stocks can be suspended for days/months. Exclude suspended periods from both features and labels.
- **L2 snapshot timing:** SZSE snapshots are ~3s intervals, not exact. Feature timestamp may not align exactly with label timestamp. Handle with nearest-prior-snapshot logic.
- **Opening/closing auction data mixing:** Auction-phase data has different properties than continuous trading. Don't mix.

### Methodological Issues
- **Train-test contamination:** Overlapping labels across folds. Must purge + embargo.
- **Non-stationarity:** CN market regimes shift dramatically. Walk-forward only.
- **Selection bias:** Choosing features/models after seeing test results. Keep hold-out set untouched.
- **Cross-sectional leakage:** Using cross-sectional features (rank, sector average) that peek into future cross-section.

### CN-Specific Pitfalls
- **T+1 ignored in backtest:** Assuming same-day buy+sell for stocks. Massively overstates performance.
- **Limit-locked positions:** When stock hits limit, you can't exit. Backtest must model inability to trade at limit.
- **Lunch break ignored:** Computing rolling features across 11:30-13:00 gap. Invalid.
- **Board mixing:** Pooling Main Board and ChiNext without accounting for different limit rules.
- **New listing dynamics:** First 5 days have no price limits. Completely different regime. Exclude or model separately.
- **Regulatory events:** Trading halts, unusual trading alerts (异常交易), CSRC investigations. Must handle gracefully.
- **Stamp duty changes:** Cost assumptions must match the period. Stamp duty was 0.1% before Aug 2023, 0.05% after.

## Research Workflow Checklist

### Before Starting
- [ ] Define hypothesis (what signal, why predictive, what mechanism)
- [ ] Specify target horizon (1min-2hr range)
- [ ] Specify universe (Main Board? ChiNext? STAR? Market cap filter?)
- [ ] Identify data requirements (L2? L3? Which exchange?)
- [ ] Set hold-out period (never touch until final validation)
- [ ] Note T+1 constraint and how it affects strategy design

### Feature Engineering
- [ ] Compute features using only past data (no lookahead)
- [ ] Handle lunch break (no windows spanning 11:30-13:00)
- [ ] Handle auction phases (separate features or exclude)
- [ ] Handle price-limit periods (detect and flag/exclude)
- [ ] Normalize per-window (expanding or rolling)
- [ ] Cross-sectional normalization at each timestamp
- [ ] Check IC and ICIR for each feature individually
- [ ] Remove redundant features (|corr| > 0.9)
- [ ] Reference quant360 skill for L2/L3 schema details

### Model Training
- [ ] Use walk-forward or purged K-fold CV
- [ ] Start with Ridge, then LightGBM, then neural net
- [ ] Tune via Optuna on validation IC
- [ ] Check SHAP feature importance — top features should be interpretable
- [ ] Run shuffled-label sanity check
- [ ] Consider board-specific or session-specific models

### Evaluation
- [ ] Compute IC, ICIR, Sharpe, max drawdown on OOS data
- [ ] Plot signal decay curve, estimate half-life
- [ ] Run cost-adjusted backtest with CN cost model (commission + stamp duty + spread + slippage)
- [ ] Report PnL by session (AM/PM), by phase (open/mid/close), by regime (bull/bear/range)
- [ ] Compute deflated Sharpe for multiple testing correction
- [ ] Estimate capacity (ADV check)
- [ ] Compare idealized vs T+1-realistic backtest

### Final Validation
- [ ] Evaluate on hold-out set (never seen during development)
- [ ] Compare to baseline (buy-and-hold index, random signal)
- [ ] Stress test: 2x costs, half liquidity, adverse regime
- [ ] Test on different boards (Main vs ChiNext/STAR)
- [ ] Document: hypothesis, data, features, model, results, limitations
