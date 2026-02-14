# Evaluation Methodology for Crypto MFT

## Table of Contents
1. [Signal Quality Metrics](#signal-quality-metrics)
2. [Strategy Performance Metrics](#strategy-performance-metrics)
3. [Overfitting Detection](#overfitting-detection)
4. [Decay Analysis](#decay-analysis)
5. [Transaction Cost Analysis](#transaction-cost-analysis)
6. [Common Pitfalls](#common-pitfalls)
7. [Research Workflow Checklist](#research-workflow-checklist)

---

## Signal Quality Metrics

### Information Coefficient (IC)
- **Rank IC (Spearman):** `spearmanr(signal_t, return_{t+h})`. Preferred over Pearson — robust to outliers.
- **Typical values:** IC > 0.02 is meaningful for MFT. IC > 0.05 is strong. IC > 0.10 is exceptional (check for bugs).
- **IC by time bucket:** Compute IC per hour/session. Signals that only work in one session are fragile.
- **IC by regime:** Split by vol regime (high/low). Signal should work in at least 2 regimes.

### ICIR (IC Information Ratio)
- `ICIR = mean(IC) / std(IC)`. Measures IC stability.
- **Target:** ICIR > 0.5 is acceptable. ICIR > 1.0 is strong.
- **Interpretation:** High IC but low ICIR = signal is inconsistent (likely overfit or regime-dependent).

### Hit Rate
- `hit_rate = P(sign(signal) == sign(return))`. Only meaningful after spread filter.
- **Threshold:** Hit rate > 51% net of costs is meaningful at MFT frequency.
- **Conditional hit rate:** Compute for high-conviction predictions only (|signal| > threshold).

### Turnover
- `turnover = mean(|position_t - position_{t-1}|)`. Measures how often signal changes.
- **Crypto MFT typical:** 20-200% daily turnover depending on horizon.
- **Cost-adjusted IC:** `IC_net = IC - turnover * cost_per_unit`. True signal after costs.

## Strategy Performance Metrics

### Return-Based
- **Sharpe ratio:** `mean(r) / std(r) * sqrt(periods_per_year)`. Annualize correctly for 24/7 crypto: `sqrt(365 * 24 * periods_per_hour)`.
- **Sortino ratio:** Penalizes only downside vol. `mean(r) / downside_std * sqrt(N)`.
- **Calmar ratio:** `annualized_return / max_drawdown`. Captures tail risk.

### Drawdown-Based
- **Max drawdown:** Worst peak-to-trough decline. Critical for position sizing.
- **Max drawdown duration:** Time to recover from max drawdown. Crypto can have multi-month drawdowns.
- **Underwater curve:** Continuous drawdown from running peak. Visualize always.

### Risk-Adjusted
- **PnL per trade:** `total_pnl / num_trades`. Must exceed 2x spread to be viable.
- **Win rate x avg_win / (loss_rate x avg_loss):** Profit factor. Must exceed 1.0 after costs.
- **Tail ratio:** `|percentile_95(returns)| / |percentile_05(returns)|`. Right-skewed preferred.

### Crypto-Specific
- **PnL by session:** Break down by Asia/EU/US sessions. Uniform = robust, concentrated = fragile.
- **PnL by funding period:** Does strategy cluster PnL around funding resets?
- **PnL by volatility regime:** Backtest in top/bottom vol quintiles separately.
- **Correlation to BTC:** `corr(strategy_returns, BTC_returns)`. Low correlation = valuable diversifier.

## Overfitting Detection

### Deflated Sharpe Ratio (DSR)
- Adjusts Sharpe for number of trials tested. `DSR = P(SR* > 0 | num_trials, skew, kurtosis)`.
- **Rule of thumb:** If you tested N strategies, required Sharpe scales as `sqrt(2 * ln(N))`.
- **Crypto MFT:** Tested 100 features? Need Sharpe > ~3.0 to be significant at 5% level.

### Probability of Backtest Overfitting (PBO)
- CPCV-based. Fraction of combinations where in-sample best strategy underperforms OOS.
- **Target:** PBO < 0.3 (less than 30% chance the best IS strategy is overfit).
- Requires combinatorial purged CV implementation (see de Prado).

### Multiple Testing Corrections
- **Bonferroni:** Divide significance level by number of tests. Conservative.
- **Holm-Bonferroni:** Step-down procedure. Less conservative than Bonferroni.
- **FDR (Benjamini-Hochberg):** Controls false discovery rate. Appropriate when testing many features.

### Practical Overfit Checks
- **IS vs OOS Sharpe ratio:** `OOS_Sharpe / IS_Sharpe`. Ratio < 0.5 = likely overfit.
- **Parameter stability:** Perturb hyperparameters +/-20%. If Sharpe drops >50%, overfit.
- **Temporal stability:** Sharpe in each quarter. Should be positive in >60% of quarters.
- **Random feature test:** Add 5 random features. If model uses them, it's memorizing noise.
- **Shuffled label test:** Shuffle labels, retrain. If performance doesn't drop to ~0, data leakage.

## Decay Analysis

### Signal Decay Curve
- Compute IC at horizons `h = [10s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1hr, 2hr, 4hr]`.
- Plot IC(h) vs h. Shape reveals signal nature:
  - **Monotonic decay:** Classic alpha. Trade at shortest viable horizon.
  - **Peak then decay:** Optimal horizon at peak. Capturing momentum.
  - **U-shape:** Two different effects (short-term reversal + medium-term momentum).

### Half-Life Estimation
- Fit `IC(h) = IC_0 * exp(-h / tau)`. Half-life = `tau * ln(2)`.
- **Crypto MFT typical:** 30s-5min for microstructure signals, 5-30min for flow signals, 1-4hr for cross-asset.
- **Implication:** Target holding period ~ half-life. Shorter = higher Sharpe but more turnover/costs.

### Optimal Holding Period
- Grid search over holding periods, maximize `Sharpe_net = Sharpe_gross - cost * turnover(holding_period)`.
- Usually optimal holding period > signal half-life due to transaction costs.

## Transaction Cost Analysis

### Cost Components for Crypto
- **Exchange fee:** Maker: -0.01% to 0.02%, Taker: 0.03% to 0.06% (varies by exchange/VIP tier).
- **Spread cost:** Half-spread per side. Highly variable: 0.5bps (BTC) to 20bps+ (small alts).
- **Slippage / market impact:** `impact = lambda * sqrt(order_size / ADV)`. Estimate lambda from historical data.
- **Funding cost:** If holding perps, net funding paid/received. Can be +/- 0.01-0.3% per 8hr period.

### Cost-Adjusted Backtest
- Deduct `2 * (fee + half_spread + slippage_estimate)` per round-trip trade.
- **Conservative estimate:** Use taker fees, median spread, no favorable fills.
- **Optimistic estimate:** Use maker fees, best-bid/ask fills. Reality is between.
- **Break-even IC:** Minimum IC required to overcome costs. `IC_min = 2 * total_cost / std(return) / sqrt(N_trades)`.

### Capacity Estimation
- **ADV test:** Strategy volume should be < 1% of ADV for target symbols.
- **Impact test:** Scale up position size, measure Sharpe degradation curve.
- **Practical limit:** For crypto MFT, $1M-$50M capacity on major pairs, $100K-$5M on altcoins.

## Common Pitfalls

### Data Issues
- **Lookahead bias:** Using future data in features or labels. Most common: normalization with future stats, using close price that includes after-hours data.
- **Survivorship bias:** Only backtesting on currently listed tokens. Must include delisted tokens.
- **Data quality:** Missing data, incorrect timestamps, duplicate entries. Crypto data is messy. Validate.
- **Exchange clock drift:** Exchange timestamps can differ by 100ms+. Align carefully for cross-exchange features.

### Methodological Issues
- **Train-test contamination:** Overlapping labels across folds. Must purge + embargo.
- **Non-stationarity:** Crypto regimes shift hard (2020 != 2022 != 2024). Walk-forward only.
- **Selection bias:** Choosing features/models after seeing test results. Keep a hold-out set that is NEVER touched until final validation.
- **Backfill bias:** Features that are backfilled (e.g., funding rate revisions) look better in backtest than live.

### Crypto-Specific Pitfalls
- **Exchange downtime:** Binance maintenance, exchange outages. Must model inability to trade.
- **Liquidation cascades:** Large moves are not Gaussian. Linear models underestimate tail risk.
- **Fee tier changes:** VIP tiers, promotional periods change cost assumptions mid-backtest.
- **Listing/delisting:** New tokens have unusual dynamics (high vol, low liquidity, wash trading).
- **Wash trading:** Some exchanges/pairs have inflated volume. Use multiple volume sources.
- **Stablecoin depeg events:** USDT/USDC depeg = effective position change. Model as separate risk factor.

## Research Workflow Checklist

### Before Starting
- [ ] Define hypothesis clearly (what signal, why predictive, what mechanism)
- [ ] Specify target horizon and instrument universe
- [ ] Identify data sources and validate data quality
- [ ] Set hold-out period (never touch until final validation)

### Feature Engineering
- [ ] Compute features using only past data (no lookahead)
- [ ] Normalize per-window (expanding or rolling, not global)
- [ ] Check IC and ICIR for each feature individually
- [ ] Remove redundant features (|corr| > 0.9)
- [ ] Log-transform skewed features, winsorize outliers

### Model Training
- [ ] Use purged K-fold or walk-forward CV
- [ ] Start with simple model (Ridge), then GBT, then neural net
- [ ] Tune hyperparameters via Optuna on validation IC
- [ ] Check feature importance (SHAP) — top features should be interpretable
- [ ] Run shuffled-label sanity check

### Evaluation
- [ ] Compute IC, ICIR, Sharpe, max drawdown on OOS data
- [ ] Plot signal decay curve, estimate half-life
- [ ] Run cost-adjusted backtest with conservative cost assumptions
- [ ] Check PnL by session, volatility regime, and over time (stability)
- [ ] Compute deflated Sharpe or PBO for multiple testing correction
- [ ] Estimate capacity (ADV check)

### Final Validation
- [ ] Evaluate on hold-out set (never seen during development)
- [ ] Compare to baseline (buy-and-hold, random signal)
- [ ] Stress test: 2x costs, half liquidity, adverse regime
- [ ] Document: hypothesis, data, features, model, results, limitations
