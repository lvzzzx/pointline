# ML Models for CN A-Share Intraday

## Table of Contents
1. [Label Design](#label-design)
2. [Model Selection Guide](#model-selection-guide)
3. [Linear Models](#linear-models)
4. [Gradient Boosted Trees](#gradient-boosted-trees)
5. [Neural Networks](#neural-networks)
6. [Ensemble & Combination](#ensemble--combination)
7. [Training Methodology](#training-methodology)
8. [Feature Selection](#feature-selection)

---

## Label Design

### Forward Return Labels
- **Raw forward return:** `r_{t+h} = (mid_{t+h} - mid_t) / mid_t`. Simple, noisy.
- **Vol-adjusted return:** `r_{t+h} / sigma_t`. Normalizes across vol regimes. Preferred.
- **VWAP-benchmarked return:** `(VWAP_{t:t+h} - mid_t) / mid_t`. More realistic execution benchmark than point-in-time price.
- **Alpha return:** `r_{t+h} - beta * r_{CSI300, t+h}`. Residual after hedging systematic risk with index futures.
- **Cost-adjusted return:** `r_{t+h} - 2 * (commission + half_spread + slippage_est)`. Only meaningful predictions survive costs.

### Classification Labels
- **Three-class:** Up/flat/down. Threshold at `+/- k * spread` to define flat zone. k=1-3 depending on horizon.
- **Binary (direction):** `sign(r_{t+h})`. Simple but ignores magnitude.
- **Quantile labels:** Top/bottom quintile of cross-sectional returns. For cross-sectional models.

### CN-Specific Label Considerations
- **Lunch break:** Labels spanning 11:30-13:00 include non-trading time. Either: (a) skip labels that cross lunch, or (b) measure returns only during trading hours.
- **Auction labels:** Do not use continuous-trading features to predict auction outcomes, and vice versa. Different information environment.
- **T+1 constraint on short signals:** If model predicts "sell," you must already own the stock. For long-short backtest, model long-side execution (buy today, sell tomorrow minimum) vs short-side (sell existing holding today). This asymmetry affects label design.
- **Price limit censoring:** When stocks hit limit, forward returns are truncated. Either: (a) exclude limit-locked observations from training, or (b) use censored regression (Tobit model).
- **Session-specific labels:** Train separate AM/PM models if signal dynamics differ significantly (they often do).

### Horizon Selection
- For 1min-2hr holding period, typical label horizons: 1m, 5m, 15m, 30m, 1hr, 2hr.
- **Multi-horizon training:** Predict multiple horizons simultaneously (multi-task). Model learns signal persistence.
- **Optimal horizon:** Grid search over horizons, maximize cost-adjusted IC.

## Model Selection Guide

| Criterion | Linear | GBT | Neural Net |
|---|---|---|---|
| Data size | Any | >100K rows | >1M rows |
| Interpretability | High | Medium (SHAP) | Low |
| Training speed | Fast | Medium | Slow |
| Non-linearity | No | Yes | Yes |
| Feature interactions | Manual | Automatic | Automatic |
| Regime adaptation | Retrain | Retrain/warm-start | Fine-tune |
| Overfitting risk | Low | Medium | High |
| Cross-sectional | Natural | Per-stock or pooled | Pooled |

**Practical path:** Ridge baseline → LightGBM for fast iteration → Neural net only when GBT plateaus and data is abundant.

## Linear Models

### Ridge Regression
- **When:** Baseline model, cross-sectional prediction, many correlated features.
- **CN intraday notes:** Strong baseline for cross-sectional alpha (rank features → Ridge → rank prediction). Fast retraining allows daily recalibration.
- **Key params:** `alpha` in `[0.01, 0.1, 1, 10, 100]`.
- **Feature prep:** Z-score or rank-normalize. Add explicit interaction terms if needed.

### Lasso / ElasticNet
- **When:** Feature selection needed, suspect many irrelevant features.
- **CN intraday notes:** Useful for discovering which of 100+ LOB features actually matter. ElasticNet handles correlated groups (e.g., multiple book imbalance measures).

### Quantile Regression
- **When:** Predict return distribution, risk-aware signals.
- **CN intraday notes:** Predict 5th/95th percentile for risk sizing. Fat-tailed CN returns make quantile regression valuable.

## Gradient Boosted Trees

### LightGBM (Recommended Default)
- **CN intraday config:**
  ```python
  params = {
      'objective': 'regression',       # or 'multiclass' for 3-class
      'metric': 'mse',                 # or custom IC metric
      'num_leaves': 31,
      'max_depth': 6,
      'learning_rate': 0.05,
      'feature_fraction': 0.7,
      'bagging_fraction': 0.7,
      'bagging_freq': 1,
      'min_child_samples': 200,        # higher for noisy intraday data
      'reg_alpha': 0.1,
      'reg_lambda': 1.0,
      'n_estimators': 500,
      'early_stopping_rounds': 50,
  }
  ```
- **Anti-overfit:** Increase `min_child_samples` (200-2000 for intraday). Reduce `num_leaves`. Use `feature_fraction < 0.8`.
- **Feature importance:** SHAP (`shap.TreeExplainer`). MDI is biased toward high-cardinality features.
- **Pooled vs per-stock:** Pool across stocks with stock-level features (market cap, sector, board) for better generalization. Per-stock models only for mega-caps with sufficient data.

### CatBoost
- **When:** Categorical features (board_type, sector, exchange, session_flag).
- **CN intraday notes:** Ordered boosting (`has_time=True`) respects temporal order. Valuable for time-series. Built-in categorical handling avoids one-hot explosion.

## Neural Networks

### MLP
- **When:** Non-linear feature interactions, >500K samples.
- **Architecture:** 2-3 layers, [256, 128, 64], BatchNorm + Dropout(0.3).
- **CN intraday notes:** Good for pre-engineered feature vectors. Not better than GBT on tabular unless data is very large.

### TCN (Temporal Convolutional Network)
- **When:** Raw time-series input (e.g., last 100 L2 snapshots or last N trade events).
- **Architecture:** Dilated causal convolutions, residual connections. Dilation = [1, 2, 4, 8, 16].
- **CN intraday notes:** Good for learning LOB patterns from raw L2/L3 sequences. Faster than LSTM. Input: multi-level book state over time.

### Transformer
- **When:** Cross-stock attention (which stocks lead?), long sequences.
- **Architecture:** 2-4 layers, 4-8 heads, d_model=64-128.
- **CN intraday notes:** Cross-stock attention discovers sector/style relationships. Expensive. Consider only with >10M samples. Promising for cross-sectional models pooling all stocks.

### Training Tips for Neural Nets
- **Learning rate:** Cosine annealing or OneCycleLR. Start 1e-3, min 1e-5.
- **Batch size:** 1024-4096.
- **Normalization:** BatchNorm for MLP, LayerNorm for Transformer.
- **Loss:** MSE for regression, focal loss for imbalanced classification.
- **Regularization:** Dropout(0.2-0.4), weight decay(1e-4), early stopping on validation IC.

## Ensemble & Combination

### Stacking
- Level-0: Ridge + LightGBM + MLP (diverse models).
- Level-1: Ridge regression on out-of-fold predictions.
- **CN intraday notes:** 5-10% IC improvement typical. Use purged CV for level-0 predictions.

### Signal Blending
- **Equal weight:** Simple average. Hard to beat.
- **IC-weighted:** Weight by rolling IC. Adapts to performance shifts.
- **Time-horizon blending:** Blend short-horizon (5min) and medium-horizon (1hr) models. Different features dominate at each horizon.

### Adversarial Validation
- Train classifier to distinguish train vs test data. If AUC >> 0.5, distribution shift exists.
- **CN intraday notes:** Essential. A-share regimes shift dramatically (2020 bull vs 2022 bear vs 2024 recovery). Check for drift between training and evaluation periods.

## Training Methodology

### Time-Series Cross-Validation

**Walk-Forward (Recommended for CN Intraday):**
```
Train: [=== 30-60 trading days ===]  Test: [= 5-10 days =]
Train: [===== 30-60 trading days =====]  Test: [= 5-10 days =]
Train: [======= 30-60 trading days =======]  Test: [= 5-10 days =]
```
- Rolling or expanding window. Rolling preferred if regimes shift (they do in CN).
- **Intraday granularity:** Train window = 30-60 trading days of intraday data. Test = 5-10 days.

**Purged K-Fold:**
```
Train: [-------]  gap  Test: [---]
```
- **Purge window:** Remove `h` bars (label horizon) before and after each test fold.
- **Embargo:** Additional gap. Typically `max(30min, 1% of sample)` for intraday.
- **Lunch break as natural purge:** The 90-minute lunch break provides a natural embargo within each day.

**Cross-Sectional CV:**
- For cross-sectional models: split by time (walk-forward), not by stock.
- Never randomly split stocks across folds — introduces lookahead.

### Sample Weighting
- **Decay weight:** Exponential decay on older samples. Recent market conditions more relevant.
- **Inverse-vol weight:** `1/sigma_t`. Focus on calm-market predictions (more reliable).
- **Uniqueness weight:** For overlapping labels, weight by average uniqueness (de Prado).
- **Session weight:** Optionally upweight AM (more volume/signal) or equalize AM/PM.

### Online Learning & Warm-Starting
- **Incremental LightGBM:** Use `init_model` to warm-start from previous model on new data. Avoids full retrain between walk-forward windows.
- **Adaptive Ridge:** Update coefficients incrementally with exponential decay on older samples. Near-zero cost retraining.
- **Use case:** Between full retrains (e.g., weekly), warm-start daily to adapt to regime shifts without full CV.
- **Risk:** Warm-starting can accumulate drift. Periodically validate warm-started model against full retrain.

### Hyperparameter Optimization
- **Optuna** with walk-forward IC as objective. Median pruner for efficiency.
- **Key insight:** Optimize for IC or cost-adjusted Sharpe, not MSE/accuracy.
- **Budget:** 100-200 trials for GBT, 50-100 for neural nets.
- **Recalibrate monthly:** CN market microstructure evolves with regulatory changes.

## Feature Selection

### Importance-Based
- **SHAP values:** Most reliable. `shap.TreeExplainer` for GBT.
- **Permutation importance (MDA):** Unbiased but slow. Compute on test set only.
- **MDI (built-in):** Fast but biased toward high-cardinality features. Use only for rough screening.

### Statistical
- **IC (Rank correlation):** `spearmanr(feature, forward_return)`. Quick per-feature screen.
- **ICIR:** `mean(IC) / std(IC)`. Stability matters more than raw IC for CN intraday.
- **Mutual Information:** Captures non-linear dependence. Good for discovering non-obvious features.

### Practical Workflow
1. Compute IC and ICIR for all features → remove IC ~ 0 features.
2. Remove correlated pairs (|corr| > 0.9), keep higher IC.
3. Train LightGBM, get SHAP → keep top N features.
4. Validate: fewer features shouldn't hurt OOS performance.
5. Re-run monthly. CN feature relevance drifts with regulatory and market regime changes.

### CN-Specific Feature Selection Notes
- **Board-dependent features:** Some features matter only for ChiNext/STAR (wider limits) or Main Board. Consider board-conditional feature selection.
- **Price-level effects:** Microstructure features behave differently for low-priced vs high-priced stocks due to tick-size effects. May need separate feature sets.
- **Session-dependent:** AM features may rank differently than PM features. Consider session-specific selection.
