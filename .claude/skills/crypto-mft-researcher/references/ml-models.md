# ML Model Zoo for Crypto MFT

## Table of Contents
1. [Label Design](#label-design)
2. [Model Selection Guide](#model-selection-guide)
3. [Linear Models](#linear-models)
4. [Gradient Boosted Trees](#gradient-boosted-trees)
5. [Neural Networks](#neural-networks)
6. [Online / Incremental Models](#online--incremental-models)
7. [Ensemble & Combination](#ensemble--combination)
8. [Training Methodology](#training-methodology)
9. [Feature Selection](#feature-selection)

---

## Label Design

Label design is the most critical decision. Poor labels = wasted compute regardless of model.

### Forward Return Labels
- **Raw forward return:** `r_{t+h} = (mid_{t+h} - mid_t) / mid_t`. Simple, but noisy.
- **Vol-adjusted return:** `r_{t+h} / sigma_t`. Normalizes across vol regimes. Preferred.
- **Risk-adjusted return (Sharpe-like):** `mean(r_{t:t+h}) / std(r_{t:t+h})`. Path-dependent.
- **Log return:** `ln(mid_{t+h} / mid_t)`. Better for multiplicative dynamics.

### Advanced Labels
- **Triple-barrier method (de Prado):** Label = which barrier hit first: upper (profit-take), lower (stop-loss), or time (vertical). Captures path, not just endpoint. Parameters: `pt_sl` ratio, `max_holding`.
- **Trend-scanning label:** Fit `price ~ a + b*t` over multiple forward windows, pick window with highest t-stat of `b`. Adaptive horizon, captures trend strength.
- **Residual return:** `r_asset - beta * r_BTC`. Alpha component after hedging systematic exposure.
- **Cost-adjusted return:** `r_{t+h} - 2 * spread - 2 * fee`. Only labels that survive transaction costs.

### Label Considerations for MFT
- Horizon `h` should match target holding period: 30s, 1m, 5m, 15m, 30m, 1hr.
- **Multi-horizon:** Train on multiple horizons simultaneously (multi-task). Model learns signal persistence.
- **Classification vs regression:** Classification (up/flat/down) is more robust for noisy MFT data. Threshold at `+/- 0.5 * spread` to define "flat" zone.
- **Sample weighting:** Weight by `1/sigma_t` (inverse vol) or by uniqueness (average uniqueness from de Prado).

## Model Selection Guide

| Criterion | Linear | GBT | Neural Net | Online |
|---|---|---|---|---|
| Data size | Any | >100K rows | >1M rows | Streaming |
| Feature types | Numeric | Numeric + ordinal | Numeric | Numeric |
| Interpretability | High | Medium (SHAP) | Low | Medium |
| Training speed | Fast | Medium | Slow | Real-time |
| Non-linearity | No | Yes | Yes | Limited |
| Feature interactions | Manual | Automatic | Automatic | Manual |
| Regime adaptation | Retrain | Retrain | Fine-tune | Continuous |
| Overfitting risk | Low | Medium | High | Low |

**Practical recommendation:** Start with LightGBM for fast iteration, add linear models as baseline, consider neural nets only when GBT plateaus and data is abundant.

## Linear Models

### Ridge Regression
- **When:** Baseline model, many correlated features, need interpretability.
- **Crypto MFT notes:** Surprisingly competitive for microstructure features. Fast to retrain.
- **Key params:** `alpha` (regularization). CV over `[0.01, 0.1, 1, 10, 100]`.
- **Feature prep:** Z-score normalize. Add explicit interaction terms if needed.

### Lasso / ElasticNet
- **When:** Feature selection needed, suspect many irrelevant features.
- **Crypto MFT notes:** Good for discovering which features matter. ElasticNet handles correlated groups.
- **Key params:** `alpha`, `l1_ratio` (ElasticNet only).

### Quantile Regression
- **When:** Predict return distribution, not just mean. Useful for risk-aware signals.
- **Crypto MFT notes:** Predict 5th/95th percentile to estimate tail risk. Use `quantile=0.05, 0.5, 0.95`.

## Gradient Boosted Trees

### LightGBM (Recommended Default)
- **When:** Primary model for tabular MFT features. Best speed/performance trade-off.
- **Crypto MFT config:**
  ```python
  params = {
      'objective': 'regression',       # or 'binary', 'multiclass'
      'metric': 'mse',                 # or custom IC metric
      'num_leaves': 31,                # start conservative
      'max_depth': 6,                  # prevent overfitting
      'learning_rate': 0.05,
      'feature_fraction': 0.7,         # column subsampling
      'bagging_fraction': 0.7,         # row subsampling
      'bagging_freq': 1,
      'min_child_samples': 100,        # increase for noisy MFT data
      'reg_alpha': 0.1,                # L1
      'reg_lambda': 1.0,               # L2
      'n_estimators': 500,
      'early_stopping_rounds': 50,
  }
  ```
- **Anti-overfit:** Increase `min_child_samples` (100-1000 for MFT), reduce `num_leaves`, use `feature_fraction < 0.8`.
- **Feature importance:** Use SHAP (`shap.TreeExplainer`). MDI (built-in) is biased toward high-cardinality features.

### XGBoost
- **When:** Need GPU training, larger datasets, or histogram-based with `tree_method='gpu_hist'`.
- **Crypto MFT notes:** Slightly slower than LightGBM for same accuracy. Better regularization options (`gamma`, `min_child_weight`).

### CatBoost
- **When:** Have categorical features (exchange_id, symbol, session_flag), want built-in ordered boosting (reduces overfitting on time-series).
- **Crypto MFT notes:** Ordered boosting is valuable for time-series. `has_time=True` respects temporal order.

## Neural Networks

### MLP (Multi-Layer Perceptron)
- **When:** Non-linear feature interactions, enough data (>500K samples).
- **Architecture:** 2-3 hidden layers, [256, 128, 64], BatchNorm + Dropout(0.3) between layers.
- **Crypto MFT notes:** Good when features are pre-engineered. Not better than GBT on raw tabular.

### Temporal Convolutional Network (TCN)
- **When:** Raw time-series input (e.g., last 100 book snapshots), want to learn temporal patterns.
- **Architecture:** Dilated causal convolutions, residual connections. Dilation = [1, 2, 4, 8, 16].
- **Crypto MFT notes:** Good for learning microstructure patterns from raw LOB data. Faster than LSTM.

### LSTM / GRU
- **When:** Sequential dependencies matter, moderate sequence lengths (50-200 steps).
- **Architecture:** 1-2 layers, hidden_size=64-128, bidirectional=False (causal).
- **Crypto MFT notes:** Captures regime transitions. Often overkill for pre-engineered features.

### Transformer (Temporal)
- **When:** Long sequences, multiple assets simultaneously, attention patterns informative.
- **Architecture:** 2-4 layers, 4-8 heads, d_model=64-128. Positional encoding = learned or sinusoidal.
- **Crypto MFT notes:** Cross-asset attention is powerful (which assets lead?). Expensive to train. Consider only with >10M samples.

### TabNet
- **When:** Want attention-based feature selection on tabular data, interpretability via attention masks.
- **Crypto MFT notes:** Competitive with GBT, built-in feature selection. Good for understanding which features matter at each prediction.

### Training Tips for Neural Nets in MFT
- **Learning rate:** Use cosine annealing or OneCycleLR. Start 1e-3, min 1e-5.
- **Batch size:** 1024-4096 for MFT (many samples). Larger = more stable gradients.
- **Normalization:** BatchNorm for MLP, LayerNorm for Transformer/LSTM.
- **Loss functions:** MSE for regression, focal loss for imbalanced classification, quantile loss for distributional.
- **Regularization:** Dropout(0.2-0.4), weight decay(1e-4), early stopping on validation IC.

## Online / Incremental Models

For real-time adaptation without full retraining:

### Online Linear Models
- **SGD Regressor/Classifier:** Scikit-learn's `SGDRegressor` with `partial_fit()`.
- **Crypto MFT notes:** Fast adaptation to regime changes. Use exponential decay on old samples.

### Incremental GBT
- **LightGBM `init_model`:** Continue training from previous model with new data.
- **XGBoost `xgb_model`:** Similar continuation.
- **Crypto MFT notes:** Retrain daily/hourly with warm start. Avoids catastrophic forgetting.

### River Library
- **When:** True streaming ML. Models update per observation.
- **Models:** `river.linear_model.LogisticRegression`, `river.tree.HoeffdingTreeClassifier`, `river.ensemble.AdaptiveRandomForestClassifier`.
- **Crypto MFT notes:** Good for features that need continuous recalibration. Latency-sensitive applications.

## Ensemble & Combination

### Stacking
- Level-0: Multiple diverse models (Ridge, LightGBM, MLP).
- Level-1: Ridge regression on out-of-fold predictions from level-0.
- **Crypto MFT notes:** 5-10% IC improvement typical. Use purged CV for level-0 predictions.

### Signal Blending
- **Equal weight:** Simple average of model predictions. Hard to beat.
- **IC-weighted:** Weight by rolling IC. Adapts to model performance shifts.
- **Inverse-vol weighted:** Weight by `1/std(prediction)`. Penalizes unstable models.

### Adversarial Validation
- Train classifier to distinguish train vs test. If AUC >> 0.5, distribution shift exists.
- **Crypto MFT notes:** Essential before any train/test split. Crypto regimes shift hard.

## Training Methodology

### Time-Series Cross-Validation

**Purged K-Fold (de Prado):**
```
Train: [-------]  gap  Test: [---]
         Fold 1

Train: [----]  gap  [---]  gap  Test: [---]
         Fold 2 (includes test from fold 1 after purge)
```
- **Purge window:** Remove `h` bars before and after each test fold (h = label horizon).
- **Embargo:** Additional gap after purge to handle serial correlation. Typically `0.01 * N`.

**Walk-Forward:**
```
Train: [==========]  Test: [==]
Train: [============]  Test: [==]
Train: [==============]  Test: [==]
```
- Expanding or rolling window. Most realistic for production.
- **Crypto MFT notes:** Use rolling (not expanding) window if regimes shift. 30-90 day train, 1-7 day test.

**Combinatorial Purged CV (CPCV):**
- All combinations of K groups as train/test. Generates more paths for overfitting tests.
- See de Prado "Advances in Financial Machine Learning" ch. 12.

### Sample Weighting
- **Uniqueness weight:** If labels overlap (triple-barrier), weight by average uniqueness.
- **Decay weight:** Exponential decay on older samples. Recent data more relevant.
- **Return-magnitude weight:** `abs(r)` weighting. Focus on meaningful moves, not noise.

### Hyperparameter Optimization
- **Optuna** with time-series CV as objective. Use median pruner.
- **Key insight:** Optimize for IC (information coefficient) or risk-adjusted return, not MSE/accuracy.
- **Budget:** 100-200 trials for GBT, 50-100 for neural nets.

## Feature Selection

### Importance-Based
- **MDI (Mean Decrease Impurity):** Built into tree models. Fast but biased to high-cardinality.
- **MDA (Mean Decrease Accuracy):** Permutation importance. Unbiased but slow. Use on test set.
- **SHAP values:** Most reliable. `shap.TreeExplainer` for GBT, `shap.DeepExplainer` for neural nets.

### Statistical
- **Mutual Information:** `sklearn.feature_selection.mutual_info_regression`. Captures non-linear dependence.
- **IC (Information Coefficient):** `spearmanr(feature, forward_return)`. Quick and interpretable.
- **IC stability:** `mean(IC) / std(IC)` (ICIR). Features with high IC but low stability are unreliable.

### Iterative
- **Sequential Feature Selection (SFS):** Forward or backward. Expensive but finds optimal subset.
- **Recursive Feature Elimination (RFE):** Remove least important, retrain, repeat.

### Practical Workflow
1. Compute IC and ICIR for all features → remove IC ~ 0 features.
2. Remove correlated pairs (|corr| > 0.9) keeping higher IC.
3. Train LightGBM, get SHAP importance → keep top N features.
4. Validate that fewer features don't hurt OOS performance.
5. Re-run periodically (monthly) as feature relevance drifts.
