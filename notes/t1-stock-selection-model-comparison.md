# Stock Selection Model Comparison for T+1 Rotation

> Analysis of modeling approaches for the cross-sectional stock selection task in T+1 rotation strategies.

---

## The Problem Formulation

```
Input:  Features for N stocks at Day T close (N ~ 500 for CSI 500)
Output: Expected return for each stock from Day T close to Day T+1 exit
        (close-to-sell-execution label)

Type: Cross-sectional regression / ranking problem
Constraint: Signal must have IC > 0.02 at 20-hour horizon
```

---

## Model Comparison

### 1. Gradient Boosted Trees (LightGBM / XGBoost)

**Recommendation: ⭐ BEST CHOICE for most practitioners**

| Aspect | Assessment |
|--------|------------|
| **Predictive Power** | ✅ Excellent - captures non-linear interactions automatically |
| **Robustness** | ✅ Built-in regularization (L1/L2, max_depth, subsampling) |
| **Feature Handling** | ✅ Handles mixed types (price ratios, raw volumes, categorical sectors) |
| **Training Speed** | ✅ LightGBM is very fast even on large datasets |
| **Interpretability** | ⚠️ Partial - feature importance available, but interactions opaque |
| **Overfitting Risk** | ⚠️ Medium - requires careful hyperparameter tuning |

**When to Use:**
- Default choice for T+1 rotation
- When you have 50+ features of mixed types
- When feature interactions are likely important (e.g., volume × volatility)

**Example Implementation:**
```python
import lightgbm as lgb

# Cross-sectional z-score features first
features = ['return_5d', 'volume_zscore', 'sue', 'sector_momentum',
            'session_imbalance', 'spread_regime', 'turnover_anomaly']

params = {
    'objective': 'regression',
    'metric': 'rmse',
    'boosting_type': 'gbdt',
    'num_leaves': 31,          # Controls complexity
    'learning_rate': 0.05,
    'feature_fraction': 0.8,   # Column sampling
    'bagging_fraction': 0.8,   # Row sampling
    'bagging_freq': 5,
    'lambda_l1': 0.1,          # L1 regularization
    'lambda_l2': 1.0,          # L2 regularization
    'verbose': -1
}

# Train with temporal cross-validation
model = lgb.train(params, train_data, valid_sets=[val_data])

# Inference: cross-sectional rank
predictions = model.predict(features_t)
z_scores = (predictions - predictions.mean()) / predictions.std()
top_stocks = stocks[z_scores > 1.0]  # Top ~16% of universe
```

**Hyperparameter Guidance:**
```
Start with:
  num_leaves: 15-31 (lower = less overfitting)
  learning_rate: 0.03-0.1
  feature_fraction: 0.6-0.9
  lambda_l1/l2: 0.1-10.0

Tune via:
  - Time-series cross-validation (never random split)
  - Target: maximize IC (not RMSE)
```

---

### 2. Linear Model with Regularization (Ridge / Elastic Net)

**Recommendation: ⭐ BEST for interpretability and baseline**

| Aspect | Assessment |
|--------|------------|
| **Predictive Power** | ⚠️ Good - if features are well-engineered |
| **Robustness** | ✅ Very robust with proper regularization |
| **Feature Handling** | ⚠️ Requires standardization (z-scores) |
| **Training Speed** | ✅ Extremely fast |
| **Interpretability** | ✅ Excellent - coefficient = feature contribution |
| **Overfitting Risk** | ✅ Low with cross-validation on alpha |

**When to Use:**
- Building your first T+1 model (baseline)
- When interpretability is critical (regulatory, PM oversight)
- When feature count > sample count (use Lasso)
- When features are already well-engineered (ratios, z-scores)

**Example Implementation:**
```python
from sklearn.linear_model import RidgeCV
from sklearn.preprocessing import StandardScaler

# All features must be cross-sectionally z-scored first
features_cs = features.groupby('date').apply(
    lambda x: (x - x.mean()) / x.std()
).reset_index(drop=True)

# Ridge with cross-validated regularization
model = RidgeCV(alphas=[0.01, 0.1, 1.0, 10.0, 100.0], cv=5)
model.fit(features_cs, labels)

# Interpretation
coefficients = pd.Series(model.coef_, index=feature_names)
print(coefficients.sort_values(ascending=False))
# Example output:
# sue                    0.35   (earnings surprise drives returns)
# return_5d             -0.28   (short-term reversal)
# session_imbalance      0.22   (order flow persists)
```

**Advantages for T+1:**
```python
# 1. Easy to enforce monotonic constraints (if desired)
# 2. Linear combination = portfolio construction is straightforward
# 3. Feature importance is unambiguous
# 4. Less prone to overfitting to microstructure noise
```

---

### 3. Random Forest

**Recommendation: ⚠️ VIABLE but generally inferior to GBDT**

| Aspect | Assessment |
|--------|------------|
| **Predictive Power** | ⚠️ Good, but usually worse than GBDT |
| **Robustness** | ✅ Very robust (bagging reduces variance) |
| **Feature Handling** | ✅ Good with mixed types |
| **Training Speed** | ⚠️ Slower than LightGBM |
| **Interpretability** | ⚠️ Feature importance available |
| **Overfitting Risk** | ✅ Lower than single trees |

**When to Use:**
- When you need extreme robustness (bagging vs boosting trade-off)
- When training data is small (< 2 years)
- As an ensemble member alongside GBDT

**GBDT vs Random Forest for T+1:**
```
GBDT (LightGBM):
  - Sequential error correction
  - Better for complex feature interactions
  - Faster training
  - More prone to overfit if not tuned
  - -> RECOMMENDED for T+1

Random Forest:
  - Parallel bagging
  - More conservative predictions
  - Slower, more memory
  - Less prone to overfit
  - -> Consider if GBDT overfits
```

---

### 4. Neural Networks (MLP / TabNet)

**Recommendation: ❌ NOT RECOMMENDED for most T+1 applications**

| Aspect | Assessment |
|--------|------------|
| **Predictive Power** | ⚠️ Can be good, but data-hungry |
| **Robustness** | ❌ Prone to overfitting with limited samples |
| **Feature Handling** | ⚠️ Requires careful normalization |
| **Training Speed** | ⚠️ Slow training, needs GPU |
| **Interpretability** | ❌ Black box (unless using attention-based) |
| **Overfitting Risk** | ❌ High with typical quant dataset sizes |

**When to Consider:**
- Very large dataset (> 5 years of daily data × 1000+ stocks)
- Hundreds of features with complex non-linear interactions
- You have regularization expertise (dropout, batch norm, early stopping)
- Running ensemble with multiple model types

**Why NOT for T+1:**
```python
# Typical T+1 training data:
n_samples = 242 days/year × 3 years × 500 stocks ≈ 363k observations
n_features = 50

# This is SMALL for neural networks
# Risk: Overfitting to noise, poor out-of-sample performance
# Alternative: GBDT achieves better results with less tuning
```

**Exception - TabNet:**
```python
# TabNet (attention-based neural network for tabular data)
# Pros: Built-in feature selection, interpretable attention
# Cons: Still data-hungry, slower than LightGBM

# Only consider if:
# - You have 5+ years of data
# - Feature interactions are highly complex
# - You've already maxed out GBDT performance
```

---

### 5. Linear Model + Hand-Crafted Interactions

**Recommendation: ⚠️ VIABLE for small feature sets**

| Aspect | Assessment |
|--------|------------|
| **Predictive Power** | ⚠️ Depends on interaction quality |
| **Robustness** | ✅ Very robust |
| **Feature Handling** | ⚠️ Requires domain expertise |
| **Training Speed** | ✅ Fast |
| **Interpretability** | ✅ Excellent |
| **Overfitting Risk** | ✅ Low (if interactions are theory-driven) |

**Example:**
```python
# Domain-knowledge interactions
features['volume_x_volatility'] = features['volume_zscore'] * features['volatility_20d']
features['sue_x_size'] = features['sue'] * np.log(features['market_cap'])
features['momentum_x_imbalance'] = features['return_5d'] * features['session_imbalance']

# Then fit linear model
model = RidgeCV(...)
```

**When to Use:**
- You have strong domain knowledge about interactions
- Feature set is small (10-20 features)
- You want maximum interpretability

---

## Decision Matrix

| Your Situation | Recommended Model | Reason |
|----------------|-------------------|--------|
| Starting out, building baseline | **Ridge/Linear** | Fast, interpretable, robust |
| Production strategy, mixed features | **LightGBM** | Best predictive power, handles complexity |
| Regulatory/PM requires interpretability | **Ridge + feature selection** | Coefficients explain decisions |
| Very small dataset (< 2 years) | **Random Forest** | More robust than GBDT |
| Hundreds of raw features | **LightGBM** | Automatic feature selection via splits |
| Already have good features, want ensemble | **Linear + LightGBM ensemble** | Diversification |
| Have 5+ years data, maxed out other models | **TabNet** | Only if simpler models plateau |

---

## Ensemble Approach (Advanced)

**Best practice for production:**

```python
# Multiple diverse models, ensemble predictions
models = {
    'linear': RidgeCV(...),
    'lightgbm': lgb.LGBMRegressor(...),
    'random_forest': RandomForestRegressor(...)
}

# Train each
for name, model in models.items():
    model.fit(X_train, y_train)

# Ensemble: weighted average or simple average
weights = {'linear': 0.3, 'lightgbm': 0.5, 'random_forest': 0.2}

predictions = sum(w * models[m].predict(X) for m, w in weights.items())
```

**Why ensemble works for T+1:**
- Linear captures linear signals (reversal, value)
- LightGBM captures non-linear interactions
- Random Forest reduces variance
- Different models make uncorrelated errors → ensemble IC > individual IC

---

## Key Recommendations

### For Most Practitioners (80% case)

**Use LightGBM with these safeguards:**

```python
1. Strict temporal cross-validation
   - Never shuffle data
   - Use expanding window or walk-forward

2. Regularization
   - lambda_l1 >= 0.1
   - lambda_l2 >= 1.0
   - num_leaves <= 31
   - max_depth <= 6

3. Feature selection
   - Use feature_fraction < 1.0
   - Drop features with < 0.01 importance

4. Target metric
   - Optimize for IC, not RMSE
   - Cross-sectional rank correlation

5. OOS testing
   - Hold out last 6 months
   - Verify IC stability
```

### For Conservative/Regulated Environments

**Use Ridge with feature engineering:**

```python
1. Careful feature engineering
   - All features cross-sectionally z-scored
   - Domain-knowledge interaction terms
   - Remove highly correlated features (|r| > 0.7)

2. Regularization path
   - Test alphas from 0.001 to 1000
   - Choose via time-series CV

3. Stability checks
   - Coefficients stable across 6-month windows
   - No large coefficient changes
```

---

## Feature Engineering Matters More Than Model Choice

**Critical insight:** For T+1 rotation, the **feature set** matters more than the **model**.

```python
# A good linear model with excellent features beats
# a poorly-tuned GBDT with mediocre features

# Focus your effort:
1. Feature engineering (60% of performance)
   - Daily aggregation of microstructure
   - Cross-sectional z-scoring
   - Proper labeling (close-to-execution)

2. Data quality (25% of performance)
   - Point-in-time correctness
   - Survivorship bias handling
   - Corporate action adjustments

3. Model selection (15% of performance)
   - LightGBM vs Ridge is secondary
   - Either can work with good features
```

---

## Summary

| Model | Recommendation | Expected Edge |
|-------|----------------|---------------|
| **LightGBM/XGBoost** | Primary recommendation | IC 0.03-0.06 |
| **Ridge/Elastic Net** | Baseline, interpretable | IC 0.02-0.05 |
| **Random Forest** | If GBDT overfits | IC 0.02-0.04 |
| **Neural Network** | Not recommended | - |
| **Ensemble** | Production best practice | IC 0.04-0.07 |

**Bottom line:** Start with **Ridge for baseline**, upgrade to **LightGBM for production**, consider **ensemble for alpha maximization**.
