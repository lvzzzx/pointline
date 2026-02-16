# H1 Implementation Plan: Auction Order-Flow-Profile Similarity Predicts Dependency Shifts

## Context

This plan implements and evaluates the fundamental hypothesis of the co-trading networks proposal:

> **H1:** Order-flow-profile similarity in call auctions predicts future return-dependency shifts better than rolling return correlation.

If H1 fails, the entire co-trading network idea is dead. This plan must be rigorous enough to give a definitive answer.

Parent proposal: `cotrading-networks-chinese-stocks.md`

## What H1 Claims, Precisely

When two stocks receive correlated institutional order flow during opening auctions over a recent window, their return dependency in the subsequent period will be higher than predicted by lagged return correlation alone.

The economic mechanism: correlated auction order placement reveals that institutional portfolios hold (or are actively rebalancing into/out of) both stocks simultaneously. This latent portfolio linkage creates future return co-movement through correlated rebalancing, common factor exposure changes, and information diffusion across the shared holder base.

The testable prediction: auction similarity at time W leads return-dependency at time W+1, controlling for lagged return-dependency at time W.

## Pipeline Overview

```
Step 1: Universe construction
  └─ Liquid SSE/SZSE A-shares with sufficient auction activity

Step 2: Auction profile extraction (per stock, per day)
  └─ cn_order_events → filter PRE_OPEN → aggregate to 6-dim profile vector

Step 3: Pairwise auction similarity (rolling window)
  └─ Profile vectors → cosine similarity over D-day windows → N×N matrix

Step 4: Return dependency measurement (realized)
  └─ cn_tick_events or cn_l2_snapshots → intraday returns → pairwise correlation

Step 5: Prediction test
  └─ Does auction similarity(W) predict dependency(W+1) beyond lagged dependency(W)?

Step 6: Baselines and comparison
  └─ Lagged correlation, industry co-membership, factor-exposure similarity

Step 7: Statistical evaluation and go/no-go
```

---

## Step 1: Universe Construction

### Selection criteria

For each trading day, include stock `i` if all conditions hold:

1. **Listed and trading:** Not suspended, not in first 5 listing days (no price limit, degenerate auction), not ST-flagged (different limit rules). Validate via `dim_symbol`.
2. **Minimum auction participation:** ≥ 50 order events during PRE_OPEN phase. Stocks with negligible auction activity produce degenerate profiles.
3. **Minimum daily liquidity:** Trailing 20-day median daily turnover ≥ 10M CNY. Illiquid stocks have noisy return correlations.
4. **Not at consecutive price limit:** Exclude stocks that closed at limit-up or limit-down on the previous day. Their next-day auction profiles are structurally one-sided (only buy orders after limit-up, only sell after limit-down).

### Expected universe size

CSI 800 constituents (CSI 300 + CSI 500) plus liquid mid-caps ≈ 1500-2000 stocks per day. This gives ~1M-2M pairs — computationally feasible.

### Implementation

```python
# Pseudo-code — actual implementation uses Polars + Pointline APIs
from pointline.research import load_events, discover_symbols, filter_by_phase, TradingPhase

# Step 1a: Get candidate symbols for a trading_date
candidates = discover_symbols(silver_root=..., exchange="szse", trading_date=date)

# Step 1b: For each candidate, count auction orders
for symbol in candidates:
    orders = load_events(
        silver_root=..., table="cn_order_events",
        exchange="szse", symbol=symbol,
        start=day_start_us, end=day_end_us,
    )
    auction_orders = filter_by_phase(
        orders, exchange="szse", phases=[TradingPhase.PRE_OPEN]
    )
    if len(auction_orders) < 50:
        exclude(symbol)

# Step 1c: Check previous-day limit status from cn_l2_snapshots
# (last snapshot of previous day: if last_price == upper_limit or lower_limit → exclude)
```

**Optimization:** Universe membership is stable day-to-day. Compute the full eligibility once, then update incrementally. Cache the per-day universe as a Parquet sidecar.

---

## Step 2: Auction Profile Extraction

### What we extract

For each stock `i` on each trading day `t`, from `cn_order_events` during `PRE_OPEN` (09:15-09:25 CST):

#### Sub-phase classification

The existing `TradingPhase.PRE_OPEN` covers the full 09:15-09:25 window. For profile construction, we need to distinguish the cancel-allowed and no-cancel sub-phases:

```python
# CST = UTC+8. Convert minute boundaries to local time.
# Cancel-allowed:  09:15:00 - 09:20:00 CST → minute_of_day 555-559
# No-cancel:       09:20:00 - 09:25:00 CST → minute_of_day 560-564

# In Polars, derive minute_of_day from ts_event_us:
local_dt = (
    pl.from_epoch(pl.col("ts_event_us"), time_unit="us")
    .dt.replace_time_zone("UTC")
    .dt.convert_time_zone("Asia/Shanghai")
)
minute_of_day = local_dt.dt.hour() * 60 + local_dt.dt.minute()

# Sub-phase column:
sub_phase = (
    pl.when(minute_of_day < 560)
    .then(pl.lit("CANCEL_ALLOWED"))     # 09:15-09:20: orders can be cancelled
    .otherwise(pl.lit("NO_CANCEL"))     # 09:20-09:25: orders locked
)
```

#### Profile features (6-dimensional vector per stock per day)

| # | Feature | Formula | Source fields | Rationale |
|---|---|---|---|---|
| 1 | **Net order imbalance** | `(Σ buy_qty_NEW − Σ sell_qty_NEW) / (Σ buy_qty_NEW + Σ sell_qty_NEW)` | `side`, `qty`, `event_kind="NEW"` | Directional pressure from all new orders. Range [-1, +1]. |
| 2 | **Volume participation** | `Σ new_order_qty / trailing_5d_median(Σ new_order_qty)` | `qty`, `event_kind="NEW"` | Relative auction activity vs recent history. Normalizes across stocks. |
| 3 | **Cancel rate** | `Σ cancel_qty / Σ new_order_qty` (cancel-allowed phase only) | `qty`, `event_kind` in {"NEW","CANCEL"}, sub_phase="CANCEL_ALLOWED" | High cancel rate = tentative/manipulative orders. Low = committed. |
| 4 | **Commitment ratio** | `Σ no_cancel_phase_new_qty / Σ all_new_qty` | `qty`, `event_kind="NEW"`, sub_phase | Orders placed during no-cancel phase (09:20-09:25) can't be withdrawn. Higher ratio = more confident late-arriving orders. |
| 5 | **Price dispersion** | `std(order_price) / mean(order_price)` across all NEW orders | `price`, `event_kind="NEW"` | Wide dispersion = uncertainty about fair value. Narrow = consensus. Coefficient of variation. |
| 6 | **Order-size concentration** | `gini(individual_order_qty)` across all NEW orders | `qty`, `event_kind="NEW"` | High Gini = few large orders dominate (institutional). Low Gini = many similar-sized orders (retail). |

#### Why these six features

- Features 1, 2 capture **what** participants are doing (direction, intensity).
- Features 3, 4 capture **conviction** (commitment vs tentative).
- Features 5, 6 capture **who** is participating (institutional vs retail signature).

Two stocks with correlated profiles across all six dimensions are receiving similar institutional attention — not just moving in the same direction, but with similar conviction and participant structure.

#### Computation

```python
def compute_auction_profile(
    auction_orders: pl.DataFrame,  # PRE_OPEN filtered cn_order_events for one stock, one day
) -> dict[str, float]:
    """Compute 6-dim auction profile from order events."""

    new_orders = auction_orders.filter(pl.col("event_kind") == "NEW")
    cancels = auction_orders.filter(pl.col("event_kind") == "CANCEL")

    # Feature 1: Net order imbalance
    buy_qty = new_orders.filter(pl.col("side") == "BUY")["qty"].sum()
    sell_qty = new_orders.filter(pl.col("side") == "SELL")["qty"].sum()
    total_qty = buy_qty + sell_qty
    imbalance = (buy_qty - sell_qty) / total_qty if total_qty > 0 else 0.0

    # Feature 2: Volume participation (raw, normalize later with trailing median)
    volume_raw = total_qty  # divide by trailing median in post-processing

    # Feature 3: Cancel rate (cancel-allowed phase only, 09:15-09:20)
    cancel_phase_new = new_orders.filter(pl.col("sub_phase") == "CANCEL_ALLOWED")
    cancel_phase_cancel = cancels.filter(pl.col("sub_phase") == "CANCEL_ALLOWED")
    cancel_new_qty = cancel_phase_new["qty"].sum()
    cancel_cancel_qty = cancel_phase_cancel["qty"].sum()
    cancel_rate = cancel_cancel_qty / cancel_new_qty if cancel_new_qty > 0 else 0.0

    # Feature 4: Commitment ratio (no-cancel phase orders / total)
    no_cancel_new = new_orders.filter(pl.col("sub_phase") == "NO_CANCEL")
    no_cancel_qty = no_cancel_new["qty"].sum()
    commitment = no_cancel_qty / total_qty if total_qty > 0 else 0.0

    # Feature 5: Price dispersion (CV of order prices)
    prices = new_orders["price"].cast(pl.Float64)
    price_mean = prices.mean()
    price_std = prices.std()
    price_dispersion = price_std / price_mean if price_mean > 0 else 0.0

    # Feature 6: Order-size concentration (Gini of order quantities)
    qtys = new_orders["qty"].cast(pl.Float64).sort()
    n = len(qtys)
    if n > 1:
        cumsum = qtys.cum_sum()
        gini = 1 - 2 * cumsum.sum() / (n * qtys.sum()) + 1 / n
    else:
        gini = 0.0

    return {
        "imbalance": imbalance,
        "volume_raw": volume_raw,
        "cancel_rate": cancel_rate,
        "commitment": commitment,
        "price_dispersion": price_dispersion,
        "size_concentration": gini,
    }
```

#### Output format

A DataFrame with schema:

```
trading_date | symbol | imbalance | volume_participation | cancel_rate | commitment | price_dispersion | size_concentration
```

One row per stock per day. `volume_participation` is computed as `volume_raw / trailing_5d_median(volume_raw)` in a post-processing step across the rolling window.

#### Batch computation strategy

Processing per-stock sequentially is slow for 2000 stocks × 250 days. Batch approach:

1. Load all `cn_order_events` for one exchange + one trading_date (partition-aligned read).
2. Add sub-phase column.
3. Filter to PRE_OPEN.
4. Group by `symbol`.
5. Compute all six features per group using Polars `group_by().agg()`.

This processes an entire day's auction orders in one pass. Polars parallelism handles the per-symbol aggregation efficiently.

```python
# Vectorized batch computation for one day
day_orders = load_all_auction_orders(exchange, trading_date)  # partition read
day_orders = add_sub_phase_column(day_orders)

profiles = (
    day_orders
    .filter(pl.col("trading_phase") == "PRE_OPEN")
    .group_by("symbol")
    .agg([
        # Feature 1: imbalance
        ((pl.col("qty").filter(pl.col("side") == "BUY", pl.col("event_kind") == "NEW").sum()
        - pl.col("qty").filter(pl.col("side") == "SELL", pl.col("event_kind") == "NEW").sum())
        / pl.col("qty").filter(pl.col("event_kind") == "NEW").sum()).alias("imbalance"),

        # Feature 2: volume_raw
        pl.col("qty").filter(pl.col("event_kind") == "NEW").sum().alias("volume_raw"),

        # ... remaining features via similar filter+agg patterns
    ])
)
```

---

## Step 3: Pairwise Auction Similarity

### Method

For a rolling window of D trading days ending at day T:

1. Collect the D profile vectors for each stock: `P_i = [p_{i,T-D+1}, ..., p_{i,T}]`, a D×6 matrix.
2. Compute pairwise similarity between stocks i and j as the **correlation of their daily profile time series**.

Why correlation of profile series (not cosine of averaged profiles):
- Cosine of the mean profile captures whether two stocks have similar *average* auction behavior. This is mostly sector/industry structure.
- Correlation of the *daily variation* in profiles captures whether two stocks' auction behavior *co-moves over time*. This is the novel signal — when one stock receives unusual institutional attention, the other does too, on the same days.

```python
def compute_auction_similarity(
    profiles: pl.DataFrame,   # columns: trading_date, symbol, f1..f6
    window_end: date,
    window_days: int,          # D = 5, 10, or 20
) -> np.ndarray:
    """Compute N×N similarity matrix from rolling auction profiles."""

    # Filter to window
    window_start = window_end - timedelta(days=window_days * 1.5)  # calendar days buffer
    window = profiles.filter(
        (pl.col("trading_date") >= window_start)
        & (pl.col("trading_date") <= window_end)
    )

    # Pivot to (D, N, 6) array
    symbols = sorted(window["symbol"].unique().to_list())
    N = len(symbols)
    feature_cols = ["imbalance", "volume_participation", "cancel_rate",
                    "commitment", "price_dispersion", "size_concentration"]

    # For each stock, stack D days × 6 features → flatten to D*6 vector
    # Then compute pairwise Pearson correlation of these flattened vectors
    # OR: compute correlation per feature, then average

    # Approach: per-feature correlation, then average
    # For feature f, correlation(stock_i_series_f, stock_j_series_f) over D days
    # Average across 6 features → final similarity

    sim_matrix = np.zeros((N, N))
    for f in feature_cols:
        # Pivot: rows=trading_date, columns=symbol, values=feature_f
        pivoted = window.pivot(
            index="trading_date", on="symbol", values=f
        ).drop("trading_date").to_numpy()  # shape: (D, N)

        # Pairwise correlation: (N, N)
        # Use np.corrcoef which returns correlation matrix
        corr = np.corrcoef(pivoted.T)  # (N, N)
        np.nan_to_num(corr, nan=0.0, copy=False)
        sim_matrix += corr

    sim_matrix /= len(feature_cols)
    return sim_matrix, symbols
```

### Design variants to test

| Variant | Description | Rationale |
|---|---|---|
| **V1: Per-feature correlation, averaged** | Correlate each of the 6 features separately, average the 6 correlation values | Default. Each feature contributes equally. |
| **V2: Flattened profile correlation** | Concatenate D×6 values into one long vector per stock, compute single correlation | Captures cross-feature co-movement but requires D*6 > 30 for stability. |
| **V3: Rank correlation (Spearman)** | Same as V1 but using Spearman instead of Pearson | Robust to outliers in auction quantities. |
| **V4: Imbalance-only** | Use only feature 1 (net order imbalance) | Ablation: is the simplest feature sufficient? |
| **V5: Commitment-weighted** | Weight each day's profile by `commitment_i * commitment_j` before correlation | Days where both stocks show high commitment are more informative. |

Start with V1 (simplest), run ablation across V1-V5 to understand which features and methods drive results.

### Window length

Test D ∈ {5, 10, 20} trading days.
- D=5: responsive, ~1 calendar week. But 5 data points for correlation is noisy.
- D=10: balanced. ~2 calendar weeks.
- D=20: stable, ~1 month. Less responsive to regime shifts.

**Minimum viable: D=10.** With 6 features averaged, effective sample is reasonable. D=5 likely too noisy; include as a robustness check.

### Sparsification

The N×N similarity matrix is dense. For downstream graph operations, sparsify:

- **k-NN:** Keep top-k most similar stocks per stock (k=30). Symmetric: if i is in j's top-k or j is in i's top-k, keep the edge.
- **Threshold:** Keep edges where similarity > τ. Set τ adaptively as the p-th percentile of the similarity distribution (p=90 or 95).

Test both. k-NN is more stable (fixed sparsity); threshold is more interpretable (similarity has meaning).

---

## Step 4: Return Dependency Measurement

### What "dependency" means operationally

Pairwise return correlation over a measurement window. This is what H1 claims auction similarity can predict.

### Computation

For each pair (i, j) and each measurement window W:

1. Compute intraday returns for each stock from `cn_tick_events` or `cn_l2_snapshots`.
2. Aggregate to a common time grid (e.g., 5-minute bars during continuous trading).
3. Compute Pearson or Spearman correlation of the return series.

```python
def compute_return_dependency(
    returns_5min: pl.DataFrame,  # columns: trading_date, time_bin, symbol, return_5min
    window_start: date,
    window_end: date,
) -> np.ndarray:
    """Compute N×N return correlation matrix over a window of trading days."""

    window = returns_5min.filter(
        (pl.col("trading_date") >= window_start)
        & (pl.col("trading_date") <= window_end)
    )

    # Pivot to (T, N) matrix: T = num_time_bins across all days in window
    symbols = sorted(window["symbol"].unique().to_list())
    pivoted = window.pivot(
        index=["trading_date", "time_bin"], on="symbol", values="return_5min"
    ).drop(["trading_date", "time_bin"]).to_numpy()

    corr = np.corrcoef(pivoted.T)  # (N, N)
    np.nan_to_num(corr, nan=0.0, copy=False)
    return corr, symbols
```

### Return computation from L2 snapshots

Using `cn_l2_snapshots` is simpler than reconstructing from tick events. Snapshots provide `last_price` at ~3-second intervals.

```python
def compute_5min_returns(
    snapshots: pl.DataFrame,  # cn_l2_snapshots, one symbol, one day
    exchange: str,
) -> pl.DataFrame:
    """Resample L2 snapshots to 5-minute returns."""

    # Filter to continuous trading only
    continuous = filter_by_phase(
        snapshots, exchange=exchange,
        phases=[TradingPhase.MORNING, TradingPhase.AFTERNOON],
    )

    # Derive 5-minute bins from local time
    # ... (group by 5-min floor, take last snapshot per bin, compute log return)
```

### Measurement window

The return dependency measurement window should match the prediction horizon. Test:

- **Next 5 trading days:** Short-term dependency prediction.
- **Next 10 trading days:** Medium-term.

Use the same window length as the auction similarity window (D) for symmetry, but test asymmetric configurations.

### Lunch-break handling

5-minute returns must NOT span the lunch break. AM session: 09:30-11:30 (24 bars). PM session: 13:00-14:55 (23 bars). Total: 47 bars per day. Over D=10 days: 470 data points per pair for correlation. Sufficient.

---

## Step 5: Prediction Test

### Test structure

For each evaluation date T:

- **Auction similarity window:** days [T-D+1, T] → compute `S_{i,j}(T)`
- **Lagged dependency:** days [T-D+1, T] → compute `ρ_{i,j}(T)` (return correlation)
- **Future dependency:** days [T+1, T+H] → compute `ρ_{i,j}(T+H)` (what we predict)

H1 test: does `S_{i,j}(T)` help predict `ρ_{i,j}(T+H)` beyond `ρ_{i,j}(T)`?

### Regression framework

**Model A (baseline):** Predict future dependency from lagged dependency alone.

```
ρ_{i,j}(T+H) = α + β₁ · ρ_{i,j}(T) + ε
```

**Model B (auction):** Add auction similarity.

```
ρ_{i,j}(T+H) = α + β₁ · ρ_{i,j}(T) + β₂ · S_{i,j}(T) + ε
```

**Model C (full):** Add controls.

```
ρ_{i,j}(T+H) = α + β₁ · ρ_{i,j}(T) + β₂ · S_{i,j}(T)
              + β₃ · same_industry_{i,j}
              + β₄ · cap_similarity_{i,j}
              + β₅ · board_match_{i,j}
              + ε
```

**H1 passes if:** β₂ is statistically significant (p < 0.01, clustered standard errors) AND economically meaningful (adding `S` improves out-of-sample R² by ≥ 0.5 percentage points).

### Why p < 0.01 and not 0.05

With ~1M pairs, even trivial effects are statistically significant at p < 0.05. We need a higher bar. Complement with effect-size metrics.

### Rank-based test (non-parametric, primary)

More robust than linear regression for fat-tailed pair distributions:

1. At evaluation date T, rank all pairs by `S_{i,j}(T)`. Form quintiles (Q1=lowest similarity, Q5=highest).
2. For each quintile, compute the average `ρ_{i,j}(T+H)` — the realized future dependency.
3. Also compute the average `ρ_{i,j}(T)` — the lagged dependency — per quintile.
4. Compute the **incremental dependency**: `ρ_{i,j}(T+H) - ρ_{i,j}(T)` per quintile.

**H1 passes if:** Monotonic increase from Q1 to Q5 in incremental dependency. Q5-Q1 spread is positive and stable across evaluation dates.

```python
def quintile_dependency_test(
    S: np.ndarray,              # N×N auction similarity at time T
    rho_lag: np.ndarray,        # N×N return correlation at time T
    rho_future: np.ndarray,     # N×N return correlation at time T+H
    symbols: list[str],
) -> pd.DataFrame:
    """Rank-based test: do high-similarity pairs show higher future dependency?"""

    N = len(symbols)
    # Extract upper triangle (exclude diagonal)
    idx = np.triu_indices(N, k=1)
    s_flat = S[idx]
    rho_lag_flat = rho_lag[idx]
    rho_future_flat = rho_future[idx]
    delta_rho = rho_future_flat - rho_lag_flat

    # Quintile assignment based on S
    quintiles = pd.qcut(s_flat, 5, labels=[1, 2, 3, 4, 5])

    results = pd.DataFrame({
        "quintile": quintiles,
        "s": s_flat,
        "rho_lag": rho_lag_flat,
        "rho_future": rho_future_flat,
        "delta_rho": delta_rho,
    })

    return results.groupby("quintile").agg(
        s_mean=("s", "mean"),
        rho_lag_mean=("rho_lag", "mean"),
        rho_future_mean=("rho_future", "mean"),
        delta_rho_mean=("delta_rho", "mean"),
        delta_rho_std=("delta_rho", "std"),
        count=("s", "count"),
    )
```

### Walk-forward evaluation

Do NOT evaluate on a single window. Roll forward:

```
Eval 1:  Similarity window [D1, D10]  → Predict dependency [D11, D20]
Eval 2:  Similarity window [D6, D15]  → Predict dependency [D16, D25]
Eval 3:  Similarity window [D11, D20] → Predict dependency [D21, D30]
...
Step size: 5 trading days.
```

Collect quintile-test results across all evaluation dates. Report mean and standard deviation of Q5-Q1 spread.

### Minimum data requirement

- D=10 similarity window + H=10 prediction window = 20 trading days per evaluation.
- 5-day step → ~48 evaluations per year.
- Minimum 2 years of data (2023-2024) → ~96 evaluations. Sufficient for distributional analysis.
- Hold-out: 2025 data (if available) as final validation.

---

## Step 6: Baselines

### Baseline 1: Lagged return correlation (autoregressive)

```
ρ_{i,j}(T+H) ≈ ρ_{i,j}(T)
```

Return correlation is persistent. This is the baseline to beat. Simple, strong. If auction similarity can't improve on "future correlation ≈ past correlation," the network adds nothing.

### Baseline 2: Industry co-membership

```
ρ_{i,j}(T+H) ≈ α + β · same_industry_{i,j}
```

Binary feature: 1 if stocks are in the same SW (申万) industry sector, 0 otherwise. If auction similarity merely rediscovers industry structure, no novel value.

### Baseline 3: Factor-exposure similarity

```
ρ_{i,j}(T+H) ≈ α + β · factor_sim_{i,j}(T)
```

Where `factor_sim` is the cosine similarity of Barra-style factor loadings (size, value, momentum, volatility, liquidity). Requires factor exposure data — can be approximated from observable characteristics (market cap, book-to-market, trailing return, realized vol, turnover).

### Baseline 4: Volume-correlation network

```
ρ_{i,j}(T+H) ≈ α + β · vol_corr_{i,j}(T)
```

Pairwise correlation of daily trading volume over the same D-day window. This tests whether auction co-activity adds value beyond general activity co-movement.

### Comparison framework

For each evaluation date, compute out-of-sample prediction error for each model:

```python
# For each baseline and the auction model:
# 1. Fit on all evaluation dates before T (expanding window)
# 2. Predict ρ_{i,j}(T+H) for all pairs
# 3. Compute MSE, rank correlation with realized ρ, and quintile spread
```

Report:
- **Incremental R²**: R²(Model B) - R²(Model A). How much does adding S improve prediction?
- **Rank IC**: `spearmanr(predicted_ρ, realized_ρ)` for each model. Does auction similarity improve ranking of pair dependencies?
- **Quintile spread stability**: Is the Q5-Q1 spread from the auction model more stable across evaluation dates than baselines?

---

## Step 7: Statistical Evaluation and Go/No-Go

### Primary metrics

| Metric | How computed | H1 pass threshold |
|---|---|---|
| **Quintile monotonicity** | Q5-Q1 spread in `ρ_future - ρ_lag` | Positive in ≥ 70% of walk-forward windows |
| **Mean Q5-Q1 spread** | Average across all evaluation dates | > 0.01 (1 percentage point of correlation) |
| **Incremental R²** | R²(Model B) - R²(Model A) | ≥ 0.5 percentage points OOS |
| **Rank IC of S vs Δρ** | `spearmanr(S_{i,j}, ρ_future - ρ_lag)` across pairs | > 0.02 mean, ICIR > 0.3 |
| **Novelty score** | Fraction of Q5 pairs NOT in same industry | > 0.3 (auction captures non-industry links) |

### Robustness checks

1. **Window length sensitivity:** Results must hold for D ∈ {5, 10, 20} without requiring narrow parameterization. Accept if ≥ 2 of 3 windows pass thresholds.
2. **Exchange robustness:** Run separately for SSE and SZSE. Results must hold on both exchanges.
3. **Liquidity tier robustness:** Run separately for CSI 300 (large-cap), CSI 500 (mid-cap), and remaining liquid stocks. Signal may be stronger in one tier, but should not reverse sign.
4. **Market regime robustness:** Split evaluation dates into bull (CSI 300 trailing 60-day return > +10%), bear (< -10%), and range-bound. Report separately. Accept if positive in ≥ 2 of 3 regimes.
5. **Cancel-phase ablation:** Recompute profiles without features 3 (cancel rate) and 4 (commitment ratio). If results degrade, these features carry information. If results are unchanged, the cancel phase is noise.
6. **Similarity variant ablation:** Compare V1 (per-feature averaged) vs V4 (imbalance-only). If V4 is competitive, the full profile adds limited value.

### Go/No-Go Decision

**GO (proceed to Milestone 3: covariance estimation):**
- Primary metrics pass thresholds.
- Results robust across ≥ 2 window lengths, both exchanges, and ≥ 2 market regimes.
- Novelty score > 0.3 (the network is not just rediscovering industries).

**CONDITIONAL GO:**
- Primary metrics pass but only for specific liquidity tier or regime.
- Proceed to Milestone 3 with restricted scope (e.g., CSI 500 only, or range-bound regimes only).

**NO-GO (stop):**
- Quintile monotonicity < 50% of windows OR mean Q5-Q1 spread < 0.005.
- Incremental R² < 0.2 percentage points.
- Novelty score < 0.15 (auction similarity ≈ industry co-membership).
- Results reverse sign across exchanges or regimes.

---

## Diagnostic Outputs

Every evaluation run produces:

### Per-window outputs
1. **Quintile table:** Q1-Q5 statistics (mean S, mean ρ_lag, mean ρ_future, mean Δρ, count).
2. **Auction similarity heatmap:** N×N matrix visualization, sorted by industry. Cross-industry clusters should be visible.
3. **Pair-level scatter:** S_{i,j} vs Δρ_{i,j} for a representative window.

### Aggregate outputs
4. **Quintile spread time series:** Q5-Q1 Δρ spread across all evaluation dates. Should be mostly positive, not regime-dependent.
5. **R² comparison table:** OOS R² for Models A, B, C across all evaluation dates.
6. **Robustness matrix:** Pass/fail for each (window_length × exchange × regime × liquidity_tier) combination.
7. **Feature ablation table:** Performance of V1-V5 similarity variants.
8. **Novelty analysis:** For Q5 pairs (highest auction similarity), what fraction are same-industry vs cross-industry? Which cross-industry linkages appear consistently?

---

## Data Requirements and Timing

### Data needed

| Table | Time range | Exchanges | Estimated size |
|---|---|---|---|
| `cn_order_events` | 2023-01-01 to 2024-12-31 (2 years) | SSE + SZSE | ~50-100 GB |
| `cn_l2_snapshots` | Same | SSE + SZSE | ~30-50 GB |
| `dim_symbol` | Full history | SSE + SZSE | Small |

2025 data reserved as hold-out if available.

### Computation estimate

- **Profile extraction:** ~2000 stocks × 500 days × 6 features. Batch-per-day reads. ~2-4 hours total.
- **Similarity matrices:** ~2M pairs × 100 evaluation windows. ~1-2 hours with vectorized NumPy.
- **Return correlation:** ~2M pairs × 100 windows from pre-computed 5-min returns. ~1-2 hours.
- **Prediction tests:** Lightweight once matrices are computed. ~minutes.
- **Total wall-clock:** ~1 day for full run. Incremental iteration is fast (rerun from cached profiles).

### Caching strategy

- Cache auction profiles as Parquet: `profiles/{exchange}/profiles_{date}.parquet`
- Cache 5-minute returns as Parquet: `returns/{exchange}/returns_5min_{date}.parquet`
- Cache similarity matrices as NumPy: `similarity/{exchange}/sim_D{d}_{date}.npy`
- Prediction test results as CSV for analysis.

---

## Implementation Order

### Phase 1a: Profile extraction pipeline (1-2 days)

1. Write `compute_auction_profiles(silver_root, exchange, trading_date) → DataFrame`.
2. Batch across all dates. Cache as Parquet.
3. Validate: distributions of each feature, NaN rates, edge cases (no auction orders, very few orders).
4. Compute trailing-5d median for volume_participation normalization.

### Phase 1b: Return dependency computation (1 day)

1. Write `compute_5min_returns(silver_root, exchange, trading_date) → DataFrame` from L2 snapshots.
2. Batch across all dates. Cache as Parquet.
3. Validate: return distributions, lunch-break handling, missing data.

### Phase 1c: Similarity and dependency matrices (1 day)

1. Write `compute_auction_similarity(profiles, window_end, D) → (N×N, symbols)`.
2. Write `compute_return_dependency(returns, window_start, window_end) → (N×N, symbols)`.
3. Validate: similarity distributions, stability, sanity-check known pairs.

### Phase 2: Prediction tests (2-3 days)

1. Implement walk-forward evaluation loop.
2. Implement quintile test and regression framework.
3. Implement all four baselines.
4. Run full evaluation across D={5,10,20}, both exchanges.
5. Generate all diagnostic outputs.
6. Write up results and go/no-go recommendation.

**Total estimated effort: 5-7 working days.**

---

## Open Questions (to resolve before starting)

1. **L2 snapshot availability:** Do we have `cn_l2_snapshots` for the full 2023-2024 period, or only `cn_tick_events`? If only ticks, derive 5-min returns from tick VWAP instead.

2. **Symbol metadata source:** For industry classification (SW sectors) and market cap, do we use `dim_symbol` enriched with Tushare data, or a separate reference table?

3. **SZSE closing auction:** The opening auction exists on both SSE and SZSE, but the closing auction is SZSE-only. Should we build a combined opening+closing profile for SZSE stocks and opening-only for SSE, or standardize on opening-only for comparability?
   Default: Opening-only as primary (both exchanges comparable). SZSE closing as ablation.

4. **Price in fixed-point:** Auction profile feature 5 (price dispersion) uses `price` which is scaled by `PRICE_SCALE=1e9`. The CV calculation (std/mean) is scale-invariant, so no decoding needed. Confirm this is correct.
