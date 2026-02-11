# Co-Trading Networks for Chinese A-Shares
## Implementation of Lu et al. (2023) arXiv:2302.09382

**Persona:** Quant Researcher (MFT)  
**Data Source:** SZSE/SSE Level 3 (l3_ticks)  
**Scope:** Cross-stock dependency modeling & covariance estimation  
**Reference:** Lu, Y., Reinert, G., & Cucuringu, M. (2023). Co-trading networks for modeling dynamic interdependency structures and estimating high-dimensional covariances in US equity markets. arXiv:2302.09382

---

## Executive Summary

This guide adapts the **co-trading network methodology** from Lu et al. (2023) to Chinese A-share markets. The core insight is that **concurrent trading activity** (stocks trading at the same time) reveals latent dependency structures that:

1. **Precede return correlations** (lead-lag at microsecond scale)
2. **Capture dynamic sector rotations** (beyond static industry classifications)
3. **Improve covariance estimation** (shrinkage toward network structure)
4. **Enhance portfolio allocation** (lower volatility, higher Sharpe)

**Key Adaptations for Chinese Markets:**
- Handle T+1 settlement effects on co-trading patterns
- Account for price limit impacts on trade synchronization
- Adjust for call auction concentration (09:25, 14:57)
- Incorporate retail-dominated trading behavior

---

## 1. Theoretical Foundation

### 1.1 Co-Trading Intuition

When two stocks trade **within milliseconds of each other**, this suggests:
- **Common information arrival** (news affecting both)
- **Cross-asset arbitrage** (index arbitrage, pairs trading)
- **Sector rotation** (funds rebalancing across related stocks)
- **Market maker inventory management** (hedging across correlated names)

**Why This Works for Chinese Stocks:**
- L3 data provides microsecond timestamps
- Retail investors create synchronized reactions to news
- T+1 creates predictable repositioning patterns
- Sector ETFs drive co-movement through creation/redemption

### 1.2 Co-Trading Similarity Measure

**Original Paper Definition:**
```
similarity(i,j) = count(trades of i and j within δt milliseconds) 
                  / sqrt(count(trades i) * count(trades j))
```

**Chinese Market Adaptations:**
- Use **microsecond precision** (`ts_local_us`)
- Define **δt = 1-10ms** (market-specific calibration)
- Apply **volume weighting** (large trades matter more)
- Exclude **call auction periods** (synchronous by design)
- Account for **T+1 inventory effects** (previous day positions)

---

## 2. Implementation Pipeline

### 2.1 Data Preparation

```python
import polars as pl
import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components
from sklearn.cluster import SpectralClustering
from pointline.research import query

def load_universe_data(
    universe: list[str],  # List of symbol_ids
    date: str,
    exclude_auctions: bool = True,
) -> dict[str, pl.DataFrame]:
    """
    Load L3 tick data for universe of stocks.
    """
    data = {}
    
    for symbol in universe:
        ticks = query.szse_l3_ticks("szse", symbol, date, date, decoded=True)
        
        # Filter to continuous trading only (optional)
        if exclude_auctions:
            ticks = ticks.filter(
                ~((pl.col("ts_exch_us").mod(86_400_000_000) >= 1_15_00_000_000) &
                  (pl.col("ts_exch_us").mod(86_400_000_000) <= 1_25_00_000_000)) &  # Opening auction
                ~((pl.col("ts_exch_us").mod(86_400_000_000) >= 6_57_00_000_000) &
                  (pl.col("ts_exch_us").mod(86_400_000_000) <= 7_00_00_000_000))    # Closing auction
            )
        
        # Keep only fills
        ticks = ticks.filter(pl.col("tick_type") == 0)
        
        data[symbol] = ticks.sort("ts_local_us")
    
    return data
```

---

### 2.2 Co-Trading Similarity Matrix

```python
def compute_cotrading_similarity(
    data: dict[str, pl.DataFrame],
    delta_t_us: int = 1_000,  # 1ms window
    volume_weighted: bool = True,
) -> pd.DataFrame:
    """
    Compute pairwise co-trading similarity matrix.
    
    similarity(i,j) = sum(min(qty_i, qty_j) for concurrent trades) 
                      / sqrt(total_qty_i * total_qty_j)
    """
    symbols = list(data.keys())
    n = len(symbols)
    
    # Initialize similarity matrix
    similarity = np.zeros((n, n))
    
    # Pre-compute total volumes
    total_volumes = {}
    for symbol in symbols:
        total_volumes[symbol] = data[symbol]["qty"].sum()
    
    # Compute pairwise co-trading
    for i, sym_i in enumerate(symbols):
        df_i = data[sym_i].select(["ts_local_us", "qty"]).to_pandas()
        
        for j, sym_j in enumerate(symbols[i+1:], start=i+1):
            df_j = data[sym_j].select(["ts_local_us", "qty"]).to_pandas()
            
            # Find concurrent trades using merge_asof
            concurrent = pd.merge_asof(
                df_i, df_j,
                on="ts_local_us",
                direction="nearest",
                tolerance=delta_t_us,
            )
            
            # Remove NaN (no match within delta_t)
            concurrent = concurrent.dropna()
            
            if volume_weighted:
                # Weight by minimum quantity
                co_trading_volume = concurrent[["qty_x", "qty_y"]].min(axis=1).sum()
            else:
                # Count matches
                co_trading_volume = len(concurrent)
            
            # Normalized similarity
            norm = np.sqrt(total_volumes[sym_i] * total_volumes[sym_j])
            similarity[i, j] = co_trading_volume / norm if norm > 0 else 0
            similarity[j, i] = similarity[i, j]
    
    return pd.DataFrame(similarity, index=symbols, columns=symbols)
```

---

### 2.3 Dynamic Network Construction

```python
def build_cotrading_network(
    similarity_matrix: pd.DataFrame,
    threshold: float = None,  # If None, use adaptive threshold
    k_nearest: int = 10,  # K-nearest neighbors
) -> nx.Graph:
    """
    Build network from similarity matrix.
    """
    import networkx as nx
    
    n = len(similarity_matrix)
    symbols = similarity_matrix.index.tolist()
    
    # Adaptive threshold (mean + std)
    if threshold is None:
        sim_values = similarity_matrix.values
        # Upper triangle only (excluding diagonal)
        mask = np.triu(np.ones_like(sim_values, dtype=bool), k=1)
        upper_tri = sim_values[mask]
        threshold = upper_tri.mean() + upper_tri.std()
    
    # Create graph
    G = nx.Graph()
    G.add_nodes_from(symbols)
    
    # Add edges above threshold OR k-nearest
    for i, sym_i in enumerate(symbols):
        # Get top-k neighbors
        neighbors = similarity_matrix.iloc[i].nlargest(k_nearest + 1)  # +1 for self
        neighbors = neighbors[neighbors.index != sym_i]  # Remove self
        
        for sym_j, sim in neighbors.items():
            if sim >= threshold:
                G.add_edge(sym_i, sym_j, weight=sim)
    
    return G
```

---

## 3. Spectral Clustering for Dynamic Sectors

### 3.1 Clustering Implementation

```python
def spectral_cluster_stocks(
    similarity_matrix: pd.DataFrame,
    n_clusters: int = 10,
    affinity: str = "precomputed",
) -> pd.DataFrame:
    """
    Apply spectral clustering to discover data-driven sectors.
    """
    # Convert similarity to affinity (Laplacian requires this)
    affinity_matrix = similarity_matrix.values.copy()
    
    # Ensure symmetric
    affinity_matrix = (affinity_matrix + affinity_matrix.T) / 2
    
    # Spectral clustering
    clustering = SpectralClustering(
        n_clusters=n_clusters,
        affinity=affinity,
        assign_labels="kmeans",
        random_state=42,
    )
    
    labels = clustering.fit_predict(affinity_matrix)
    
    return pd.DataFrame({
        "symbol": similarity_matrix.index,
        "cluster": labels,
        "cluster_name": [f"CT_{n_clusters}_{l}" for l in labels],
    })
```

---

### 3.2 Cluster Stability Analysis

```python
def analyze_cluster_stability(
    universe: list[str],
    start_date: str,
    end_date: str,
    n_clusters: int = 10,
) -> dict:
    """
    Analyze how clusters evolve over time.
    """
    from datetime import datetime, timedelta
    
    clusters_over_time = []
    
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        
        try:
            data = load_universe_data(universe, date_str)
            sim = compute_cotrading_similarity(data)
            clusters = spectral_cluster_stocks(sim, n_clusters)
            clusters["date"] = date_str
            clusters_over_time.append(clusters)
        except Exception as e:
            print(f"Skipping {date_str}: {e}")
        
        current += timedelta(days=1)
    
    all_clusters = pd.concat(clusters_over_time, ignore_index=True)
    
    # Calculate cluster persistence
    persistence = {}
    for symbol in universe:
        sym_clusters = all_clusters[all_clusters["symbol"] == symbol].sort_values("date")
        cluster_changes = (sym_clusters["cluster"].diff() != 0).sum()
        persistence[symbol] = 1 - (cluster_changes / len(sym_clusters))
    
    return {
        "clusters_over_time": all_clusters,
        "persistence": persistence,
        "avg_persistence": np.mean(list(persistence.values())),
    }
```

---

## 4. Co-Trading Based Covariance Estimation

### 4.1 Network-Shrunk Covariance

```python
def network_shrunk_covariance(
    returns: pd.DataFrame,  # T x N returns matrix
    similarity_matrix: pd.DataFrame,
    shrinkage_intensity: float = 0.5,
) -> pd.DataFrame:
    """
    Estimate covariance with shrinkage toward network structure.
    
    Σ_shrunk = (1-λ) * Σ_sample + λ * Σ_network
    
    where Σ_network is derived from co-trading network.
    """
    # Sample covariance
    sample_cov = returns.cov()
    
    # Network-implied covariance
    # High co-trading similarity → high covariance
    network_cov = similarity_matrix.copy()
    
    # Scale to match sample covariance diagonal
    scale_factor = np.diag(sample_cov).mean() / np.diag(network_cov).mean()
    network_cov = network_cov * scale_factor
    
    # Shrinkage
    shrunk_cov = (1 - shrinkage_intensity) * sample_cov + shrinkage_intensity * network_cov
    
    return shrunk_cov
```

---

### 4.2 Graphical Lasso with Network Prior

```python
def network_graphical_lasso(
    returns: pd.DataFrame,
    similarity_matrix: pd.DataFrame,
    alpha: float = 0.1,  # Regularization
) -> pd.DataFrame:
    """
    Graphical Lasso with network-based penalty.
    
    Penalize less for edges present in co-trading network.
    """
    from sklearn.covariance import GraphicalLassoCV
    
    # Create penalty matrix (lower penalty for high co-trading)
    penalty = 1 - similarity_matrix.values
    np.fill_diagonal(penalty, 0)  # No penalty for diagonal
    
    # Fit with custom penalty (requires modified sklearn or manual implementation)
    # This is a simplified version - full implementation needs custom solver
    
    gl = GraphicalLassoCV(alphas=[alpha], cv=3)
    gl.fit(returns)
    
    precision = pd.DataFrame(
        gl.precision_,
        index=returns.columns,
        columns=returns.columns,
    )
    
    return precision
```

---

### 4.3 Time-Varying Covariance (Dynamic Networks)

```python
def dynamic_covariance_estimate(
    returns: pd.DataFrame,
    cotrading_data: dict[str, pd.DataFrame],
    lookback_days: int = 20,
    half_life: int = 10,
) -> pd.DataFrame:
    """
    EWMA covariance with co-trading network weighting.
    """
    # EWMA weights
    decay = np.log(2) / half_life
    weights = np.exp(-decay * np.arange(lookback_days))
    weights /= weights.sum()
    
    covariances = []
    
    for t in range(lookback_days, len(returns)):
        # Get recent returns
        recent_returns = returns.iloc[t-lookback_days:t]
        
        # Get co-trading network for this period
        date = returns.index[t]
        if date in cotrading_data:
            sim = cotrading_data[date]
            
            # Weighted combination
            sample_cov = recent_returns.cov()
            network_cov = network_shrunk_covariance(recent_returns, sim, shrinkage_intensity=0.3)
            
            covariances.append({
                "date": date,
                "covariance": network_cov,
            })
    
    return pd.DataFrame(covariances)
```

---

## 5. Portfolio Applications

### 5.1 Mean-Variance Optimization with Co-Trading Covariance

```python
def cotrading_mean_variance_portfolio(
    expected_returns: pd.Series,
    similarity_matrix: pd.DataFrame,
    returns_history: pd.DataFrame,
    risk_aversion: float = 1.0,
    long_only: bool = True,
) -> pd.Series:
    """
    Construct mean-variance optimal portfolio using co-trading covariance.
    """
    from scipy.optimize import minimize
    
    # Estimate covariance
    cov = network_shrunk_covariance(returns_history, similarity_matrix, shrinkage_intensity=0.5)
    
    n = len(expected_returns)
    symbols = expected_returns.index
    
    # Objective: max mu'w - 0.5 * gamma * w'Cov w
    def negative_utility(w):
        ret = expected_returns @ w
        risk = w @ cov.values @ w
        return -(ret - 0.5 * risk_aversion * risk)
    
    # Constraints
    constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1}]  # Fully invested
    
    if long_only:
        bounds = [(0, 1) for _ in range(n)]
    else:
        bounds = [(-0.3, 0.3) for _ in range(n)]  # Position limits
    
    # Initial guess: equal weight
    w0 = np.ones(n) / n
    
    # Optimize
    result = minimize(
        negative_utility,
        w0,
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
    )
    
    return pd.Series(result.x, index=symbols)
```

---

### 5.2 Risk Parity with Network Clusters

```python
def network_risk_parity(
    similarity_matrix: pd.DataFrame,
    returns_history: pd.DataFrame,
    clusters: pd.DataFrame,
) -> pd.Series:
    """
    Risk parity portfolio with cluster constraints.
    
    Equal risk contribution from each co-trading cluster.
    """
    cov = network_shrunk_covariance(returns_history, similarity_matrix, shrinkage_intensity=0.5)
    
    # Get cluster assignments
    cluster_dict = clusters.set_index("symbol")["cluster"].to_dict()
    unique_clusters = clusters["cluster"].unique()
    
    # Initial: equal weight within cluster, equal risk across clusters
    # ... (simplified - full implementation needs iterative optimization)
    
    symbols = cov.index
    n = len(symbols)
    
    # Start with equal weight
    w = pd.Series(np.ones(n) / n, index=symbols)
    
    # Calculate marginal risk contributions
    portfolio_vol = np.sqrt(w @ cov.values @ w)
    marginal_risk = (cov.values @ w) / portfolio_vol
    
    # Group by cluster
    cluster_risk = {}
    for c in unique_clusters:
        cluster_symbols = [s for s in symbols if cluster_dict.get(s) == c]
        cluster_risk[c] = sum(marginal_risk[symbols.get_loc(s)] * w[s] for s in cluster_symbols)
    
    # Iterative adjustment (simplified)
    for _ in range(10):  # Iterations
        for c in unique_clusters:
            cluster_symbols = [s for s in symbols if cluster_dict.get(s) == c]
            target_risk = portfolio_vol / len(unique_clusters)
            current_risk = cluster_risk[c]
            
            # Adjust weights (simplified)
            adjustment = target_risk / (current_risk + 1e-6)
            for s in cluster_symbols:
                w[s] *= adjustment ** 0.5  # Dampen adjustment
        
        # Renormalize
        w = w / w.sum()
        
        # Recalculate
        portfolio_vol = np.sqrt(w @ cov.values @ w)
    
    return w
```

---

## 6. Chinese Market Specific Adaptations

### 6.1 T+1 Settlement Effects

```python
def adjust_for_t1_effects(
    data: dict[str, pl.DataFrame],
    previous_day_positions: pd.Series,
) -> dict[str, pl.DataFrame]:
    """
    Adjust co-trading analysis for T+1 settlement.
    
    Key insight: Stocks with high previous-day buying may show
    different co-trading patterns due to trapped inventory.
    """
    adjusted_data = {}
    
    for symbol, df in data.items():
        if symbol in previous_day_positions.index:
            position = previous_day_positions[symbol]
            
            # If large long position from T-1, expect selling pressure at T open
            if position > 0:
                # Weight early session trades more heavily
                df = df.with_columns([
                    pl.when(pl.col("ts_exch_us").mod(86_400_000_000) < 5_00_00_000_000)
                    .then(pl.col("qty") * 1.5)  # Boost morning session
                    .otherwise(pl.col("qty"))
                    .alias("qty"),
                ])
            
            adjusted_data[symbol] = df
        else:
            adjusted_data[symbol] = df
    
    return adjusted_data
```

---

### 6.2 Price Limit Impact

```python
def handle_price_limits(
    data: dict[str, pl.DataFrame],
    price_limits: dict[str, tuple],  # symbol -> (lower_limit, upper_limit)
) -> dict[str, pl.DataFrame]:
    """
    Adjust for price limit effects on co-trading.
    
    When stock hits limit, co-trading may be artificially suppressed.
    """
    adjusted_data = {}
    
    for symbol, df in data.items():
        if symbol in price_limits:
            lower, upper = price_limits[symbol]
            
            # Identify limit-hitting periods
            df = df.with_columns([
                (pl.col("price") <= lower * 1.001).alias("at_lower_limit"),
                (pl.col("price") >= upper * 0.999).alias("at_upper_limit"),
            ])
            
            # Mark trades near limits (may be less informative)
            df = df.with_columns([
                (pl.col("at_lower_limit") | pl.col("at_upper_limit")).alias("at_limit"),
            ])
            
            # Optional: Downweight limit trades in co-trading calculation
            df = df.with_columns([
                pl.when(pl.col("at_limit"))
                .then(pl.col("qty") * 0.5)  # Reduce weight
                .otherwise(pl.col("qty"))
                .alias("qty_adjusted"),
            ])
        
        adjusted_data[symbol] = df
    
    return adjusted_data
```

---

### 6.3 Call Auction Exclusion

```python
def separate_auction_cotrading(
    data: dict[str, pl.DataFrame],
) -> tuple[dict, dict, dict]:
    """
    Separate co-trading analysis by session type.
    
    Returns:
        - continuous: 09:30-11:30, 13:00-14:57
        - open_auction: 09:15-09:25
        - close_auction: 14:57-15:00
    """
    continuous_data = {}
    open_auction_data = {}
    close_auction_data = {}
    
    for symbol, df in data.items():
        tod = df["ts_exch_us"] % 86_400_000_000
        
        # Continuous trading
        continuous_data[symbol] = df.filter(
            ((tod >= 1_30_00_000_000) & (tod <= 3_30_00_000_000)) |  # Morning
            ((tod >= 5_00_00_000_000) & (tod <= 6_57_00_000_000))    # Afternoon
        )
        
        # Opening auction
        open_auction_data[symbol] = df.filter(
            (tod >= 1_15_00_000_000) & (tod <= 1_25_00_000_000)
        )
        
        # Closing auction
        close_auction_data[symbol] = df.filter(
            (tod >= 6_57_00_000_000) & (tod <= 7_00_00_000_000)
        )
    
    return continuous_data, open_auction_data, close_auction_data
```

---

## 7. Empirical Validation

### 7.1 Co-Trading vs Return Correlation

```python
def validate_cotrading_predicts_correlation(
    universe: list[str],
    start_date: str,
    end_date: str,
    delta_t_us: int = 1_000,
) -> dict:
    """
    Test if co-trading similarity predicts future return correlation.
    """
    from datetime import datetime, timedelta
    
    results = []
    
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current < end:
        date_t = current.strftime("%Y-%m-%d")
        date_t1 = (current + timedelta(days=1)).strftime("%Y-%m-%d")
        
        try:
            # Day T: compute co-trading similarity
            data_t = load_universe_data(universe, date_t)
            sim_t = compute_cotrading_similarity(data_t, delta_t_us)
            
            # Day T+1: compute return correlation
            returns_t1 = get_returns(universe, date_t1)  # Need to implement
            corr_t1 = returns_t1.corr()
            
            # Compare
            symbols = sim_t.index
            for i, sym_i in enumerate(symbols):
                for sym_j in symbols[i+1:]:
                    results.append({
                        "date": date_t,
                        "sym_i": sym_i,
                        "sym_j": sym_j,
                        "cotrading_sim": sim_t.loc[sym_i, sym_j],
                        "return_corr": corr_t1.loc[sym_i, sym_j],
                    })
        except Exception as e:
            print(f"Skipping {date_t}: {e}")
        
        current += timedelta(days=1)
    
    results_df = pd.DataFrame(results)
    
    # Calculate predictive power
    correlation = results_df["cotrading_sim"].corr(results_df["return_corr"])
    
    # Regression: corr ~ alpha + beta * cotrading
    from scipy import stats
    slope, intercept, r_value, p_value, std_err = stats.linregress(
        results_df["cotrading_sim"],
        results_df["return_corr"],
    )
    
    return {
        "predictive_correlation": correlation,
        "r_squared": r_value ** 2,
        "beta": slope,
        "p_value": p_value,
        "results": results_df,
    }
```

---

### 7.2 Portfolio Backtest

```python
def backtest_cotrading_portfolios(
    universe: list[str],
    start_date: str,
    end_date: str,
    rebalance_freq: str = "W",  # Weekly
) -> pd.DataFrame:
    """
    Backtest mean-variance portfolios using co-trading covariance.
    """
    from datetime import datetime, timedelta
    
    portfolio_values = []
    
    # Initialize
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Initial capital
    capital = 1_000_000
    weights = pd.Series(1/len(universe), index=universe)
    
    while current_date < end:
        # Rebalance period
        period_start = current_date
        if rebalance_freq == "W":
            period_end = current_date + timedelta(weeks=1)
        elif rebalance_freq == "M":
            period_end = current_date + timedelta(days=30)
        else:
            period_end = current_date + timedelta(days=1)
        
        # Compute co-trading similarity using lookback
        lookback_start = period_start - timedelta(days=20)
        data = load_universe_data(universe, lookback_start.strftime("%Y-%m-%d"))
        sim = compute_cotrading_similarity(data)
        
        # Get expected returns (can use simple momentum or factor model)
        expected_returns = get_expected_returns(universe, period_start.strftime("%Y-%m-%d"))
        
        # Get historical returns for covariance
        hist_returns = get_historical_returns(
            universe,
            (period_start - timedelta(days=60)).strftime("%Y-%m-%d"),
            period_start.strftime("%Y-%m-%d"),
        )
        
        # Optimize portfolio
        optimal_weights = cotrading_mean_variance_portfolio(
            expected_returns,
            sim,
            hist_returns,
        )
        
        # Simulate period performance
        period_returns = get_period_returns(
            universe,
            period_start.strftime("%Y-%m-%d"),
            period_end.strftime("%Y-%m-%d"),
        )
        
        portfolio_return = (optimal_weights * period_returns).sum()
        capital *= (1 + portfolio_return)
        
        portfolio_values.append({
            "date": period_end,
            "capital": capital,
            "return": portfolio_return,
            "volatility": period_returns.std(),
        })
        
        current_date = period_end
    
    return pd.DataFrame(portfolio_values)
```

---

## 8. Summary & Recommendations

### 8.1 Key Findings from Paper

| Finding | Implication for Chinese Stocks |
|---------|-------------------------------|
| Co-trading predicts return correlation | Should work better due to retail synchronization |
| Dynamic clusters outperform static sectors | More valuable given rapid sector rotation in China |
| Network-shrunk covariance improves portfolios | Even more important given high dimensionality |
| Microsecond-level timing matters | L3 data provides necessary precision |

### 8.2 Expected Advantages in Chinese Markets

1. **Higher Predictive Power:** Retail-driven markets show stronger co-trading patterns
2. **Faster Cluster Evolution:** Dynamic clustering captures rapid sector rotations
3. **T+1 Inventory Effects:** Predictable repositioning creates stable co-trading patterns
4. **Price Limit Impacts:** Network analysis identifies limit-hit contagion effects

### 8.3 Implementation Checklist

- [ ] Build L3 tick data pipeline for universe
- [ ] Calibrate δt (1-10ms) for optimal similarity
- [ ] Compare spectral clusters vs static sectors
- [ ] Validate: co-trading similarity → future correlation
- [ ] Implement network-shrunk covariance estimator
- [ ] Backtest mean-variance vs risk parity vs equal weight
- [ ] Monitor cluster stability over time
- [ ] Adjust for T+1 and price limit effects

### 8.4 Potential Extensions

1. **Lead-Lag Networks:** Directed co-trading (which stock leads?)
2. **Sector ETFs:** Include ETF flows in co-trading calculation
3. **News Events:** Conditional co-trading (before/after news)
4. **Intraday Regimes:** Separate networks for morning/afternoon
5. **Cross-Market:** Connect SZSE and SSE stocks via co-trading

---

**Document Version:** 1.0  
**Reference:** Lu, Y., Reinert, G., & Cucuringu, M. (2023). Co-trading networks for modeling dynamic interdependency structures and estimating high-dimensional covariances in US equity markets. arXiv:2302.09382  
**Last Updated:** 2024
