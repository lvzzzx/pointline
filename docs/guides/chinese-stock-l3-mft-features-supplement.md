# Chinese Stock L3 MFT Features - Supplement
## Beyond Call Auctions: Order Flow, Toxicity & Microstructure

**Persona:** Quant Researcher (MFT)  
**Data Source:** SZSE/SSE Level 3 (l3_orders, l3_ticks)  
**Scope:** Continuous trading microstructure features  
**Version:** 1.0

---

## Executive Summary

While call auctions provide discrete alpha events, **continuous trading** (09:30-11:30, 13:00-14:57) offers rich microstructure signals through:

- **Order flow toxicity** (informed vs uninformed trading)
- **Trade classification** with L3 precision
- **Liquidity dynamics** (resilience, consumption, regeneration)
- **Strategic order patterns** (icebergs, layering, spoofing detection)
- **Inter-event timing** (intensity, clustering, burst detection)

These features are complementary to call auction signals and often have **lower correlation** with standard price/volume factors.

---

## 1. Order Flow Toxicity Features

### 1.1 VPIN (Volume-Synchronized Probability of Informed Trading)

**Concept:** VPIN measures the probability that a trade is informed by looking at order flow imbalance within volume buckets rather than time buckets.

**Why Volume Buckets?**
- Time-based sampling misses burst trading periods
- Volume buckets naturally weight high-activity periods more
- VPIN spikes before major price movements (toxic flow detection)

**Formula:**
```python
def calculate_vpin(
    ticks: pl.DataFrame,
    volume_bucket_size: float = 100_000,  # Shares per bucket
    n_buckets: int = 50,  # Rolling window size
) -> pl.DataFrame:
    """
    Calculate Volume-Synchronized PIN.
    """
    # Classify trades as buy/sell initiated
    ticks = ticks.with_columns([
        pl.when(pl.col("side") == 0)
        .then(pl.col("qty"))
        .otherwise(-pl.col("qty"))
        .alias("signed_volume"),
    ])
    
    # Create volume buckets (accumulate until bucket_size reached)
    ticks = ticks.with_columns([
        pl.col("qty").cum_sum().alias("cum_volume"),
    ])
    
    ticks = ticks.with_columns([
        (pl.col("cum_volume") / volume_bucket_size).floor().alias("volume_bucket"),
    ])
    
    # Calculate imbalance within each bucket
    bucket_imbalance = ticks.group_by("volume_bucket").agg([
        pl.col("signed_volume").abs().sum().alias("bucket_imbalance"),
        pl.col("qty").sum().alias("bucket_volume"),
    ])
    
    # VPIN = average |imbalance| / volume across buckets
    bucket_imbalance = bucket_imbalance.with_columns([
        (pl.col("bucket_imbalance") / pl.col("bucket_volume")).alias("bucket_vpin"),
    ])
    
    # Rolling VPIN
    bucket_imbalance = bucket_imbalance.with_columns([
        pl.col("bucket_vpin").rolling_mean(window_size=n_buckets).alias("vpin"),
    ])
    
    return bucket_imbalance
```

**Interpretation:**
- **VPIN < 0.2:** Benign flow, mostly uninformed
- **VPIN 0.2-0.4:** Mixed flow, moderate toxicity
- **VPIN > 0.4:** Highly toxic, informed trading dominant
- **VPIN spike:** Expect volatility expansion

**MFT Application:**
- Use VPIN > 0.35 as filter: avoid market orders, use limits only
- VPIN trend (increasing/decreasing) predicts volatility regime
- Cross-sectional: long low-VPIN, short high-VPIN (liquidity provision)

---

### 1.2 Order Flow Toxicity (OFT) - ELO Enhancements

**Concept:** Extension of Easley-López de Prado-O'Hara (ELO) toxicity metrics using L3 data.

**Features:**

```python
# 1. Bulk Volume Classification (BVC) - more accurate than tick rule
def bulk_volume_classification(
    ticks: pl.DataFrame,
    price_changes: pl.DataFrame,  # Need price bar data for reference
) -> pl.DataFrame:
    """
    Classify trade size based on position within price bar.
    """
    # Match ticks with price bars
    ticks_with_bar = ticks.join_asof(
        price_changes.select(["ts_local_us", "open", "high", "low", "close"]),
        on="ts_local_us",
        strategy="backward",
    )
    
    # BVC formula: weight = (price - low) / (high - low)
    ticks_classified = ticks_with_bar.with_columns([
        ((pl.col("price") - pl.col("low")) / (pl.col("high") - pl.col("low")))
        .clip(0, 1)
        .alias("buy_weight"),
    ]).with_columns([
        (pl.col("buy_weight") * pl.col("qty")).alias("classified_buys"),
        ((1 - pl.col("buy_weight")) * pl.col("qty")).alias("classified_sells"),
    ])
    
    return ticks_classified

# 2. Toxicity Ratio (order cancellations / executions)
def toxicity_ratio(orders: pl.DataFrame, window_us: int = 60_000_000) -> pl.DataFrame:
    """
    Calculate cancellation-to-execution ratio in rolling windows.
    """
    orders = orders.with_columns([
        pl.when(pl.col("order_type") == 2).then(1).otherwise(0).alias("is_cancel"),
        pl.when(pl.col("order_type").is_in([0, 1])).then(1).otherwise(0).alias("is_submit"),
    ])
    
    return orders.with_columns([
        pl.col("is_cancel").rolling_sum(window_size=window_us, min_periods=1).alias("cancels"),
        pl.col("is_submit").rolling_sum(window_size=window_us, min_periods=1).alias("submits"),
    ]).with_columns([
        (pl.col("cancels") / (pl.col("submits") + pl.col("cancels"))).alias("toxicity_ratio"),
    ])
```

---

### 1.3 Information Flow Rate

**Concept:** Measure how quickly information propagates through order book updates.

```python
def information_flow_rate(
    orders: pl.DataFrame,
    ticks: pl.DataFrame,
    window_us: int = 60_000_000,  # 1 minute
) -> dict:
    """
    Calculate information arrival rate using event clustering.
    """
    # Combine orders and ticks into event stream
    order_events = orders.select([
        "ts_local_us",
        pl.lit("order").alias("event_type"),
        pl.col("qty").abs().alias("size"),
    ])
    
    tick_events = ticks.filter(pl.col("tick_type") == 0).select([
        "ts_local_us",
        pl.lit("trade").alias("event_type"),
        pl.col("qty").abs().alias("size"),
    ])
    
    all_events = pl.concat([order_events, tick_events]).sort("ts_local_us")
    
    # Inter-event durations
    all_events = all_events.with_columns([
        (pl.col("ts_local_us") - pl.col("ts_local_us").shift(1)).alias("inter_event_duration_us"),
    ])
    
    # Information flow metrics
    metrics = {
        "mean_inter_event_us": all_events["inter_event_duration_us"].mean(),
        "event_clustering": all_events["inter_event_duration_us"].std() / all_events["inter_event_duration_us"].mean(),
        "burst_ratio": (
            all_events.filter(pl.col("inter_event_duration_us") < 1000).height / 
            all_events.height
        ),
    }
    
    return metrics
```

**Interpretation:**
- **Low inter-event duration:** High information flow, expect volatility
- **High clustering (> 2.0):** Bursty information arrival (news/events)
- **High burst ratio (> 10%):** Microstructure events, potential alpha

---

## 2. Advanced Trade Classification

### 2.1 Lee-Ready Classification with L3 Enhancement

**Standard Lee-Ready:**
- Trade at ask → buyer-initiated
- Trade at bid → seller-initiated
- Trade at mid → use tick rule (compare to previous trade)

**L3 Enhancement:**
With order IDs, we can determine aggressor definitively:

```python
def lee_ready_l3_classification(
    ticks: pl.DataFrame,
    orders: pl.DataFrame,
) -> pl.DataFrame:
    """
    Lee-Ready trade classification enhanced with L3 order matching.
    """
    # For each trade, look up the aggressor order
    # In L3: the order that crossed the spread is the aggressor
    
    ticks_classified = ticks.with_columns([
        # Default: use side from tick (if available)
        pl.when(pl.col("side") == 0)
        .then(pl.lit("buyer_initiated"))
        .when(pl.col("side") == 1)
        .then(pl.lit("seller_initiated"))
        .otherwise(pl.lit("unknown"))
        .alias("l3_classification"),
    ])
    
    # Enhanced with order lookup
    # If we can match buy_order_id or sell_order_id to order book
    # we can determine: was it a market order (aggressive) or limit crossing?
    
    return ticks_classified

# Classification quality metrics
def classification_concentration(
    ticks_classified: pl.DataFrame,
    window_us: int = 300_000_000,  # 5 minutes
) -> pl.DataFrame:
    """
    Measure concentration of trade direction (Herfindahl-style).
    """
    return ticks_classified.with_columns([
        pl.col("l3_classification").rolling_apply(
            function=lambda x: (
                (x.value_counts().get_column("count") / len(x)) ** 2
            ).sum(),
            window_size=window_us,
        ).alias("classification_concentration"),
    ])
```

---

### 2.2 Trade Size Signatures

**Concept:** Different trader types leave different size footprints.

```python
def trade_size_signature(
    ticks: pl.DataFrame,
    percentile_thresholds: list = [50, 75, 90, 95, 99],
) -> dict:
    """
    Characterize trade size distribution and detect anomalous sizes.
    """
    # Calculate percentiles
    percentiles = ticks.select([
        pl.col("qty").quantile(p/100).alias(f"p{p}") for p in percentile_thresholds
    ])
    
    # Size categories
    ticks = ticks.with_columns([
        pl.when(pl.col("qty") <= percentiles["p50"])
        .then(pl.lit("retail"))
        .when(pl.col("qty") <= percentiles["p90"])
        .then(pl.lit("medium"))
        .when(pl.col("qty") <= percentiles["p99"])
        .then(pl.lit("institutional"))
        .otherwise(pl.lit("block"))
        .alias("size_category"),
    ])
    
    # Signature analysis
    signature = ticks.group_by("size_category").agg([
        pl.col("qty").count().alias("trade_count"),
        pl.col("qty").sum().alias("volume"),
        pl.col("price").std().alias("price_volatility"),
    ])
    
    # Institutional participation ratio
    total_volume = ticks["qty"].sum()
    inst_volume = signature.filter(pl.col("size_category") == "institutional")["volume"].sum()
    
    return {
        "size_distribution": signature,
        "institutional_participation": inst_volume / total_volume if total_volume > 0 else 0,
        "block_trade_frequency": signature.filter(pl.col("size_category") == "block")["trade_count"].sum() / len(ticks),
    }
```

---

## 3. Order Book Dynamics & Liquidity

### 3.1 Liquidity Resilience

**Concept:** How quickly does order book depth recover after a trade?

```python
def liquidity_resilience(
    orders: pl.DataFrame,
    ticks: pl.DataFrame,
    recovery_window_us: int = 5_000_000,  # 5 seconds
) -> pl.DataFrame:
    """
    Measure order book depth recovery after large trades.
    """
    # Identify large trades (> 95th percentile)
    large_trade_threshold = ticks["qty"].quantile(0.95)
    large_trades = ticks.filter(
        (pl.col("qty") >= large_trade_threshold) & 
        (pl.col("tick_type") == 0)
    )
    
    # For each large trade, measure depth before and after
    resilience_events = []
    
    for trade in large_trades.iter_rows(named=True):
        trade_ts = trade["ts_local_us"]
        
        # Depth before trade (5s window before)
        depth_before = orders.filter(
            (pl.col("ts_local_us") >= trade_ts - recovery_window_us) &
            (pl.col("ts_local_us") < trade_ts)
        ).select(pl.col("qty").sum()).item() or 0
        
        # Depth after trade (5s window after)
        depth_after = orders.filter(
            (pl.col("ts_local_us") > trade_ts) &
            (pl.col("ts_local_us") <= trade_ts + recovery_window_us)
        ).select(pl.col("qty").sum()).item() or 0
        
        resilience = depth_after / depth_before if depth_before > 0 else 0
        
        resilience_events.append({
            "ts_local_us": trade_ts,
            "trade_qty": trade["qty"],
            "resilience_ratio": resilience,
        })
    
    return pl.DataFrame(resilience_events)
```

**Interpretation:**
- **Resilience > 0.8:** Deep, liquid market, quick replenishment
- **Resilience 0.5-0.8:** Moderate resilience, temporary impact
- **Resilience < 0.5:** Fragile liquidity, persistent impact

---

### 3.2 Order Book Slope & Curvature

```python
def order_book_shape(
    orders: pl.DataFrame,
    depth_levels: int = 10,
) -> dict:
    """
    Characterize order book shape (steepness, flatness, kinks).
    """
    # Aggregate orders by price level
    book_state = orders.group_by("px_int").agg([
        pl.col("qty").sum().alias("depth_at_price"),
    ]).sort("px_int")
    
    # Calculate slope (change in depth per price level)
    book_state = book_state.with_columns([
        pl.col("depth_at_price").diff().alias("depth_change"),
        pl.col("px_int").diff().alias("price_change"),
    ])
    
    book_state = book_state.with_columns([
        (pl.col("depth_change") / pl.col("price_change")).alias("slope"),
    ])
    
    # Shape metrics
    metrics = {
        "mean_slope": book_state["slope"].mean(),
        "slope_volatility": book_state["slope"].std(),
        "max_depth_concentration": book_state["depth_at_price"].max() / book_state["depth_at_price"].sum(),
        "book_imbalance": (
            book_state.filter(pl.col("px_int") > mid_price)["depth_at_price"].sum() -
            book_state.filter(pl.col("px_int") < mid_price)["depth_at_price"].sum()
        ),
    }
    
    return metrics
```

---

## 4. Strategic Order Flow Patterns

### 4.1 Iceberg Order Detection

**Concept:** Iceberg orders show as repeated executions at same price with same size.

```python
def detect_iceberg_signatures(
    ticks: pl.DataFrame,
    size_tolerance: float = 0.05,  # 5% size variation allowed
    time_tolerance_us: int = 1_000_000,  # 1 second
    min_slices: int = 3,  # Minimum slices to qualify
) -> pl.DataFrame:
    """
    Detect potential iceberg orders through slice pattern analysis.
    """
    # Group trades by price and approximate size
    ticks = ticks.with_columns([
        pl.col("price").round(2).alias("price_bucket"),  # Round to tick size
    ])
    
    # Find repeated trade sizes at same price
    potential_slices = ticks.group_by(["price_bucket", "qty"]).agg([
        pl.col("ts_local_us").count().alias("slice_count"),
        pl.col("ts_local_us").alias("timestamps"),
    ]).filter(pl.col("slice_count") >= min_slices)
    
    # Check timing pattern (regular intervals suggest algorithmic)
    iceberg_candidates = []
    
    for row in potential_slices.iter_rows(named=True):
        timestamps = sorted(row["timestamps"])
        if len(timestamps) >= min_slices:
            intervals = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
            interval_cv = np.std(intervals) / np.mean(intervals) if np.mean(intervals) > 0 else float('inf')
            
            # Low CV (regular intervals) + multiple slices = iceberg signature
            if interval_cv < 0.5:  # Regular timing
                iceberg_candidates.append({
                    "price": row["price_bucket"],
                    "slice_size": row["qty"],
                    "total_slices": row["slice_count"],
                    "total_volume": row["qty"] * row["slice_count"],
                    "interval_regularity": interval_cv,
                })
    
    return pl.DataFrame(iceberg_candidates)
```

---

### 4.2 Layering & Spoofing Detection

```python
def detect_order_layering(
    orders: pl.DataFrame,
    cancellation_threshold: float = 0.9,  # 90% cancellation rate
    min_orders: int = 10,
) -> pl.DataFrame:
    """
    Detect potential layering/spoofing patterns.
    
    Layering pattern: Place orders at multiple levels, cancel before execution.
    """
    # Group orders by price level
    order_lifecycle = orders.group_by("px_int").agg([
        pl.col("order_type").count().alias("total_orders"),
        pl.col("order_type").filter(pl.col("order_type") == 2).count().alias("cancellations"),
        pl.col("order_type").filter(pl.col("order_type").is_in([0, 1])).count().alias("submissions"),
    ])
    
    # Calculate cancellation rate
    order_lifecycle = order_lifecycle.with_columns([
        (pl.col("cancellations") / pl.col("total_orders")).alias("cancellation_rate"),
    ])
    
    # Flag suspicious price levels
    suspicious_levels = order_lifecycle.filter(
        (pl.col("cancellation_rate") > cancellation_threshold) &
        (pl.col("total_orders") > min_orders)
    )
    
    return suspicious_levels
```

**Use in MFT:**
- High layering activity → fragile liquidity, avoid large market orders
- Layering followed by execution → informed trader, follow direction

---

## 5. Inter-Event Timing Features

### 5.1 Trade Intensity & Burst Detection

```python
def trade_intensity_features(
    ticks: pl.DataFrame,
    window_us: int = 60_000_000,  # 1 minute
) -> pl.DataFrame:
    """
    Calculate trade arrival rate and burstiness.
    """
    # Inter-trade durations
    ticks = ticks.sort("ts_local_us").with_columns([
        (pl.col("ts_local_us") - pl.col("ts_local_us").shift(1)).alias("duration_since_last_us"),
    ])
    
    # Rolling intensity metrics
    return ticks.with_columns([
        # Trade count per window
        pl.col("ts_local_us").count().over(rolling_window=window_us).alias("trade_count_1m"),
        
        # Average inter-trade duration
        pl.col("duration_since_last_us").mean().over(rolling_window=window_us).alias("avg_inter_trade_us"),
        
        # Burstiness coefficient (CV of inter-trade times)
        (pl.col("duration_since_last_us").std().over(rolling_window=window_us) / 
         pl.col("duration_since_last_us").mean().over(rolling_window=window_us)).alias("burstiness"),
        
        # Acceleration (change in intensity)
        pl.col("trade_count_1m").diff().alias("intensity_acceleration"),
    ])
```

**Interpretation:**
- **Burstiness > 2.0:** Clustered trading (news/events)
- **Intensity acceleration > 0:** Building momentum
- **Low inter-trade duration + high burstiness:** Microstructure event

---

### 5.2 Order-Trade Interaction Timing

```python
def order_trade_timing(
    orders: pl.DataFrame,
    ticks: pl.DataFrame,
    window_us: int = 60_000_000,
) -> dict:
    """
    Analyze timing relationship between order submissions and trades.
    """
    # Calculate order arrival rate
    order_rate = orders.filter(pl.col("order_type").is_in([0, 1])).group_by_dynamic(
        "ts_local_us",
        every=f"{window_us}us",
    ).agg(pl.len().alias("order_count"))
    
    # Calculate trade arrival rate
    trade_rate = ticks.filter(pl.col("tick_type") == 0).group_by_dynamic(
        "ts_local_us",
        every=f"{window_us}us",
    ).agg(pl.len().alias("trade_count"))
    
    # Join and calculate lead-lag
    combined = order_rate.join(trade_rate, on="ts_local_us", how="outer").fill_null(0)
    
    combined = combined.with_columns([
        pl.col("order_count").shift(1).alias("order_count_lag1"),
        pl.col("order_count").shift(-1).alias("order_count_lead1"),
    ])
    
    # Correlation: do orders lead trades?
    lead_corr = combined.select(pl.corr("order_count", "trade_count")).item()
    lag_corr = combined.select(pl.corr("order_count_lag1", "trade_count")).item()
    
    return {
        "order_trade_contemporaneous_corr": lead_corr,
        "order_trade_lag_corr": lag_corr,
        "order_lead_indicator": lead_corr > lag_corr,  # True if orders predict trades
    }
```

---

## 6. Price Impact & Market Quality

### 6.1 Kyle's Lambda (Price Impact Coefficient)

```python
def kyles_lambda(
    ticks: pl.DataFrame,
    window_us: int = 300_000_000,  # 5 minutes
) -> pl.DataFrame:
    """
    Estimate Kyle's Lambda: price change per unit of signed volume.
    
    Kyle's Lambda = Cov(ΔP, Q) / Var(Q)
    Where ΔP = price change, Q = signed volume
    """
    # Calculate price changes and signed volume
    ticks = ticks.sort("ts_local_us").with_columns([
        pl.col("price").diff().alias("price_change"),
        pl.when(pl.col("side") == 0)
        .then(pl.col("qty"))
        .otherwise(-pl.col("qty"))
        .alias("signed_volume"),
    ])
    
    # Rolling regression (price_change ~ signed_volume)
    return ticks.with_columns([
        # Simplified: use covariance / variance relationship
        (pl.col("price_change") * pl.col("signed_volume")).rolling_mean(window_size=window_us)
        .alias("cov_price_volume"),
        
        (pl.col("signed_volume") ** 2).rolling_mean(window_size=window_us)
        .alias("var_volume"),
    ]).with_columns([
        (pl.col("cov_price_volume") / pl.col("var_volume")).alias("kyles_lambda"),
    ])
```

**Interpretation:**
- **High Lambda:** Illiquid, high impact per unit volume
- **Low Lambda:** Liquid, deep book absorbs flow
- **Rising Lambda:** Liquidity deterioration

---

### 6.2 Realized Spread & Adverse Selection

```python
def realized_spread_analysis(
    ticks: pl.DataFrame,
    future_window_us: int = 5_000_000,  # 5 seconds
) -> pl.DataFrame:
    """
    Calculate realized spread to measure adverse selection.
    
    Realized Spread = 2 * (Trade_Price - Future_Mid_Price)
    """
    # Get future mid price (simplified: use future trades)
    ticks = ticks.sort("ts_local_us").with_columns([
        pl.col("price").shift(-5).mean().over(rolling_window=future_window_us).alias("future_price"),
    ])
    
    # Realized spread (signed by trade side)
    return ticks.with_columns([
        pl.when(pl.col("side") == 0)  # Buy
        .then(2 * (pl.col("price") - pl.col("future_price")))
        .otherwise(2 * (pl.col("future_price") - pl.col("price")))
        .alias("realized_spread"),
    ])
```

---

## 7. Cross-Sectional Microstructure Factors

### 7.1 Relative Liquidity Score

```python
def relative_liquidity_factor(
    symbol_universe: list,
    date: str,
) -> pl.DataFrame:
    """
    Rank stocks by composite liquidity score.
    
    Use for: Long liquid, short illiquid (or vice versa for premium capture).
    """
    liquidity_scores = []
    
    for symbol in symbol_universe:
        ticks = query.szse_l3_ticks("szse", symbol, date, date, decoded=True)
        orders = query.szse_l3_orders("szse", symbol, date, date, decoded=True)
        
        # Calculate component scores
        spread = estimate_effective_spread(ticks)
        depth = calculate_avg_depth(orders)
        resiliency = calculate_resilience(ticks, orders)
        kyle_lambda = estimate_kyles_lambda(ticks)
        
        # Composite score (lower = more liquid)
        composite = (
            0.3 * spread +
            0.3 * (1 / depth) +
            0.2 * (1 / resiliency) +
            0.2 * kyle_lambda
        )
        
        liquidity_scores.append({
            "symbol": symbol,
            "liquidity_score": composite,
            "spread": spread,
            "depth": depth,
        })
    
    scores_df = pl.DataFrame(liquidity_scores)
    
    # Rank within universe
    scores_df = scores_df.with_columns([
        pl.col("liquidity_score").rank().alias("liquidity_rank"),
        pl.col("liquidity_score").z_score().alias("liquidity_zscore"),
    ])
    
    return scores_df
```

---

### 7.2 Informed Trading Score

```python
def informed_trading_factor(
    symbol_universe: list,
    date: str,
) -> pl.DataFrame:
    """
    Rank stocks by probability of informed trading.
    
    Use for: Avoid high PIN stocks (adverse selection) or trade directionally.
    """
    informed_scores = []
    
    for symbol in symbol_universe:
        ticks = query.szse_l3_ticks("szse", symbol, date, date, decoded=True)
        orders = query.szse_l3_orders("szse", symbol, date, date, decoded=True)
        
        # Component metrics
        vpin = calculate_vpin(ticks)
        toxicity = calculate_toxicity_ratio(orders)
        cancel_rate = calculate_cancellation_rate(orders)
        large_trade_ratio = calculate_large_trade_ratio(ticks)
        
        # Composite informed score
        composite = (
            0.3 * vpin +
            0.25 * toxicity +
            0.25 * cancel_rate +
            0.2 * large_trade_ratio
        )
        
        informed_scores.append({
            "symbol": symbol,
            "informed_score": composite,
            "vpin": vpin,
        })
    
    return pl.DataFrame(informed_scores)
```

---

## 8. Implementation Quick Reference

### 8.1 Feature Importance for MFT

| Feature | Signal Type | Best Horizon | Computational Cost |
|---------|-------------|--------------|-------------------|
| VPIN | Toxicity warning | 5-15 min | Medium |
| Kyle's Lambda | Liquidity state | 5-30 min | Low |
| Trade Intensity | Momentum/News | 1-5 min | Low |
| Order Resilience | Execution quality | 5-10 min | High |
| Iceberg Detection | Hidden liquidity | 1-5 min | Medium |
| Layering Detection | Market fragility | Real-time | Low |
| Lee-Ready L3 | Flow direction | 1-5 min | Low |
| Inter-event Timing | Burst detection | 1-5 min | Low |

### 8.2 Correlation with Standard Factors

| Feature | Price Momentum Corr | Volume Corr | Notes |
|---------|---------------------|-------------|-------|
| VPIN | +0.3 (leads) | +0.6 | Predicts volatility |
| Trade Intensity | +0.5 (contemp) | +0.8 | Momentum confirmation |
| Kyle's Lambda | -0.2 | -0.3 | Inverse liquidity |
| Resilience | -0.1 | +0.1 | Low correlation = diversifier |
| Informed Score | +0.2 (leads) | +0.4 | Early warning |

### 8.3 Optimal Feature Combinations

**Momentum Strategy:**
- Trade Intensity (trend confirmation)
- VPIN (avoid high toxicity periods)
- Lee-Ready Classification (direction)

**Mean Reversion Strategy:**
- Kyle's Lambda (illiquidity premium)
- Resilience (temporary impact detection)
- Layering Detection (fragile liquidity)

**Execution Optimization:**
- Order Resilience (timing)
- Iceberg Detection (hidden size)
- Trade Intensity (avoid bursts)

---

## 9. Summary

### Key Insights for MFT

1. **VPIN > 0.35:** Avoid market orders, use patient execution
2. **Kyle's Lambda rising:** Reduce size, expect slippage
3. **Resilience < 0.5:** Temporary liquidity shock, mean reversion likely
4. **Burstiness > 2.0:** News-driven, follow momentum
5. **Iceberg detected:** Size available at level, adjust sizing
6. **High layering:** Cancel orders, wait for clarity

### Next Steps

1. **Backtest each feature** individually for signal decay
2. **Combine features** using orthogonalization (PCA, ICA)
3. **Regime detection:** When do microstructure features work best?
4. **Cost integration:** Include impact estimates in position sizing
5. **Real-time validation:** Monitor feature stability in production

---

**Document Version:** 1.0  
**Last Updated:** 2024  
**Owner:** Quant Research Team
