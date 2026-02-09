# Volume Bar Features - Quick Reference

**Target**: Crypto MFT (15s - 5min holding periods)
**Updated**: 2026-02-09

---

## 1-Minute Setup

```python
from pointline.research import query
from pointline.research.spines import VolumeBarConfig, get_builder
from pointline.research.resample import assign_to_buckets

# Load trades
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-07", decoded=True)

# Build volume bar spine (100 BTC per bar)
spine_builder = get_builder("volume")
spine = spine_builder.build_spine(
    symbol_id=12345,
    start_ts_us=...,
    end_ts_us=...,
    config=VolumeBarConfig(volume_threshold=100.0),
    source_data=trades.lazy()
)

# Assign trades to bars
bucketed = assign_to_buckets(trades.lazy(), spine, "ts_local_us")

# Aggregate features
features = bucketed.group_by("bucket_start").agg([
    pl.col("price").first().alias("open"),
    pl.col("price").last().alias("close"),
    ((pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()).alias("vwap"),
    # Add more features...
])
```

---

## Feature Cheat Sheet

### Order Flow (Most Predictive)

```python
# Flow imbalance: [-1, 1]
flow_imb = (buy_vol - sell_vol) / (buy_vol + sell_vol)

# Flow persistence (autocorr)
flow_persistence = flow_imb_t * flow_imb_{t-1}

# Aggressive ratio: [0, 1]
aggressive_ratio = aggressive_volume / total_volume
```

### Microstructure

```python
# Spread in basis points
spread_bps = ((ask - bid) / mid) * 10000

# Book imbalance (top 5 levels)
book_imb = (bid_depth - ask_depth) / (bid_depth + ask_depth)

# Book-flow divergence (informed flow)
book_flow_div = book_imb - flow_imb
```

### Price Dynamics

```python
# VWAP reversion (mean reversion)
vwap_reversion = (close - vwap) / vwap

# Momentum (different horizons)
ret_1bar = close / close_{t-1} - 1
ret_5bar = close / close_{t-5} - 1

# Volume-weighted return
vw_return = sum(qty * log(price / first_price)) / sum(qty)
```

### Volatility

```python
# Realized volatility (std of log returns)
realized_vol = std(log(price_t / price_{t-1}))

# High-low range
hl_range_pct = (high - low) / vwap

# Volume-weighted volatility
vw_vol = sqrt(sum(qty * (log_ret - vw_return)^2) / sum(qty))
```

### Trade Size (Retail vs Whale)

```python
# Average trade size
avg_size = mean(trade_sizes)

# Size ratio (institutional activity)
size_ratio = avg_size / rolling_median(avg_size, 50)

# Max trade size (whale detection)
max_size = max(trade_sizes)
```

---

## Volume Threshold Selection

| Market | Daily Volume | Threshold (0.2%) | Bars/Day |
|--------|-------------|------------------|----------|
| BTC-PERP | 50,000 BTC | 100 BTC | ~500 |
| ETH-PERP | 300,000 ETH | 600 ETH | ~500 |
| SOL-PERP | 5M SOL | 10K SOL | ~500 |

**Rule of thumb**: 0.1% - 0.5% of daily volume per bar

**For MFT**: Target 300-1000 bars/day (adaptive to market regime)

---

## Information Coefficient (IC) Benchmarks

| Feature | Typical IC | Significance |
|---------|-----------|--------------|
| flow_imbalance | 0.05 - 0.15 | ⭐⭐⭐ Strong |
| book_flow_divergence | 0.03 - 0.10 | ⭐⭐ Good |
| vwap_reversion | 0.02 - 0.08 | ⭐⭐ Good |
| spread_bps | -0.02 - 0.05 | ⭐ Weak |
| realized_vol | -0.01 - 0.03 | ⚠️ Noisy |

**Threshold**: IC > 0.02 with p < 0.05

**Combo**: Flow imbalance + book divergence often > 0.20 IC

---

## Latency Budget (MFT)

| Component | Latency | Optimization |
|-----------|---------|--------------|
| Data load | 20-50ms | ✓ Partition pruning |
| Volume bar assignment | 10-20ms | ✓ Polars join_asof |
| Feature aggregation | 30-60ms | ✓ Vectorized |
| Quality gates | 50-100ms | ⚠️ Skip in prod |
| **Total** | **110-230ms** | **Target: <100ms** |

**Production optimizations**:
- Skip reproducibility gate: -50ms
- Pre-compute spine offline: -10ms
- Incremental bar tracking: -40ms
- **Result**: 50-80ms ✓

---

## Common Pitfalls

### ❌ Lookahead Bias

```python
# WRONG: Uses bar close price (not known until bar closes)
ret_t = (price[t] - price[t-1]) / price[t-1]

# CORRECT: Use previous bar close
ret_t = (price[t-1] - price[t-2]) / price[t-2]
```

### ❌ Feature Leakage

```python
# WRONG: Forward-looking feature
spread_change_future = spread[t+1] - spread[t]

# CORRECT: Backward-looking only
spread_change_past = spread[t] - spread[t-1]
```

### ❌ Non-Stationary Features

```python
# WRONG: Raw price (trending, non-stationary)
feature = close[t]

# CORRECT: Returns or z-score (stationary)
feature = (close[t] - close[t-1]) / close[t-1]
# OR
feature = (close[t] - mean(close)) / std(close)
```

### ❌ Ignoring Missing Data

```python
# WRONG: Assume quotes always available
spread = ask_price - bid_price

# CORRECT: Handle nulls
spread = (ask_price - bid_price).fill_null(method="forward")
# OR filter out bars with missing quotes
```

---

## Pipeline v2 Template (JSON)

```json
{
  "schema_version": "2.0",
  "request_id": "btc_mft_v1",
  "mode": "bar_then_feature",

  "timeline": {
    "start": "2024-05-01T00:00:00Z",
    "end": "2024-05-31T23:59:59Z",
    "ts_col": "ts_local_us"
  },

  "sources": [
    {"name": "trades_src", "table": "trades", "filters": {"exchange": "binance-futures", "symbol": "BTCUSDT"}},
    {"name": "quotes_src", "table": "quotes", "filters": {"exchange": "binance-futures", "symbol": "BTCUSDT"}},
    {"name": "book_src", "table": "book_snapshot_25", "filters": {"exchange": "binance-futures", "symbol": "BTCUSDT"}}
  ],

  "spine": {
    "type": "volume",
    "config": {"volume_threshold": 100.0, "use_absolute_volume": true}
  },

  "operators": [
    {"name": "vwap", "stage": "aggregate", "agg": "trade_vwap", "source": "trades_src"},
    {"name": "ohlcv", "stage": "aggregate", "agg": "ohlcv", "source": "trades_src"},
    {"name": "flow", "stage": "aggregate", "agg": "flow_imbalance", "source": "trades_src"},
    {"name": "spread", "stage": "aggregate", "agg": "spread_bps", "source": "quotes_src"},
    {"name": "book", "stage": "aggregate", "agg": "book_imbalance_top5", "source": "book_src"}
  ],

  "labels": [
    {"name": "fwd_ret_5", "type": "forward_return", "window": 5, "column": "vwap_px"}
  ],

  "evaluation": {
    "metrics": [
      {"type": "row_count"},
      {"type": "null_ratio", "columns": ["flow_imbalance", "spread_bps"]}
    ]
  },

  "constraints": {
    "forbid_lookahead": true,
    "require_pit_ordering": true,
    "max_unassigned_ratio": 0.01
  },

  "artifacts": {
    "emit_lineage": true,
    "output_dir": "/data/research_outputs"
  }
}
```

---

## Validation Checklist

Before deployment, verify:

- [ ] **PIT check**: All features backward-looking only
- [ ] **Null ratio**: < 1% nulls in critical features
- [ ] **Stationarity**: ADF test p-value < 0.05
- [ ] **IC validation**: IC > 0.02, p < 0.05 for key features
- [ ] **Outliers**: < 0.1% outliers (> 5 sigma)
- [ ] **Feature correlation**: Max pairwise correlation < 0.9
- [ ] **Reproducibility**: Same inputs → same outputs
- [ ] **Latency**: End-to-end < 100ms for production

---

## Production Workflow

### Research Phase (Offline)

```python
# 1. Generate features with full gates
output = pipeline(request)
assert output["decision"]["status"] == "accept"

# 2. Validate features
features = pl.read_parquet(output["artifacts"]["paths"][0])
validation = validate_features(features)

# 3. Compute IC
for col in ["flow_imbalance", "vwap_reversion"]:
    ic = compute_ic(features, col, "forward_return_5bar")
    print(f"{col}: IC = {ic:.4f}")

# 4. Train model
model = GradientBoostingRegressor()
model.fit(X, y)
```

### Production Deployment (Online)

```python
# 1. Initialize tracker
tracker = VolumeBarTracker(threshold=100.0)

# 2. Stream trades
for trade in websocket:
    bar = tracker.add_trade(trade)

    if bar:  # Bar completed
        # 3. Compute features (incremental)
        features = compute_features_fast(bar)

        # 4. Predict
        signal = model.predict([features])[0]

        # 5. Execute
        if abs(signal) > threshold:
            place_order(side="BUY" if signal > 0 else "SELL")
```

---

## Further Reading

- **Full Guide**: `docs/guides/volume-bar-features-crypto-mft.md`
- **Architecture**: `docs/architecture/research-framework-deep-review.md`
- **Example Code**: `examples/crypto_mft_volume_bars_example.py`
- **Academic**: López de Prado (2018) - "Advances in Financial Machine Learning"

---

## Quick Commands

```bash
# Run example
python examples/crypto_mft_volume_bars_example.py

# Check data coverage
pointline symbol search BTCUSDT --exchange binance-futures

# Run backtest
python scripts/backtest_volume_bar_strategy.py --symbol BTCUSDT --threshold 100

# Deploy to production
python scripts/deploy_live_trader.py --config volume_bar_config.json
```

---

**Pro Tip**: Start with `flow_imbalance` + `vwap_reversion` features only. Add complexity after validating baseline IC > 0.05.
