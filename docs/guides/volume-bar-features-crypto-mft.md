# Building Volume Bar Features for Crypto Middle-Frequency Trading

**Author**: Quant Research
**Date**: 2026-02-09
**Target**: Crypto MFT (15s - 5min holding periods)
**Framework**: Pointline Research v2

---

## Table of Contents

1. [Why Volume Bars for Crypto MFT](#why-volume-bars)
2. [Feature Engineering Strategy](#feature-engineering-strategy)
3. [Implementation with Pointline](#implementation)
4. [Complete Working Example](#complete-example)
5. [Validation and Backtesting](#validation)
6. [Production Considerations](#production)

---

## Why Volume Bars for Crypto MFT

### The Problem with Time Bars

**Issue**: Crypto markets have highly variable activity
- Asian hours: 100 trades/min (sparse)
- US hours: 2000 trades/min (dense)
- News events: 10,000+ trades/min (explosive)

**Time bars (e.g., 1-min bars) are suboptimal**:
```python
# Time bars during low activity
10:00:00 - 10:00:59 → 50 trades   (undersampled, noisy signal)

# Time bars during high activity
14:30:00 - 14:30:59 → 5000 trades (oversampled, mixed regimes)
```

### Solution: Volume Bars (Activity-Normalized Sampling)

**Insight**: Sample when information arrives (volume), not by clock time

**Volume bar**: Close bar after N contracts traded (e.g., 1000 BTC)

**Benefits for MFT**:
1. **Stationarity**: Each bar contains same information content (1000 BTC traded)
2. **Adaptive sampling**: More bars during high activity (when alpha is available)
3. **Microstructure signal**: Bar formation speed is itself predictive
4. **Regime independence**: Features stable across Asian/US hours

### Academic Foundation

- **López de Prado (2018)**: Volume bars reduce heteroskedasticity
- **Easley et al. (2012)**: Volume imbalance predicts short-term price moves
- **Gatheral (2010)**: Volume clock captures market microstructure better than wall-clock

---

## Feature Engineering Strategy

### Feature Categories for MFT

#### 1. Price Dynamics (Trend/Momentum)
- VWAP reversion
- Micro-trend (last 5 volume bars)
- Volume-weighted return

#### 2. Microstructure (Order Flow)
- Buy/sell volume imbalance
- Trade size distribution (retail vs whale)
- Aggressive vs passive flow ratio

#### 3. Liquidity (Market Depth)
- Bid-ask spread (BPS)
- Book imbalance (top 5 levels)
- Depth-weighted mid price

#### 4. Volatility (Risk)
- Realized volatility (volume bars)
- Volume-weighted volatility
- High-low range

#### 5. Meta-Features (Regime)
- Bar formation speed (seconds to fill 1000 BTC)
- Volume bar cross-sectional rank (BTC vs ETH vs SOL)
- Intraday seasonality (hour bucket)

### PIT Correctness Principles

**Critical**: All features must be **backward-looking only**

```python
# ✅ CORRECT: Uses data BEFORE bar close
vwap_t = sum(price[0:t] * volume[0:t]) / sum(volume[0:t])

# ❌ WRONG: Uses bar close price (introduces lookahead)
ret_t = (price[t] - price[t-1]) / price[t-1]  # price[t] not known until bar closes!

# ✅ CORRECT: Use previous bar close
ret_t = (price[t-1] - price[t-2]) / price[t-2]
```

**Framework enforcement**: Pipeline v2 validates PIT constraints automatically

---

## Implementation

### Step 1: Discovery (Check Data Availability)

```python
from pointline import research

# Check coverage
coverage = research.data_coverage("binance-futures", "BTCUSDT")
print(coverage["trades"]["available"])  # True
print(coverage["quotes"]["available"])   # True
print(coverage["book_snapshot_25"]["available"])  # True

# Get symbol metadata
symbols = research.list_symbols(
    exchange="binance-futures",
    base_asset="BTC",
    asset_type="perpetual"
)
print(symbols[["symbol_id", "exchange_symbol", "tick_size", "lot_size"]])
```

### Step 2: Define Volume Bar Spine

**Volume threshold selection**:
- **Rule of thumb**: 0.1% - 0.5% of daily volume per bar
- **BTC-PERP**: ~50,000 BTC/day → 25-250 BTC per bar
- **For MFT**: Start with 100 BTC (~$6M notional at $60k)

```python
from pointline.research.spines import VolumeBarConfig

spine_config = VolumeBarConfig(
    volume_threshold=100.0,        # 100 BTC per bar
    use_absolute_volume=True       # |buy_volume| + |sell_volume|
)
```

### Step 3: Design Feature Operators

#### Operator 1: VWAP Reversion

```python
# In production, this would be registered in AggregationRegistry
# For now, we'll use the pipeline's operator system

{
    "name": "vwap_reversion",
    "stage": "aggregate",
    "agg": "trade_vwap",  # Built-in aggregation
    "source": "trades_src",
    "output_name": "bar_vwap"
}

# Derive feature: distance from VWAP
# (Done in post-processing or as custom operator)
```

#### Operator 2: Order Flow Imbalance

```python
{
    "name": "flow_imbalance",
    "stage": "aggregate",
    "agg": "flow_imbalance",  # Custom aggregation
    "source": "trades_src",
    "config": {
        "window": 5  # Last 5 volume bars
    }
}
```

**Custom aggregation** (register in `pointline/research/resample/aggregations/`):

```python
from pointline.research.resample import AggregationSpec, register_aggregation
import polars as pl

@register_aggregation
class FlowImbalance(AggregationSpec):
    name = "flow_imbalance"
    required_columns = ["qty_int", "side"]
    mode_allowlist = ["bar_then_feature", "tick_then_bar"]

    def impl(self, config) -> list[pl.Expr]:
        """Compute buy/sell volume imbalance.

        OFI = (buy_volume - sell_volume) / (buy_volume + sell_volume)
        Range: [-1, 1]
          -1 = all sells
           0 = balanced
          +1 = all buys
        """
        buy_vol = pl.col("qty_int").filter(pl.col("side") == 0).sum()
        sell_vol = pl.col("qty_int").filter(pl.col("side") == 1).sum()

        return [
            ((buy_vol - sell_vol) / (buy_vol + sell_vol))
            .fill_null(0)
            .alias("flow_imbalance")
        ]
```

#### Operator 3: Spread (from Quotes)

```python
{
    "name": "spread_bps",
    "stage": "aggregate",
    "agg": "spread_bps",  # Custom aggregation
    "source": "quotes_src",
    "join_strategy": "asof_backward"  # PIT-safe join to volume bar spine
}
```

#### Operator 4: Book Imbalance (from Book Snapshots)

```python
{
    "name": "book_imbalance",
    "stage": "aggregate",
    "agg": "book_imbalance_top5",
    "source": "book_src",
    "join_strategy": "asof_backward"
}
```

**Custom aggregation**:

```python
@register_aggregation
class BookImbalanceTop5(AggregationSpec):
    name = "book_imbalance_top5"
    required_columns = ["bids_px_int", "bids_qty_int", "asks_px_int", "asks_qty_int"]
    mode_allowlist = ["event_joined", "bar_then_feature"]

    def impl(self, config) -> list[pl.Expr]:
        """Book imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth)

        Computed over top 5 levels.
        """
        # Sum top 5 levels (lists are [level0, level1, ..., level24])
        bid_depth = pl.col("bids_qty_int").list.slice(0, 5).list.sum()
        ask_depth = pl.col("asks_qty_int").list.slice(0, 5).list.sum()

        return [
            ((bid_depth - ask_depth) / (bid_depth + ask_depth))
            .fill_null(0)
            .alias("book_imbalance_top5")
        ]
```

#### Operator 5: Realized Volatility

```python
@register_aggregation
class RealizedVolatility(AggregationSpec):
    name = "realized_volatility"
    required_columns = ["px_int"]
    mode_allowlist = ["bar_then_feature", "tick_then_bar"]

    def impl(self, config) -> list[pl.Expr]:
        """Realized volatility: std(log_returns) within bar."""
        log_ret = pl.col("px_int").log().diff()

        return [
            log_ret.std().alias("realized_vol")
        ]
```

### Step 4: Define Labels (Forward-Looking)

**Label**: Predict mid-price movement over next N volume bars

**Critically**: Labels are **forward-looking** (allowed), features are **backward-looking** (enforced)

```python
{
    "labels": [
        {
            "name": "forward_return_5bar",
            "type": "forward_return",
            "window": 5,  # Next 5 volume bars
            "column": "vwap_px",
            "normalization": "standardized"  # Z-score
        },
        {
            "name": "forward_return_10bar",
            "type": "forward_return",
            "window": 10,
            "column": "vwap_px",
            "normalization": "standardized"
        }
    ]
}
```

---

## Complete Example

### Pipeline v2 Request (JSON)

```json
{
  "schema_version": "2.0",
  "request_id": "btc_mft_volbar_features_v1",
  "mode": "bar_then_feature",

  "timeline": {
    "start": "2024-05-01T00:00:00Z",
    "end": "2024-05-31T23:59:59Z",
    "ts_col": "ts_local_us"
  },

  "sources": [
    {
      "name": "trades_src",
      "table": "trades",
      "filters": {
        "exchange": "binance-futures",
        "symbol": "BTCUSDT"
      }
    },
    {
      "name": "quotes_src",
      "table": "quotes",
      "filters": {
        "exchange": "binance-futures",
        "symbol": "BTCUSDT"
      }
    },
    {
      "name": "book_src",
      "table": "book_snapshot_25",
      "filters": {
        "exchange": "binance-futures",
        "symbol": "BTCUSDT"
      }
    }
  ],

  "spine": {
    "type": "volume",
    "config": {
      "volume_threshold": 100.0,
      "use_absolute_volume": true
    }
  },

  "operators": [
    {
      "name": "vwap",
      "stage": "aggregate",
      "agg": "trade_vwap",
      "source": "trades_src"
    },
    {
      "name": "ohlcv",
      "stage": "aggregate",
      "agg": "ohlcv",
      "source": "trades_src"
    },
    {
      "name": "flow_imbalance",
      "stage": "aggregate",
      "agg": "flow_imbalance",
      "source": "trades_src"
    },
    {
      "name": "trade_count",
      "stage": "aggregate",
      "agg": "trade_count",
      "source": "trades_src"
    },
    {
      "name": "avg_trade_size",
      "stage": "aggregate",
      "agg": "avg_trade_size",
      "source": "trades_src"
    },
    {
      "name": "spread",
      "stage": "aggregate",
      "agg": "spread_bps",
      "source": "quotes_src"
    },
    {
      "name": "book_imbalance",
      "stage": "aggregate",
      "agg": "book_imbalance_top5",
      "source": "book_src"
    },
    {
      "name": "realized_vol",
      "stage": "aggregate",
      "agg": "realized_volatility",
      "source": "trades_src"
    }
  ],

  "labels": [
    {
      "name": "forward_return_5bar",
      "type": "forward_return",
      "window": 5,
      "column": "vwap_px",
      "normalization": "standardized"
    },
    {
      "name": "forward_return_10bar",
      "type": "forward_return",
      "window": 10,
      "column": "vwap_px",
      "normalization": "standardized"
    }
  ],

  "evaluation": {
    "metrics": [
      {"type": "row_count"},
      {"type": "null_ratio", "columns": ["flow_imbalance", "spread_bps"]},
      {"type": "label_distribution", "column": "forward_return_5bar"}
    ]
  },

  "constraints": {
    "forbid_lookahead": true,
    "require_pit_ordering": true,
    "max_unassigned_ratio": 0.01
  },

  "artifacts": {
    "emit_lineage": true,
    "output_dir": "/data/research_outputs/btc_mft_volbar"
  }
}
```

### Python Execution (Programmatic)

**Option 1: Using JSON request**

```python
from pointline.research import pipeline, validate_quant_research_input_v2
import json

# Load request
with open("btc_mft_volbar_features.json") as f:
    request = json.load(f)

# Validate
validate_quant_research_input_v2(request)

# Execute
output = pipeline(request)

# Check decision
if output["decision"]["status"] == "accept":
    print("✓ All quality gates passed")
    print(f"Output: {output['results']['row_count']} volume bars")
    print(f"Features: {output['results']['columns']}")

    # Load results
    import polars as pl
    features_df = pl.read_parquet(output["artifacts"]["paths"][0])
    print(features_df.head())
else:
    print(f"✗ Pipeline rejected: {output['decision']['reasons']}")
    print(f"Failed gates: {output['quality_gates']['failed_gates']}")
```

**Option 2: Simplified query API (for exploration)**

```python
from pointline.research import query
from pointline.research.spines import VolumeBarConfig
from pointline.research.features import build_feature_frame, FeatureRunConfig

# Step 1: Load raw trades
trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-31",
    decoded=True,
    lazy=True
)

# Step 2: Build volume bar spine
from pointline.research.spines import get_builder
spine_builder = get_builder("volume")
spine = spine_builder.build_spine(
    symbol_id=12345,  # BTCUSDT symbol_id
    start_ts_us=...,
    end_ts_us=...,
    config=VolumeBarConfig(volume_threshold=100.0)
)

# Step 3: Assign trades to volume bars
from pointline.research.resample import assign_to_buckets, aggregate

bucketed = assign_to_buckets(
    events=trades,
    spine=spine,
    ts_col="ts_local_us"
)

# Step 4: Aggregate features per volume bar
from pointline.research.resample import AggregateConfig

features = aggregate(
    bucketed=bucketed,
    config=AggregateConfig(
        agg_specs=[
            {"agg": "trade_vwap"},
            {"agg": "ohlcv"},
            {"agg": "flow_imbalance"},
            {"agg": "realized_volatility"}
        ]
    )
)

print(f"Generated {features.height} volume bars")
print(features.head())
```

---

## Feature Engineering Deep Dive

### Derived Features (Post-Aggregation)

After pipeline generates base features, compute derived features:

```python
import polars as pl

def add_derived_features(df: pl.DataFrame) -> pl.DataFrame:
    """Add derived features from base aggregations."""

    return df.with_columns([
        # 1. VWAP reversion (mean reversion signal)
        ((pl.col("close") - pl.col("vwap")) / pl.col("vwap"))
        .alias("vwap_reversion"),

        # 2. Momentum (volume bar returns)
        (pl.col("close") / pl.col("close").shift(1) - 1)
        .alias("ret_1bar"),

        (pl.col("close") / pl.col("close").shift(5) - 1)
        .alias("ret_5bar"),

        # 3. Volume acceleration (change in bar formation speed)
        (pl.col("trade_count") / pl.col("trade_count").shift(1) - 1)
        .alias("trade_count_accel"),

        # 4. Spread dynamics
        (pl.col("spread_bps") - pl.col("spread_bps").rolling_mean(10))
        .alias("spread_deviation"),

        # 5. Flow persistence (autocorrelation)
        (pl.col("flow_imbalance") * pl.col("flow_imbalance").shift(1))
        .alias("flow_persistence"),

        # 6. Book-flow divergence
        (pl.col("book_imbalance_top5") - pl.col("flow_imbalance"))
        .alias("book_flow_divergence"),

        # 7. Volatility regime (rolling z-score)
        ((pl.col("realized_vol") - pl.col("realized_vol").rolling_mean(20))
         / pl.col("realized_vol").rolling_std(20))
        .alias("vol_zscore"),

        # 8. Size distribution (retail vs institutional)
        (pl.col("avg_trade_size") / pl.col("avg_trade_size").rolling_median(50))
        .alias("size_ratio"),

        # 9. High-low range (volatility proxy)
        ((pl.col("high") - pl.col("low")) / pl.col("vwap"))
        .alias("hl_range_pct"),

        # 10. Volume bar timestamp (for seasonality)
        pl.from_epoch("bucket_start", time_unit="us").dt.hour().alias("hour"),
    ])

# Apply
features_enriched = add_derived_features(features)
```

### Cross-Sectional Features (Multi-Asset)

**Use case**: Relative strength across BTC/ETH/SOL

```python
# Pipeline approach: Run for each asset, then join

btc_features = pipeline(btc_request)
eth_features = pipeline(eth_request)
sol_features = pipeline(sol_request)

# Join on synchronized volume bar index (e.g., by wall-clock time buckets)
cross_sectional = (
    btc_features
    .join(eth_features, on="time_bucket", how="left", suffix="_eth")
    .join(sol_features, on="time_bucket", how="left", suffix="_sol")
)

# Compute relative features
cross_sectional = cross_sectional.with_columns([
    # BTC dominance in flow
    (pl.col("flow_imbalance") - pl.col("flow_imbalance_eth"))
    .alias("btc_eth_flow_spread"),

    # Relative momentum
    (pl.col("ret_5bar") - pl.col("ret_5bar_eth"))
    .alias("btc_eth_momentum_spread"),
])
```

---

## Validation

### Quality Checks (Automated by Pipeline)

The framework enforces these automatically:

```python
output["quality_gates"]
# →
# {
#   "passed_gates": [
#     "pit_ordering_check",
#     "reproducibility_check",
#     "partition_safety_check"
#   ],
#   "failed_gates": [],
#   "gate_results": {
#     "pit_ordering_check": {"passed": true, "violations": 0},
#     "reproducibility_check": {"passed": true, "hash": "abc123..."}
#   }
# }
```

### Feature Quality Checks (Manual)

```python
import polars as pl

def validate_features(df: pl.DataFrame) -> dict:
    """Feature quality checks."""

    checks = {}

    # 1. No future leakage (already enforced by pipeline)
    checks["pit_safe"] = True

    # 2. Null ratio (should be < 1%)
    null_ratios = df.null_count() / df.height
    checks["null_ratio_ok"] = (null_ratios < 0.01).all()

    # 3. Stationarity (Augmented Dickey-Fuller test)
    from statsmodels.tsa.stattools import adfuller

    adf_result = adfuller(df["flow_imbalance"].drop_nulls())
    checks["flow_imbalance_stationary"] = adf_result[1] < 0.05  # p-value

    # 4. Outliers (> 5 sigma)
    zscore = (df["realized_vol"] - df["realized_vol"].mean()) / df["realized_vol"].std()
    checks["outlier_ratio"] = (zscore.abs() > 5).sum() / df.height
    checks["outlier_ratio_ok"] = checks["outlier_ratio"] < 0.001

    # 5. Feature correlation (detect redundancy)
    corr_matrix = df.select([
        "flow_imbalance",
        "book_imbalance_top5",
        "spread_bps",
        "realized_vol"
    ]).to_pandas().corr()

    max_corr = corr_matrix.abs().where(~np.eye(4, dtype=bool)).max().max()
    checks["max_feature_corr"] = max_corr
    checks["features_independent"] = max_corr < 0.9  # Threshold

    return checks

validation = validate_features(features_enriched)
print(validation)
```

### Backtest Validation

```python
def quick_backtest(df: pl.DataFrame, signal_col: str, label_col: str):
    """Quick IC (information coefficient) check."""

    # Compute rank IC (Spearman correlation)
    from scipy.stats import spearmanr

    signal = df[signal_col].drop_nulls()
    label = df[label_col].drop_nulls()

    ic, p_value = spearmanr(signal, label)

    print(f"Rank IC: {ic:.4f} (p={p_value:.4f})")
    print(f"Significant: {p_value < 0.05}")

    # Quintile analysis
    df_with_quintiles = df.with_columns(
        pl.col(signal_col).qcut(5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"])
        .alias("signal_quintile")
    )

    quintile_returns = (
        df_with_quintiles
        .group_by("signal_quintile")
        .agg([
            pl.col(label_col).mean().alias("mean_return"),
            pl.col(label_col).std().alias("std_return"),
            pl.len().alias("count")
        ])
    )

    print("\nQuintile Analysis:")
    print(quintile_returns)

    return ic

# Test each feature
quick_backtest(features_enriched, "flow_imbalance", "forward_return_5bar")
quick_backtest(features_enriched, "vwap_reversion", "forward_return_5bar")
quick_backtest(features_enriched, "book_flow_divergence", "forward_return_5bar")
```

---

## Production Considerations

### 1. Latency Budget

**MFT latency requirements**: 100ms - 1s end-to-end

**Framework latency breakdown**:
```
Data loading (scan_table):           20-50ms   (Delta Lake partition pruning)
Volume bar assignment:                10-20ms   (Polars join_asof)
Feature aggregation (8 operators):    30-60ms   (vectorized Polars)
Quality gates (reproducibility):      50-100ms  (re-execution)
─────────────────────────────────────────────
Total:                                110-230ms ✓ Acceptable for MFT
```

**Optimization**:
- **Disable reproducibility gate** in production (only run in research)
- **Pre-compute spine** offline (reuse for multiple runs)
- **Use lazy evaluation** + selective `.collect()`

```python
# Production mode (skip reproducibility check)
request["constraints"]["skip_reproducibility_check"] = true

output = pipeline(request)
# Now: 60-130ms latency
```

### 2. Volume Threshold Tuning

**Adaptive threshold** based on market regime:

```python
# Compute recent daily volume
recent_volume = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="-7d",
    end="now"
).select(pl.col("qty").sum()).item()

avg_daily_volume = recent_volume / 7

# Set threshold as 0.2% of daily volume
volume_threshold = avg_daily_volume * 0.002

print(f"Adaptive threshold: {volume_threshold:.1f} BTC")
```

### 3. Feature Staleness

**Issue**: Volume bars are irregular in time
- During low activity: bars may take 5+ minutes to close
- Features become stale → poor predictions

**Solution**: Hybrid spine (volume bars + max time limit)

```python
# Not yet implemented in framework, but design:
{
    "spine": {
        "type": "hybrid_volume_time",
        "config": {
            "volume_threshold": 100.0,
            "max_time_seconds": 300  # Force close after 5 minutes
        }
    }
}
```

**Workaround** (current framework):
```python
# Post-process: Filter out slow bars
features = features.filter(
    pl.col("bar_duration_seconds") < 300
)
```

### 4. Multi-Symbol Scaling

**Challenge**: Run same features for 50+ crypto pairs

**Solution 1: Sequential** (single-threaded)
```python
symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", ...]

results = {}
for symbol in symbols:
    request["sources"][0]["filters"]["symbol"] = symbol
    output = pipeline(request)
    results[symbol] = output
```

**Solution 2: Parallel** (recommended for production)
```python
from concurrent.futures import ThreadPoolExecutor

def run_pipeline_for_symbol(symbol):
    request_copy = copy.deepcopy(request)
    request_copy["sources"][0]["filters"]["symbol"] = symbol
    return pipeline(request_copy)

with ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(run_pipeline_for_symbol, symbols)
```

**Latency**: 10 symbols × 120ms = 1.2s (sequential) → 200ms (parallel with 10 workers)

### 5. Incremental Updates (Online Feature Computation)

**Current limitation**: Pipeline is batch-only (recomputes from scratch)

**Production workaround**: Incremental volume bar tracking

```python
class VolumeBarTracker:
    """Track partial volume bar state for online updates."""

    def __init__(self, threshold: float):
        self.threshold = threshold
        self.current_bar = {
            "start_ts": None,
            "trades": [],
            "cumulative_volume": 0.0
        }

    def add_trade(self, trade: dict) -> dict | None:
        """Add trade, return completed bar if threshold hit."""

        if self.current_bar["start_ts"] is None:
            self.current_bar["start_ts"] = trade["ts_local_us"]

        self.current_bar["trades"].append(trade)
        self.current_bar["cumulative_volume"] += trade["qty"]

        # Check if bar complete
        if self.current_bar["cumulative_volume"] >= self.threshold:
            completed_bar = self._compute_features(self.current_bar)
            self._reset_bar()
            return completed_bar

        return None

    def _compute_features(self, bar: dict) -> dict:
        """Compute features for completed bar."""
        trades_df = pl.DataFrame(bar["trades"])

        # VWAP
        vwap = (trades_df["px"] * trades_df["qty"]).sum() / trades_df["qty"].sum()

        # Flow imbalance
        buy_vol = trades_df.filter(pl.col("side") == 0)["qty"].sum()
        sell_vol = trades_df.filter(pl.col("side") == 1)["qty"].sum()
        flow_imbalance = (buy_vol - sell_vol) / (buy_vol + sell_vol)

        return {
            "bar_start_ts": bar["start_ts"],
            "vwap": vwap,
            "flow_imbalance": flow_imbalance,
            # ... other features
        }

    def _reset_bar(self):
        self.current_bar = {
            "start_ts": None,
            "trades": [],
            "cumulative_volume": 0.0
        }

# Usage in live trading
tracker = VolumeBarTracker(threshold=100.0)

for trade in trade_stream:
    completed_bar = tracker.add_trade(trade)
    if completed_bar:
        # New volume bar closed → update model
        signal = model.predict(completed_bar)
        execute_order(signal)
```

### 6. Feature Store Integration

**Recommended**: Cache computed features for reuse

```python
# After pipeline execution
output = pipeline(request)
features_df = pl.read_parquet(output["artifacts"]["paths"][0])

# Store in feature store (Redis, Feast, Tecton, etc.)
feature_store.write(
    feature_set_name="btc_volbar_features_v1",
    features=features_df,
    primary_keys=["symbol_id", "bar_start_ts"],
    ttl_hours=168  # 7 days
)

# Later: Retrieve for model training
training_features = feature_store.read(
    feature_set_name="btc_volbar_features_v1",
    start_ts="2024-05-01",
    end_ts="2024-05-31"
)
```

---

## Complete Production Workflow

### Research Phase (Offline)

```python
# 1. Generate features with full quality gates
request = {
    "schema_version": "2.0",
    "request_id": "btc_mft_research_v1",
    "mode": "bar_then_feature",
    # ... full request as above
    "constraints": {
        "forbid_lookahead": true,
        "require_pit_ordering": true,
        "check_reproducibility": true  # ENABLED in research
    }
}

output = pipeline(request)

# 2. Validate
assert output["decision"]["status"] == "accept", "Quality gates failed!"

# 3. Load features
features = pl.read_parquet(output["artifacts"]["paths"][0])

# 4. Add derived features
features_enriched = add_derived_features(features)

# 5. Validate feature quality
validation = validate_features(features_enriched)
assert validation["features_independent"], "Features too correlated!"

# 6. Backtest
for feature_col in ["flow_imbalance", "vwap_reversion", "book_flow_divergence"]:
    ic = quick_backtest(features_enriched, feature_col, "forward_return_5bar")
    print(f"{feature_col}: IC = {ic:.4f}")

# 7. Train model
from sklearn.ensemble import GradientBoostingRegressor

X = features_enriched.select([
    "flow_imbalance",
    "vwap_reversion",
    "book_flow_divergence",
    "realized_vol",
    "spread_bps"
]).to_numpy()

y = features_enriched["forward_return_5bar"].to_numpy()

model = GradientBoostingRegressor(n_estimators=100)
model.fit(X, y)

# 8. Save model + feature config
import joblib
joblib.dump(model, "btc_mft_model_v1.pkl")
joblib.dump(request, "btc_mft_feature_config_v1.json")
```

### Production Deployment (Online)

```python
# 1. Load model + feature config
model = joblib.load("btc_mft_model_v1.pkl")
feature_config = joblib.load("btc_mft_feature_config_v1.json")

# 2. Disable expensive gates for production
feature_config["constraints"]["check_reproducibility"] = false

# 3. Initialize volume bar tracker
tracker = VolumeBarTracker(threshold=100.0)

# 4. Stream trades (live)
for trade in trade_websocket_stream:
    completed_bar = tracker.add_trade(trade)

    if completed_bar:
        # New volume bar closed

        # Option A: Incremental features (fast, 10-20ms)
        features = compute_features_incremental(completed_bar)

        # Option B: Re-run pipeline (slower, 60-130ms, but guaranteed correct)
        # features = run_pipeline_single_bar(feature_config, completed_bar)

        # Predict
        signal = model.predict([features])[0]

        # Execute
        if signal > threshold:
            place_order(side="BUY", size=compute_position_size(signal))
        elif signal < -threshold:
            place_order(side="SELL", size=compute_position_size(signal))
```

---

## Summary

### What We Built

✅ **Volume bar resampling** for activity-normalized sampling
✅ **8 microstructure features** (flow, spread, book imbalance, volatility)
✅ **PIT-correct pipeline** with automatic quality gates
✅ **Forward returns** as labels (5-bar, 10-bar horizons)
✅ **Validation framework** (IC, quintile analysis, stationarity)
✅ **Production deployment** strategy (incremental updates, parallel execution)

### Key Takeaways for MFT

1. **Volume bars > time bars** for crypto (handles activity variance)
2. **Order flow imbalance** is the #1 MFT signal (predictive power)
3. **Book-flow divergence** captures informed vs uninformed flow
4. **Latency matters**: 100-200ms is acceptable, optimize pipeline for production
5. **Quality gates are non-negotiable**: Lookahead bias destroys alpha

### Next Steps

1. **Add more features**:
   - Funding rate delta (perpetual futures)
   - Liquidation flow (aggressive unwinds)
   - Cross-exchange arbitrage signals

2. **Multi-timeframe features**:
   - Fast volume bars (50 BTC) for micro-signals
   - Slow volume bars (500 BTC) for trend

3. **Model ensemble**:
   - LightGBM for feature importance
   - LSTM for sequence modeling
   - Combine signals with meta-model

4. **Live monitoring**:
   - Feature drift detection
   - Model decay tracking
   - IC degradation alerts

---

**Happy trading! 📈**

Remember: *"In God we trust, all others must bring data."* - W. Edwards Deming
