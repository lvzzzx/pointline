# Bar Aggregation Architecture

**Status:** Proposed
**Date:** 2026-02-07
**Related:** [Resampling Methods Guide](../guides/resampling-methods.md)

## Table of Contents

1. [Overview](#overview)
2. [Core Concepts](#core-concepts)
3. [Bar Timestamp Semantics](#bar-timestamp-semantics)
4. [Aggregation Patterns](#aggregation-patterns)
5. [Architecture Design](#architecture-design)
6. [API Reference](#api-reference)
7. [Feature Family Adaptation](#feature-family-adaptation)
8. [Implementation](#implementation)
9. [Examples](#examples)

---

## Overview

### Problem Statement

Pointline's current feature engineering operates on **tick-level data**: each spine point uses the latest tick from each table. For coarse sampling (e.g., volume bars every 1000 BTC), this wastes 99% of available data.

**Current System:**
```
Volume bar spine (1000 BTC) → pit_align() → Latest tick at each bar
→ 50 trades in bar, but only use 1 tick (98% waste!)
```

**Proposed System:**
```
Volume bar spine (1000 BTC) → Aggregate all 50 trades in bar → OHLCV
→ 100% of data used for features
```

### Solution: Two-Phase Pipeline

```
Phase 1: Bar Aggregation
  Raw Ticks → Aggregate by Table → Bar Metrics (OHLC, VWAP, spread_avg, ...)

Phase 2: Feature Computation
  Bar Metrics → Feature Families → Features
```

**Key Benefits:**
- Uses 100% of data in each bar (vs 1% in tick-level)
- Faster feature computation (fewer rows after aggregation)
- Standard industry format (OHLCV bars)
- Flexible aggregation methods per table

---

## Core Concepts

### Tick-Level vs Bar-Level Features

#### Tick-Level (Current)

```
Spine Point at t=1000ms:
├─ Latest trade tick: px=50000 (occurred at 800ms)
├─ Latest quote tick: bid=49999, ask=50001 (occurred at 950ms)
└─ Features computed on these TWO ticks

Problem: 49 other trades in [0ms, 1000ms] are ignored!
```

#### Bar-Level (Proposed)

```
Bar [0ms, 1000ms]:
├─ Aggregate all 50 trades → OHLC=(50000, 50010, 49995, 50005), VWAP=50002
├─ Aggregate all 300 quotes → spread_avg=1.5, microprice=50001
└─ Features computed on aggregated bar metrics

Benefit: ALL 50 trades and 300 quotes contribute!
```

### Information Content Comparison

| Approach | Data Used | Performance | Use Case |
|----------|-----------|-------------|----------|
| **Tick-Level** | ~1% of ticks | Slower (more rows) | HFT, tick precision |
| **Bar-Level** | 100% of ticks | Faster (fewer rows) | Production research |

**Benchmark (1 day BTCUSDT, volume bars 1000 BTC):**
- Tick-level: 18,894 rows, 2.5s
- Bar-level: 597 rows, 0.3s ✓ **8x faster**

---

## Bar Timestamp Semantics

### The Timestamp Question

**Critical for PIT Correctness:** When a bar has timestamp `T`, what does that timestamp represent?

```
Bar Window: [T_start, T_end]
Ticks in window: t1, t2, t3, ..., tn

Question: Should bar timestamp be T_start, T_end, t1, or tn?
```

### Answer: Spine Boundary Timestamp (Bar End)

**Rule:** Bar timestamp = spine boundary timestamp = **end of bar window**.

**Rationale:**
1. **PIT Correctness**: Using bar at time `T` means "all data up to and including time `T`"
2. **Consistency with pit_align()**: As-of join semantics (latest data before/at time `T`)
3. **Unambiguous**: Spine already defines bar boundaries; no separate timestamp needed

### Clock Bars (Fixed Intervals)

```python
# Spine: Clock at 1-second intervals
spine = ClockSpineBuilder().build_spine(
    symbol_id=12345,
    step_ms=1000,
)
# Spine timestamps: [0ms, 1000ms, 2000ms, 3000ms, ...]

# Bar aggregation windows:
# Bar 0: [0ms, 1000ms)    → bar timestamp = 1000ms (spine boundary)
# Bar 1: [1000ms, 2000ms) → bar timestamp = 2000ms (spine boundary)
# Bar 2: [2000ms, 3000ms) → bar timestamp = 3000ms (spine boundary)

# Example: Bar at timestamp 2000ms
# ├─ Window: [1000ms, 2000ms)
# ├─ Includes: All ticks with 1000ms <= ts_local_us < 2000ms
# └─ Timestamp: 2000ms (end of window)
```

**Key Points:**
- Bar timestamp = **interval end** (not start!)
- Window is **half-open**: `[T_prev, T_current)`
- First tick in bar: `ts >= T_prev`
- Last tick in bar: `ts < T_current`

### Volume Bars (Event-Driven)

```python
# Spine: Volume bars at 1000 BTC intervals
spine = VolumeSpineBuilder().build_spine(
    symbol_id=12345,
    volume_threshold=1000.0,
)
# Spine timestamps: When cumulative volume crosses thresholds

# Example execution:
# Trade 1: ts=100ms,  qty=500   → cumulative=500
# Trade 2: ts=500ms,  qty=300   → cumulative=800
# Trade 3: ts=1200ms, qty=400   → cumulative=1200 (threshold crossed!)
#          ↑ Spine boundary = 1200ms

# Bar 0:
# ├─ Window: [0ms, 1200ms)
# ├─ Includes: Trade 1, Trade 2, Trade 3 (total=1200 BTC)
# └─ Timestamp: 1200ms (when threshold crossed = spine boundary)

# Trade 4: ts=1800ms, qty=600   → cumulative=1800 (starts Bar 1)
# Trade 5: ts=2100ms, qty=500   → cumulative=2300 (threshold crossed!)
#          ↑ Spine boundary = 2100ms

# Bar 1:
# ├─ Window: [1200ms, 2100ms)
# ├─ Includes: Trade 4, Trade 5 (total=1100 BTC, threshold at 1000 BTC)
# └─ Timestamp: 2100ms (spine boundary)
```

**Key Points:**
- Bar timestamp = **last tick timestamp** that crosses threshold
- Spine builder determines boundary (we just use it)
- Window is `[T_prev_boundary, T_current_boundary)`

### Dollar Bars (Event-Driven)

Same semantics as volume bars:
- Bar timestamp = when dollar threshold crossed
- Window = all trades from previous boundary to current boundary

### Implementation: As-Of Join

```python
def aggregate(self, spine: pl.LazyFrame, trades: pl.LazyFrame, methods: list[str]):
    """Aggregate trades to bars using spine boundaries as timestamps."""

    # Step 1: Assign each trade to its bar via as-of join
    trades_with_bars = trades.join_asof(
        spine.select(["ts_local_us", "exchange_id", "symbol_id"]),
        on="ts_local_us",
        by=["exchange_id", "symbol_id"],
        strategy="backward",  # Trade belongs to most recent bar boundary
    )
    # Result: Each trade gets bar boundary timestamp from spine

    # Step 2: Group by bar boundary timestamp
    bars = trades_with_bars.group_by([
        "exchange_id", "symbol_id", "ts_local_us"  # ← ts_local_us from spine (bar end)
    ]).agg(agg_exprs)

    # Step 3: Join back to spine (preserve all boundaries)
    return spine.join(bars, on=["exchange_id", "symbol_id", "ts_local_us"], how="left")
```

**How as-of join works:**
```
Spine boundaries:     T0=0ms    T1=1000ms    T2=2000ms
                       │          │            │
Trades:          t1=50ms  t2=800ms  t3=1500ms  t4=1900ms
                  │        │         │          │
Assigned to:      T1       T1        T2         T2
                  ↑        ↑         ↑          ↑
                  backward join: latest boundary before/at trade
```

### PIT Correctness Guarantee

**Invariant:** When using bar at timestamp `T`, all ticks in that bar have `ts_local_us <= T`.

**Proof:**
1. Spine defines boundary timestamps: `T0, T1, T2, ...`
2. Bar at `Ti` contains ticks in window `[T_{i-1}, Ti)`
3. All ticks satisfy: `T_{i-1} <= ts_local_us < Ti`
4. Therefore: `ts_local_us < Ti` ✓ **No lookahead bias**

**Edge case:** Bar at `Ti` uses ticks with `ts < Ti` (strict inequality for all except boundary).

### Bar Window Semantics Summary

| Spine Type | Bar Timestamp | Window Start | Window End | Includes |
|------------|---------------|--------------|------------|----------|
| **Clock** | Interval end | Previous boundary | Current boundary | `[T_prev, T_current)` |
| **Volume** | Threshold crossing | Previous boundary | Current boundary | `[T_prev, T_current)` |
| **Dollar** | Threshold crossing | Previous boundary | Current boundary | `[T_prev, T_current)` |
| **Trades** | Every Nth trade | Previous event | Current event | `[T_prev, T_current)` |

**Universal Rule:** Bar timestamp = spine boundary timestamp = end of aggregation window.

### Feature Computation Implications

When computing features on bars:

```python
# Bar at timestamp T=2000ms contains:
# - OHLC from trades in [1000ms, 2000ms)
# - spread_avg from quotes in [1000ms, 2000ms)
# - All metrics represent data UP TO time 2000ms

# Feature computation at bar T=2000ms:
features = bars.with_columns([
    # This spread_bps uses data from [1000ms, 2000ms)
    (pl.col("spread_avg") / pl.col("vwap") * 10000).alias("spread_bps")
])

# PIT guarantee: spread_bps at T=2000ms only uses data where ts <= 2000ms
```

### Configuration (Not Needed!)

**No configuration parameter needed** because:
- Spine already defines boundaries → bars use those timestamps
- Alternative timestamps (first tick, window start) would break PIT correctness
- Consistent semantics across all spine types

**Anti-pattern (DO NOT ADD):**
```python
# ❌ BAD: Don't add this!
@dataclass
class BarAggregationConfig:
    bar_timestamp: Literal["spine_boundary", "first_tick", "last_tick"]
    # This would create confusion and potential PIT violations
```

**Correct approach:**
```python
# ✅ GOOD: Implicit - bar timestamp = spine boundary
bars = aggregator.aggregate(spine, trades, methods)
# bars.ts_local_us directly from spine (no ambiguity)
```

---

## Aggregation Patterns

### The Key Distinction

Two fundamentally different approaches to aggregation:

**Pattern A: Aggregate Raw Data First**
```
f(aggregate(x)) where f = feature function
```

**Pattern B: Compute Features First, Then Aggregate**
```
aggregate(f(x)) where f = tick-level feature
```

For **non-linear functions**, these produce **different results**!

### Pattern A: Aggregate Raw → Compute Features

**Flow:**
```
Raw Ticks → Aggregate Raw Values → Compute Features on Aggregates
```

**Example: Spread Feature**
```python
# Step 1: Aggregate raw spread values
quotes_bar = quotes.group_by("bar_id").agg([
    (pl.col("ask_px") - pl.col("bid_px")).mean().alias("spread_avg")
])

# Step 2: Convert to basis points
spread_bps = spread_avg / mid * 10000
```

**Use Cases:**
- OHLCV bars (open, high, low, close)
- VWAP (volume-weighted average price)
- Average metrics (spread_avg, volume)
- Count metrics (trade_count, quote_count)

**Pros:**
- ✅ Simple: One aggregation step
- ✅ Efficient: Aggregate once, compute once
- ✅ Standard: Industry-standard approach

**Cons:**
- ❌ No distribution info: Can't see volatility within bar
- ❌ Limited to simple aggregates

### Pattern B: Compute Features → Aggregate Features

**Flow:**
```
Raw Ticks → Compute Tick Features → Aggregate Tick Features → Bar Features
```

**Example: Spread Feature with Distribution**
```python
# Step 1: Compute spread_bps on EACH tick
quotes = quotes.with_columns([
    ((pl.col("ask_px") - pl.col("bid_px")) / pl.col("bid_px") * 10000)
    .alias("spread_bps")
])

# Step 2: Aggregate tick features
bars = quotes.group_by("bar_id").agg([
    pl.col("spread_bps").mean().alias("spread_bps_mean"),
    pl.col("spread_bps").std().alias("spread_bps_std"),
    pl.col("spread_bps").min().alias("spread_bps_min"),
    pl.col("spread_bps").max().alias("spread_bps_max"),
])
```

**Use Cases:**
- Feature volatility (spread_std, imbalance_std)
- Outlier metrics (spread_max, depth_min)
- Percentiles (p5, p50, p95)
- Distribution statistics (mean, std, skew)

**Pros:**
- ✅ Distribution statistics: mean, std, min, max, percentiles
- ✅ Volatility measures: Shows feature stability within bar
- ✅ Outlier detection: min/max/percentiles show extremes

**Cons:**
- ❌ More computation: Compute on every tick, then aggregate
- ❌ More complex: Two-stage pipeline

### When Results Differ

For **non-linear functions**, Pattern A ≠ Pattern B:

```python
# Example: Effective spread
f(x) = 2 * |trade_price - mid| / mid * 10000

# Pattern A: f(mean(x))
trades_bar = aggregate_trades(...)  # vwap = 50000
quotes_bar = aggregate_quotes(...)  # mid_avg = 49999.83
effective_spread = f(50000, 49999.83) = 0.07 bps

# Pattern B: mean(f(x))
tick_spreads = [f(trade_1), f(trade_2), f(trade_3)]
              = [0.2, 2.0, 2.0]
effective_spread_mean = mean([0.2, 2.0, 2.0]) = 1.4 bps
effective_spread_std = std([0.2, 2.0, 2.0]) = 1.0 bps

# Different results!
```

**Which is correct?**
- **Pattern A** answers: "What's the effective spread of the average trade?"
- **Pattern B** answers: "What's the average effective spread of individual trades?"

Both are valid—choose based on your research question.

### Aggregation Method Configuration

```python
@dataclass(frozen=True)
class AggregationMethod:
    """Defines how to aggregate a metric."""

    name: str
    columns: list[str]
    pattern: Literal["raw", "computed"]  # Which pattern to use

    # For pattern="raw": Aggregate raw values
    raw_agg: dict[str, pl.Expr] | None = None

    # For pattern="computed": Tick-level computation
    tick_compute: pl.Expr | None = None
    tick_agg: dict[str, pl.Expr] | None = None

# Example: Pattern A
SPREAD_AVG = AggregationMethod(
    name="spread_avg",
    columns=["spread_avg"],
    pattern="raw",
    raw_agg={"spread_avg": (pl.col("ask_px") - pl.col("bid_px")).mean()}
)

# Example: Pattern B
SPREAD_DISTRIBUTION = AggregationMethod(
    name="spread_distribution",
    columns=["spread_bps_mean", "spread_bps_std", "spread_bps_min", "spread_bps_max"],
    pattern="computed",
    tick_compute=((pl.col("ask_px") - pl.col("bid_px")) / pl.col("bid_px") * 10000).alias("spread_bps"),
    tick_agg={
        "spread_bps_mean": pl.col("spread_bps").mean(),
        "spread_bps_std": pl.col("spread_bps").std(),
        "spread_bps_min": pl.col("spread_bps").min(),
        "spread_bps_max": pl.col("spread_bps").max(),
    }
)
```

---

## Architecture Design

### High-Level Pipeline

```
┌──────────────────────────────────────────────────────────────┐
│ Phase 0: Spine Building (EXISTING)                          │
├──────────────────────────────────────────────────────────────┤
│ Input:  symbol_id, time_range, SpineBuilderConfig           │
│ Output: LazyFrame[ts_local_us, exchange_id, symbol_id]      │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         v
┌──────────────────────────────────────────────────────────────┐
│ Phase 1: Bar Aggregation (NEW)                              │
├──────────────────────────────────────────────────────────────┤
│ Per-Table Aggregators:                                       │
│ ┌─────────────────────┐  ┌─────────────────────┐           │
│ │ TradeBarAggregator  │  │ QuoteBarAggregator  │           │
│ │ - OHLC              │  │ - BBO snapshot      │           │
│ │ - VWAP              │  │ - Spread (Pattern A)│           │
│ │ - Volume            │  │ - Spread dist (B)   │           │
│ └─────────────────────┘  └─────────────────────┘           │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         v
┌──────────────────────────────────────────────────────────────┐
│ Phase 2: Feature Computation (MODIFIED)                     │
├──────────────────────────────────────────────────────────────┤
│ Feature families operate on bar data:                        │
│ - families.microstructure(bars)                              │
│ - families.trade_flow(bars)                                  │
└──────────────────────────────────────────────────────────────┘
```

### Per-Table Aggregators

**Why Separate Aggregators?**

Different tables have fundamentally different data structures:

| Table | Data Type | Natural Aggregations |
|-------|-----------|---------------------|
| **Trades** | Scalar (px, qty, side) | OHLC, VWAP, volume |
| **Quotes** | Scalar (bid_px, ask_px, sizes) | BBO snapshot, spread metrics |
| **Book** | Arrays (25-level depth) | Depth snapshot, imbalance |
| **Deriv** | Periodic snapshots | Funding avg, OI bars |

Can't use same logic for scalar prices vs 25-level arrays!

**Architecture: One Aggregator Per Table**

```python
class TradeBarAggregator:
    """Aggregates trades to bar-level OHLCV."""

    def aggregate(self, spine, trades, methods):
        # Decode fixed-point
        trades = self._decode_fixed_point(trades)

        # Assign to bars via as-of join
        trades_with_bars = trades.join_asof(spine, on="ts_local_us", ...)

        # Compute aggregations
        agg_exprs = []
        if "ohlc" in methods:
            agg_exprs.extend([
                pl.col("px").first().alias("open"),
                pl.col("px").max().alias("high"),
                pl.col("px").min().alias("low"),
                pl.col("px").last().alias("close"),
            ])

        if "vwap" in methods:
            agg_exprs.append(
                ((pl.col("px") * pl.col("qty").abs()).sum() / pl.col("qty").abs().sum())
                .alias("vwap")
            )

        # Group and aggregate
        bars = trades_with_bars.group_by([
            "exchange_id", "symbol_id", "ts_local_us"
        ]).agg(agg_exprs)

        # Join back to spine (preserve all spine rows)
        return spine.join(bars, on=["exchange_id", "symbol_id", "ts_local_us"], how="left")
```

**Key Design Points:**
1. **As-of Join**: Each tick assigned to most recent bar boundary
2. **Left Join Preservation**: All spine rows kept (even if no data in bar)
3. **Single Groupby**: Efficient—one groupby for all methods
4. **Pattern Support**: Can mix Pattern A and B methods

### Cross-Table Alignment

Bar aggregation ensures perfect alignment across tables:

```python
# All aggregators use SAME spine → guaranteed alignment
def aggregate_bars(spine, tables, config):
    result = {}

    for table_name, table_lf in tables.items():
        aggregator = get_aggregator(table_name)
        result[table_name] = aggregator.aggregate(
            spine,  # ← SAME spine for all tables!
            table_lf,
            config.get_methods(table_name),
        )

    # All results have same (ts_local_us, exchange_id, symbol_id) keys
    # No pit_align needed - already aligned!
    return result
```

**Benefit:** All metrics computed over exact same time window.

---

## API Reference

### Configuration

```python
@dataclass(frozen=True)
class BarAggregationConfig:
    """Configuration for bar-level aggregation."""

    # Trade aggregations (Pattern A + B)
    trade_methods: list[str] | None = field(default_factory=lambda: [
        "ohlc", "vwap", "volume", "trade_count"
    ])

    # Quote aggregations (Pattern A + B)
    quote_methods: list[str] | None = field(default_factory=lambda: [
        "bid_ask_snapshot",        # Pattern A: Last BBO
        "spread_avg",              # Pattern A: Average spread
        "spread_distribution",     # Pattern B: Spread mean/std/min/max
    ])

    # Book aggregations
    book_methods: list[str] | None = field(default_factory=lambda: [
        "depth_snapshot", "imbalance_avg"
    ])

    # Derivative aggregations
    deriv_methods: list[str] | None = field(default_factory=lambda: [
        "funding_avg", "oi_snapshot"
    ])
```

### Usage: Volume Bars with OHLCV

```python
from pointline.research.features import (
    build_feature_frame,
    FeatureRunConfig,
    EventSpineConfig,
    VolumeBarConfig,
    BarAggregationConfig,
)

# Configure volume bars with aggregation
config = FeatureRunConfig(
    spine=EventSpineConfig(
        builder_config=VolumeBarConfig(volume_threshold=1000.0)
    ),
    aggregation=BarAggregationConfig(
        trade_methods=["ohlc", "vwap", "volume"],
        quote_methods=["bid_ask_snapshot", "spread_distribution"],
    ),
    include_microstructure=True,
)

# Build features
lf = build_feature_frame(
    symbol_id=12345,
    start_ts_us="2024-05-01",
    end_ts_us="2024-05-02",
    config=config,
)

df = lf.collect()
# Columns: ts_local_us, open, high, low, close, vwap, volume,
#          bid_px, ask_px, spread_bps_mean, spread_bps_std, ...
```

### Usage: Mixed Granularity

```python
# Bar-level trades, tick-level quotes
config = FeatureRunConfig(
    spine=EventSpineConfig(
        builder_config=VolumeBarConfig(volume_threshold=1000.0)
    ),
    aggregation=BarAggregationConfig(
        trade_methods=["ohlc", "vwap"],  # Aggregate trades
        quote_methods=None,               # Keep quotes as ticks
    ),
    include_microstructure=True,
)
```

### Usage: Pattern A vs Pattern B

```python
# Pattern A only: Simple aggregates
config = BarAggregationConfig(
    quote_methods=["bid_ask_snapshot", "spread_avg"],  # No distribution
)

# Pattern B added: Distribution statistics
config = BarAggregationConfig(
    quote_methods=[
        "bid_ask_snapshot",        # Pattern A: Last BBO
        "spread_avg",              # Pattern A: Average spread
        "spread_distribution",     # Pattern B: mean/std/min/max
        "microprice_distribution", # Pattern B: mean/std
    ],
)

# Result includes both:
# - spread_avg (Pattern A - single value)
# - spread_bps_mean, spread_bps_std, spread_bps_min, spread_bps_max (Pattern B)
```

---

## Feature Family Adaptation

### The Schema Problem

**Current feature families expect tick-level columns:**

```python
# pointline/research/features/families/microstructure.py (CURRENT)

def microstructure(aligned: pl.LazyFrame) -> pl.LazyFrame:
    """Compute microstructure features on tick data."""

    # Expects tick-level columns:
    # - bid_px, ask_px (from latest quote tick)
    # - bids_sz[], asks_sz[] (from latest book tick)

    return aligned.with_columns([
        # Spread from quote tick
        ((pl.col("ask_px") - pl.col("bid_px")) / pl.col("bid_px") * 10000)
        .alias("spread_bps"),

        # Depth imbalance from book tick
        (
            (pl.col("bids_sz").list.sum() - pl.col("asks_sz").list.sum())
            / (pl.col("bids_sz").list.sum() + pl.col("asks_sz").list.sum())
        ).alias("depth_imbalance"),
    ])
```

**Bar data has different columns:**

```python
# After bar aggregation:
# - quote_bars: spread_avg (Pattern A), spread_bps_mean (Pattern B), bid_px, ask_px
# - trade_bars: open, high, low, close, vwap, volume
# - book_bars: imbalance_avg, bids_px[], asks_px[]

# Problem: microstructure() expects bid_px/ask_px but bar has spread_avg!
```

### Solution: Polymorphic Feature Families

Feature families need to **detect and adapt** to available columns.

#### Approach 1: Column Detection (Recommended)

```python
def microstructure(data: pl.LazyFrame) -> pl.LazyFrame:
    """Compute microstructure features on tick OR bar data.

    Adapts to available columns:
    - If spread_avg exists → use it (bar mode, Pattern A)
    - If spread_bps_mean exists → use it (bar mode, Pattern B)
    - Else compute from bid_px/ask_px (tick mode)
    """

    features = []

    # Feature 1: Spread
    if "spread_bps_mean" in data.columns:
        # Bar mode (Pattern B): Use pre-computed distribution
        features.extend([
            pl.col("spread_bps_mean").alias("spread_bps"),
            pl.col("spread_bps_std").alias("spread_volatility"),  # NEW!
        ])
    elif "spread_avg" in data.columns:
        # Bar mode (Pattern A): Convert to bps
        features.append(
            (pl.col("spread_avg") / pl.col("vwap") * 10000).alias("spread_bps")
        )
    else:
        # Tick mode: Compute from raw bid/ask
        features.append(
            ((pl.col("ask_px") - pl.col("bid_px")) / pl.col("bid_px") * 10000)
            .alias("spread_bps")
        )

    # Feature 2: Depth Imbalance
    if "imbalance_avg" in data.columns:
        # Bar mode: Use pre-aggregated
        features.append(pl.col("imbalance_avg").alias("depth_imbalance"))
    else:
        # Tick mode: Compute from book arrays
        features.append(
            (
                (pl.col("bids_sz").list.sum() - pl.col("asks_sz").list.sum())
                / (pl.col("bids_sz").list.sum() + pl.col("asks_sz").list.sum())
            ).alias("depth_imbalance")
        )

    # Feature 3: Effective Spread (only in bar mode with VWAP)
    if "vwap" in data.columns and "bid_px" in data.columns:
        # Bar mode: VWAP vs BBO
        features.append(
            (2 * (pl.col("vwap") - pl.col("bid_px")) / pl.col("bid_px") * 10000)
            .alias("effective_spread_bps")
        )

    return data.with_columns(features)
```

**Key Pattern:** Feature families become **schema-polymorphic**.

#### Column Mapping: Tick vs Bar

| Feature | Tick-Level Column | Bar-Level Column (Pattern A) | Bar-Level Column (Pattern B) |
|---------|-------------------|------------------------------|------------------------------|
| **Spread** | `ask_px - bid_px` | `spread_avg` | `spread_bps_mean`, `spread_bps_std` |
| **Depth Imbalance** | `bids_sz.sum() - asks_sz.sum()` | `imbalance_avg` | `imbalance_mean`, `imbalance_std` |
| **Price** | `px` | `close` or `vwap` | `vwap` (typically) |
| **Volume** | `qty` | `volume` | `volume` |
| **Microprice** | Compute from `bid_px`, `ask_px`, sizes | `microprice_avg` | `microprice_mean`, `microprice_std` |

#### Example: Trade Flow Features

```python
def trade_flow(data: pl.LazyFrame) -> pl.LazyFrame:
    """Trade flow features on tick or bar data."""

    features = []

    # Feature 1: Volume Imbalance
    if "buy_volume" in data.columns and "sell_volume" in data.columns:
        # Bar mode: Use pre-aggregated buy/sell volumes
        features.append(
            (
                (pl.col("buy_volume") - pl.col("sell_volume"))
                / (pl.col("buy_volume") + pl.col("sell_volume"))
            ).alias("volume_imbalance")
        )
    else:
        # Tick mode: Compute from side
        features.append(
            (
                pl.when(pl.col("side") == 0).then(pl.col("qty"))
                .otherwise(-pl.col("qty"))
            ).alias("signed_volume")
        )

    # Feature 2: VWAP Deviation (bar mode only)
    if "vwap" in data.columns and "close" in data.columns:
        features.append(
            ((pl.col("close") - pl.col("vwap")) / pl.col("vwap") * 10000)
            .alias("vwap_deviation_bps")
        )

    # Feature 3: Trade Count (bar mode only)
    if "trade_count" in data.columns:
        features.append(pl.col("trade_count").alias("bar_activity"))

    return data.with_columns(features)
```

### Bar-Only Features (NEW)

Some features **only make sense** on bar data:

| Feature | Requires | Description |
|---------|----------|-------------|
| **VWAP Deviation** | `vwap`, `close` | Close vs VWAP spread |
| **Price Range** | `high`, `low` | High-low spread in bar |
| **Spread Volatility** | `spread_bps_std` | Spread stability (Pattern B) |
| **Volume Imbalance** | `buy_volume`, `sell_volume` | Buy vs sell pressure |
| **Trade Activity** | `trade_count` | Bar intensity metric |

**Implementation:**

```python
def bar_dynamics(bars: pl.LazyFrame) -> pl.LazyFrame:
    """Features that only work on bar data (not ticks)."""

    # Require bar columns
    required = ["open", "high", "low", "close", "vwap", "volume"]
    if not all(col in bars.columns for col in required):
        raise ValueError(f"bar_dynamics requires bar data with {required}")

    return bars.with_columns([
        # Price range
        ((pl.col("high") - pl.col("low")) / pl.col("close") * 10000)
        .alias("price_range_bps"),

        # VWAP deviation
        ((pl.col("close") - pl.col("vwap")) / pl.col("vwap") * 10000)
        .alias("vwap_deviation_bps"),

        # Intrabar volatility proxy
        ((pl.col("high") - pl.col("low")) / ((pl.col("high") + pl.col("low")) / 2))
        .alias("intrabar_volatility"),

        # Body ratio (close-open vs high-low)
        (
            (pl.col("close") - pl.col("open")).abs()
            / (pl.col("high") - pl.col("low"))
        ).alias("body_ratio"),
    ])
```

### Feature Family Registry Updates

**Modify `FeatureRunConfig` to include bar-only families:**

```python
@dataclass(frozen=True)
class FeatureRunConfig:
    # Existing (work with tick or bar)
    include_microstructure: bool = True      # Polymorphic
    include_trade_flow: bool = True          # Polymorphic
    include_book_shape: bool = True          # Polymorphic

    # NEW: Bar-only features
    include_bar_dynamics: bool = False       # Only works with bars
    include_vpin: bool = False               # Only works with volume bars
```

### Migration Strategy

**Phase 1: Add Polymorphism**
- Modify existing families to detect columns
- Prefer pre-aggregated metrics when available
- Fall back to tick computation

**Phase 2: Add Bar-Only Features**
- Create new families for bar-specific metrics
- VPIN, bar dynamics, OHLC-based features

**Phase 3: Optimize**
- Remove redundant computation (don't recompute what bars provide)
- Add warnings when bar data available but not used

### Example: Complete Microstructure Family

```python
def microstructure(data: pl.LazyFrame) -> pl.LazyFrame:
    """Microstructure features (polymorphic: tick or bar data)."""

    features = []

    # 1. Spread
    if "spread_bps_mean" in data.columns:
        # Pattern B: Distribution available
        features.extend([
            pl.col("spread_bps_mean").alias("spread_bps"),
            pl.col("spread_bps_std").alias("spread_volatility"),
            pl.col("spread_bps_min").alias("spread_min"),
            pl.col("spread_bps_max").alias("spread_max"),
        ])
    elif "spread_avg" in data.columns:
        # Pattern A: Average available
        features.append(
            (pl.col("spread_avg") / pl.col("vwap") * 10000).alias("spread_bps")
        )
    else:
        # Tick mode: Compute from raw
        features.append(
            ((pl.col("ask_px") - pl.col("bid_px")) / pl.col("bid_px") * 10000)
            .alias("spread_bps")
        )

    # 2. Depth Imbalance
    if "imbalance_avg" in data.columns:
        features.append(pl.col("imbalance_avg").alias("depth_imbalance"))
    else:
        features.append(
            ((pl.col("bids_sz").list.sum() - pl.col("asks_sz").list.sum())
             / (pl.col("bids_sz").list.sum() + pl.col("asks_sz").list.sum()))
            .alias("depth_imbalance")
        )

    # 3. Effective Spread (requires both trade and quote data)
    if "vwap" in data.columns:
        # Bar mode: Use VWAP
        if "microprice_avg" in data.columns:
            # Pattern A: Pre-computed microprice
            features.append(
                (2 * (pl.col("vwap") - pl.col("microprice_avg")) / pl.col("microprice_avg") * 10000)
                .alias("effective_spread_bps")
            )
        elif "bid_px" in data.columns and "ask_px" in data.columns:
            # Bar mode: Compute microprice from BBO snapshot
            mid = (pl.col("bid_px") + pl.col("ask_px")) / 2
            features.append(
                (2 * (pl.col("vwap") - mid) / mid * 10000).alias("effective_spread_bps")
            )
    elif "px" in data.columns:
        # Tick mode: Use trade price
        mid = (pl.col("bid_px") + pl.col("ask_px")) / 2
        features.append(
            (2 * (pl.col("px") - mid) / mid * 10000).alias("effective_spread_bps")
        )

    return data.with_columns(features)
```

### Testing Feature Families

**Test both tick and bar modes:**

```python
def test_microstructure_tick_mode():
    """Verify microstructure works on tick data."""
    aligned = build_test_tick_data()  # Has bid_px, ask_px, etc.

    features = families.microstructure(aligned).collect()

    assert "spread_bps" in features.columns
    assert "depth_imbalance" in features.columns
    # Should NOT have bar-only columns
    assert "spread_volatility" not in features.columns

def test_microstructure_bar_mode_pattern_a():
    """Verify microstructure works on Pattern A bar data."""
    bars = build_test_bar_data_pattern_a()  # Has spread_avg, vwap, etc.

    features = families.microstructure(bars).collect()

    assert "spread_bps" in features.columns  # Computed from spread_avg
    assert "depth_imbalance" in features.columns
    # Should NOT have Pattern B columns
    assert "spread_volatility" not in features.columns

def test_microstructure_bar_mode_pattern_b():
    """Verify microstructure works on Pattern B bar data."""
    bars = build_test_bar_data_pattern_b()  # Has spread_bps_mean, spread_bps_std

    features = families.microstructure(bars).collect()

    assert "spread_bps" in features.columns  # From spread_bps_mean
    assert "spread_volatility" in features.columns  # From spread_bps_std (NEW!)
    assert "spread_min" in features.columns  # Pattern B benefit
    assert "spread_max" in features.columns  # Pattern B benefit
```

### Summary: Feature Family Changes

**Required Changes:**

1. **Column Detection:** Check for bar columns before tick columns
2. **Prefer Pre-Aggregated:** Use Pattern A/B outputs when available
3. **Add Bar-Only Features:** New features that require OHLC/VWAP
4. **Backward Compatible:** Tick mode continues to work

**Benefits:**

- ✅ Same API: `families.microstructure(data)` works for both
- ✅ Richer Features: Pattern B gives distribution stats
- ✅ Less Computation: Reuse bar aggregations
- ✅ No Breaking Changes: Existing code works

**Example Workflow:**

```python
# User code (unchanged!)
config = FeatureRunConfig(
    spine=EventSpineConfig(builder_config=VolumeBarConfig(volume_threshold=1000.0)),
    aggregation=BarAggregationConfig(
        quote_methods=["spread_distribution"],  # Pattern B
    ),
    include_microstructure=True,  # Same as before!
)

lf = build_feature_frame(symbol_id=12345, config=config)
df = lf.collect()

# Result: microstructure() detected bar data and used spread_bps_mean!
# Bonus: Got spread_volatility for free (Pattern B benefit)
```

---

## Implementation

### Phase 1: Core Infrastructure

**Create:**
- `pointline/research/features/aggregations/base.py` - Protocols
- `pointline/research/features/aggregations/trades.py` - Trade aggregator
- `pointline/research/features/aggregations/quotes.py` - Quote aggregator
- `pointline/research/features/aggregations/book.py` - Book aggregator

**TradeBarAggregator Implementation:**

```python
class TradeBarAggregator:
    @property
    def supported_methods(self) -> dict[str, AggregationMethod]:
        return {
            "ohlc": AggregationMethod(
                name="ohlc",
                columns=["open", "high", "low", "close"],
                pattern="raw",
                raw_agg={
                    "open": pl.col("px").first(),
                    "high": pl.col("px").max(),
                    "low": pl.col("px").min(),
                    "close": pl.col("px").last(),
                }
            ),
            "vwap": AggregationMethod(...),
            "volume": AggregationMethod(...),
        }

    def aggregate(self, spine, trades, methods):
        # 1. Decode fixed-point
        # 2. Assign to bars
        # 3. Separate Pattern A vs Pattern B methods
        # 4. Compute tick features (Pattern B)
        # 5. Aggregate (both patterns in one groupby)
        # 6. Join back to spine
```

### Phase 2: Integration

**Modify:**
- `pointline/research/features/runner.py` - Add aggregation step

```python
def build_feature_frame(..., config):
    # 1. Build spine
    spine = build_event_spine(...)

    # 2. Load raw tables
    tables = _load_tables(...)

    # 3. NEW: Aggregate to bars if requested
    if config.aggregation is not None:
        tables = aggregate_bars(spine, tables, config.aggregation)

    # 4. Align (or skip if already aligned via aggregation)
    aligned = tables if config.aggregation else pit_align(spine, tables)

    # 5. Compute features
    features = families.microstructure(aligned)

    return features
```

### Phase 3: Testing

```python
def test_trade_bar_aggregator_ohlc():
    """Verify OHLC computation."""
    spine = build_test_spine()
    trades = build_test_trades()

    aggregator = TradeBarAggregator()
    bars = aggregator.aggregate(spine, trades, methods=["ohlc"])

    df = bars.collect()
    assert df["open"][0] == 50000  # First trade price
    assert df["high"][0] == 50010  # Max price
    assert df["low"][0] == 49990   # Min price
    assert df["close"][0] == 50005 # Last trade price

def test_pattern_a_vs_pattern_b():
    """Verify Pattern A and B produce different results."""
    spine = build_test_spine()
    quotes = build_test_quotes()

    aggregator = QuoteBarAggregator()

    # Pattern A
    bars_a = aggregator.aggregate(spine, quotes, methods=["spread_avg"])
    spread_a = bars_a.collect()["spread_avg"][0]

    # Pattern B
    bars_b = aggregator.aggregate(spine, quotes, methods=["spread_distribution"])
    spread_b_mean = bars_b.collect()["spread_bps_mean"][0]

    # For non-linear features, should differ
    assert spread_a != spread_b_mean
```

---

## Examples

### Example 1: Volume Bar OHLCV with Pattern B

```python
config = FeatureRunConfig(
    spine=EventSpineConfig(
        builder_config=VolumeBarConfig(volume_threshold=1000.0)
    ),
    aggregation=BarAggregationConfig(
        trade_methods=["ohlc", "vwap", "volume"],
        quote_methods=[
            "bid_ask_snapshot",        # Pattern A
            "spread_distribution",     # Pattern B: mean, std, min, max
        ],
    ),
    include_microstructure=True,
)

lf = build_feature_frame(symbol_id=12345, ..., config=config)
df = lf.collect()

# Available columns:
# - Bar data: open, high, low, close, vwap, volume
# - Pattern A: bid_px, ask_px (last BBO)
# - Pattern B: spread_bps_mean, spread_bps_std, spread_bps_min, spread_bps_max
# - Features: depth_imbalance, microprice, effective_spread, ...

print(df.select([
    "ts_local_us", "open", "close", "volume",
    "spread_bps_mean", "spread_bps_std"  # Pattern B distribution
]))
```

### Example 2: VPIN (Natural Fit with Volume Bars)

```python
# VPIN requires volume buckets - volume bars are perfect!
config = FeatureRunConfig(
    spine=EventSpineConfig(
        builder_config=VolumeBarConfig(volume_threshold=1000.0)
    ),
    aggregation=BarAggregationConfig(
        trade_methods=["volume", "buy_sell_volume"],
    ),
)

lf = build_feature_frame(symbol_id=12345, ..., config=config)

# Compute VPIN
vpin = lf.with_columns([
    # Signed volume imbalance
    (pl.col("buy_volume") - pl.col("sell_volume")).alias("signed_volume"),

    # VPIN: rolling average of |signed_volume| / total_volume
    (
        pl.col("signed_volume").abs().rolling_sum(window_size=50)
        / pl.col("volume").rolling_sum(window_size=50)
    ).alias("vpin"),
])

df = vpin.collect()
print(f"VPIN mean: {df['vpin'].mean():.4f}")
```

### Example 3: Custom Pattern B Aggregation

```python
from pointline.research.features.aggregations import custom_aggregation

@custom_aggregation(name="spread_volatility", pattern="computed")
class SpreadVolatilityAgg:
    """Compute spread volatility (CV) on tick-level spreads."""

    @staticmethod
    def tick_compute(df: pl.LazyFrame) -> pl.LazyFrame:
        """Compute spread_bps on each tick."""
        return df.with_columns([
            ((pl.col("ask_px") - pl.col("bid_px")) / pl.col("bid_px") * 10000)
            .alias("spread_bps")
        ])

    @staticmethod
    def tick_aggregate(df: pl.LazyFrame) -> dict[str, pl.Expr]:
        """Aggregate tick features."""
        return {
            "spread_mean": pl.col("spread_bps").mean(),
            "spread_std": pl.col("spread_bps").std(),
            "spread_cv": pl.col("spread_bps").std() / pl.col("spread_bps").mean(),
        }

# Usage
config = BarAggregationConfig(
    quote_methods=["spread_volatility"]  # Custom Pattern B method
)
```

---

## Aggregation Method Catalog

### Pattern A Methods (Aggregate Raw)

**Trades:**
- `ohlc` → open, high, low, close
- `vwap` → vwap
- `volume` → volume
- `buy_sell_volume` → buy_volume, sell_volume
- `trade_count` → trade_count

**Quotes:**
- `bid_ask_snapshot` → bid_px, ask_px, bid_sz, ask_sz (last BBO)
- `spread_avg` → spread_avg
- `quote_count` → quote_count

**Book:**
- `depth_snapshot` → bids_px[], asks_px[], bids_sz[], asks_sz[] (last full book)
- `imbalance_avg` → imbalance_avg

### Pattern B Methods (Aggregate Computed)

**Quotes:**
- `spread_distribution` → spread_bps_mean, spread_bps_std, spread_bps_min, spread_bps_max
- `microprice_distribution` → microprice_mean, microprice_std
- `effective_spread_distribution` → eff_spread_mean, eff_spread_std, eff_spread_p95

**Book:**
- `imbalance_distribution` → imbalance_mean, imbalance_std, imbalance_skew
- `depth_distribution` → total_depth_mean, total_depth_std

---

## When to Use Each Approach

### Use Tick-Level Features When:
1. High-frequency sampling (spine interval < 100ms)
2. Tick-precision required (HFT, market making)
3. Event-driven features (trade burst detection)
4. Exploratory analysis (quick prototyping)

### Use Bar-Level Features (Pattern A) When:
1. Coarse sampling (spine interval > 1 second)
2. Standard metrics (OHLCV, VWAP, volume)
3. Production research (backtests, signal generation)
4. Performance critical (minimize computation)

### Use Bar-Level Features (Pattern B) When:
1. Need distribution statistics (mean, std, percentiles)
2. Volatility matters (spread stability, feature variance)
3. Outlier detection (max effective spread, min liquidity)
4. Feature engineering for ML (richer feature set)

### Use Both Patterns When:
Most production use cases! Combine for comprehensive view:

```python
config = BarAggregationConfig(
    quote_methods=[
        "bid_ask_snapshot",        # Pattern A: Last BBO
        "spread_avg",              # Pattern A: Average spread
        "spread_distribution",     # Pattern B: Distribution stats
        "microprice_distribution", # Pattern B: Distribution stats
    ]
)
```

---

## Summary

### Key Design Decisions

1. **Two-Phase Pipeline**: Aggregation → Features (separation of concerns)
2. **Per-Table Aggregators**: One aggregator per table type (maintainable, extensible)
3. **Pattern A vs Pattern B**: Support both aggregation approaches (different use cases)
4. **Perfect Alignment**: All tables aggregated over same windows (temporal consistency)
5. **Backward Compatible**: Existing tick-level code continues to work

### Performance Impact

- **Upfront Cost**: Bar aggregation adds groupby overhead
- **Net Benefit**: Fewer rows for features → 8x faster overall
- **Memory**: Reduced (597 bars vs 18,894 ticks)

### Migration Path

**Stage 1: Opt-In (v1.0.0)**
- Bar aggregation optional (`aggregation=None` by default)
- Existing code unchanged

**Stage 2: Recommended (v1.1.0)**
- Documentation emphasizes bar aggregation
- Warnings for inefficient patterns

**Stage 3: Default (v2.0.0)**
- Bar aggregation default for bar-based spines
- Tick-level requires explicit opt-out

---

## References

1. **López de Prado, M.** (2018). *Advances in Financial Machine Learning*, Ch. 2: Financial Data Structures
2. **Easley, D., López de Prado, M., O'Hara, M.** (2012). "Flow Toxicity and Liquidity in a High-frequency World"

For implementation questions, see:
- [Resampling Methods Guide](../guides/resampling-methods.md)
- Code: `pointline/research/features/aggregations/`
