# Resampling Methods Guide

Pointline provides extensible resampling methods (spine builders) for feature engineering. Different resampling strategies can significantly impact research quality by addressing non-stationarity, heteroskedasticity, and microstructure noise.

## Overview

**What is a Spine?** A spine is a sequence of timestamps at which features are computed. The choice of resampling method determines when we sample the market state.

**Available Methods:**

| Method | Description | Use Case |
|--------|-------------|----------|
| **Clock** | Fixed time intervals | General-purpose, regulatory reporting |
| **Trades** | Every trade event | Tick-by-tick analysis, event studies |
| **Volume** | Every N contracts | Activity-normalized sampling |
| **Dollar** | Every $N notional | Economic-significance normalized |

**Coming Soon:** Tick bars, imbalance bars, quote-event bars, time-weighted bars

## Quick Start

```python
from pointline.research.features import (
    build_feature_frame,
    EventSpineConfig,
    FeatureRunConfig,
    VolumeBarConfig,
)

# Volume bars: Sample every 1000 contracts
config = FeatureRunConfig(
    spine=EventSpineConfig(
        builder_config=VolumeBarConfig(volume_threshold=1000.0)
    ),
    include_microstructure=True,
    include_order_flow=True,
)

lf = build_feature_frame(
    symbol_id=12345,
    start_ts_us="2024-05-01",
    end_ts_us="2024-05-02",
    config=config,
)

df = lf.collect()
```

## Clock Spine (Fixed Time Intervals)

**Description:** Sample at regular time intervals (e.g., every 1 second).

**Pros:**
- Simple and interpretable
- Regulatory compliance (e.g., SEC requires 1-second snapshots)
- Easy to compare across symbols/exchanges

**Cons:**
- Non-stationary during inactive periods (low signal-to-noise)
- Over-samples during low activity, under-samples during high activity
- Clock time ≠ information time

**Usage:**

```python
from pointline.research.features import EventSpineConfig, ClockSpineConfig

config = EventSpineConfig(builder_config=ClockSpineConfig(step_ms=1000))
```

**Parameters:**
- `step_ms` (int): Time step in milliseconds (default: 1000 = 1 second)
- `max_rows` (int): Safety limit for maximum rows (default: 5M)

**Example:**

```python
# Sample BTCUSDT every 5 seconds
from pointline.research.features import build_event_spine, EventSpineConfig, ClockSpineConfig

spine = build_event_spine(
    symbol_id=12345,
    start_ts_us="2024-05-01T00:00:00Z",
    end_ts_us="2024-05-01T01:00:00Z",
    config=EventSpineConfig(builder_config=ClockSpineConfig(step_ms=5000)),
)

df = spine.collect()
# Result: ~720 rows (3600 seconds / 5 = 720 samples)
```

## Trades Spine (Event-Driven)

**Description:** Sample at every trade event.

**Pros:**
- Captures every market microstructure event
- No information loss
- Ideal for tick-by-tick analysis

**Cons:**
- Very high frequency (millions of rows per day for liquid symbols)
- Computationally expensive
- May include noise trades

**Usage:**

```python
from pointline.research.features import EventSpineConfig, TradesSpineConfig

config = EventSpineConfig(builder_config=TradesSpineConfig())
```

**Parameters:**
- `max_rows` (int): Safety limit (default: 5M)

**Example:**

```python
# Every trade for ETHUSDT
from pointline.research.features import build_event_spine, EventSpineConfig, TradesSpineConfig

spine = build_event_spine(
    symbol_id=67890,
    start_ts_us="2024-05-01T00:00:00Z",
    end_ts_us="2024-05-01T00:05:00Z",
    config=EventSpineConfig(builder_config=TradesSpineConfig()),
)

df = spine.collect()
# Result: 1 row per trade (could be thousands for liquid symbols)
```

## Volume Bars

**Description:** Sample every N contracts/shares.

**Theory:** Volume bars normalize sampling by trading activity, leading to more stationary price dynamics. Proposed by Easley, López de Prado, and O'Hara (2012) for analyzing flow toxicity.

**Pros:**
- More stationary returns than clock sampling
- Activity-normalized (handles inactive vs. active periods)
- Better for volatility modeling
- Reduces autocorrelation

**Cons:**
- Less intuitive than clock time
- Threshold selection is parameter-dependent
- May over-sample low-liquidity periods

**Usage:**

```python
from pointline.research.features import EventSpineConfig, VolumeBarConfig

config = EventSpineConfig(
    builder_config=VolumeBarConfig(
        volume_threshold=1000.0,       # Sample every 1000 contracts
        use_absolute_volume=True,      # Ignore side (buy + sell)
    )
)
```

**Parameters:**
- `volume_threshold` (float): Sample every N contracts (default: 1000)
- `use_absolute_volume` (bool): If True, ignore side (buy+sell); if False, use signed volume (default: True)
- `max_rows` (int): Safety limit (default: 5M)

**Example:**

```python
# Volume bars: 1000 contracts
from pointline.research.features import build_event_spine, EventSpineConfig, VolumeBarConfig

spine = build_event_spine(
    symbol_id=12345,
    start_ts_us="2024-05-01",
    end_ts_us="2024-05-02",
    config=EventSpineConfig(builder_config=VolumeBarConfig(volume_threshold=1000.0)),
)

df = spine.collect()
# Result: Variable number of rows depending on trading activity
```

**Selecting volume_threshold:**

```python
# Analyze average trade size
from pointline.research import query

trades = query.trades(
    "binance-futures",
    "BTCUSDT",
    "2024-05-01",
    "2024-05-02",
    decoded=True,
)

avg_trade_size = trades["qty"].mean()
# Set threshold to ~100-1000× average trade size
volume_threshold = avg_trade_size * 500
```

## Dollar Bars

**Description:** Sample every $N notional value.

**Theory:** Dollar bars normalize by economic significance rather than physical volume. This accounts for price variations and provides more consistent bars across different price levels.

**Pros:**
- Economic significance normalized
- Handles price level changes (e.g., BTC at $30k vs $60k)
- More stationary than volume bars for assets with price drift
- Better for cross-asset comparison

**Cons:**
- Requires price data (not just volume)
- More computationally expensive
- Threshold selection depends on asset price level

**Usage:**

```python
from pointline.research.features import EventSpineConfig, DollarBarConfig

config = EventSpineConfig(
    builder_config=DollarBarConfig(dollar_threshold=100_000.0)  # Sample every $100k notional
)
```

**Parameters:**
- `dollar_threshold` (float): Sample every $N notional (default: 100,000)
- `max_rows` (int): Safety limit (default: 5M)

**Example:**

```python
# Dollar bars: $100k notional
from pointline.research.features import build_event_spine, EventSpineConfig, DollarBarConfig

spine = build_event_spine(
    symbol_id=12345,
    start_ts_us="2024-05-01",
    end_ts_us="2024-05-02",
    config=EventSpineConfig(builder_config=DollarBarConfig(dollar_threshold=100_000.0)),
)

df = spine.collect()
# Result: Variable number of rows depending on trading volume × price
```

**Selecting dollar_threshold:**

```python
# Analyze average notional per bar
trades = query.trades(
    "binance-futures",
    "BTCUSDT",
    "2024-05-01",
    "2024-05-02",
    decoded=True,
)

avg_notional = (trades["px"] * trades["qty"]).abs().mean()
# Set threshold to ~100-1000× average notional
dollar_threshold = avg_notional * 500
```

## PIT Correctness Guarantees

All spine builders preserve point-in-time correctness:

1. **Deterministic Ordering:** Every spine is sorted by `(exchange_id, symbol_id, ts_local_us)`
2. **No Lookahead:** Spine points use `ts_local_us` (arrival time), not `ts_exch_us`
3. **Reproducibility:** Same inputs → same spine (no randomness, stable sorting)
4. **As-of Joins:** `pit_align()` uses backward-looking joins (no future information)

**Verification:**

```python
# Build spine twice
spine1 = build_event_spine(..., config=config)
spine2 = build_event_spine(..., config=config)

df1 = spine1.collect()
df2 = spine2.collect()

# Should be identical
assert df1.equals(df2)
```

## Performance Considerations

**Lazy Evaluation:** All spine builders return `pl.LazyFrame` for efficient query optimization.

**Memory Safety:** All builders enforce `max_rows` limits to prevent accidental full scans.

```python
# This will fail fast if too many rows
config = EventSpineConfig(
    mode="clock",
    builder_config=ClockSpineConfig(step_ms=1, max_rows=1000),
)

# Raises RuntimeError: "Clock spine would generate too many rows: 60000 > 1000"
spine = build_event_spine(
    symbol_id=12345,
    start_ts_us="2024-05-01T00:00:00Z",
    end_ts_us="2024-05-01T00:01:00Z",
    config=config,
)
```

**Optimization Tips:**

1. **Use `collect()` only when needed:** Keep LazyFrame as long as possible
2. **Filter early:** Use `start_ts_us` and `end_ts_us` to reduce data scanned
3. **Batch processing:** For long time ranges, split into daily chunks
4. **Partition pruning:** Delta Lake automatically prunes by `(exchange, date, symbol_id)`

## Integration with Feature Families

All spine types work seamlessly with existing feature families:

```python
from pointline.research.features import (
    build_feature_frame,
    EventSpineConfig,
    FeatureRunConfig,
    VolumeBarConfig,
)

# Volume bars + microstructure features
config = FeatureRunConfig(
    spine=EventSpineConfig(builder_config=VolumeBarConfig(volume_threshold=1000.0)),
    include_microstructure=True,     # Spread, depth, imbalance
    include_order_flow=True,         # VPIN, trade toxicity
    include_volatility=True,         # Realized volatility
    include_cross_venue_basis=True,  # Basis vs other exchanges
)

lf = build_feature_frame(
    symbol_id=12345,
    start_ts_us="2024-05-01",
    end_ts_us="2024-05-02",
    config=config,
)

df = lf.collect()
# Result: Volume-bar resampled features
```

## When to Use Which Method?

| Use Case | Recommended Method | Rationale |
|----------|-------------------|-----------|
| **General research** | Clock (1-5s) | Simple, interpretable, standard |
| **Regulatory reporting** | Clock (1s) | SEC compliance |
| **Volatility modeling** | Volume bars | More stationary returns |
| **Cross-asset comparison** | Dollar bars | Economic significance normalized |
| **Event studies** | Trades | Tick-by-tick precision |
| **Microstructure research** | Volume/Dollar bars | Better signal-to-noise |
| **Backtesting (MFT)** | Volume/Dollar bars | Activity-normalized execution |

## References

**Academic:**
- Easley, D., López de Prado, M., O'Hara, M. (2012). "Flow Toxicity and Liquidity in a High-frequency World." *Review of Financial Studies*, 25(5), 1457-1493.
- López de Prado, M. (2018). *Advances in Financial Machine Learning*, Chapter 2: Financial Data Structures.
- Harris, L. (2003). *Trading and Exchanges: Market Microstructure for Practitioners*, Chapter 7.

**Implementation:**
- Pointline Spine Builder System: `/pointline/research/features/spines/`
- Feature Engineering Guide: `docs/guides/feature-engineering.md` (TODO)

## API Reference

**Quick Links:**
- `build_event_spine()`: `pointline/research/features/core.py:125`
- `ClockSpineConfig`: `pointline/research/features/spines/clock.py:17`
- `TradesSpineConfig`: `pointline/research/features/spines/trades.py:17`
- `VolumeBarConfig`: `pointline/research/features/spines/volume.py:21`
- `DollarBarConfig`: `pointline/research/features/spines/dollar.py:21`

**Registry Functions:**
```python
from pointline.research.features.spines import (
    list_builders,      # List all registered builders
    get_builder,        # Get builder by name
    detect_builder,     # Auto-detect builder from mode string
)

# List available builders
print(list_builders())
# → ['clock', 'dollar', 'trades', 'volume']

# Get builder directly
builder = get_builder("volume")
spine = builder.build_spine(symbol_id=12345, ...)
```

## Advanced: Custom Spine Builders

See `docs/guides/custom-spine-builders.md` (TODO) for how to implement your own resampling methods (tick bars, imbalance bars, quote-event bars, etc.).

**Plugin Pattern:**
1. Implement `SpineBuilder` protocol
2. Register via `register_builder()`
3. Use via `EventSpineConfig(mode="your_builder")`

Example skeleton:

```python
from pointline.research.features.spines import SpineBuilder, SpineBuilderConfig, register_builder

class CustomSpineBuilder:
    @property
    def name(self) -> str:
        return "custom"

    def build_spine(self, symbol_id, start_ts_us, end_ts_us, config):
        # Your logic here
        pass

# Register
register_builder(CustomSpineBuilder())
```
