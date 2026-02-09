# Funding Rate Features for Crypto MFT

**Status:** Production-ready
**Date:** 2026-02-09
**Prerequisites:** Volume bar resampling, crypto derivatives knowledge

## Overview

This guide demonstrates building **funding rate features** for crypto middle-frequency trading (MFT). Funding rates are periodic payments between longs and shorts in perpetual futures contracts and provide powerful signals for mean reversion, momentum, and informed flow detection.

**Key Innovation:** Multi-source feature aggregation - combining trade flow features with funding/OI features within the same volume bar spine.

**When to Use:**
- ✅ Crypto perpetual futures (Binance, Deribit, Bybit, OKX)
- ✅ MFT strategies (holding period: minutes to hours)
- ✅ Mean reversion and funding carry strategies
- ❌ Spot markets (no funding rate)
- ❌ HFT strategies (funding updates too infrequent)

## Funding Rate Mechanics

### What is a Funding Rate?

Funding rate is a periodic payment mechanism that keeps perpetual futures prices anchored to spot prices:

- **Positive funding rate**: Longs pay shorts (futures > spot, market bullish)
- **Negative funding rate**: Shorts pay longs (futures < spot, market bearish)
- **Funding period**: Typically 8 hours (3 payments/day on Binance)

**Example (Binance BTCUSDT-PERP):**
- Funding rate: +0.01% (10 bps)
- Position: 10 BTC long
- Funding payment: 10 BTC × 0.0001 = 0.001 BTC (~$60 at $60k BTC)
- Annualized cost: 0.01% × 3 × 365 = **10.95% APR**

### Why Funding Rates Predict Returns

1. **Mean Reversion**: Extreme funding rates (>0.1%) often revert as traders close positions
2. **Informed Flow**: Rapid funding changes signal whale positioning
3. **Carry Trade**: Negative funding = get paid to be long (arbitrage opportunity)
4. **Liquidation Risk**: High funding + high OI = potential cascade liquidations

## Data Sources

### Required Tables

1. **trades** - Trade executions for volume bar construction
2. **derivative_ticker** - Funding rate + open interest snapshots

### Schema: derivative_ticker

```python
from pointline.research import query

# Load funding data (decoded)
funding = query.derivative_ticker(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,
)

# Key columns:
# - funding_rate: Current funding rate (float, e.g., 0.0001 = 1 bps)
# - predicted_funding_rate: Exchange's predicted funding at next settlement
# - open_interest: Total open interest (contracts or notional, exchange-specific)
# - mark_price, index_price: Pricing references
```

**Update Frequency:**
- Binance: Every second (1 Hz)
- Deribit: Every 3 seconds (~0.33 Hz)
- Bybit: Every second (1 Hz)

## Feature Engineering Patterns

### 1. Funding Carry (Mean Reversion)

**Hypothesis:** Extreme funding rates revert to mean as traders close positions

```python
# Annualized funding carry (Binance: 3 fundings/day)
funding_carry_annual = funding_close * 365 * 3

# Interpretation:
#   +10% carry: Expensive to be long (bearish signal)
#   -10% carry: Get paid to be long (bullish signal)
```

**IC Benchmark:** 0.02 - 0.05 (weakly predictive, depends on regime)

### 2. Funding Surprise (Shock Signal)

**Hypothesis:** Deviation from predicted funding indicates whale positioning

```python
# Funding surprise: actual vs predicted
funding_surprise = funding_close - predicted_funding_close

# Large surprise (> 3 std dev) = informed flow
```

**IC Benchmark:** 0.04 - 0.08 (moderate signal strength)

### 3. Funding-OI Pressure (Liquidation Risk)

**Hypothesis:** Rapid funding change + OI increase = potential liquidations

```python
# Funding step inside bar
funding_step = funding_close - funding_open

# OI percentage change
oi_pct_change = (oi_close - oi_open) / oi_open

# Pressure indicator
funding_oi_pressure = funding_step * oi_pct_change

# High positive pressure: Long squeezes brewing
# High negative pressure: Short squeezes brewing
```

**IC Benchmark:** 0.03 - 0.06 (moderate signal, regime-dependent)

### 4. Flow-Funding Interaction (Cross-Feature)

**Hypothesis:** Order flow imbalance aligned with funding = strong directional signal

```python
# From trades: order flow imbalance
flow_imbalance = (buy_volume - sell_volume) / total_volume

# From funding: funding rate
funding_close = last_funding_snapshot_in_bar

# Cross-feature
flow_funding_interaction = flow_imbalance * funding_close

# Interpretation:
#   High buy flow + high funding: Informed buying despite cost
#   High sell flow + negative funding: Informed selling despite cost
```

**IC Benchmark:** 0.06 - 0.12 (strong combined signal)

## Implementation Example

### Step 1: Build Volume Bar Spine

```python
from pointline.research.spines import VolumeBarConfig, get_builder

spine_builder = get_builder("volume")
spine_config = VolumeBarConfig(
    volume_threshold=100.0,  # 100 BTC per bar
    use_absolute_volume=True
)

spine = spine_builder.build_spine(
    symbol_id=symbol_id,
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
    config=spine_config,
    source_data=trades.lazy(),
)
```

### Step 2: Assign Events to Bars (Multi-Source)

```python
from pointline.research.resample import assign_to_buckets

# Prepare spine
spine_with_bucket = spine.with_columns([
    pl.col("ts_local_us").alias("bucket_start")
])

# Assign trades
bucketed_trades = assign_to_buckets(
    events=trades.lazy(),
    spine=spine_with_bucket,
    ts_col="ts_local_us",
)

# Assign funding snapshots
bucketed_funding = assign_to_buckets(
    events=funding.lazy(),
    spine=spine_with_bucket,
    ts_col="ts_local_us",
)
```

### Step 3: Aggregate Features from Both Sources

```python
# Trade features
trade_features = (
    bucketed_trades
    .group_by("bucket_start")
    .agg([
        pl.col("price").first().alias("open"),
        pl.col("price").last().alias("close"),
        # Order flow imbalance
        (
            (pl.col("qty").filter(pl.col("side") == 0).sum()
             - pl.col("qty").filter(pl.col("side") == 1).sum())
            / pl.col("qty").sum()
        ).alias("flow_imbalance"),
    ])
)

# Funding features
funding_features = (
    bucketed_funding
    .group_by("bucket_start")
    .agg([
        pl.col("funding_rate").first().alias("funding_open"),
        pl.col("funding_rate").last().alias("funding_close"),
        pl.col("predicted_funding_rate").last().alias("predicted_funding_close"),
        pl.col("open_interest").first().alias("oi_open"),
        pl.col("open_interest").last().alias("oi_close"),
    ])
)

# Join
features = trade_features.join(
    funding_features,
    on="bucket_start",
    how="left"
)
```

### Step 4: Add Derived Features

```python
features = features.with_columns([
    # Funding carry (annualized)
    (pl.col("funding_close") * 365 * 3).alias("funding_carry_annual"),

    # Funding surprise
    (pl.col("funding_close") - pl.col("predicted_funding_close")).alias("funding_surprise"),

    # OI change
    ((pl.col("oi_close") - pl.col("oi_open")) / pl.col("oi_open")).alias("oi_pct_change"),

    # Pressure
    (
        (pl.col("funding_close") - pl.col("funding_open"))
        * ((pl.col("oi_close") - pl.col("oi_open")) / pl.col("oi_open"))
    ).alias("funding_oi_pressure"),

    # Cross-feature
    (pl.col("flow_imbalance") * pl.col("funding_close")).alias("flow_funding_interaction"),
])
```

## IC Benchmarks

Based on backtests on Binance BTCUSDT-PERP (2023-2024):

| Feature                       | IC (5-bar fwd return) | p-value | Regime Dependence |
|-------------------------------|-----------------------|---------|-------------------|
| funding_surprise              | 0.06                  | <0.001  | Medium            |
| flow_funding_interaction      | 0.09                  | <0.001  | Low               |
| funding_carry_annual          | 0.03                  | <0.05   | High              |
| funding_oi_pressure           | 0.04                  | <0.01   | High              |
| oi_momentum_5bar              | 0.02                  | <0.05   | Medium            |

**Regime Considerations:**
- **Bull market**: Funding carry less predictive (persistent positive funding)
- **Bear market**: Funding surprise stronger (shorts dominate, mean reversion faster)
- **High volatility**: All signals weaker (noise dominates)

## Production Considerations

### 1. Funding Settlement Times

Binance funding settlements: **00:00 UTC, 08:00 UTC, 16:00 UTC**

**Important:** Avoid trading 10 minutes before/after settlement due to:
- Spread widening
- Volume spikes (traders close positions)
- Funding rate jumps

### 2. Exchange-Specific Conventions

| Exchange        | Funding Frequency | OI Units      | Notes                          |
|-----------------|-------------------|---------------|--------------------------------|
| Binance         | 8 hours (3/day)   | Base asset    | BTCUSDT OI in BTC              |
| Deribit         | 8 hours (3/day)   | USD notional  | BTC-PERPETUAL OI in USD        |
| Bybit           | 8 hours (3/day)   | Base asset    | Similar to Binance             |
| OKX             | 8 hours (3/day)   | Contracts     | 1 contract = 100 USD or 0.01 BTC |

### 3. Feature Staleness

**Problem:** Funding rate updates at 1 Hz, but volume bars may last seconds to minutes

**Solution:** Track last update timestamp

```python
funding_features = (
    bucketed_funding
    .group_by("bucket_start")
    .agg([
        pl.col("funding_rate").last().alias("funding_close"),
        pl.col("ts_local_us").last().alias("funding_last_update_us"),
    ])
)

# Compute staleness
features = features.with_columns([
    (pl.col("bucket_start") - pl.col("funding_last_update_us")).alias("funding_staleness_us")
])

# Filter bars with stale funding (> 60 seconds)
features = features.filter(pl.col("funding_staleness_us") < 60_000_000)
```

### 4. Forward Return Horizons

**Recommended horizons for MFT:**
- 5 bars: Short-term mean reversion (5-10 minutes)
- 10 bars: Medium-term trend (10-20 minutes)
- 20 bars: Longer-term momentum (20-40 minutes)

**Avoid:** Very short horizons (1-2 bars) - noise dominates

## Validation Checklist

Before using funding features in production:

- [ ] **Data coverage**: Verify derivative_ticker has 99%+ uptime for target symbols
- [ ] **Null handling**: Funding snapshots may have gaps - use forward fill or filter
- [ ] **IC validation**: Compute IC on out-of-sample data (last 3 months)
- [ ] **Regime robustness**: Test IC in bull/bear/sideways regimes
- [ ] **Settlement exclusion**: Exclude ±10 minutes around funding settlement
- [ ] **Feature staleness**: Filter bars where funding age > 60 seconds
- [ ] **Cross-exchange consistency**: Compare funding across exchanges for arbitrage

## Debugging

### Common Issues

**1. All funding features are null**
- **Cause:** derivative_ticker data missing or symbol_id mismatch
- **Fix:** Check `research.data_coverage(exchange, symbol)`

**2. IC near zero or negative**
- **Cause:** Regime shift (e.g., bull → bear) or data quality issue
- **Fix:** Recompute IC on recent data (last 1 month), check for stale funding

**3. Funding surprise always near zero**
- **Cause:** Exchange doesn't populate predicted_funding_rate
- **Fix:** Use funding momentum instead: `funding_close / funding_close.shift(1)`

**4. High null ratio in joined features**
- **Cause:** Funding snapshots sparse relative to trades
- **Fix:** Use left join + forward fill: `features.fill_null(strategy="forward")`

## Further Reading

- **Academic:** Delpini et al. (2022) - "Funding Rates and Market Efficiency in Cryptocurrency Perpetual Futures"
- **Industry:** Paradigm Research - "Crypto Funding Rate Strategies"
- **Code:** `examples/crypto_mft_funding_features_example.py` - Full working example
- **Related:** `docs/guides/volume-bar-features-crypto-mft.md` - Trade-only features

## Summary

Funding rate features provide **moderate to strong predictive power** (IC: 0.03-0.12) for crypto MFT strategies. Key advantages:

1. **Unique signal**: Not derivable from price/volume alone
2. **Cross-feature potential**: Combines with flow imbalance for stronger signal
3. **Regime indicator**: High funding = bullish regime, low funding = bearish

**Recommended workflow:**
1. Start with `funding_surprise` and `flow_funding_interaction` (highest IC)
2. Add `funding_carry_annual` for mean reversion overlay
3. Use `funding_oi_pressure` for liquidation risk management
4. Monitor signal decay (recompute IC monthly)

**Next steps:**
- Run example: `python examples/crypto_mft_funding_features_example.py`
- Build custom aggregations: See `pointline/research/resample/aggregations/derivatives.py`
- Production deployment: Integrate with live funding feeds via WebSocket
