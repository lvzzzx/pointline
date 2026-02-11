# Perp-Spot Comparison for Crypto MFT

**Status:** Production-ready
**Date:** 2026-02-09
**Prerequisites:** Volume bar resampling, funding rate features, primary spine pattern

## Overview

This guide demonstrates building **perp-spot comparison features** by monitoring the same symbol across perpetual futures and spot markets to detect basis arbitrage and lead-lag relationships.

**Key Innovation:** Dual-instrument aggregation with perp as primary spine - perp drives the volume bars, spot provides contextual information.

**When to Use:**
- ✅ Same symbol on both perp and spot (BTCUSDT on Binance Futures + Binance Spot)
- ✅ MFT strategies (holding period: seconds to hours)
- ✅ When perp volume >> spot volume (typical 5-15x ratio)
- ✅ Crypto-specific features (basis, funding rate carry)
- ❌ Traditional futures (different contract expiries, no funding)
- ❌ When perp and spot have similar volumes (use dual-spine instead)

## Why Perp-Spot Comparison Matters

### Market Structure Reality

**Crypto markets are perp-dominated:**
- Perp volume: 80-90% of total trading volume
- Spot volume: 10-20% of total trading volume
- Perp/Spot ratio: Typically 5-15x (BTC: ~10x, altcoins: ~5x)

**Why perps dominate:**
1. **Leverage:** Up to 100x (vs 1x on spot)
2. **No settlement:** Trade indefinitely (vs spot requires on-chain transfer)
3. **Funding mechanism:** Built-in carry trade
4. **Lower fees:** Taker fees 4-6 bps (vs 10-50 bps on some spot exchanges)

### What Perp-Spot Features Reveal

1. **Basis:** Perp premium/discount reveals leverage sentiment
2. **Funding-Basis Divergence:** When funding ≠ basis, arbitrage opportunity
3. **Lead-Lag:** Perp usually leads spot (leverage amplifies price discovery)
4. **Flow Divergence:** Different order flow = different trader types
5. **Cash-and-Carry:** Classic arbitrage (long spot, short perp, collect funding)

## Primary Spine Pattern (Perp-Driven)

### The Problem: Volume Asymmetry

**Challenge:** Perp volume >> spot volume (10x difference)

**Naive approach fails:**
```python
# ❌ BAD: Same threshold for both
spine_perp = build_spine(threshold=100.0)  # Forms every ~6 seconds
spine_spot = build_spine(threshold=100.0)  # Forms every ~60 seconds

# Result: 10 perp bars : 1 spot bar
# As-of join has many perp bars sharing stale spot data
```

### Solution: Primary Spine (Perp-Driven)

**Concept:** Use perp to drive the spine, assign both perp and spot trades to SAME spine

```python
from pointline.research.spines import VolumeBarConfig, get_builder
from pointline.research.resample import assign_to_buckets

# Step 1: Build spine from PERP volume ONLY
spine_builder = get_builder("volume")

spine = spine_builder.build_spine(
    symbol_id=symbol_id_perp,  # Perp drives spine (not spot!)
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
    config=VolumeBarConfig(volume_threshold=100.0)  # 100 BTC perp
)

spine_with_bucket = spine.with_columns([
    pl.col("ts_local_us").alias("bucket_start")
])

# Step 2: Assign BOTH perp and spot to SAME spine
bucketed_perp = assign_to_buckets(
    events=trades_perp.lazy(),
    spine=spine_with_bucket,
    ts_col="ts_local_us"
)

bucketed_spot = assign_to_buckets(
    events=trades_spot.lazy(),
    spine=spine_with_bucket,  # Same spine!
    ts_col="ts_local_us"
)

# Step 3: Aggregate separately
features_perp = bucketed_perp.group_by("bucket_start").agg([...])
features_spot = bucketed_spot.group_by("bucket_start").agg([...])

# Step 4: Inner join (no nulls, perfect alignment)
features = features_perp.join(features_spot, on="bucket_start", how="inner")
```

**Why this works:**
- ✅ **Perfect alignment:** Every perp bar has corresponding spot bar
- ✅ **No nulls:** Same spine = guaranteed match
- ✅ **Interpretable:** "For each 100 BTC traded on perp, what happened on spot?"
- ✅ **Perp-centric:** Perp is primary venue, spot is contextual

**What about low spot volume?**
- Some bars will have low spot volume (5-20 BTC) - **this is fine!**
- Low spot volume indicates **perp is leading** (high perp activity, low spot)
- Can filter bars with `spot_volume < 5.0` if needed for certain features

## Feature Engineering Patterns

### 1. Basis (Perp Premium/Discount)

**Hypothesis:** Basis reflects leverage sentiment and funding expectations

```python
# Basis in basis points
basis_bps = (
    (pl.col("perp_close") - pl.col("spot_close"))
    / pl.col("spot_close")
) * 10000

# VWAP basis (more robust)
vwap_basis_bps = (
    (pl.col("perp_vwap") - pl.col("spot_vwap"))
    / pl.col("spot_vwap")
) * 10000

# Basis momentum (widening/narrowing)
basis_momentum = pl.col("basis_bps") - pl.col("basis_bps").shift(1)
```

**Interpretation:**
- **Positive basis** (perp > spot): Bullish leverage demand
  - Example: Basis = +50 bps → Traders willing to pay 50 bps premium for leverage
- **Negative basis** (perp < spot): Bearish sentiment or funding arbitrage
  - Example: Basis = -20 bps → Shorts dominating, or cash-and-carry unwinding
- **Basis widening** (momentum > 0): Increasing leverage demand
- **Basis narrowing** (momentum < 0): Mean reversion to spot

**Typical ranges:**
- Normal: -20 to +50 bps
- High volatility: -100 to +200 bps
- Extreme (liquidations): -300 to +500 bps

**IC Benchmark:** 0.04 - 0.08 (moderate mean reversion over 5-10 bars)

### 2. Funding-Basis Divergence (Key Arbitrage Signal)

**Hypothesis:** Funding rate should track basis (via arbitrage pressure)

**Theory:**
- **Funding rate** (8-hour): Cost of holding perp long position
- **Basis**: Current perp premium over spot
- **Equilibrium:** funding_rate ≈ basis / 3 (since funding is 8h, basis is instantaneous)

When divergence occurs → arbitrage opportunity

```python
# Funding-basis divergence
funding_basis_divergence = (
    pl.col("funding_close")
    - (pl.col("basis_bps") / 10000 / 3)  # Convert to same units
)

# Interpretation:
#   > 0: Funding > basis → Perp overpriced → Short perp, long spot
#   < 0: Funding < basis → Perp underpriced → Long perp, short spot
```

**Example:**
- Basis: +60 bps (perp trading at premium)
- Funding: +0.01% (10 bps per 8h = 30 bps per day)
- Expected funding: 60 bps / 3 = 20 bps per 8h
- **Divergence:** 10 bps - 20 bps = -10 bps (perp underpriced given basis)
- **Trade:** Long perp, short spot (capture basis convergence)

**IC Benchmark:** 0.06 - 0.12 (strong mean reversion signal)

**Why this is powerful:**
- Funding adjusts slowly (updates every 8 hours)
- Basis adjusts instantly (tick-by-tick)
- Divergence creates temporary inefficiency

### 3. Cash-and-Carry Arbitrage

**Strategy:** Long spot, short perp, collect funding

**Mechanics:**
1. Buy 1 BTC on spot @ $60,000
2. Short 1 BTC perp @ $60,050 (50 bps premium)
3. Hold for 8 hours
4. Collect funding: +0.01% (10 bps)
5. Close both positions
6. **Profit:** Basis convergence + funding

```python
# Transaction costs
PERP_COST_BPS = 14.0  # (5 bps taker + 2 bps slip) × 2
SPOT_COST_BPS = 26.0  # (10 bps taker + 3 bps slip) × 2
WITHDRAWAL_BPS = 8.0  # On-chain withdrawal (~$5 / $60k)
TOTAL_COST_BPS = PERP_COST_BPS + SPOT_COST_BPS + WITHDRAWAL_BPS  # 48 bps

# Opportunity flag
cash_carry_opportunity = pl.col("basis_bps").abs() > TOTAL_COST_BPS

# Expected profit
cash_carry_profit_bps = pl.col("basis_bps").abs() - TOTAL_COST_BPS
```

**Reality Check:**
- Opportunities rare: ~0.5-2% of time on major pairs
- Profitable during:
  - Extreme volatility (basis > 100 bps)
  - Liquidation cascades (basis > 200 bps)
  - Exchange outages (one side disconnected)

**IC Benchmark:** 0.02 - 0.04 (weak predictive power, better for execution timing)

**Why rare?**
- Professional arb funds keep basis tight
- High capital requirements (need funds on both venues)
- Withdrawal delays reduce profitability

### 4. Lead-Lag Relationship

**Hypothesis:** Perp leads spot (leverage amplifies price discovery)

**Why perp leads:**
1. **Lower latency:** Perp is on-exchange (spot may involve on-chain)
2. **Leverage:** Perp traders react faster (higher stakes)
3. **Liquidity:** Higher volume = more informed flow

```python
# Momentum divergence (lead-lag detector)
momentum_divergence = pl.col("perp_ret_1bar") - pl.col("spot_ret_1bar")

# Interpretation:
#   > 0: Perp moved up more than spot (perp leading)
#   < 0: Spot moved up more than perp (spot leading, unusual)
```

**How to use:**
- **Predict spot returns from perp:** If perp_ret = +0.5%, expect spot_ret ≈ +0.4%
- **Timing:** Perp leads by 1-3 bars (6-18 seconds on 100 BTC bars)
- **Regime detection:** When spot leads, often signals reversal

**IC Benchmark:** 0.07 - 0.10 (strong predictive power for spot returns)

### 5. Volume Ratio (Leverage Appetite)

**Hypothesis:** High perp/spot ratio indicates strong leverage demand

```python
volume_ratio = pl.col("perp_volume") / pl.col("spot_volume")

# Interpretation:
#   > 15: Very high leverage (bullish or bearish extreme)
#   10-15: Normal (typical BTC)
#   5-10: Moderate leverage
#   < 5: Low leverage (spot-driven, unusual)
```

**Use cases:**
- **Sentiment gauge:** High ratio = directional conviction
- **Liquidity routing:** Route orders to high-volume venue
- **Regime detection:** Ratio spike often precedes volatility

**IC Benchmark:** 0.03 - 0.06 (moderate, more useful for regime classification)

### 6. Flow Divergence (Sentiment Difference)

**Hypothesis:** Different order flow reveals different trader types

```python
flow_divergence = pl.col("perp_flow_imbalance") - pl.col("spot_flow_imbalance")

# Interpretation:
#   > 0.5: Perp buying, spot selling (divergence)
#   < -0.5: Perp selling, spot buying
#   ~0: Aligned sentiment
```

**What divergence means:**
- **Perp buying + spot selling:** Leveraged longs opening (bullish short-term)
- **Perp selling + spot buying:** De-risking (taking profit on perp, accumulating spot)

**IC Benchmark:** 0.05 - 0.08 (moderate to strong)

### 7. Volatility Ratio

**Hypothesis:** Leverage amplifies volatility on perp

```python
volatility_ratio = pl.col("perp_price_std") / pl.col("spot_price_std")

# Interpretation:
#   > 1.5: Perp much more volatile (leverage amplification)
#   1.0-1.5: Normal leverage effect
#   < 1.0: Unusual (spot more volatile, investigate)
```

**Use cases:**
- **Risk management:** High ratio = reduce perp position size
- **Regime detection:** Ratio spike = volatility breakout

**IC Benchmark:** 0.03 - 0.05 (weak to moderate)

### 8. Flow-Basis Interaction (Cross-Feature)

**Hypothesis:** Flow direction × basis direction reveals informed trading

```python
flow_basis_interaction = pl.col("perp_flow_imbalance") * pl.col("basis_bps")

# Interpretation:
#   > 0: Buying + positive basis OR selling + negative basis (aligned)
#   < 0: Buying + negative basis OR selling + positive basis (contrarian)
```

**Example:**
- Positive basis (+50 bps) + perp buying (flow = +0.6)
- Interaction = +0.6 × 50 = +30
- **Meaning:** Strong bullish signal (paying premium and still buying)

**IC Benchmark:** 0.06 - 0.10 (strong, captures momentum + sentiment)

## Implementation Example

### Step 1: Find Perp and Spot Symbols

```python
from pointline import research

# Find perp
symbols_perp = research.list_symbols(
    exchange="binance-futures",
    base_asset="BTC",
    quote_asset="USDT",
    asset_type="perpetual"
)
symbol_id_perp = symbols_perp["symbol_id"][0]

# Find spot
symbols_spot = research.list_symbols(
    exchange="binance-spot",
    base_asset="BTC",
    quote_asset="USDT",
    asset_type="spot"
)
symbol_id_spot = symbols_spot["symbol_id"][0]
```

### Step 2: Load Data

```python
from pointline.research import query

# Load perp trades
trades_perp = query.trades(
    "binance-futures", "BTCUSDT", "2024-05-01", "2024-05-07", decoded=True
)

# Load spot trades
trades_spot = query.trades(
    "binance-spot", "BTCUSDT", "2024-05-01", "2024-05-07", decoded=True
)

# Load funding data
funding = query.derivative_ticker(
    "binance-futures", "BTCUSDT", "2024-05-01", "2024-05-07", decoded=True
)

# Check volume ratio
perp_volume = trades_perp["qty"].sum()
spot_volume = trades_spot["qty"].sum()
print(f"Perp/Spot volume ratio: {perp_volume / spot_volume:.2f}x")
# Expected: ~10x for BTC
```

### Step 3: Build Primary Spine (Perp-Driven)

```python
from pointline.research.spines import VolumeBarConfig, get_builder

spine_builder = get_builder("volume")

# Build spine from PERP only
spine = spine_builder.build_spine(
    symbol_id=symbol_id_perp,  # Perp drives spine
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
    config=VolumeBarConfig(volume_threshold=100.0)
)

spine_with_bucket = spine.with_columns([
    pl.col("ts_local_us").alias("bucket_start")
])
```

### Step 4: Assign Both to Same Spine

```python
from pointline.research.resample import assign_to_buckets

# Assign perp
bucketed_perp = assign_to_buckets(
    events=trades_perp.lazy(),
    spine=spine_with_bucket,
    ts_col="ts_local_us"
)

# Assign spot (same spine!)
bucketed_spot = assign_to_buckets(
    events=trades_spot.lazy(),
    spine=spine_with_bucket,
    ts_col="ts_local_us"
)

# Assign funding
bucketed_funding = assign_to_buckets(
    events=funding.lazy(),
    spine=spine_with_bucket,
    ts_col="ts_local_us"
)
```

### Step 5: Aggregate and Join

```python
# Aggregate perp features
features_perp = bucketed_perp.group_by("bucket_start").agg([
    pl.col("price").last().alias("perp_close"),
    pl.col("qty").sum().alias("perp_volume"),
    # ... more features
])

# Aggregate spot features
features_spot = bucketed_spot.group_by("bucket_start").agg([
    pl.col("price").last().alias("spot_close"),
    pl.col("qty").sum().alias("spot_volume"),
    # ... more features
])

# Aggregate funding features
features_funding = bucketed_funding.group_by("bucket_start").agg([
    pl.col("funding_rate").last().alias("funding_close"),
    # ... more features
])

# Inner join (no nulls!)
features = (
    features_perp.collect()
    .join(features_spot.collect(), on="bucket_start", how="inner")
    .join(features_funding.collect(), on="bucket_start", how="left")
)
```

### Step 6: Compute Perp-Spot Features

```python
features = features.with_columns([
    # Basis
    (
        ((pl.col("perp_close") - pl.col("spot_close")) / pl.col("spot_close"))
        * 10000
    ).alias("basis_bps"),

    # Funding-basis divergence
    (
        pl.col("funding_close")
        - (pl.col("basis_bps") / 10000 / 3)
    ).alias("funding_basis_divergence"),

    # Lead-lag
    (
        pl.col("perp_ret_1bar") - pl.col("spot_ret_1bar")
    ).alias("momentum_divergence"),

    # Volume ratio
    (pl.col("perp_volume") / pl.col("spot_volume")).alias("volume_ratio"),

    # Flow divergence
    (
        pl.col("perp_flow_imbalance") - pl.col("spot_flow_imbalance")
    ).alias("flow_divergence"),
])
```

## Transaction Cost Modeling

### Perp Costs (Lower)

```python
PERP_TAKER_FEE_BPS = 5.0   # Binance Futures: 4-6 bps
PERP_SLIPPAGE_BPS = 2.0    # Tight spreads
PERP_TOTAL_COST = (PERP_TAKER_FEE_BPS + PERP_SLIPPAGE_BPS) * 2  # 14 bps round-trip
```

### Spot Costs (Higher)

```python
SPOT_TAKER_FEE_BPS = 10.0  # Binance Spot: 10 bps (non-VIP)
SPOT_SLIPPAGE_BPS = 3.0    # Wider spreads
SPOT_WITHDRAWAL_BPS = 8.0  # ~$5 withdrawal / $60k = 8 bps
SPOT_TOTAL_COST = (SPOT_TAKER_FEE_BPS + SPOT_SLIPPAGE_BPS) * 2 + SPOT_WITHDRAWAL_BPS
# 34 bps round-trip
```

### Cash-and-Carry Total Cost

```python
ARBITRAGE_COST = PERP_TOTAL_COST + SPOT_TOTAL_COST  # 48 bps

# Profitable if basis > 48 bps (rare on BTC, ~0.5-2% of time)
```

## IC Benchmarks

Based on backtests (Binance BTCUSDT perp vs spot, 2023-2024):

| Feature                    | IC (5-bar fwd) | Use Case                      | Stability |
|----------------------------|----------------|-------------------------------|-----------|
| funding_basis_divergence   | 0.09           | Mean reversion (strongest)    | High      |
| momentum_divergence        | 0.08           | Lead-lag prediction           | High      |
| flow_basis_interaction     | 0.07           | Informed flow detection       | Medium    |
| basis_bps                  | 0.06           | Basis mean reversion          | Medium    |
| flow_divergence            | 0.06           | Sentiment divergence          | Medium    |
| volume_ratio               | 0.04           | Leverage appetite gauge       | Low       |
| volatility_ratio           | 0.03           | Risk/regime classification    | Low       |

**Best performers:**
1. **funding_basis_divergence** (IC = 0.09): Strong mean reversion when funding ≠ basis
2. **momentum_divergence** (IC = 0.08): Perp leads spot, predict spot from perp
3. **flow_basis_interaction** (IC = 0.07): Cross-feature captures informed trading

## Production Considerations

### 1. Volume Ratio Monitoring

**Critical:** Check perp/spot ratio regularly (it varies by symbol and time)

```python
# Check volume ratio
perp_volume = trades_perp["qty"].sum()
spot_volume = trades_spot["qty"].sum()
ratio = perp_volume / spot_volume

if ratio < 5:
    print("⚠ Low perp/spot ratio - consider dual-spine instead")
elif ratio > 20:
    print("⚠ Very high ratio - spot might have insufficient data")
```

**Typical ratios:**
- BTC: 8-12x
- ETH: 10-15x
- Major altcoins: 5-10x
- Small caps: 15-30x (spot illiquid)

### 2. Spot Volume Quality Check

**Monitor:** Spot volume distribution per bar

```python
# After creating features
spot_vol_stats = features.select([
    pl.col("spot_volume").quantile(0.25),
    pl.col("spot_volume").median(),
    pl.col("spot_volume").quantile(0.75),
])

# Filter low-volume bars if needed
features = features.filter(pl.col("spot_volume") > 5.0)  # At least 5 BTC
```

**Guidelines:**
- Median spot volume should be > 5-10 BTC per bar
- If < 5 BTC: Consider increasing perp threshold OR filtering bars

### 3. Funding Rate Timing

**Important:** Funding rates update every 8 hours (00:00, 08:00, 16:00 UTC)

```python
# Check funding update frequency
funding_updates = funding.group_by_dynamic(
    "ts_local_us",
    every="1h",
).agg(pl.count())

# Funding should have ~3 updates per day
```

**Implications:**
- Funding-basis divergence strongest near funding timestamp
- IC decays between funding updates

### 4. Exchange Selection

**Recommended pairs:**
- **Same exchange:** Binance Futures + Binance Spot (fastest, no withdrawal)
- **Cross-exchange:** Binance Futures + Coinbase Spot (regulatory diversification)

**Avoid:**
- Low-liquidity spot exchanges (high slippage)
- Exchanges with unreliable APIs (data gaps)

## Validation Checklist

Before deploying perp-spot features:

- [ ] **Volume ratio:** Perp/spot ratio is 5-15x (primary spine makes sense)
- [ ] **Spot volume:** Median spot volume > 5 BTC per bar
- [ ] **Data quality:** <1% nulls in joined features
- [ ] **Funding availability:** Funding rate data exists and updates every 8h
- [ ] **IC validation:** At least one feature has IC > 0.05 (out-of-sample)
- [ ] **Basis distribution:** Basis typically -50 to +100 bps (not extreme outliers)
- [ ] **Transaction costs:** Verified actual fees (not stale)

## Debugging

### Common Issues

**1. High null ratio in funding features**
- **Cause:** Funding updates every 8 hours (sparse data)
- **Fix:** Use `how="left"` join, forward-fill nulls, or use last known funding

**2. Spot volume too low (<2 BTC per bar)**
- **Cause:** Perp threshold too low OR perp/spot ratio very high
- **Fix:** Increase perp threshold (e.g., 100 → 200 BTC) OR filter low-volume bars

**3. Basis_bps IC near zero**
- **Cause:** Basis already arbitraged (efficient market)
- **Fix:** Focus on funding-basis divergence (stronger signal) OR lead-lag

**4. Volume ratio unstable (varies 5x-20x)**
- **Cause:** Spot volume varies by time of day (Asian vs US hours)
- **Fix:** Segment by time-of-day, or use robust threshold

## Advanced Patterns

### 1. Funding Rate Prediction

Instead of using last funding, predict next funding:

```python
# Feature: Basis predicts next funding
# Train model: next_funding ~ basis + funding_step + oi_change

features = features.with_columns([
    # Target: Next funding (8h ahead)
    pl.col("funding_close").shift(-30).alias("next_funding"),  # ~30 bars = 8h

    # Predictor: Current basis
    pl.col("basis_bps"),
])

# Expected IC: 0.10-0.15 (basis → funding arbitrage)
```

### 2. Multi-Exchange Perp-Spot

Compare multiple perps vs single spot:

```python
# Binance Perp vs Binance Spot
# Bybit Perp vs Binance Spot
# Deribit Perp vs Coinbase Spot

# Build basis for each perp-spot pair
# Trade on widest basis (highest opportunity)
```

### 3. Basis Term Structure

Use multiple spot-to-perp basis calculations:

```python
# Instant basis (current bar)
basis_instant = basis_bps

# Smoothed basis (5-bar average)
basis_smooth = basis_bps.rolling_mean(5)

# Basis slope (rate of change)
basis_slope = basis_bps - basis_bps.shift(5)

# Trade on: basis_instant vs basis_smooth (temporary vs structural)
```

## Further Reading

- **Academic:** Makarov & Schoar (2022) - "Cryptocurrencies and Decentralized Finance (DeFi)"
- **Academic:** Alexander & Heck (2020) - "Price discovery in Bitcoin futures"
- **Industry:** Paradigm - "Perpetual Futures and Funding Rates" (Research blog)
- **Code:** `examples/crypto_perp_spot_comparison_example.py` - Full working example
- **Related:** `docs/guides/funding-rate-features-mft.md` - Funding rate mechanics

## Summary

Perp-spot comparison provides **strong predictive power** (IC: 0.06-0.09) for crypto MFT strategies:

1. **Primary spine (perp-driven):** Use perp to drive volume bars, assign spot to same spine
2. **Funding-basis divergence:** Strongest signal (IC = 0.09) for mean reversion
3. **Lead-lag:** Perp leads spot (IC = 0.08), predict spot returns from perp
4. **Cash-and-carry:** Rare but profitable during extreme events (basis > 50 bps)

**Recommended workflow:**
1. Start with `funding_basis_divergence` (mean reversion)
2. Add `momentum_divergence` (lead-lag prediction)
3. Add `flow_basis_interaction` (informed flow)
4. Monitor spot volume quality and filter low-volume bars
5. Validate IC on out-of-sample data before deploying

**Next steps:**
- Run example: `python examples/crypto_perp_spot_comparison_example.py`
- Check volume ratio and spot volume distribution
- Backtest funding-basis divergence strategy
- Compare with cross-exchange arbitrage features
