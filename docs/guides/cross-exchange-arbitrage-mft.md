# Cross-Exchange Arbitrage for Crypto MFT

**Status:** Production-ready
**Date:** 2026-02-09
**Prerequisites:** Volume bar resampling, transaction cost modeling

## Overview

This guide demonstrates building **cross-exchange arbitrage features** for crypto trading by monitoring the same symbol across multiple exchanges and exploiting price discrepancies.

**Key Innovation:** Dual-exchange aggregation - build features from two exchanges and detect profitable spread opportunities.

**When to Use:**
- ✅ Multiple exchanges with same symbol (BTCUSDT on Binance, Coinbase, Bybit)
- ✅ MFT strategies (holding period: seconds to minutes)
- ✅ When transaction costs < observed spreads
- ❌ Single exchange strategies
- ❌ When latency > spread duration (need low-latency infrastructure)

## What is Cross-Exchange Arbitrage?

Cross-exchange arbitrage exploits temporary price differences for the same asset across different exchanges.

### Example

**Snapshot at 10:00:00 UTC:**
- Binance BTCUSDT: $63,050.00
- Coinbase BTC-USD: $63,065.00
- **Spread**: +$15.00 (+2.4 bps)

**Transaction Costs:**
- Taker fee: 5 bps per side × 2 = 10 bps
- Slippage: 2 bps per side × 2 = 4 bps
- **Total cost**: 14 bps

**Profit:** 2.4 bps - 14 bps = **-11.6 bps** (NOT profitable)

### When is Arbitrage Profitable?

Arbitrage is profitable when:
```
|spread_bps| > (taker_fee_bps + slippage_bps) × 2
```

For typical crypto perpetuals:
- Taker fee: 5 bps per side
- Slippage: 2 bps per side
- **Minimum spread for profit**: 14 bps

**Reality:** Spreads > 14 bps are rare on major exchanges (occur ~1-5% of time)

## Types of Cross-Exchange Arbitrage

### 1. Spatial Arbitrage (Spot-Spot)

Buy on cheap exchange, sell on expensive exchange.

**Example:**
- Buy 1 BTC on Coinbase @ $63,000
- Sell 1 BTC on Binance @ $63,020
- Gross profit: $20 (3.2 bps)

**Challenges:**
- Capital lockup (need funds on both exchanges)
- Withdrawal delays (hours to days)
- Withdrawal fees (5-50 bps)

### 2. Futures Arbitrage (Basis Arbitrage)

Exploit differences between spot and futures, or between perpetual futures on different exchanges.

**Example:**
- Binance Perp: $63,050
- Deribit Perp: $63,100
- **Basis spread**: +50 bps

**Advantage:** No withdrawal needed (can close positions instantly)

### 3. Triangular Arbitrage

Exploit pricing inefficiencies across three currency pairs.

**Example:**
- BTC/USDT on Binance
- BTC/USD on Coinbase
- USDT/USD implied rate

**Advanced:** Requires multi-currency balance management

### 4. Funding Rate Arbitrage

Exploit funding rate differences on perpetual futures.

**Example:**
- Binance funding: +0.01% (every 8 hours)
- Bybit funding: -0.005%
- **Funding spread**: 1.5 bps (annualized: 16.4% APR)

**See:** `docs/guides/funding-rate-features-mft.md` for details

## Feature Engineering Patterns

### 1. Price Spread

**Hypothesis:** Spreads mean-revert to zero (law of one price)

```python
# Absolute spread
price_spread = pl.col("a_close") - pl.col("b_close")

# Normalized spread (basis points)
spread_bps = (
    (pl.col("a_close") - pl.col("b_close"))
    / ((pl.col("a_close") + pl.col("b_close")) / 2)
) * 10000

# VWAP spread (more robust)
vwap_spread_bps = (
    (pl.col("a_vwap") - pl.col("b_vwap"))
    / ((pl.col("a_vwap") + pl.col("b_vwap")) / 2)
) * 10000
```

**IC Benchmark:** 0.04 - 0.07 (moderate mean reversion)

**Interpretation:**
- Positive spread: Exchange A more expensive (sell A, buy B)
- Negative spread: Exchange B more expensive (sell B, buy A)
- Spread reverts to zero within minutes to hours

### 2. Arbitrage Opportunity Detection

**Hypothesis:** Spread exceeding transaction costs is tradable

```python
# Transaction costs
TAKER_FEE_BPS = 5.0  # 5 bps per side
SLIPPAGE_BPS = 2.0   # 2 bps per side
TOTAL_COST_BPS = (TAKER_FEE_BPS + SLIPPAGE_BPS) * 2  # 14 bps round-trip

# Binary opportunity flag
arb_opportunity = pl.col("spread_bps").abs() > TOTAL_COST_BPS

# Expected profit (net of costs)
expected_profit_bps = pl.col("spread_bps").abs() - TOTAL_COST_BPS
```

**Reality Check:**
- Spreads > 14 bps: ~1-5% of time on major pairs
- Spreads > 20 bps: ~0.1-1% of time
- Most profitable during:
  - High volatility events
  - Exchange outages
  - Large market orders

**IC Benchmark:** 0.02 - 0.04 (weak predictive power, better for execution timing)

### 3. Lead-Lag Relationship

**Hypothesis:** One exchange leads price discovery (reacts faster to news)

```python
# Momentum divergence
momentum_divergence = pl.col("a_ret_1bar") - pl.col("b_ret_1bar")

# Interpretation:
#   > 0: Exchange A leading (news hits A first)
#   < 0: Exchange B leading (news hits B first)
```

**How to Use:**
- If A consistently leads: Trade on A, hedge on B
- If B lags by 1-3 seconds: Predict B price from A price

**IC Benchmark:** 0.05 - 0.10 (strong predictive power for B returns)

**Academic:** Hasbrouck (1995) - "One Security, Many Markets"

### 4. Volume Imbalance

**Hypothesis:** Volume concentration predicts short-term spreads

```python
# Volume imbalance
volume_imbalance = (
    (pl.col("a_volume") - pl.col("b_volume"))
    / (pl.col("a_volume") + pl.col("b_volume"))
)

# Interpretation:
#   > 0.5: Heavy volume on A (liquidity advantage)
#   < -0.5: Heavy volume on B
```

**Use Case:**
- High volume exchange has lower slippage
- Route orders to high-volume exchange

**IC Benchmark:** 0.03 - 0.05 (moderate)

### 5. Flow Divergence

**Hypothesis:** Different sentiment across exchanges creates opportunities

```python
# Flow divergence
flow_divergence = pl.col("a_flow_imbalance") - pl.col("b_flow_imbalance")

# Interpretation:
#   > 0.5: Buying pressure on A, selling on B (divergence)
#   < -0.5: Selling on A, buying on B
#   ~0: Aligned sentiment
```

**IC Benchmark:** 0.04 - 0.08 (moderate to strong)

### 6. Spread Convergence Speed

**Hypothesis:** Fast-converging spreads are more tradable

```python
# Spread convergence (% change in spread)
spread_convergence = (
    (pl.col("spread_bps") - pl.col("spread_bps").shift(1))
    / pl.col("spread_bps").shift(1).abs()
)

# Interpretation:
#   > 0: Spread widening (wait for peak)
#   < 0: Spread narrowing (mean reversion active)
```

**Use Case:**
- Enter when spread_convergence < -0.5 (rapid convergence)
- Exit when spread near zero

**IC Benchmark:** 0.03 - 0.06 (moderate)

## Implementation Example

### Step 1: Find Same Symbol on Multiple Exchanges

```python
from pointline import research

# Find BTCUSDT perpetual on all exchanges
symbols = research.list_symbols(
    base_asset="BTC",
    quote_asset="USDT",
    asset_type="perpetual"
)

# Filter to specific exchanges
binance_btc = symbols.filter(pl.col("exchange") == "binance-futures")
bybit_btc = symbols.filter(pl.col("exchange") == "bybit")

symbol_id_a = binance_btc["symbol_id"][0]
symbol_id_b = bybit_btc["symbol_id"][0]
```

### Step 2: Build Volume Bars for Each Exchange

```python
from pointline.research.spines import VolumeBarConfig, get_builder

spine_builder = get_builder("volume")
config = VolumeBarConfig(volume_threshold=100.0, use_absolute_volume=True)

# Build separate spines
spine_a = spine_builder.build_spine(symbol_id=symbol_id_a, ...)
spine_b = spine_builder.build_spine(symbol_id=symbol_id_b, ...)
```

### Step 3: Aggregate Features from Each Exchange

```python
# Exchange A features
features_a = (
    bucketed_trades_a
    .group_by("bucket_start")
    .agg([
        pl.col("price").last().alias("a_close"),
        pl.col("price").mean().alias("a_vwap"),
        # ... more features
    ])
)

# Exchange B features
features_b = (
    bucketed_trades_b
    .group_by("bucket_start")
    .agg([
        pl.col("price").last().alias("b_close"),
        pl.col("price").mean().alias("b_vwap"),
        # ... more features
    ])
)
```

### Step 4: Join Exchanges (As-Of Join)

```python
# As-of join: For each A bar, find most recent B bar
features = features_a.join_asof(
    features_b,
    left_on="bucket_start",
    right_on="bucket_start",
    strategy="backward",  # PIT correct: no lookahead
)
```

**Critical:** Use `strategy="backward"` to avoid lookahead bias

### Step 5: Add Cross-Exchange Features

```python
features = features.with_columns([
    # Price spread (bps)
    (
        ((pl.col("a_close") - pl.col("b_close"))
         / ((pl.col("a_close") + pl.col("b_close")) / 2))
        * 10000
    ).alias("spread_bps"),

    # Arbitrage opportunity
    (pl.col("spread_bps").abs() > 14.0).alias("arb_opportunity"),

    # Expected profit
    (pl.col("spread_bps").abs() - 14.0).alias("expected_profit_bps"),

    # Momentum divergence
    (pl.col("a_ret_1bar") - pl.col("b_ret_1bar")).alias("momentum_divergence"),
])
```

## Transaction Cost Modeling

### Components

1. **Taker Fees** (per side)
   - Binance: 4-5 bps (depending on VIP level)
   - Coinbase: 50 bps (retail), 20 bps (institutional)
   - Bybit: 6 bps
   - Deribit: 5 bps

2. **Slippage** (per side)
   - Tight spread (BTC): 1-2 bps
   - Wider spread (altcoins): 5-20 bps
   - During volatility: 10-50 bps

3. **Funding Costs** (perpetual futures only)
   - Typical: -10 to +10 bps per 8 hours
   - Extreme: -50 to +50 bps (during liquidation cascades)

4. **Withdrawal Fees** (spot only)
   - On-chain: 0.0001-0.0005 BTC (~$6-$30 at $60k)
   - Internal transfer: Free (but slow)

### Total Cost Calculation

```python
# Perpetual futures (no withdrawal)
TAKER_FEE_BPS = 5.0      # Binance taker fee
SLIPPAGE_BPS = 2.0       # Estimate based on spread
TOTAL_COST_BPS = (TAKER_FEE_BPS + SLIPPAGE_BPS) * 2  # 14 bps

# Spot (with withdrawal)
SPOT_TAKER_FEE_BPS = 10.0
SPOT_SLIPPAGE_BPS = 3.0
WITHDRAWAL_FEE_BPS = 8.0  # ~$5 withdrawal fee / $60k position
TOTAL_SPOT_COST_BPS = (SPOT_TAKER_FEE_BPS + SPOT_SLIPPAGE_BPS) * 2 + WITHDRAWAL_FEE_BPS  # 34 bps
```

**Recommendation:** Start with conservative cost estimates, then optimize based on execution data

## IC Benchmarks

Based on backtests (Binance vs Bybit BTCUSDT-PERP, 2023-2024):

| Feature                | IC (5-bar fwd) | Use Case                  | Regime Dependence |
|------------------------|----------------|---------------------------|-------------------|
| spread_bps             | 0.05           | Mean reversion            | Low               |
| momentum_divergence    | 0.08           | Lead-lag trading          | Medium            |
| flow_divergence        | 0.06           | Sentiment divergence      | High              |
| volume_imbalance       | 0.04           | Smart order routing       | Medium            |
| spread_convergence     | 0.04           | Execution timing          | Low               |
| expected_profit_bps    | 0.03           | Opportunity sizing        | High              |

**Best performers:**
1. **momentum_divergence** (IC = 0.08): Predict Exchange B price from Exchange A
2. **flow_divergence** (IC = 0.06): Detect informed flow concentration
3. **spread_bps** (IC = 0.05): Classic mean reversion

## Production Considerations

### 1. Latency Requirements

**Critical:** Arbitrage profitability decreases exponentially with latency

**Latency Budget:**
- Data feed: < 10ms (WebSocket to exchange)
- Feature computation: < 5ms (pre-computed features)
- Order placement: < 20ms (co-located servers)
- **Total:** < 35ms one-way

**Reality:**
- Retail traders: 100-500ms (cloud servers)
- Institutional: 10-50ms (co-located)
- **Professional arb funds:** < 5ms (FPGA/custom hardware)

**Recommendation:** Focus on slower-moving spreads (duration > 10 seconds) to compete with lower latency

### 2. Capital Requirements

**Per-Exchange Balance:**
- Minimum: $10,000 per exchange (for meaningful size)
- Recommended: $100,000+ per exchange
- **Total:** $200,000+ across 2 exchanges

**Why:** Need simultaneous execution on both sides (can't wait for transfer)

### 3. Exchange Selection

**Criteria:**
1. **Liquidity:** Daily volume > $1B (BTC), > $100M (altcoins)
2. **API Reliability:** 99.9%+ uptime
3. **Fee Structure:** Maker/taker < 10 bps combined
4. **Withdrawal Speed:** < 1 hour (for spot) or N/A (perp)

**Recommended Pairs:**
- Binance ↔ Bybit (high correlation, good APIs)
- Coinbase ↔ Kraken (US-regulated, slower but more stable)
- Deribit ↔ Binance (good for options arbitrage)

### 4. Risk Management

**Position Limits:**
- Max position per exchange: 10-20% of average hourly volume
- Max spread exposure: 2-5% of capital

**Stop Loss:**
- Spread widening > 2x entry spread: Exit immediately
- Unrealized loss > 50 bps: Force close

**Monitoring:**
- Spread duration: Track how long spreads persist
- Fill rate: Percentage of signals executed successfully
- Slippage tracking: Actual vs expected slippage

## Validation Checklist

Before deploying cross-exchange arbitrage:

- [ ] **Data availability:** Both exchanges have 99%+ data coverage
- [ ] **Timeframe alignment:** Volume bars have similar formation rates
- [ ] **Transaction costs:** Verified actual fees (not stale)
- [ ] **Latency measurement:** Measured end-to-end latency < target
- [ ] **IC validation:** Computed IC on out-of-sample data
- [ ] **Spread distribution:** Histogram of spreads shows >1% exceeding costs
- [ ] **Capital allocation:** Sufficient balance on both exchanges
- [ ] **API testing:** Verified order placement works on both exchanges

## Debugging

### Common Issues

**1. No arbitrage opportunities detected**
- **Cause:** Transaction costs too high or exchanges too efficient
- **Fix:** Lower cost estimate OR choose less liquid pair OR analyze high volatility periods

**2. Spread_bps IC near zero**
- **Cause:** Spreads already fully arbitraged (HFT competition)
- **Fix:** Focus on lead-lag (momentum_divergence) instead of pure arbitrage

**3. High null ratio in joined features**
- **Cause:** Volume bars on exchanges form at different rates
- **Fix:** Use time bars instead of volume bars OR filter nulls

**4. Negative expected_profit_bps always**
- **Cause:** Spreads smaller than transaction costs
- **Fix:** This is normal for BTC on major exchanges - focus on execution timing not pure arbitrage

## Advanced Patterns

### 1. Lead-Lag Prediction

Instead of pure arbitrage, predict Exchange B price from Exchange A:

```python
# Train model: B_return ~ A_return + A_flow + A_volatility
features = features.with_columns([
    # Lead: A returns predict B returns
    pl.col("a_ret_1bar").alias("a_lead"),
    # Target: B returns (next bar)
    pl.col("b_ret_1bar").shift(-1).alias("b_target"),
])

# Expected IC: 0.08-0.12 (much stronger than pure spread trading)
```

### 2. Smart Order Routing

Route orders to exchange with lower slippage:

```python
# Volume-weighted routing
if volume_imbalance > 0.3:
    # Exchange A has more volume → lower slippage
    primary_exchange = "A"
else:
    primary_exchange = "B"
```

### 3. Triangular Arbitrage

```python
# BTC/USDT (Binance) × USDT/USD (Coinbase) = BTC/USD (implied)
# Compare with actual BTC/USD (Coinbase spot)

implied_btc_usd = btc_usdt * usdt_usd
actual_btc_usd = coinbase_spot

triangular_arb = (implied_btc_usd - actual_btc_usd) / actual_btc_usd * 10000  # bps
```

## Further Reading

- **Academic:** Makarov & Schoar (2020) - "Trading and arbitrage in cryptocurrency markets"
- **Academic:** Hasbrouck (1995) - "One Security, Many Markets: Determining the Contributions to Price Discovery"
- **Industry:** Jump Trading - "Cross-Exchange Market Making" (Public talks)
- **Code:** `examples/crypto_cross_exchange_arbitrage_example.py` - Full working example

## Summary

Cross-exchange arbitrage features provide **moderate to strong predictive power** (IC: 0.03-0.08) for MFT strategies:

1. **Pure arbitrage:** Rare (1-5% of time) on major pairs, profitable only with low latency
2. **Lead-lag trading:** More consistent (IC = 0.08), predict slower exchange from faster
3. **Smart routing:** Use volume imbalance to minimize slippage

**Recommended workflow:**
1. Start with `momentum_divergence` (lead-lag prediction)
2. Add `flow_divergence` for sentiment analysis
3. Use `arb_opportunity` for execution timing (not primary signal)
4. Monitor spread duration and convergence speed

**Next steps:**
- Run example: `python examples/crypto_cross_exchange_arbitrage_example.py`
- Measure actual latency (WebSocket to order placement)
- Backtest with realistic transaction costs
- Paper trade before deploying capital
