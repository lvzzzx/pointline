# 可转债 T+0 + L3 Microstructure Strategy Guide

> Date: 2026-02-18
> Subject: Deep dive into the optimal strategy without 融券 access

---

## Executive Summary

**可转债 T+0 + L3** combines the **structural advantage** of convertible bonds (true T+0) with the **informational advantage** of Level 3 order book data. This is arguably the **best available strategy** for traders without 融券 access.

**Expected Performance:**
- Sharpe ratio: 2.0-4.0
- IC: 0.04-0.12
- Capacity: 20-50B CNY
- Holding period: 10 min - 4 hours

---

## The Core Edge: Three Layers of Alpha

```
Layer 1: T+0 Structure      → No overnight risk, intraday round-trips
Layer 2: Higher Volatility  → ±20% limits vs ±10% for stocks  
Layer 3: L3 Microstructure  → Order flow, toxicity, informed trading detection
         ↓
    Combined Edge: IC 0.04-0.12, Sharpe 2.0-4.0
```

---

## 1. Data Availability (Quant360 ConFI)

可转债 has the **same L2/L3 data infrastructure** as stocks:

| Data Type | File Pattern | Availability | Notes |
|-----------|--------------|--------------|-------|
| **Order stream (L3)** | `order_new_ConFI_SH/SZ_<date>.7z` | ✅ SSE & SZSE | Same schema as stocks |
| **Trade stream (tick)** | `tick_new_ConFI_SH/SZ_<date>.7z` | ✅ SSE & SZSE | Tick-by-tick executions |
| **L2 snapshots** | `L2_new_ConFI_SZ_<date>.7z` | ✅ SZSE only | 10-level book |
| **Schema** | Same as `STK` | No special parsing | Reuse stock infrastructure |

**Key advantage:** You can reuse your stock L3 infrastructure for 可转债.

---

## 2. Why 可转债 is Better Than Stocks for L3 Strategies

### Comparison Table

| Feature | Stocks | 可转债 | Advantage |
|---------|--------|--------|-----------|
| **Settlement** | T+1 | **T+0** | ✅ Round-trip same day |
| **Price limits** | ±10% | **±20%** | ✅ Bigger moves, more alpha |
| **Day 1 limits** | ±44%/-36% | **±57.3%/-43.3%** | ✅ Extreme volatility capture |
| **Short selling** | Limited (融券) | **Not available** | ❌ Same constraint |
| **Bond floor** | N/A | **~100 CNY** | ✅ Downside protection |
| **Retail participation** | 60-70% | **70-80%** | ✅ More behavioral alpha |
| **Options embedded** | No | **Yes** | ✅ Convexity effects |
| **Institutional coverage** | High | **Lower** | ✅ Less efficient pricing |
| **Tick size** | 0.01 CNY | 0.01 CNY | Same |
| **Lot size** | 100/200 shares | **10 units** | ✅ Lower entry barrier |

### Key Insight: Retail Dominance

**可转债 markets are even more retail-dominated than stocks:**
- Retail investors: ~70-80% of turnover
- Institutional investors: ~20-30%
- **Result:** More behavioral biases, overreaction, herding → exploitable patterns

### Bond Floor Protection

```
Scenario: Market crash, stock down -10%

Stock: Direct -10% loss
可转债: Bond floor at ~100 CNY limits downside
        Typical max loss: -5% to -8% (unless credit event)

This asymmetry creates better risk-adjusted returns.
```

---

## 3. L3 Feature Engineering for 可转债

### Category A: Order Flow Features (L3)

| Feature | Formula | Predictive Logic | Expected IC |
|---------|---------|------------------|-------------|
| **OFI (Order Flow Imbalance)** | `sum(buy_qty - sell_qty)` / `total_qty` | Aggressive buying predicts short-term rise | 0.03-0.06 |
| **Cancel Imbalance** | `(bid_cancels - ask_cancels)` / `total_cancels` | One-sided cancellation = liquidity withdrawal | 0.02-0.04 |
| **Add/Cancel Ratio** | `new_orders` / `cancels` | Low ratio = spoofing or uncertainty | 0.01-0.03 |
| **Large Order Detection** | Orders > 95th percentile size | Institutional footprint | 0.02-0.05 |
| **Order Arrival Asymmetry** | `buy_arrival_rate` / `sell_arrival_rate` | Directional pressure before execution | 0.02-0.04 |
| **Fleeting Liquidity** | Orders cancelled < 100ms | Unreliable depth, HFT activity | 0.01-0.02 |
| **Queue Position** | Depth at best bid/ask | Rapid depletion = imminent move | 0.02-0.04 |

**可转债-specific considerations:**
- Tick size same as stocks (0.01 CNY)
- Lot size: 10 units (vs 100/200 shares for stocks)
- **Higher tick-to-price ratio** for cheap bonds (< 110 CNY) → wider effective spreads

### Category B: Trade Flow Features (Tick)

| Feature | Formula | Predictive Logic | Expected IC |
|---------|---------|------------------|-------------|
| **Aggressor Volume** | `buy_aggressor_vol` - `sell_aggressor_vol` | Net buying pressure | 0.03-0.06 |
| **Trade Direction Runs** | Consecutive same-side trades | Long runs = momentum; breaks = reversal | 0.02-0.05 |
| **Signed Autocorrelation** | `corr(sign_t, sign_{t-5})` | Positive = trending; negative = mean-reverting | 0.02-0.04 |
| **Trade Clustering** | Trades within X milliseconds | Algo/iceberg detection | 0.01-0.03 |
| **Volume Acceleration** | `d(volume)/dt` | Sudden activity = information arrival | 0.02-0.04 |
| **Large Trade Ratio** | Volume from trades > 95th pct / total | Institutional proxy | 0.02-0.04 |
| **Retail Intensity** | Min-lot trades / total count | Retail participation (mean-reversion signal) | 0.02-0.04 |

### Category C: Bond-Specific Microstructure

| Feature | Formula | Predictive Logic | Expected IC |
|---------|---------|------------------|-------------|
| **Conversion Premium Change** | `Δ(instant_premium)` | Premium contraction = bullish | 0.03-0.06 |
| **Implied Vol Change** | `Δ(IV_from_pricing_model)` | IV spike = event anticipation | 0.02-0.04 |
| **Bond-Stock Lead-Lag** | `corr(return_bond, return_stock_lag)` | Which market leads? | 0.02-0.05 |
| **Delta-Neutral Volume** | Volume not explained by delta | Pure convexity trading | 0.01-0.03 |
| **Premium Z-Score** | `(premium - avg_premium) / std_premium` | Extreme premium = reversal | 0.02-0.04 |
| **Distance to Bond Floor** | `(price - 100) / 100` | Near floor = limited downside | 0.01-0.02 |

---

## 4. Strategy Architecture

### Framework: Two-Model Approach

```
┌─────────────────────────────────────────────────────────────┐
│                    BOND SELECTION MODEL                      │
│                   (Daily Frequency)                          │
│  ── Filters for trading universe (liquidity, premium, etc.)  │
└───────────────────────┬─────────────────────────────────────┘
                        │ Top 50-100 bonds
┌───────────────────────▼─────────────────────────────────────┐
│                 MICROSTRUCTURE SIGNAL MODEL                  │
│               (Intraday, L3-Based)                          │
│  ── Predicts 5-60 min returns using order flow              │
└───────────────────────┬─────────────────────────────────────┘
                        │ Entry signals
┌───────────────────────▼─────────────────────────────────────┐
│                   EXECUTION ENGINE                           │
│  ── T+0 entry/exit, risk controls, position sizing          │
└─────────────────────────────────────────────────────────────┘
```

### Step 1: Universe Selection (Daily)

```python
def select_universe(date):
    bonds = all_convertible_bonds where:
        # Liquidity filters (critical for L3 strategies)
        - daily_turnover_20d > 30_000_000  # 30M CNY minimum
        - avg_spread_20d < 50bps           # Reasonable spread
        - min_depth_20d > 500_000          # 500K CNY at touch
        
        # Bond characteristics
        - bond_price between 100 and 140   # Avoid deep discounts/premiums
        - conversion_premium between 0% and 50%  # Active conversion zone
        - days_to_maturity > 90            # Avoid redemption pressure
        - implied_vol > 15%                # Sufficient optionality
        
        # Volatility filter
        - realized_vol_20d > 20%           # Enough movement to trade
        - avg_true_range > 1.0%            # Daily range sufficient
        
        # Credit filter
        - credit_rating >= AA              # Avoid default risk
        - no_recent_credit_events          # Clean history
        
        # Sector filter (optional, momentum overlay)
        - underlying_sector in active_themes  # AI, chips, EV, etc.
    
    return bonds  # Target: 50-100 bonds
```

### Step 2: Intraday Signal Generation (L3-Based)

```python
def generate_signals(bond, current_time):
    # L3 Features (5-15 min windows)
    l3_features = {
        # Order flow features
        'ofi_5m': calculate_ofi(bond, window='5min'),
        'ofi_15m': calculate_ofi(bond, window='15min'),
        'ofi_trend': calculate_ofi_slope(bond, window='10min'),
        
        'cancel_imbalance_5m': calculate_cancel_imbalance(bond, window='5min'),
        'large_order_ratio_5m': calculate_large_order_ratio(bond, window='5min'),
        'large_order_count_5m': count_large_orders(bond, window='5min', threshold=95),
        
        'order_arrival_asymmetry': calculate_arrival_asymmetry(bond, window='5min'),
        'fleeting_liquidity_ratio': calculate_fleeting_ratio(bond, max_hold_ms=100),
        
        # Book features (from L2 or reconstructed L3)
        'book_imbalance_l5': calculate_book_imbalance(bond, levels=5),
        'book_imbalance_l10': calculate_book_imbalance(bond, levels=10),
        'spread_bps': calculate_spread(bond) * 10000,
        'depth_at_touch_bid': calculate_depth(bond, side='bid', level=1),
        'depth_at_touch_ask': calculate_depth(bond, side='ask', level=1),
        'depth_imbalance': (depth_bid - depth_ask) / (depth_bid + depth_ask),
    }
    
    # Tick/Trade features
    tick_features = {
        'aggressor_imbalance_5m': calculate_aggressor_imbalance(bond, window='5min'),
        'aggressor_imbalance_15m': calculate_aggressor_imbalance(bond, window='15min'),
        
        'trade_run_length_avg': calculate_avg_trade_runs(bond, window='5min'),
        'trade_run_max': calculate_max_trade_runs(bond, window='5min'),
        
        'signed_acf_lag1': calculate_signed_autocorr(bond, lag=1),
        'signed_acf_lag5': calculate_signed_autocorr(bond, lag=5),
        
        'volume_accel': calculate_volume_acceleration(bond, window='5min'),
        'relative_volume': current_volume / expected_volume_tod(bond),
        
        'large_trade_ratio': calculate_large_trade_ratio(bond, percentile=95),
        'retail_intensity': calculate_retail_intensity(bond),  # Min-lot trades
        'trade_clustering': calculate_trade_clustering(bond, window_ms=1000),
    }
    
    # Bond-specific features
    bond_features = {
        'premium_current': calculate_conversion_premium(bond),
        'premium_change_5m': calculate_premium_change(bond, window='5min'),
        'premium_change_30m': calculate_premium_change(bond, window='30min'),
        'premium_zscore': calculate_premium_zscore(bond, lookback=20),
        
        'implied_vol_current': calculate_implied_vol(bond),
        'iv_change_5m': calculate_iv_change(bond, window='5min'),
        
        'delta_current': calculate_delta(bond),
        'gamma_exposure': calculate_gamma(bond) * position_size,
        
        'bond_stock_lead_lag': calculate_lead_lag(bond, stock, window='10min'),
        'bond_stock_beta': calculate_beta(bond, stock, lookback=20),
        
        'distance_to_floor': (bond_price - 100) / 100,
        'distance_to_ceiling': calculate_distance_to_conversion(bond),
    }
    
    # Cross-sectional features (relative to universe)
    cross_features = {
        'return_rank_5m': rank_return(bond, universe, window='5min'),
        'volume_rank': rank_volume(bond, universe),
        'ofi_rank': rank_ofi(bond, universe, window='5min'),
        'premium_rank': rank_premium(bond, universe),
    }
    
    # Combine all features
    features = {**l3_features, **tick_features, **bond_features, **cross_features}
    
    # Model prediction
    signal = ml_model.predict(features)  # Expected return next 15-30 min
    confidence = ml_model.predict_proba(features) if classification else None
    
    return signal, confidence
```

### Step 3: Entry/Exit Logic (T+0)

```python
class TradingEngine:
    def __init__(self):
        self.max_positions = 10
        self.position_size_max = 0.10  # 10% of portfolio per bond
        self.signal_threshold_long = 0.015  # 1.5% expected return
        self.signal_threshold_short = -0.015
        self.take_profit = 0.03  # 3%
        self.stop_loss = -0.02   # 2%
        self.max_hold_minutes = 120  # 2 hours
        self.flatten_time = time(14, 45)  # Force flatten before close
    
    def run(self, timestamp, universe):
        signals = {}
        
        # Generate signals for all bonds
        for bond in universe:
            signal, conf = generate_signals(bond, timestamp)
            signals[bond] = {'signal': signal, 'confidence': conf}
        
        # Rank by signal strength
        ranked = sorted(signals.items(), key=lambda x: x[1]['signal'], reverse=True)
        
        # Entry logic
        for bond, data in ranked:
            if len(self.positions) >= self.max_positions:
                break
            
            if bond in self.positions:
                continue  # Already in position
            
            signal = data['signal']
            
            # Long entry
            if signal > self.signal_threshold_long:
                if self.risk_checks_pass(bond, 'long'):
                    size = self.calculate_position_size(bond, signal)
                    self.enter_long(bond, size)
            
            # Note: True short not available for 可转债
            # Use relative value: long A / short B (if B also 可转债)
        
        # Exit logic (T+0 mandatory)
        for bond, position in list(self.positions.items()):
            current_pnl = self.calculate_unrealized_pnl(position)
            hold_time = timestamp - position.entry_time
            
            # Time-based exit
            if timestamp.time() >= self.flatten_time:
                self.close_position(position, reason='time_stop')
                continue
            
            if hold_time > timedelta(minutes=self.max_hold_minutes):
                self.close_position(position, reason='max_hold')
                continue
            
            # Profit target
            if current_pnl > self.take_profit:
                self.close_position(position, reason='take_profit')
                continue
            
            # Stop loss
            if current_pnl < self.stop_loss:
                self.close_position(position, reason='stop_loss')
                continue
            
            # Signal decay exit
            current_signal = signals.get(bond, {}).get('signal', 0)
            if abs(current_signal) < abs(position.entry_signal) * 0.3:
                self.close_position(position, reason='signal_decay')
                continue
    
    def risk_checks_pass(self, bond, direction):
        """Pre-trade risk checks"""
        checks = [
            # Liquidity
            self.get_current_spread(bond) < 0.005,  # < 50 bps
            self.get_current_depth(bond) > 300_000,  # > 300K CNY
            
            # Position limits
            self.get_sector_exposure(bond.sector) < 0.30,
            self.get_total_exposure() < 0.80,
            
            # Bond-specific
            bond.distance_to_conversion > 0.10,  # Not too close to conversion
            bond.days_to_maturity > 90,
            
            # Market conditions
            not self.is_market_stressed(),
            not bond.is_near_limit(),
        ]
        return all(checks)
```

---

## 5. Specific Strategy Patterns

### Pattern A: Order Flow Momentum

**Concept:** Persistent order flow imbalance predicts short-term price movement

**Detection:**
```python
ofi_5m = calculate_ofi(bond, window='5min')
volume_5m = calculate_volume(bond, window='5min')
ofi_normalized = ofi_5m / volume_5m  # -1 to +1

Entry signals:
- Long: ofi_normalized > +0.3 AND volume_5m > avg_volume * 1.5
- Short: ofi_normalized < -0.3 AND volume_5m > avg_volume * 1.5
```

**Entry:**
- Immediate market order OR
- Aggressive limit at best ask (long) / best bid (short)

**Exit conditions:**
- OFI reverts to near 0 (|ofi| < 0.1)
- 15-30 minutes elapsed
- Price target reached (+1.5% to +3%)
- Stop loss hit (-2%)

**Expected performance:**
- Win rate: 55-60%
- Reward/risk: 1.5:1
- Hold time: 10-30 minutes

---

### Pattern B: Toxicity Fade (VPIN-Based)

**Concept:** High toxicity (informed trading) predicts volatility; fade extreme moves

**Detection:**
```python
vpin = calculate_vpin(bond, window='5min')  # Volume-synchronized PIN
price_change_10m = calculate_return(bond, window='10min')

Entry signals:
- Short: vpin > 0.7 AND price_change_10m > +2.5%  (overbought)
- Long: vpin > 0.7 AND price_change_10m < -2.5%   (oversold)
```

**Logic:** High toxicity + extreme move = informed traders have already acted; retail chasing = reversal likely

**Entry:**
- Wait for first pullback (don't catch falling knife)
- Enter on volume confirmation of reversal

**Exit conditions:**
- VPIN normalizes (< 0.4)
- Price retraces 50% of extreme move
- 10-20 minutes elapsed

**Expected performance:**
- Win rate: 50-55% (lower but asymmetric)
- Reward/risk: 2:1 (asymmetric payoff)
- Hold time: 10-20 minutes

---

### Pattern C: Bond-Stock Lead-Lag

**Concept:** Bonds and stocks are connected via conversion option; detect which market leads

**Detection:**
```python
stock_return_5m = calculate_return(bond.underlying_stock, window='5min')
bond_return_5m = calculate_return(bond, window='5min')
premium_change = calculate_premium_change(bond, window='5m')

Entry signals:
- Long bond: stock_return_5m > +2% AND bond_return_5m < +1% AND premium_change < -1%
  (Stock moved, bond lagging, premium compressed → catch-up play)

- Short bond (relative): long bond A, short bond B where A has lagged more
```

**Logic:** Bond should eventually follow stock due to conversion arbitrage forces

**Entry:**
- Buy bond immediately on detection
- Hedge with short in another bond or ETF if available

**Exit conditions:**
- Premium returns to historical average
- Bond catches up to stock (> 80% of stock move)
- 20-40 minutes elapsed
- Stock reverses (invalidates premise)

**Expected performance:**
- Win rate: 52-58%
- Reward/risk: 1.2:1
- Hold time: 20-40 minutes

---

### Pattern D: Auction Imbalance (SZSE Closing Auction)

**Concept:** Closing auction (14:57-15:00) reveals end-of-day positioning

**Detection (14:50-14:57):**
```python
auction_imbalance = calculate_auction_imbalance(bond)  # SZSE only
moc_pressure = calculate_moc_order_flow(bond)

Entry signals (14:50-14:57):
- Long: auction_imbalance > +0.4 (heavy buy-side accumulation)
- Short: auction_imbalance < -0.4 (heavy sell-side pressure)
```

**Logic:** 
- Imbalance predicts auction close direction
- Can also predict next-day open (information persists overnight)
- T+0 allows holding through auction if desired

**Entry:**
- 14:50-14:57 based on accumulated auction orders

**Exit conditions:**
- Auction completion (15:00) — take auction close
- OR hold overnight and exit next morning (T+0 allows both)
- Next day: exit on next-day open VWAP

**Expected performance:**
- Win rate: 58-65% for auction direction
- Win rate: 55-60% for next-day open direction
- Reward/risk: 1.3:1

---

### Pattern E: Gamma Scalping (Advanced)

**Concept:** Exploit gamma of embedded option via delta hedging

**Detection:**
```python
delta = calculate_delta(bond)
gamma = calculate_gamma(bond)
vega = calculate_vega(bond)

Trade when:
- gamma > threshold (high convexity)
- vega cheap vs realized vol (option underpriced)
- Large price move expected (event catalyst)
```

**Execution:**
```
Buy bond (long gamma)
Hedge delta by shorting underlying stock (if possible)
Rebalance delta hedge as price moves (gamma scalping)
Profit from:
  1. Large move in either direction (gamma)
  2. Vega expansion if vol increases
```

**Note:** Requires ability to short underlying stock (融券) or use futures proxy

**Expected performance:**
- Win rate: 45-55% (lower, but convexity)
- Reward/risk: 3:1 (asymmetric)
- Complexity: High

---

## 6. Risk Management

### Specific Risks to 可转债 T+0

| Risk | Detection | Mitigation | Priority |
|------|-----------|------------|----------|
| **强赎 (Forced Conversion)** | Monitor 转股价/正股 price ratio > 130% | Avoid bonds near强赎 trigger (typically > 125%) | 🔴 Critical |
| **下修失败 (Conversion Price Cut Failure)** | Track shareholder meeting dates | Exit before vote if outcome uncertain | 🟡 High |
| **正股涨停 (Underlying Limit-Lock)** | Real-time limit monitoring | Skip if 正股 at limit (can't delta hedge) | 🔴 Critical |
| **Liquidity Evaporation** | Real-time spread/depth monitoring | Dynamic position sizing based on current liquidity | 🟡 High |
| **Credit Event** | News monitoring, credit spread widening | Rating filter (AA+), avoid distressed issuers | 🟠 Medium |
| **Gamma Squeeze** | Unusual price move + high gamma | Position size limits on high-gamma bonds | 🟡 High |
| **Sector Rotation** | Correlation spike in portfolio | Sector exposure limits (max 30%) | 🟠 Medium |

### Position Sizing Framework

```python
def calculate_position_size(bond, signal_strength):
    # Base size on signal
    base_size = portfolio_value * 0.10  # 10% max per bond
    signal_adjustment = min(abs(signal_strength) / 0.02, 1.0)  # Scale by signal
    
    # Liquidity adjustment
    daily_volume = bond.avg_daily_volume
    max_impact_size = daily_volume * 0.01  # Max 1% of ADV
    
    # Volatility adjustment
    vol = bond.realized_vol_20d
    vol_factor = 0.20 / max(vol, 0.20)  # Reduce size for high vol
    
    # Liquidity (spread/depth) adjustment
    spread = bond.current_spread
    depth = bond.current_depth
    liquidity_score = min(depth / 1_000_000, 1.0) * (0.005 / max(spread, 0.005))
    
    # Final size
    size = base_size * signal_adjustment * vol_factor * liquidity_score
    size = min(size, max_impact_size)
    size = min(size, portfolio_value * 0.10)  # Hard cap at 10%
    
    return size
```

### Intraday Risk Controls

```python
# Running checks every minute
risk_checks = {
    # Portfolio level
    'portfolio_drawdown': portfolio_pnl > -0.02,  # 2% daily loss limit
    'sector_concentration': max_sector_exposure < 0.30,
    'correlation_spike': avg_pairwise_correlation < 0.60,
    
    # Position level
    'per_position_loss': all(pos.unrealized_pnl > -0.03 for pos in positions),
    'limit_proximity': all(pos.bond.distance_to_limit > 0.03 for pos in positions),
    'time_stop': all(pos.hold_time < timedelta(hours=2) for pos in positions),
    
    # Market level
    'market_stress': not vix_equivalent_spiked(),
    'liquidity_adequate': overall_market_spread < 0.01,
}

if not all(risk_checks.values()):
    reduce_positions_by(0.50)  # Cut exposure in half
```

---

## 7. Technology Infrastructure

### Data Pipeline

```
Quant360 ConFI Feed
       ↓
Real-time Parser (same as stocks)
       ↓
Feature Engine (L3 calculations)
       ↓
ML Inference (sub-millisecond)
       ↓
Execution Engine
       ↓
Broker API (T+0 orders)
```

### Latency Requirements

| Component | Target Latency | Notes |
|-----------|----------------|-------|
| Data ingestion | < 10 ms | L3 tick-to-system |
| Feature calculation | < 5 ms | 50-100 features |
| Model inference | < 2 ms | LightGBM/Neural Net |
| Order submission | < 10 ms | To exchange |
| **Total round-trip** | **< 50 ms** | Critical for fast signals |

### Hardware Requirements

| Component | Specification | Purpose |
|-----------|-------------|---------|
| CPU | 32+ cores | Parallel feature calculation |
| RAM | 128+ GB | In-memory order book reconstruction |
| Storage | NVMe SSD | Historical data, model storage |
| Network | 10Gbps | Low-latency market data |
| Co-location | Exchange data center | Minimize latency (optional) |

---

## 8. Performance Expectations

### Realistic Targets (Without Capacity Constraints)

| Metric | Conservative | Moderate | Aggressive |
|--------|--------------|----------|------------|
| **Sharpe ratio** | 1.5-2.0 | 2.0-3.0 | 3.0-4.0 |
| **Annual return** | 15-25% | 25-40% | 40-60% |
| **Max drawdown** | 8-12% | 10-15% | 15-20% |
| **Win rate** | 52-55% | 54-58% | 56-62% |
| **Avg win/avg loss** | 1.2:1 | 1.4:1 | 1.6:1 |
| **Daily turnover** | 150-200% | 200-300% | 300-500% |
| **Positions/day** | 20-40 | 40-80 | 80-150 |

### Capacity Estimates

| Strategy Variant | Capacity | Bottleneck |
|------------------|----------|------------|
| Single bond, high frequency | 100-300M | Bond liquidity |
| 50-bond universe | 5-10B | Execution capacity |
| 100-bond universe | 10-20B | Operational complexity |
| Full universe (500 bonds) | 20-50B | Liquidity dispersion |

---

## 9. Comparison with Alternative Strategies

### 可转债 T+0 vs Other T+0 Options

| Dimension | 可转债 T+0 | ETF T+0 | 融券T+0 (Stocks) | Futures T+0 |
|-----------|-----------|---------|------------------|-------------|
| **Alpha potential** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Universe size** | ~500 | ~10 | ~500 (券源) | 4 contracts |
| **Volatility** | High (±20%) | Low-Medium | Medium | High |
| **Downside protection** | ✅ Bond floor | ❌ None | ❌ None | ❌ None |
| **L3 data value** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Short selling** | ❌ No | Via 融券 | ✅ Yes | ✅ Yes |
| **Complexity** | High | Low | Medium | Medium |
| **Retail access** | ✅ Full | ✅ Full | ❌ Limited | ✅ Qualified |
| **Sharpe potential** | 2.0-4.0 | 1.0-2.5 | 3.0-8.0 | 2.0-5.0 |

### Verdict

**可转债 T+0 + L3 is the best choice when:**
- ✅ You don't have 融券 access
- ✅ You want true T+0 without restrictions
- ✅ You can handle complexity of conversion math
- ✅ You want downside protection (bond floor)
- ✅ You want larger universe than ETFs

**Choose ETF T+0 instead when:**
- You want simplicity
- You don't need L3 data
- You prefer index-level exposure

**Choose Futures T+0 instead when:**
- You want pure leverage
- You don't need bond-specific features
- You want deepest liquidity

---

## 10. Implementation Roadmap

### Phase 1: Foundation (Weeks 1-4)

```
□ Set up Quant360 ConFI data feed
□ Build L3 parser (reuse stock infrastructure)
□ Implement basic order book reconstruction
□ Calculate simple features (OFI, spread, depth)
□ Backtest infrastructure with T+0 logic
```

### Phase 2: Signal Development (Weeks 5-12)

```
□ Feature engineering (50-100 features)
□ Label design (forward returns, 15-60 min)
□ Model training (LightGBM baseline)
□ Signal validation (IC, decay curves)
□ Paper trading
```

### Phase 3: Execution (Weeks 13-20)

```
□ Execution engine development
□ Risk management implementation
□ Position sizing logic
□ Real-time monitoring dashboard
□ Small capital live test
```

### Phase 4: Scaling (Weeks 21-30)

```
□ Gradual capital increase
□ Multi-bond portfolio optimization
□ Advanced ML models (if data supports)
□ Performance attribution
□ Strategy refinement
```

---

## Bottom Line

**可转债 T+0 + L3 is the optimal strategy for traders without 融券 access** because it:

1. ✅ **Exploits true T+0** — No overnight risk, full alpha extraction
2. ✅ **Uses fastest signals** — L3 microstructure fully exploitable
3. ✅ **Benefits from retail dominance** — 70-80% retail = behavioral alpha
4. ✅ **Provides downside protection** — Bond floor limits worst-case
5. ✅ **Offers large universe** — ~500 bonds vs ~10 ETFs
6. ✅ **Has high volatility** — ±20% limits create bigger moves

**The trade-off:** Higher complexity (conversion math, bond-specific risks) but worth it for the structural advantages.

**Expected outcome:** With proper execution, this strategy can achieve **Sharpe 2.0-4.0** with **20-50B CNY capacity** — competitive with institutional-grade strategies.

---

## References

- Quant360 ConFI data specification
- `cn-ashare-microstructure-researcher` skill (L3 features)
- `convertible_bonds_cn_ashare_trading.md` (bond basics)
- SSE/SZSE 可转债交易实施细则 (2025)

---

*Last updated: 2026-02-18*
