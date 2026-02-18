# Futures HFT in Chinese A-Share Markets

> Date: 2026-02-18
> Subject: High-frequency trading strategies on CFFEX index futures (IF/IH/IC/IM)

---

## Executive Summary

**Futures HFT** is one of the most viable T+0 strategies in Chinese markets due to:
- ✅ **True T+0 settlement** (unlike stocks)
- ✅ **No short-selling restrictions** (unlike stocks)
- ✅ **Deep liquidity** in major contracts
- ✅ **Leverage** (5-8x) amplifies returns
- ✅ **Lead-lag relationship** with spot market
- ✅ **Lower costs** than stocks (no stamp duty)

**Expected Performance:**
- Sharpe ratio: 2.0-5.0
- Daily turnover: 500-2000%
- Win rate: 45-60% (depending on strategy)
- Capacity: Large (10B+ CNY possible)

---

## 1. Available Futures Contracts (CFFEX)

### Contract Specifications

| Contract | Underlying | Margin | Notional Value | Tick Size | Typical Spread |
|----------|------------|--------|----------------|-----------|----------------|
| **IF** | CSI 300 | ~12% | ~1.2M CNY | 0.2 index points | 0.2-0.5 points |
| **IH** | SSE 50 | ~12% | ~800K CNY | 0.2 index points | 0.2-0.4 points |
| **IC** | CSI 500 | ~14% | ~1.0M CNY | 0.2 index points | 0.4-0.8 points |
| **IM** | CSI 1000 | ~14% | ~600K CNY | 0.2 index points | 0.6-1.2 points |

### Trading Rules

| Parameter | Value | Notes |
|-----------|-------|-------|
| **Trading Hours** | 09:30-11:30, 13:00-15:00 | Same as stock market |
| **Settlement** | T+0 | Same-day round-trip |
| **Price Limits** | ±10% | Daily circuit breaker |
| **Last Trading Day** | Third Friday of expiry month | Monthly expiry |
| **Circuit Breaker** | ±10% daily limit | Hard stop |

### Trading Cost Structure

| Component | Stocks | Futures | Advantage |
|-----------|--------|---------|-----------|
| **Commission** | 0.02-0.03% | ~0.01-0.015% | ✅ 50% lower |
| **Stamp duty** | 0.05% (sell) | 0% | ✅ None |
| **Exchange fee** | Included | ~0.002-0.005% | Low |
| **Spread** | 3-20 bps | 1-5 bps | ✅ Tighter |
| **Total round-trip** | **~20-45 bps** | **~3-10 bps** | ✅ **3-5x cheaper** |

---

## 2. Why Futures HFT is Attractive

### 2.1 Structural Advantages

| Feature | Stocks | Futures | Impact on Strategy |
|---------|--------|---------|-------------------|
| **Settlement** | T+1 | **T+0** | ✅ True intraday round-trips |
| **Short selling** | Limited (融券) | **Unrestricted** | ✅ True market-neutral |
| **Leverage** | None | **5-8x** | ✅ Capital efficiency |
| **Stamp duty** | 0.05% (sell) | **0%** | ✅ Significant cost savings |
| **Price limits** | ±10%/±20% | **±10%** | ✅ Predictable risk bounds |
| **Shorting cost** | 8-18% annual (融券) | **None** | ✅ No lending fees |

### 2.2 Market Microstructure Advantages

| Aspect | Stocks | Futures | HFT Implication |
|--------|--------|---------|-----------------|
| **Centralized exchange** | SSE/SZSE | CFFEX | ✅ Single venue, no fragmentation |
| **Market maker program** | Limited | Active | ✅ Better liquidity provision |
| **Tick size** | 0.01 CNY | 0.2 index points | ✅ Appropriate granularity |
| **Co-location** | Available | Available | ✅ Low-latency access |
| **Depth** | Variable | Deep | ✅ Large size execution |

### 2.3 Price Discovery Relationship

```
Information flow:

Macro news → Futures (1-5 min lead) → Spot market (lag)
                    ↓
            Faster price discovery
                    ↓
         Trading opportunity: front-run spot with futures
```

**Key insight:** Futures often price in new information before the spot market, creating exploitable lead-lag relationships.

---

## 3. HFT Strategy Categories

### 3.1 Market Making (做市策略)

**Concept:** Provide liquidity by continuously quoting bid/ask, capture spread

**Mechanism:**
```python
def market_making_strategy(contract):
    # Quote around mid price
    mid = (best_bid + best_ask) / 2
    
    # Dynamic spread based on volatility
    spread = calculate_dynamic_spread(contract)
    
    # Place quotes
    bid_price = mid - spread/2 - 0.1  # Slightly inside
    ask_price = mid + spread/2 + 0.1
    
    bid_size = calculate_bid_size(contract)
    ask_size = calculate_ask_size(contract)
    
    place_order('BUY', bid_price, bid_size)
    place_order('SELL', ask_price, ask_size)
    
    # Profit when both sides fill
    profit_per_round = spread - 2 * commission
```

**Key Requirements:**
- **Latency:** < 1ms round-trip (co-location essential)
- **Quote rate:** 100-500 quotes/second per contract
- **Fill ratio:** Target 40-60% of quotes result in trades
- **Inventory management:** Keep net position close to zero

**Expected Performance:**
| Metric | Target |
|--------|--------|
| Profit per contract | 0.1-0.3 index points |
| Daily round-trips | 1,000-5,000 per contract |
| Win rate | 50-55% (small profits, tight stops) |
| Sharpe ratio | 2.0-4.0 |
| Capacity | 1-5B CNY |

**Risks:**
- **Adverse selection:** Informed flow hits your quotes
- **Inventory risk:** Position builds in trending market
- **Latency competition:** HFT arms race
- **Market stress:** Spreads widen, fills decrease

**Mitigation:**
- Toxic flow detection algorithms
- Aggressive inventory rebalancing
- Dynamic spread widening under stress
- Kill switches for abnormal conditions

---

### 3.2 Statistical Arbitrage (统计套利)

#### A) Futures-Spot Basis Arbitrage

**Concept:** Capture deviation between futures price and underlying index value

**Mechanism:**
```python
def basis_arbitrage(futures_price, spot_index, etf_price):
    basis = (futures_price - spot_index) / spot_index
    basis_pct = basis * 100
    
    # Historical basis statistics
    basis_mean = get_historical_mean(window='20d')
    basis_std = get_historical_std(window='20d')
    z_score = (basis_pct - basis_mean) / basis_std
    
    # Trading signals
    if basis_pct > 0.5 and z_score > 2.0:  # Futures expensive
        # Short futures + Buy ETF basket
        sell_futures()
        buy_etf_basket()
        
    elif basis_pct < -0.5 and z_score < -2.0:  # Futures cheap
        # Long futures + Sell ETF basket
        buy_futures()
        sell_etf_basket()
    
    # Exit when basis normalizes
    if abs(basis_pct) < 0.1:
        flatten_all_positions()
```

**Basis Dynamics:**
| Market Condition | Basis Behavior | Strategy |
|------------------|----------------|----------|
| **Contango (normal)** | Futures > Spot | Short futures at extreme |
| **Backwardation** | Futures < Spot | Long futures at extreme |
| **Before expiry** | Basis → 0 | Convergence trade |
| **High volatility** | Basis widens | Widen entry thresholds |

**Expected Performance:**
| Metric | Target |
|--------|--------|
| Profit per trade | 0.1-0.5% of notional |
| Win rate | 60-70% |
| Holding period | 1-30 minutes |
| Sharpe ratio | 1.5-3.0 |

**Challenge:** Requires simultaneous execution in futures and spot (ETF) markets

#### B) Cross-Contract Arbitrage

**Concept:** Trade relative value between related futures contracts

**Pairs:**
| Spread | Relationship | Trade Logic |
|--------|--------------|-------------|
| IF vs IH | CSI 300 vs SSE 50 | Large-cap vs mega-cap |
| IC vs IF | CSI 500 vs CSI 300 | Mid-cap vs large-cap |
| IM vs IC | CSI 1000 vs CSI 500 | Small-cap vs mid-cap |

**Mechanism:**
```python
def cross_contract_arbitrage():
    # Calculate spreads
    spread_if_ih = price_IF - price_IH * beta_if_ih
    spread_ic_if = price_IC - price_IF * beta_ic_if
    
    # Z-scores
    z_if_ih = (spread_if_ih - mean_if_ih) / std_if_ih
    z_ic_if = (spread_ic_if - mean_ic_if) / std_ic_if
    
    # Trading signals
    if z_if_ih > 2.0:
        # IF expensive relative to IH
        sell_IF()
        buy_IH()
    elif z_if_ih < -2.0:
        buy_IF()
        sell_IH()
```

**Expected Performance:**
| Metric | Target |
|--------|--------|
| Win rate | 55-65% |
| Profit per trade | 0.05-0.2 index points |
| Sharpe ratio | 1.5-2.5 |

---

### 3.3 Momentum/Trend Following (趋势跟踪)

**Concept:** Ride intraday momentum with ultra-fast entry/exit

**Mechanism:**
```python
def momentum_scalping(contract):
    # Short-term signals
    price_change_1m = calculate_return(contract, window='1min')
    price_change_5m = calculate_return(contract, window='5min')
    volume_ratio = current_volume / avg_volume_20d
    
    # Order flow
    ofi = calculate_order_flow_imbalance(contract, window='1min')
    trade_imbalance = calculate_trade_imbalance(contract, window='1min')
    
    # Entry signals
    if price_change_1m > 0.1 and volume_ratio > 2.0 and ofi > 0.5:
        # Breakout with volume and flow confirmation
        enter_long(contract)
        
    elif price_change_1m < -0.1 and volume_ratio > 2.0 and ofi < -0.5:
        enter_short(contract)
    
    # Exit management
    for position in open_positions:
        if unrealized_pnl(position) > 0.005:  # +0.5%
            take_profit(position)
        elif unrealized_pnl(position) < -0.003:  # -0.3%
            stop_loss(position)
        elif position.hold_time > 300:  # 5 minutes
            time_exit(position)
```

**HFT twist:** Enter within milliseconds of signal detection, hold briefly (scalping)

**Expected Performance:**
| Metric | Target |
|--------|--------|
| Win rate | 45-55% |
| Reward/risk | 1.5:1 to 2:1 |
| Sharpe ratio | 1.0-2.5 |
| Holding period | 30 seconds - 5 minutes |
| Daily turnover | 1000-3000% |

---

### 3.4 Lead-Lag Arbitrage (领先滞后)

**Concept:** Exploit futures leading spot by 1-5 minutes

**Mechanism:**
```python
def lead_lag_strategy():
    # Futures lead detection
    futures_return_1m = return_IF(window='1min')
    futures_return_5m = return_IF(window='5min')
    
    # Spot index lagging
    spot_return_1m = return_CSI300(window='1min')
    
    # Lead-lag correlation
    correlation = rolling_correlation(futures_return, spot_return, lag=1)
    
    # Signal: Futures moved, spot hasn't yet
    if futures_return_1m > 0.2 and spot_return_1m < 0.05:
        # Futures lead up, spot will follow
        buy_futures()
        
    elif futures_return_1m < -0.2 and spot_return_1m > -0.05:
        sell_futures()
    
    # Exit when spot catches up
    if abs(spot_return_1m - futures_return_1m) < 0.05:
        flatten_positions()
```

**Expected Performance:**
| Metric | Target |
|--------|--------|
| Win rate | 52-58% |
| Holding period | 2-10 minutes |
| Sharpe ratio | 1.2-2.0 |

---

### 3.5 Order Flow Imbalance (订单流不平衡)

**Concept:** Predict short-term price movement from aggressive order flow

**Mechanism:**
```python
def order_flow_strategy(contract):
    # Real-time order book analysis
    bid_depth = sum(bid_volumes[0:5])  # Top 5 levels
    ask_depth = sum(ask_volumes[0:5])
    book_imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth)
    
    # Recent trade flow
    buy_aggressor_vol = sum(trade.volume for trade in trades_1m if trade.aggressor == 'BUY')
    sell_aggressor_vol = sum(trade.volume for trade in trades_1m if trade.aggressor == 'SELL')
    trade_imbalance = (buy_aggressor_vol - sell_aggressor_vol) / (buy_aggressor_vol + sell_aggressor_vol)
    
    # Combined signal
    flow_signal = 0.6 * book_imbalance + 0.4 * trade_imbalance
    
    # Trading logic
    if flow_signal > 0.6:
        # Strong buying pressure
        long_futures(contract, hold_time=60)  # 1 minute
        
    elif flow_signal < -0.6:
        short_futures(contract, hold_time=60)
```

**Expected Performance:**
| Metric | Target |
|--------|--------|
| IC (1-5 min) | 0.03-0.06 |
| Win rate | 50-55% |
| Sharpe ratio | 1.5-3.0 |
| Holding | 30 seconds - 3 minutes |

---

## 4. Infrastructure Requirements

### 4.1 Latency Targets

| Component | Target Latency | Competitive Level |
|-----------|----------------|-------------------|
| Market data to system | < 100 μs | Elite: < 50 μs |
| Signal generation | < 50 μs | Elite: < 20 μs |
| Risk check | < 30 μs | Elite: < 10 μs |
| Order transmission | < 200 μs | Elite: < 100 μs |
| Exchange acknowledgment | < 500 μs | Variable |
| **Total round-trip** | **< 500 μs** | **Elite: < 200 μs** |

### 4.2 Hardware Requirements

| Component | Specification | Purpose |
|-----------|---------------|---------|
| **Servers** | 32+ cores, 3.5GHz+ | Parallel processing |
| **RAM** | 128+ GB | Order book reconstruction |
| **Storage** | NVMe SSD RAID | Low-latency logging |
| **Network** | 10GbE, kernel bypass | Sub-millisecond connectivity |
| **Co-location** | CFFEX data center (Shanghai) | Minimum latency |
| **FPGA** | Xilinx/Intel (optional) | Sub-microsecond processing |
| **Redundancy** | Hot standby servers | Failover capability |

### 4.3 Software Stack

| Layer | Technology | Notes |
|-------|------------|-------|
| **Kernel bypass** | DPDK, Solarflare OpenOnload, RDMA | Eliminate OS latency |
| **Language** | C++, Rust | Zero-overhead abstractions |
| **Order management** | Custom OMS | Microsecond-level |
| **Risk engine** | FPGA or optimized C++ | Pre-trade checks |
| **Data storage** | TimescaleDB, kdb+ | Tick data management |
| **Monitoring** | Real-time dashboards | Latency tracking |

### 4.4 Data Feeds

| Feed | Latency | Cost | Purpose |
|------|---------|------|---------|
| **CFFEX Level 2** | ~1-3 ms | Medium | 5-level book, tick data |
| **CFFEX Level 1** | ~1 ms | Low | Best bid/ask, last trade |
| **Spot index real-time** | ~1-2 ms | Low | Basis calculation |
| **ETF quotes** | ~2-5 ms | Medium | Basket execution |
| **News feed** | Variable | High | Event detection |

---

## 5. Risk Management

### 5.1 Specific Risks to Futures HFT

| Risk | Description | Mitigation |
|------|-------------|------------|
| **Flash crashes** | Sudden price dislocation ±10% | Circuit breakers; position limits; kill switches |
| **Adverse selection** | Informed flow hits your quotes | Toxic flow detection; dynamic spread adjustment |
| **Inventory buildup** | Net position accumulates | Aggressive flattening; max position limits |
| **Latency spikes** | Network/infra delays | Auto-cancel on delay > threshold; redundancy |
| **Fat fingers** | Erroneous large orders | Order size limits; price bands; confirmation |
| **Co-location failure** | Hardware/connectivity issues | Hot standby; backup lines; graceful degradation |
| **Market stress** | Volatility spike, liquidity dry-up | Dynamic spread widening; position reduction |

### 5.2 Position Controls

```python
risk_limits = {
    # Per-contract limits
    'max_position': {
        'IF': 100,  # contracts (~120M notional)
        'IH': 150,
        'IC': 100,
        'IM': 200
    },
    
    # Portfolio limits
    'max_gross_exposure': 0.50,  # 50% of capital
    'max_net_exposure': 0.20,    # 20% directional
    
    # Loss limits
    'max_daily_drawdown': 0.02,  # 2% of capital
    'max_loss_per_trade': 0.001, # 10 bps
    'max_loss_per_hour': 0.01,   # 1% per hour
    
    # Latency limits
    'max_latency_ms': 5,         # Cancel all if exceeded
    
    # Auto-flatten triggers
    'flatten_on': [
        'daily_loss > 2%',
        'latency > 5ms',
        'market_stress_detected',
        'manual_override'
    ]
}

# Pre-trade risk check (must complete < 50μs)
def pre_trade_check(order):
    checks = [
        order.size <= max_position[order.contract],
        order.price within daily_limit,
        gross_exposure + order.notional <= max_gross_exposure,
        latency_current < max_latency_ms,
        not market_stress_flag,
    ]
    return all(checks)
```

### 5.3 Kill Switch Protocol

```python
class KillSwitch:
    def __init__(self):
        self.triggers = {
            'daily_loss': -0.02,      # -2% capital
            'latency_spike': 5,       # 5ms
            'position_buildup': 200,  # 200 contracts
            'spread_widening': 2.0,   # 2x normal spread
        }
    
    def monitor(self):
        if any(trigger.activated() for trigger in self.triggers):
            self.emergency_flatten()
            self.cancel_all_orders()
            self.notify_operators()
            self.halt_trading()
```

---

## 6. Comparison with Other T+0 Strategies

### 6.1 Strategy Comparison Matrix

| Dimension | Futures HFT | 可转债 T+0 | 融券T+0 (Stocks) | ETF T+0 |
|-----------|-------------|------------|------------------|---------|
| **Infrastructure** | ⭐⭐⭐⭐⭐ Very High | ⭐⭐⭐ Medium | ⭐⭐⭐⭐ High | ⭐⭐ Low |
| **Latency required** | < 1ms | < 10ms | < 5ms | < 50ms |
| **Capital efficiency** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Sharpe potential** | 2.0-5.0 | 2.0-4.0 | 3.0-8.0 | 1.0-2.5 |
| **Capacity** | Large (10B+) | Medium (20B) | Limited | Small (5B) |
| **Complexity** | ⭐⭐⭐⭐⭐ Very High | ⭐⭐⭐⭐ High | ⭐⭐⭐⭐ High | ⭐⭐ Medium |
| **Short selling** | ✅ Unlimited | ❌ No | ✅ Yes | Via 融券 |
| **Universe** | 4 contracts | ~500 bonds | ~500 stocks | ~10 ETFs |
| **Trading costs** | ⭐⭐⭐⭐⭐ Very Low | ⭐⭐⭐⭐ Low | ⭐⭐⭐ Medium | ⭐⭐⭐⭐ Low |
| **Leverage** | ⭐⭐⭐⭐⭐ 5-8x | None | None | None |

### 6.2 When to Choose Each Strategy

| Your Situation | Best Strategy | Why |
|----------------|---------------|-----|
| **Have HFT infrastructure** | Futures HFT | Capital efficiency, lowest costs |
| **No 融券, have L3 data** | 可转债 T+0 | Best available without 融券 |
| **Have 融券 access** | 融券T+0 + L3 | Highest theoretical returns |
| **Limited infrastructure** | ETF T+0 | Lowest barrier to entry |
| **Large capital (10B+)** | Futures HFT | Capacity matches size |
| **Research-focused team** | 可转债 T+0 | More alpha research, less infra |
| **Engineering-first team** | Futures HFT | Infra is the edge |

---

## 7. Who Should Pursue Futures HFT?

### 7.1 ✅ Suitable For

#### 1. Top-Tier Quant Funds
- Existing HFT infrastructure
- Co-location already in place
- Can hire FPGA/microwave engineers
- 50M+ CNY technology budget

#### 2. Prop Trading Firms
- Risk tolerance for HFT
- Technology-first culture
- Experience in other HFT markets (US, Europe)
- Fast decision-making

#### 3. Well-Funded Startups
- 100M+ CNY initial capital
- Experienced HFT team (ex-Jane Street, Citadel, etc.)
- 2-3 year runway to profitability
- VC backing for technology investment

### 7.2 ❌ Not Suitable For

#### 1. Individual Traders
- Infrastructure costs prohibitive (5-20M CNY setup)
- Cannot compete on latency with institutions
- Regulatory requirements (qualified investor)

#### 2. Small Funds (< 500M CNY)
- Technology investment not justified by capacity
- Better risk-adjusted returns from simpler strategies
- Focus should be on alpha research, not infra

#### 3. Research-First Teams
- HFT is 80% engineering, 20% research
- Requires different skill set (systems programming, networking)
- Alpha decay is fast; infrastructure is the moat

#### 4. Risk-Averse Managers
- HFT has fat tail risk (flash crashes)
- Technology failures can cause large losses quickly
- Requires 24/7 monitoring and on-call rotation

---

## 8. Implementation Roadmap

### Phase 1: Foundation (Months 1-6)

```
□ Secure CFFEX membership and trading permissions
□ Establish co-location at CFFEX data center
□ Build basic market data infrastructure
□ Implement order management system (OMS)
□ Set up risk management framework
□ Build monitoring and alerting systems
```

**Investment:** 5-10M CNY
**Team:** 5-10 people (infra, devops, trading)

### Phase 2: Strategy Development (Months 4-12)

```
□ Develop market making algorithms
□ Build basis arbitrage capabilities
□ Implement order flow models
□ Backtest with tick-accurate simulation
□ Paper trading and latency measurement
□ Optimize execution paths
```

**Investment:** Additional 3-5M CNY
**Team:** Add 3-5 quantitative researchers

### Phase 3: Live Trading (Months 10-18)

```
□ Small capital live test (10-50M CNY)
□ Gradual capacity increase
□ Performance monitoring and optimization
□ Strategy diversification
□ Risk model validation
```

**Investment:** Trading capital 100-500M CNY
**Target:** Sharpe > 1.5 net of all costs

### Phase 4: Scale (Months 18-36)

```
□ Full capacity deployment
□ Advanced strategies (ML, alternative data)
□ Multi-contract optimization
□ Cross-market arbitrage
□ Continuous improvement
```

**Investment:** Trading capital 1-10B CNY
**Target:** Sharpe > 2.5, capacity 10B+

---

## 9. Performance Expectations

### Realistic Targets by Experience Level

| Metric | Beginner (Year 1) | Intermediate (Year 2) | Elite (Year 3+) |
|--------|-------------------|----------------------|-----------------|
| **Sharpe ratio** | 1.0-2.0 | 2.0-3.5 | 3.0-5.0 |
| **Daily return** | 0.03-0.05% | 0.05-0.10% | 0.08-0.15% |
| **Max drawdown** | 5-10% | 3-6% | 2-4% |
| **Win rate** | 48-52% | 50-55% | 52-58% |
| **Daily turnover** | 500-1000% | 1000-2000% | 2000-5000% |
| **Technology cost** | 5-10% of PnL | 3-5% of PnL | 2-3% of PnL |

### Benchmark Comparison

| Strategy Type | Sharpe | Capacity | Complexity |
|---------------|--------|----------|------------|
| **Futures HFT** | 2.0-5.0 | 10B+ | ⭐⭐⭐⭐⭐ |
| **可转债 T+0 + L3** | 2.0-4.0 | 20-50B | ⭐⭐⭐⭐ |
| **融券T+0 (if available)** | 3.0-8.0 | 5-10B | ⭐⭐⭐⭐ |
| **ETF T+0** | 1.0-2.5 | 5-10B | ⭐⭐ |
| **Stock selection (T+1)** | 1.5-3.0 | 100B+ | ⭐⭐⭐ |

---

## 10. Regulatory Considerations

### CFFEX Trading Rules

| Rule | Implication |
|------|-------------|
| **Position limits** | Max 1,200 contracts for IF near expiry |
| **Large trader reporting** | >500 contracts requires disclosure |
| **Order-to-trade ratio** | >50:1 monitored for spoofing |
| **Circuit breakers** | ±10% daily limit |
| **Settlement** | T+0, cash-settled |

### Compliance Requirements

- Real-time monitoring of position limits
- Order audit trails (5-year retention)
- Reporting of large positions
- Anti-manipulation controls
- Best execution policies

---

## Bottom Line

### Futures HFT is the best choice IF you have:

✅ **Deep technology expertise** — Low-latency systems, kernel bypass, possibly FPGA  
✅ **Significant capital for infrastructure** — 10-50M CNY setup cost  
✅ **Low-latency trading experience** — Team with HFT background  
✅ **Risk tolerance for HFT** — Comfortable with technology and market risks  
✅ **Co-location access** — Physical presence at CFFEX data center  
✅ **Engineering-first culture** — Infrastructure is the primary edge  

### Futures HFT is NOT the best choice IF you:

❌ **Lack co-location access** — Latency disadvantage is fatal  
❌ **Cannot invest in sub-millisecond infrastructure** — Cannot compete  
❌ **Prefer research over engineering** — HFT is 80% engineering  
❌ **Have limited capital (< 500M CNY)** — Setup cost not justified  
❌ **Are risk-averse** — Flash crashes and tech failures happen  

### The Final Ranking (No Capacity Constraints)

| Rank | Strategy | Sharpe | Best For |
|------|----------|--------|----------|
| 🥇 | **融券T+0 + L3** | 3.0-8.0 | Those with 融券 access |
| 🥈 | **Futures HFT** | 2.0-5.0 | **HFT-capable firms without 融券** |
| 🥉 | **可转债 T+0 + L3** | 2.0-4.0 | Research-focused teams |
| 4 | **Futures medium-frequency** | 1.5-3.0 | Standard infrastructure |
| 5 | **ETF T+0** | 1.0-2.5 | Minimal infrastructure |

**Futures HFT is the best T+0 strategy for firms with HFT capability but without 融券 access.**

---

## References

- CFFEX trading rules and specifications
- cn-ashare-microstructure-researcher skill (market structure)
- Industry reports on Chinese futures market microstructure
- Academic literature on HFT in index futures

---

*Last updated: 2026-02-18*
