# T+0 ETF Strategies: Usage Among Top Chinese Quantitative Firms

> Date: 2026-02-18
> Subject: Analysis of T+0 ETF role in top-tier Chinese quant strategies

---

## Executive Summary

**T+0 ETF is NOT the primary strategy for top-tier quant firms**, but it IS used as a **complementary/ancillary strategy**.

Top firms focus more on:
1. **Stock selection with futures hedging** (T+1 rotation)
2. **融券T+0** (before 2024 regulatory restrictions)
3. **Futures-spot basis arbitrage**

---

## The Short Answer

| Question | Answer |
|----------|--------|
| Do top firms use T+0 ETF? | ✅ Yes, but as supporting strategy |
| Is it their primary alpha source? | ❌ No |
| Why not? | Capacity constraints, limited universe, crowded space |
| Who uses it as primary? | Mid-tier firms, prop desks, sophisticated retail |

---

## Why T+0 ETF is Secondary for Top Firms

### 1. Capacity Constraint

The most important factor: **top firms manage too much capital** for ETF strategies to be primary.

| Instrument | Universe Size | Typical Capacity | Suitable For |
|------------|---------------|------------------|--------------|
| **Individual stocks** | ~5,000 listed | Very Large (100B+) | Core allocation |
| **ETFs (liquid)** | ~10-20 names | Medium (5-20B) | Satellite allocation |
| **Index futures** | 4 main contracts | Large (50B+) | Hedging/core |
| **可转债** | ~500 bonds | Medium (10-30B) | Opportunistic |

**Top firms manage 50B-600B CNY** — ETF universe is too small for primary allocation.

**Example:**
- A 100B fund allocating 20% to ETFs = 20B
- With 10 liquid ETFs, that's 2B per ETF
- Many ETFs have daily turnover of 500M-2B — position size becomes problematic

### 2. Alpha Source Limitation

| Strategy | Alpha Type | Predictability | Capacity | Top Firm Preference |
|----------|------------|----------------|----------|---------------------|
| **Stock selection** | Cross-sectional, multi-factor | High | Very Large | ⭐⭐⭐⭐⭐ Primary |
| **Futures basis** | Market microstructure | High | Large | ⭐⭐⭐⭐ Core strategy |
| **ETF arbitrage** | Mean-reversion, technical | Medium | Medium | ⭐⭐⭐ Supporting |
| **可转债 T+0** | Event-driven, volatility | Medium | Medium | ⭐⭐⭐ Niche |
| **ETF pairs** | Relative value | Medium | Small | ⭐⭐ Limited use |

Top firms' edge comes from **stock-level factor models** with hundreds of features, not ETF technical patterns.

### 3. Competition & Margin Compression

**ETF T+0 space is crowded:**
- Lower barriers to entry (no 融券 needed, simpler data)
- Attracts more participants → **Sharpe ratios compressed**
- Strategies become commoditized faster

**Stock selection has higher barriers:**
- Requires sophisticated L2/L3 data infrastructure
- Complex factor research and alpha combination
- Better risk-adjusted returns at scale
- Harder to replicate

---

## How Top Firms Actually Use ETFs

### Usage 1: Hedging Layer (Most Common)

**Mechanism:**
```
Long stock portfolio (T+1) + Short 300ETF/500ETF (T+0)
→ Alternative to futures hedge
→ Better for sector-specific portfolios
```

**Example:**
- Portfolio: Long CSI 500 constituent stocks
- Traditional hedge: Short IC futures
- ETF hedge: Short 510500 (500ETF)
- **Advantage:** ETF short more precise for sector-specific portfolios than broad futures

**When used:**
- When futures basis is unfavorable
- For sector rotation strategies
- For precise beta adjustment

### Usage 2: Cash Equivalents (Common)

**Mechanism:**
```
Idle cash → Buy money-market ETFs or bond ETFs
→ Earn yield while waiting for signals
→ T+0 liquidity for quick deployment
```

**Examples:**
- 511880 (银华日利) — money market ETF
- 511990 (华宝添益) — money market ETF
- Bond ETFs for slightly higher yield

**Benefits:**
- T+0 liquidity (can exit same day if signal appears)
- Earns yield vs cash
- Lower volatility than equity ETFs

### Usage 3: Arbitrage Overlay (Less Common)

**Mechanism:**
```
ETF vs Component Basket arbitrage
→ Capture creation/redemption premium/discount
→ Requires sophisticated infrastructure
```

**Requirements:**
- Real-time basket composition data
- High-speed execution
- Large capital for creation/redemption units
- Prime broker relationships

**Typical returns:**
- 10-30 bps per trade
- Low capacity (creation units are large)
- Execution-intensive

### Usage 4: Sector Rotation (Niche)

**Mechanism:**
```
Rotate between sector ETFs based on macro/momentum signals
→ 300ETF vs 500ETF (large vs mid cap)
→ Sector ETFs (tech, finance, healthcare)
```

**Constraints:**
- Limited sector ETF liquidity in China
- Fewer sectors available vs US (SPY sector ETFs)
- Often better to trade underlying stocks

---

## What Top Firms Actually Do (The "Big Six")

The "六巨头" (Six Giants) of Chinese quant:
- **幻方量化 (High-Flyer)**
- **九坤投资 (Ubiquant)**
- **灵均投资 (Lingjun)**
- **明汯投资 (Minghong)**
- **诚奇资产 (Chenqi)**
- **衍复投资 (Yanfu)**

### Typical Strategy Allocation

| Strategy Category | Typical Allocation | Description | Notes |
|-------------------|-------------------|-------------|-------|
| **Stock T+1 rotation (hedged)** | 60-80% | Core alpha from stock selection | Multi-factor, ML-enhanced |
| **Index futures strategies** | 10-20% | Basis arbitrage, directional | IF/IH/IC/IM |
| **融券T+0** (pre-2024) | 5-15% | Intraday mean reversion | Now heavily restricted |
| **ETF strategies** | 3-8% | Hedging, cash management | Supporting only |
| **可转债/Options** | 2-5% | Niche opportunities | Event-driven |

### Key Characteristics of Top Firms

1. **Stock selection dominates** — 500-5000 factor models per firm
2. **Cross-sectional focus** — Ranking stocks within universe
3. **Futures hedging standard** — Market-neutral or controlled beta
4. **T+1 is acceptable** — Alpha survives overnight holding
5. **T+0 is enhancement** — Used opportunistically, not core

---

## Who Uses T+0 ETF as PRIMARY Strategy?

### Mid-Tier Quant Firms (10B-50B AUM)

**Why:**
- No 融券 access (restricted to top-tier)
- Need T+0 for risk management
- Simpler execution infrastructure
- Focus on shorter horizons

**Typical approach:**
- 50-70% T+0 ETF strategies
- 30-50% stock strategies with ETF hedge

### Proprietary Trading Desks

**Why:**
- Short-term alpha harvesting
- Lower capacity requirements
- Higher risk tolerance
- Faster strategy turnover

**Typical approach:**
- Pure intraday strategies
- High turnover (100%+ daily)
- Tight risk controls

### Sophisticated Retail/Private Traders

**Why:**
- Accessible T+0 without 融券
- Lower capital requirements
- Simpler than stock strategies
- Direct market access available

**Typical approach:**
- Momentum/mean reversion on 2-3 ETFs
- Technical indicator-based
- 1-5 positions simultaneously

### Foreign Quant Funds (QFII/RQFII)

**Why:**
- Regulatory constraints on stock shorting
- ETF provides liquid short vehicle
- Familiar instrument (ETFs global)
- Easier operational setup

---

## Comparative Analysis: Strategy Characteristics

| Dimension | Stock Selection (Top Firms) | T+0 ETF (Mid/Small Firms) |
|-----------|----------------------------|---------------------------|
| **Primary alpha** | Cross-sectional factors | Time-series patterns |
| **Signal decay** | Hours to days | Minutes to hours |
| **Holding period** | Overnight (T+1) | Intraday (T+0) |
| **Capacity** | Very large | Medium |
| **Sharpe ratio** | 1.5-3.0 | 1.0-2.0 |
| **Complexity** | High | Medium |
| **Data requirements** | L2/L3, fundamental, alternative | L1/L2, technical |
| **Infrastructure** | Heavy (HPC, low-latency) | Moderate |
| **Team size** | 50-200 researchers | 10-30 researchers |
| **Regulatory risk** | Moderate (融券 changes) | Low |

---

## Practical Implications by Participant Type

### Individual/Retail Traders

**Recommended Mix:**
```
60% 可转债 T+0 (primary alpha source)
30% ETF pairs (supporting, lower frequency)
10% Cash/money-market ETFs
```

**Rationale:**
- No 融券 access → need natural T+0 instruments
- 可转债 offers higher volatility + T+0
- ETFs provide diversification
- Lower capacity not a constraint for small capital

### Small Funds (100M-1B CNY)

**Recommended Mix:**
```
50% Cross-sectional stocks (T+1 rotation)
30% Cross-sectional with ETF hedge
15% 可转债 T+0
5% Cash ETFs
```

**Rationale:**
- Scale large enough for stock strategies
- ETF hedge simpler than futures for small teams
- 可转债 provides uncorrelated alpha
- Capacity matches fund size

### Mid-Tier Firms (1B-10B CNY)

**Recommended Mix:**
```
60% Stock selection (T+1, hedged with futures)
25% Futures basis/spread strategies
10% T+0 ETF/可转债 opportunistic
5% Experimental/research
```

**Rationale:**
- Futures hedging more efficient at scale
- T+0 strategies for excess capacity deployment
- Need to diversify alpha sources
- Research budget for new strategies

### Aspiring Top-Tier Firms

**Target Path:**
```
Phase 1: Master T+0 ETF/可转债 (build execution capability)
Phase 2: Add stock selection with ETF hedge (scale up)
Phase 3: Migrate to futures hedging (institutional standard)
Phase 4: Multi-horizon, multi-instrument integration
Phase 5: Alternative data, ML-driven alpha (top-tier)
```

---

## Key Insights

### 1. T+0 ETF is a "Gateway Strategy"

- **Entry point** for quant trading in China
- Lower barrier than 融券-requiring strategies
- Builds execution and risk management infrastructure
- **But:** To compete with top firms, must graduate to stock strategies

### 2. The Real Division

| Capability Level | Strategy Focus |
|------------------|----------------|
| **Beginner** | T+0 ETF, 可转债 (accessible T+0) |
| **Intermediate** | Stock selection + ETF hedge |
| **Advanced** | Stock selection + futures hedge + 融券 overlay |
| **Elite** | Multi-strategy, multi-horizon, alternative data |

### 3. Regulatory Environment Impact

**2023-2025 融券 restrictions have changed the landscape:**
- Before: Top firms used 融券T+0 extensively
- After: Everyone (including top firms) more reliant on T+1
- **Result:** T+0 ETF/可转债 relatively more attractive for everyone

### 4. Data Cost Consideration

| Data Type | Annual Cost | Justification |
|-----------|-------------|---------------|
| L1 (basic quotes) | Low | Essential |
| L2 snapshots | Medium | ETF strategies sufficient |
| L3 order-by-order | High | Need stock strategies to justify |

**Rule of thumb:** Don't pay for L3 unless running stock strategies.

---

## Bottom Line

### For most practitioners:

**T+0 ETF is an excellent starting point:**
- ✅ Works without 融券 access
- ✅ True intraday round-trips
- ✅ Lower complexity than stock strategies
- ✅ Builds foundational skills

**But recognize its limitations:**
- ❌ Limited universe (~10 liquid ETFs)
- ❌ Lower capacity than stocks
- ❌ More crowded = lower alpha margins
- ❌ Top firms don't rely on it primarily

### The path to competing with top firms:

```
T+0 ETF/可转债 → Stock selection (ETF hedge) → Stock selection (futures hedge) → Multi-strategy
     (Phase 1)         (Phase 2)                 (Phase 3)                      (Phase 4)
```

**T+0 ETF is a valuable stepping stone, not the destination.**

---

## References

- Industry reports on Chinese quant hedge funds (2024-2025)
- "六巨头" analysis from 21世纪经济报道, 私募排排网
- Strategy structure from cn-ashare-microstructure-researcher skill
- Regulatory updates on 融券 restrictions (CSRC 2023-2025)

---

*Last updated: 2026-02-18*
