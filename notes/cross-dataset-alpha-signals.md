# Cross-Dataset Alpha Signals Exploration

**Date**: 2025-02-15
**Analyst**: Kimi Code CLI
**Datasets**: options_chain (Deribit), derivative_ticker (BitMEX), liquidations (Deribit)

---

## Executive Summary

This note documents alpha signal discoveries from analyzing three ingested datasets:
- **options_chain**: 16.6M rows (Deribit, Sep 1, 2020) - BTC & ETH options
- **derivative_ticker**: 189K rows (BitMEX, Sep 1, 2020) - 5 perpetuals funding/OI
- **liquidations**: 944 rows (Deribit, Sep 1, 2021) - BTC & ETH perp liquidations

**Key Finding**: DeFi Summer 2020 created massive dislocations between options IV and perp funding costs, creating clear arbitrage opportunities.

---

## 1. Volatility Regime Analysis (Options Chain)

### 1.1 IV Comparison: ETH vs BTC (Sep 1, 2020)

| Asset | Avg IV | Median IV | Max IV | Observation |
|-------|--------|-----------|--------|-------------|
| **ETH** | 102% | 97% | 190% | Extreme vol from DeFi mania |
| **BTC** | 70% | 68% | 150% | Relatively calm |
| **Spread** | +32pp | +29pp | +40pp | ETH 45% premium to BTC |

**Signal**: ETH's vol premium was historically extreme — mean reversion candidate or structural shift?

### 1.2 Put/Call Skew Analysis

| Asset | 25d Call IV | 25d Put IV | Skew | Interpretation |
|-------|-------------|------------|------|----------------|
| BTC | 58.2% | 56.9% | +1.3% | Balanced sentiment |
| ETH | 101.6% | 88.7% | **+12.9%** | Extreme upside chasing |

**Alpha Signal**: ETH call skew >10% suggests euphoria. Contrarian indicator when combined with high funding.

### 1.3 IV Term Structure (ATM Options)

**BTC Term Structure**:
- Sep 1 (0d): 45% IV
- Sep 2 (1d): 38% IV
- Sep 11 (10d): 44% IV
- Sep 25 (24d): 49% IV

**ETH Term Structure**:
- Sep 1 (0d): 71% IV
- Sep 4 (3d): 88% IV ← Spike!
- Sep 25 (24d): 83% IV
- Mar 26 (207d): 96% IV

**Calendar Spread Opportunity**: Sep 3 expiry showed 88% IV vs Sep 1 at 71% — 17 point premium for 2 days.

### 1.4 High Gamma Opportunities

Top gamma scalping candidates (Gamma > 0.07, OTM, reasonable IV):
- **ETH-1SEP20-4450-P**: Gamma 0.078, Delta -0.35, IV 67%
  - Same-day expiry, high gamma, cheap vol
  - Strategy: Delta-hedged gamma scalping

---

## 2. Funding Rate Analysis (Derivative Ticker)

### 2.1 BitMEX Funding Hierarchy (Sep 1, 2020)

| Asset | Avg 8h Funding | Annualized | Max OI | Signal Strength |
|-------|----------------|------------|--------|-----------------|
| **ETHUSD** | 0.0765% | **83.7%** | 382M | 🔴 Extreme long bias |
| LTCUSD | 0.0494% | 54.1% | 6.9B | 🟡 Elevated |
| BCHUSD | 0.0314% | 34.4% | 2.5B | 🟡 Elevated |
| XRPUSD | 0.0186% | 20.4% | 24B | 🟢 Normal |
| XBTUSD | 0.0151% | 16.6% | 787B | 🟢 Normal |

**Key Insight**: Long ETH perps cost 83% annually — cheaper to buy options!

### 2.2 Mark-Index Basis (Perp Efficiency)

| Asset | Avg Basis (bps) | Basis Vol | Max Deviation |
|-------|-----------------|-----------|---------------|
| ETHUSD | 4.13 | 2.56 | 10.08 bps |
| LTCUSD | 2.79 | 2.63 | 9.48 bps |
| BCHUSD | 1.79 | 1.65 | 6.70 bps |
| XRPUSD | 0.83 | 1.52 | 5.14 bps |
| XBTUSD | 0.80 | 0.59 | 2.54 bps |

**Observation**: All spreads <0.1% — efficient perpetual pricing across all assets.

---

## 3. Liquidation Analysis (Sep 1, 2021)

### 3.1 Liquidation Profile

| Symbol | Type | Count | Total Qty | Avg Size | Max Size |
|--------|------|-------|-----------|----------|----------|
| ETH-PERP | Short Liq (buy) | 550 | 22.7M | 41K | 726K |
| ETH-PERP | Long Liq (sell) | 9 | 60K | 6.7K | 10K |
| BTC-PERP | Short Liq (buy) | 227 | 12.1M | 53K | 299K |
| BTC-PERP | Long Liq (sell) | 158 | 8.9M | 56K | 675K |

**Key Finding**: 82% were short liquidations — massive short squeeze day.

### 3.2 Temporal Clustering

| Hour (UTC) | Count | Total Qty | Context |
|------------|-------|-----------|---------|
| 16 | 428 | 19.8M | Peak liquidation cascade |
| 9 | 181 | 11.2M | Morning continuation |
| 1 | 93 | 6.4M | Late night/early Asia |

**Pattern**: 45% of daily liquidations happened in a single hour (4PM UTC).

### 3.3 Size Distribution

| Size Bucket | Count | % of Total | Total Qty | % of Volume |
|-------------|-------|------------|-----------|-------------|
| Whale (>100K) | 105 | 11% | 22.7M | **50%** |
| Large (10K-100K) | 576 | 61% | 20.3M | 45% |
| Medium (1K-10K) | 173 | 18% | 747K | 2% |
| Small (<1K) | 90 | 10% | 44K | <1% |

**Power Law**: Top 11% of liquidations accounted for 50% of liquidated volume.

### 3.4 Notable Whale Liquidations

| Rank | Symbol | Size | Price | Time | Type |
|------|--------|------|-------|------|------|
| 1 | ETH-PERP | 726,280 | $35,499 | 09:10 UTC | Short Liq |
| 2 | ETH-PERP | 676,540 | $35,540 | 09:11 UTC | Short Liq |
| 3 | BTC-PERP | 674,600 | $46,589 | 01:11 UTC | Long Liq |
| 4 | ETH-PERP | 657,290 | $35,530 | 09:11 UTC | Short Liq |
| 5 | ETH-PERP | 609,350 | $37,920 | 16:46 UTC | Short Liq |

---

## 4. Cross-Dataset Alpha Signals

### 4.1 Signal: Options vs Perp Arbitrage (Sep 1, 2020)

**Setup**:
- ETH Options IV: 102%
- ETH Perp Funding: 83% annualized
- Edge: 19% cost advantage to options

**Trade Structure**:
```
Long: ATM Call options (pay 102% vol)
Short: Perpetual swap (receive 83% funding)
Net: Synthetic put + 19% carry advantage
```

**Risk**: Options theta decay vs funding accumulation timing.

### 4.2 Signal: Gamma Squeeze Detection

**Indicators**:
1. High gamma OTM options (Gamma > 0.05)
2. Low time to expiry (<7 days)
3. Rising funding rates
4. Increasing liquidation velocity

**Trigger**: All four conditions met → Expect vol expansion

### 4.3 Signal: Funding Regime Contrarian

**Conditions**:
- Funding > 0.1% per 8h (extreme long bias)
- Call skew > 10% (extreme euphoria)
- IV > 100% (expensive insurance)

**Action**: Short perp + buy put = Crash protection with funding income

### 4.4 Signal: Liquidation Cascade Fade

**Pattern Recognition**:
- Liq velocity > 100/hour (cascade detected)
- 80%+ same direction (one-sided squeeze)
- Whale liqs (>100K) clustering

**Strategy**:
- Wait for velocity peak
- Fade the move (buy if short liqs, sell if long liqs)
- Hold until next funding period

### 4.5 Signal: IV Term Structure Dislocation

**Opportunity**: Sep 3 expiry trading at 88% IV vs Sep 1 at 71%

**Trade**: Sell Sep 3 straddle, buy Sep 1 straddle
- Collect 17 vol points
- Risk: Weekend gap exposure

---

## 5. Feature Engineering Recommendations

### 5.1 Core Features

| Feature | Formula | Source Tables | Update Freq |
|---------|---------|---------------|-------------|
| iv_funding_spread | IV% - (Funding × 3 × 365) | options + ticker | 1 hour |
| gamma_oi_weighted | Σ(Gamma × OI) / Σ(OI) | options | Real-time |
| skew_zscore | (Skew - 30d_mean) / Std | options | 1 hour |
| liq_velocity | ΔLiqs/Δt (5min window) | liquidations | 1 min |
| whale_liq_ratio | Whale_liqs / Total_liqs | liquidations | 1 hour |
| term_slope | (30d_IV - 7d_IV) / 23 | options | 1 hour |

### 5.2 Composite Signals

**Euphoria Index** (0-100):
```
= 30 × (Funding_percentile)
+ 30 × (Call_skew_percentile)
+ 20 × (IV_percentile)
+ 20 × (Liq_velocity_percentile)
```

Interpretation:
- 0-25: Fear (long opportunity)
- 26-50: Neutral
- 51-75: Greed (caution)
- 76-100: Euphoria (contrarian short)

---

## 6. Strategic Trade Ideas

### 6.1 DeFi Summer 2020 Replay

If similar conditions detected:
1. Buy ETH ATM calls (3-7 day expiry)
2. Short ETH perps (capture funding)
3. Delta hedge to isolate vol/funding edge

Expected edge: 15-25% annualized from funding differential.

### 6.2 Liquidation Scalping

Setup:
- Monitor liq velocity in real-time
- When velocity > 2 std above mean:
  - If short liqs dominate → Buy spot/perp
  - If long liqs dominate → Sell spot/perp
- Hold 1-4 hours
- Take profit at next funding

### 6.3 Calendar Spread Arbitrage

Scan for:
- Adjacent expiries with >10 vol point difference
- No events between dates
- Sufficient liquidity in both

Example from data:
- Sell Sep 3 88% IV
- Buy Sep 1 71% IV
- Collect 17 vol points

---

## 7. Data Quality Notes

### 7.1 Validated Metrics
- ✅ Zero nulls in critical fields
- ✅ Zero duplicates detected
- ✅ All timestamps chronologically ordered
- ✅ Mark-index spreads <0.1% (efficient)
- ✅ Value ranges within expected bounds

### 7.2 Limitations
- Options and perp data from different exchanges (Deribit vs BitMEX)
- Liquidations data from different date (2021 vs 2020)
- Single-day snapshots only
- No order book depth data

### 7.3 Recommended Extensions
- [ ] Add trades data for volume confirmation
- [ ] Add order book data for spread analysis
- [ ] Extend to multi-day time series
- [ ] Add delta-hedged P&L backtesting

---

## 8. Key Takeaways

1. **ETH was the wild child**: 2× BTC vol, massive call skew, 83% funding cost
2. **Shorts got destroyed**: 82% of liquidations were short liquidations
3. **Options were cheap**: vs perp funding, calls offered better leverage
4. **Liquidations cluster**: 45% of daily liquidations in 1 hour
5. **Whales dominate**: Top 11% of liquidations = 50% of volume

---

## Appendix: Query Snippets

### Load and filter options data
```python
from deltalake import DeltaTable
import polars as pl

dt = DeltaTable('/Users/zjx/data/lake/silver/options_chain')
df = pl.from_arrow(dt.to_pyarrow_table())

# Filter for high gamma opportunities
gamma_opps = df.filter(
    (pl.col('gamma') > 0.05) &
    (pl.col('mark_iv') < 100) &
    (pl.col('delta').abs().is_between(0.15, 0.35))
)
```

### Calculate funding annualized
```python
df.with_columns(
    (pl.col('funding_rate') * 3 * 365).alias('funding_annualized')
)
```

### Detect liquidation cascades
```python
df.with_columns(
    pl.from_epoch(pl.col('ts_event_us'), time_unit='us').dt.hour().alias('hour')
).group_by('hour').agg(
    pl.len().alias('liq_count')
).filter(pl.col('liq_count') > 100)  # Cascade threshold
```

---

*End of Notes*
