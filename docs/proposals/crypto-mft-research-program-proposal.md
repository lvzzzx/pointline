# Crypto MFT Signal Research Program - Full Picture Proposal

**Persona:** Quant Researcher (MFT)
**Status:** Proposed
**Date:** 2026-02-13
**Scope:** Unified research program design across all signal families

---

## Executive Summary

This proposal defines the **complete MFT signal research program** for crypto markets on Pointline. It organizes all existing and proposed signal families into a coherent taxonomy, identifies structural gaps, maps data dependencies, and provides a unified falsification framework.

The program covers **12 signal families** across **4 layers**:

| Layer | Signal Families | Data Source |
|-------|----------------|-------------|
| **Microstructure** | Order book shape, Flow toxicity, Trade classification | `orderbook_updates`, `trades`, `quotes` |
| **Derivatives** | Funding/crowding, Perp-spot basis, Options/vol surface, Liquidation cascades | `derivative_ticker`, `liquidations`, `options_chain`, `trades` |
| **Cross-Dimensional** | Cross-asset contagion, Cross-venue discovery, Realized vol microstructure | Multi-symbol `trades`/`quotes` |
| **Meta/Conditioning** | Multi-timeframe, Adaptive timeframe, Temporal/calendar | Derived from all streams |

**Existing proposals** (5) cover derivatives and meta layers well. **This document adds 7 new families** that fill the microstructure core and cross-dimensional layers, then provides the unified evaluation and phasing framework.

**Central claim:** A complete MFT stack requires signals from all four layers. No single layer is sufficient. Microstructure provides short-horizon edge; derivatives provide regime context; cross-dimensional provides diversification; meta/conditioning sharpens everything.

---

## Table of Contents

1. [Signal Taxonomy](#1-signal-taxonomy)
2. [Layer 1: Microstructure](#2-layer-1-microstructure)
   - [2.4 Microstructure-to-MFT Bridge Patterns](#24-microstructure-to-mft-bridge-how-fast-signals-serve-slow-strategies)
3. [Layer 2: Derivatives](#3-layer-2-derivatives)
4. [Layer 3: Cross-Dimensional](#4-layer-3-cross-dimensional)
5. [Layer 4: Meta / Conditioning](#5-layer-4-meta--conditioning)
6. [Unified Evaluation Framework](#6-unified-evaluation-framework)
7. [Interaction Map](#7-interaction-map)
8. [Data Dependency Matrix](#8-data-dependency-matrix)
9. [Phased Research Plan](#9-phased-research-plan)
10. [Decision Architecture](#10-decision-architecture)
11. [Risk Register](#11-risk-register)
12. [Success Criteria](#12-success-criteria)

---

## 1. Signal Taxonomy

### 1.1 Why a Taxonomy Matters

Individual signal proposals risk local optimization: each family is evaluated in isolation, interaction effects are missed, and the overall program lacks coherence. A taxonomy forces:

- **Coverage analysis:** Are we missing entire signal dimensions?
- **Redundancy detection:** Which proposals overlap?
- **Interaction planning:** Which families should be evaluated jointly?
- **Phasing logic:** What must come first?

### 1.2 Four-Layer Architecture

```
Layer 4: Meta / Conditioning
  ┌─────────────────────────────────────────────────┐
  │  Multi-Timeframe  │  Adaptive TF  │  Calendar   │
  └─────────────────────────────────────────────────┘
                        ▲ conditions ▲
Layer 3: Cross-Dimensional
  ┌─────────────────────────────────────────────────┐
  │  Cross-Asset  │  Cross-Venue  │  Realized Vol   │
  └─────────────────────────────────────────────────┘
                        ▲ diversifies ▲
Layer 2: Derivatives
  ┌─────────────────────────────────────────────────┐
  │  Funding  │  Perp-Spot  │  Options  │  Liq.     │
  └─────────────────────────────────────────────────┘
                        ▲ contextualizes ▲
Layer 1: Microstructure (CORE)
  ┌─────────────────────────────────────────────────┐
  │  Order Book Shape  │  Flow Toxicity  │  Trade   │
  │                    │                 │  Classif. │
  └─────────────────────────────────────────────────┘
```

**Information flow:** Layer 1 generates the primary short-horizon signals. Layer 2 provides regime context that modulates Layer 1 behavior. Layer 3 provides diversification and cross-instrument alpha. Layer 4 conditions and sharpens all lower layers.

### 1.3 Existing vs New

| Family | Status | Proposal |
|--------|--------|----------|
| Funding/Crowding | Existing | `funding-rate-features-mft-research-proposal.md` |
| Perp-Spot Basis | Existing | `perp-spot-features-mft-research-proposal.md` |
| Options/Vol Surface | Existing | `options-volatility-surface-proposal.md` |
| Multi-Timeframe | Existing | `multitimeframe-features-mft-research-proposal.md` |
| Adaptive Timeframe | Existing | `adaptive-timeframe-features-mft-research-proposal.md` |
| **Order Book Microstructure** | **NEW** | This document, Section 2.1 |
| **Flow Toxicity** | **NEW** | This document, Section 2.2 |
| **Trade Classification** | **NEW** | This document, Section 2.3 |
| **Liquidation Cascades** | **NEW** | This document, Section 3.4 |
| **Cross-Asset Contagion** | **NEW** | This document, Section 4.1 |
| **Cross-Venue Discovery** | **NEW** | This document, Section 4.2 |
| **Realized Vol Microstructure** | **NEW** | This document, Section 4.3 |
| **Temporal / Calendar** | **NEW** | This document, Section 5.3 |

---

## 2. Layer 1: Microstructure

**Data:** `orderbook_updates`, `trades`, `quotes` — all ingested, all PIT-safe.

**Role:** Core short-horizon alpha layer. This is the missing foundation of the current research program.

### 2.1 Order Book Shape Features

**Thesis:** The shape of the limit order book encodes supply-demand asymmetry that trades alone cannot capture. Depth gradients, imbalance profiles, and resilience dynamics predict short-horizon price direction and volatility.

**Conceptual model:**
1. **Static shape:** depth distribution at a point in time reveals resting interest.
2. **Dynamic shape:** how the book reacts to aggressive flow reveals participant conviction.
3. **Shape-flow interaction:** aggressive flow into thin depth = continuation; into thick depth = absorption.

#### Hypothesis set

**H1: Depth imbalance predicts short-horizon direction.**
Bid-heavy books should precede upward moves; ask-heavy books should precede downward moves. Effect should be strongest at 1-3 bar horizons and decay with distance from top of book.

**H2: Book gradient captures urgency asymmetry.**
Steep depth curves (concentrated near best price) indicate urgency; flat curves indicate patience. Gradient asymmetry between sides should predict direction.

**H3: Microprice outperforms mid-price as fair value.**
Depth-weighted microprice (Stoikov 2018) should reduce noise in return computation, improving IC of downstream features.

**H4: Resilience speed signals informed vs noise flow.**
Fast depth recovery after a sweep indicates uninformed aggression (noise); slow recovery indicates informed flow (depletion of resting interest).

**H5: Book event velocity contains information beyond static shape.**
Cancel rate, replace rate, and add rate asymmetries may front-run static imbalance signals.

#### Feature families

1. **Depth imbalance:** `(bid_depth_N - ask_depth_N) / (bid_depth_N + ask_depth_N)` at levels N = 1, 3, 5, 10.
2. **Book gradient:** slope of cumulative depth curve per side; gradient ratio = bid_slope / ask_slope.
3. **Microprice:** `(bid_price * ask_qty + ask_price * bid_qty) / (bid_qty + ask_qty)` — depth-weighted fair value.
4. **Resilience:** time to recover N% of swept depth after aggressive trade. Requires event-time measurement.
5. **Book pressure:** net depth within K ticks of mid, aggregated and normalized.
6. **Book event flow:** cancel/add/replace rate imbalance per side per window.

#### Key risks

- Latency sensitivity: book shape is stale by the time we observe it. Must evaluate on arrival timestamps (`ts_local_us`), not exchange time.
- Spoofing contamination: large resting orders may be fake. Must test whether features degrade during known spoofing-heavy periods.
- Reconstruction fidelity: `orderbook_updates` are incremental deltas; book reconstruction must be verified against snapshots.

#### References

- Cont, Stoikov & Talreja (2010) — "A Stochastic Model for Order Book Dynamics"
- Cartea, Jaimungal & Penalva (2015) — "Algorithmic and High-Frequency Trading" (Ch. 3)
- Stoikov (2018) — "The Micro-Price"

---

### 2.2 Flow Toxicity Features

**Thesis:** Not all order flow is equal. Decomposing flow into informed vs uninformed components provides a higher-quality directional signal than raw signed volume. Toxicity metrics measure the probability that a counterparty is informed.

**Conceptual model:**
1. **Toxicity level:** probability of informed trading in recent flow.
2. **Toxicity change:** acceleration of informed activity (regime transition signal).
3. **Toxicity-price interaction:** toxic flow during thin books = danger; during thick books = absorption opportunity.

#### Hypothesis set

**H1: VPIN predicts short-horizon volatility.**
High VPIN (Volume-synchronized Probability of Informed Trading) should precede volatility spikes, as informed traders front-run news.

**H2: Kyle's lambda captures price impact efficiency.**
Higher lambda (price impact per unit signed volume) indicates less liquidity and higher information content per trade. Lambda changes should predict regime shifts.

**H3: Adverse selection component of spread predicts direction.**
When the realized spread (post-trade price movement against the initiator) is large relative to the effective spread, the market is in an adverse-selection regime; flow direction becomes more informative.

**H4: Flow persistence separates informed from noise.**
Informed traders split orders across time (autocorrelated signed flow). Noise traders are random. Autocorrelation of signed flow should predict continuation strength.

**H5: Toxicity interacts with book state.**
VPIN should have higher predictive power when combined with thin book depth (toxic flow + thin book = move).

#### Feature families

1. **VPIN:** volume-synchronized bulk-classification metric. Uses volume bars (already implemented in Pointline spine builders) as natural sampling.
2. **Kyle's lambda:** regression coefficient from `delta_price ~ beta * signed_volume` over rolling windows.
3. **Realized spread:** post-trade mid-price change at fixed horizons (1s, 5s, 30s) as adverse-selection proxy.
4. **Flow autocorrelation:** autocorrelation of signed volume at multiple lags.
5. **Trade size entropy:** Shannon entropy of trade-size distribution. Low entropy = institutional clustering; high entropy = retail noise.

#### Key risks

- Bulk classification (VPIN) assigns trade direction probabilistically. In crypto, the `side` field is often directly available, which may reduce VPIN's value vs raw signed flow.
- Lambda estimation is noisy at short windows. Requires careful window selection and regularization.
- Adverse selection measurement requires precise timestamp alignment between trades and quotes.

#### References

- Easley, Lopez de Prado & O'Hara (2012) — "Flow Toxicity and Liquidity in a High-Frequency World"
- Kyle (1985) — "Continuous Auctions and Insider Trading"
- Glosten & Milgrom (1985) — "Bid, Ask and Transaction Prices in a Specialist Market"

---

### 2.3 Trade Classification Features

**Thesis:** Segmenting trade flow by size, timing, and execution pattern reveals participant type (institutional vs retail, market maker vs directional). Different participant segments carry different information content.

**Conceptual model:**
1. **Size segmentation:** trade size distribution reveals participant mix.
2. **Timing segmentation:** trades clustered in time suggest algorithm execution; isolated trades suggest manual.
3. **Aggression segmentation:** repeated taker-side trades in one direction suggest conviction.

#### Hypothesis set

**H1: Large-trade flow outpredicts small-trade flow.**
Large trades carry more information per unit volume. Net signed volume from the top decile of trade sizes should have higher IC than raw flow imbalance.

**H2: Trade clustering detects algorithmic execution.**
Sequences of same-direction, similar-sized trades within short windows indicate order-splitting. The direction of clustered flow should predict continuation.

**H3: Size-conditional flow divergence signals regime change.**
When large and small trades disagree in direction, subsequent returns should resolve in the direction of large trades.

**H4: Maker-taker asymmetry is informative.**
The `is_buyer_maker` field directly identifies aggressor side. Asymmetry in maker vs taker volume by size bucket should refine flow direction.

#### Feature families

1. **Size-bucketed flow:** signed volume partitioned into quantile buckets (e.g., bottom 50%, 50-90%, top 10%). Net direction per bucket.
2. **Trade arrival clustering:** inter-trade time distribution. Burstiness coefficient, mean inter-arrival by direction.
3. **Large/small flow divergence:** sign disagreement between top-decile and bottom-decile trades.
4. **Execution pattern score:** consecutive same-direction trade count / total trades (persistence measure).

#### Key risks

- Size thresholds are instrument-specific and time-varying. Must use adaptive quantile-based cutoffs, not fixed dollar thresholds.
- Crypto trade sizes may be less informative than equity (retail can trade large on leverage).
- Trade splitting by sophisticated participants may look like small trades.

#### References

- Lee & Ready (1991) — "Inferring Trade Direction from Intraday Data"
- Chakrabarty, Moulton & Shkilko (2012) — "Short Sales, Long Sales, and the Lee-Ready Trade Classification Algorithm Revisited"

---

### 2.4 Microstructure-to-MFT Bridge: How Fast Signals Serve Slow Strategies

**Problem statement:** Raw microstructure features (depth imbalance, microprice deviation, single-trade toxicity) have peak IC at 1-3 ticks and decay to noise within seconds. MFT strategies hold for 10s of seconds to minutes. Naively feeding tick-level features into an MFT model wastes them — by the time the signal is consumed, the information is stale.

**Solution:** Microstructure features do not enter the MFT model as raw values. They are transformed through one of six bridge patterns that convert short-lived tick information into MFT-relevant state, regime, or timing variables.

#### Bridge Pattern 1: Aggregated State Variables

**Concept:** Aggregate tick-level signals over MFT-relevant windows. The aggregated value captures persistent microstructure state, not the transient tick.

```
Raw signal:     depth_imbalance[t]          → IC peak at 1-3 ticks, decays by tick 10
Aggregated:     mean(depth_imbalance, 50)   → "the book has been bid-heavy for 50 bars"
                ema(depth_imbalance, 100)    → exponential smoothing preserves recent bias
```

**Why it extends horizon:** A single tick of bid-heavy book is noise. Fifty consecutive bars of bid-heavy book is a resting demand regime. The aggregated state predicts not "next tick up" but "buyers are absorbing, next large sell will be cushioned" — an MFT-relevant statement.

**Feature examples:**
- `mean_depth_imbalance_50bar` — average book imbalance over 50 volume bars
- `ema_microprice_deviation_100bar` — smoothed microprice vs mid spread
- `cumulative_signed_flow_200bar` — accumulated net flow over a window
- `fraction_time_bid_heavy_100bar` — percentage of bars where bid depth > ask depth

**Testable hypothesis:** Aggregated imbalance at 50-bar and 100-bar windows should have IC > 0.03 at 5-10 bar forward horizons, even though the raw tick signal decays by bar 3.

---

#### Bridge Pattern 2: Regime Classification

**Concept:** Use microstructure features to classify the current regime, then condition MFT signals on that regime. The microstructure signal does not need to predict returns — it only needs to tell you what kind of market you are in.

```
Microstructure regime label:
  - TOXIC:     VPIN > 0.7, Kyle's lambda elevated
  - THIN:      total book depth < 20th percentile
  - INFORMED:  large-trade flow > 3σ
  - NORMAL:    none of the above

MFT model behavior:
  - TOXIC regime  → funding-based signals get 2x weight (squeeze more likely)
  - THIN regime   → reduce position sizing (slippage risk)
  - INFORMED regime → flow signals get 2x weight (smart money present)
  - NORMAL → default weights
```

**Why it extends horizon:** Regimes persist for minutes to hours, even though the microstructure data generating the label updates every tick. Regime transitions are infrequent events with MFT-horizon consequences.

**Feature examples:**
- `toxicity_regime` — categorical: TOXIC / ELEVATED / NORMAL / SAFE
- `liquidity_regime` — categorical: THIN / NORMAL / DEEP
- `flow_type_regime` — categorical: INFORMED / MIXED / NOISE

**Testable hypothesis:** MFT signals (funding, basis, flow imbalance) conditioned on microstructure regime should have higher IC than unconditioned signals. Specifically, funding crowding signals should have higher IC during TOXIC regimes.

---

#### Bridge Pattern 3: Persistence and Duration Metrics

**Concept:** Instead of the level of a microstructure signal, measure how long it has been in a given state. Persistence carries MFT-horizon information that the instantaneous level does not.

```
Raw signal:     depth_imbalance[t] = 0.3          → "bid-heavy right now"
Persistence:    bars_since_imbalance_flipped = 87  → "bid-heavy for 87 bars straight"
Duration:       mean_imbalance_duration = 45 bars  → "typical bid-heavy streak is 45 bars"
```

**Why it extends horizon:** A microstructure state that has persisted for 100 bars is more likely to persist for 10 more bars (mean-revert slowly) than one that just started. Persistence transforms a fast signal into a slow state variable.

**Feature examples:**
- `bars_since_imbalance_sign_flip` — how long current book bias has persisted
- `vpin_above_threshold_duration` — how long toxicity has been elevated
- `consecutive_same_direction_large_trades` — length of current large-trade run
- `depth_regime_age` — bars since last regime transition

**Testable hypothesis:** Persistence features should have IC at 5-10 bar horizons. Long persistence (>95th percentile) should predict mean reversion; short persistence after a flip should predict continuation.

---

#### Bridge Pattern 4: Transition Detection (Event Signals)

**Concept:** Microstructure regime transitions are events with MFT-horizon consequences. The transition itself is the signal — not the continuous level. When the book suddenly thins, or VPIN spikes from 0.3 to 0.8, or large-trade flow reverses direction, these are discrete events that forecast the next several minutes.

```
Continuous:     vpin[t] = 0.75                    → IC decays in 3 ticks
Transition:     vpin crossed 0.7 threshold 2 bars ago → event flag with MFT relevance
                bars_since_vpin_spike = 2          → "toxicity just arrived"
```

**Why it extends horizon:** Markets take time to process new information. A sudden microstructure shift (book thins, toxicity spikes, informed flow appears) triggers an adjustment process that plays out over minutes, not ticks. The event flag marks the start of that process.

**Feature examples:**
- `vpin_crossed_above_07` — binary event flag, time since occurrence
- `book_depth_dropped_below_p20` — sudden thinning event
- `large_trade_cluster_started` — first large trade after quiet period
- `microprice_diverged_from_mid_3sigma` — extreme microprice dislocation event
- `resilience_failure` — depth did not recover within expected time after sweep

**Testable hypothesis:** Event flags should have IC > 0.04 at 3-10 bar forward horizons. IC should decay more slowly than raw microstructure level features (half-life of 5-10 bars vs 1-3 bars).

---

#### Bridge Pattern 5: Entry/Exit Timing Within MFT Signals

**Concept:** MFT signals from Layer 2-3 (funding, basis, options) determine direction. Microstructure features determine WHEN to enter and exit within that directional view. This is the "trigger" role — microstructure provides execution timing, not alpha.

```
MFT signal:     funding_zscore = -2.5 → "short bias for next 10 minutes"
Micro timing:   wait until depth_imbalance < -0.2 AND vpin < 0.4
                → "enter short NOW: ask-heavy book + low toxicity = good entry"

Without timing: enter immediately at signal generation → wider spread, worse fill
With timing:    enter when book is favorable → tighter spread, better fill
```

**Why it works:** This pattern does not require microstructure signals to predict returns at MFT horizons. It only requires them to predict the next few seconds of price/spread behavior — which is exactly where they are strongest.

**Implementation:** This is not a feature in the traditional IC sense. It is an execution policy conditioned on microstructure state. Evaluation metric is implementation shortfall, not IC.

**Testable hypothesis:** MFT strategies that condition entry timing on book state should achieve 15-30% lower implementation shortfall than strategies that enter immediately on signal.

---

#### Bridge Pattern 6: Integral / Cumulative Features

**Concept:** Instead of the instantaneous microstructure value, use its cumulative integral over a window. This is mathematically related to Pattern 1 (aggregation) but focuses on the running sum rather than the mean — capturing total information arrival, not average state.

```
Raw:        signed_flow[t]                           → single bar flow direction
Integral:   cumsum(signed_flow, reset_every=100bar)  → "net flow since last reset"
            ewm_integral(depth_imbalance, span=50)   → exponential integral of book bias
```

**Why it extends horizon:** Cumulative flow divergence over 100 bars captures sustained buying or selling pressure. Even if each bar's flow imbalance has near-zero IC at 10-bar horizons, the cumulative sum over 100 bars measures structural demand/supply buildup that resolves over minutes.

**Feature examples:**
- `cumulative_signed_flow_100bar` — running sum of net signed volume
- `cumulative_toxicity_integral_50bar` — integral of VPIN above baseline
- `net_book_pressure_integral` — integral of depth imbalance, exponentially weighted

**Testable hypothesis:** Integral features should have IC > 0.03 at 5-10 bar horizons even when the per-bar feature has IC < 0.01 at those horizons. Integral features should also have lower turnover than raw features.

---

#### Summary: Bridge Pattern Selection Guide

| Pattern | Input | Output | MFT Use | When to Use |
|---------|-------|--------|---------|-------------|
| **1. Aggregation** | Tick-level signal | Smoothed state | Direct feature | Default choice; always try first |
| **2. Regime** | Multiple micro signals | Categorical label | Conditioning variable | When you need to modulate other signals |
| **3. Persistence** | Signal sign/threshold | Duration counter | Duration feature | When signal persistence is more informative than level |
| **4. Transition** | Signal level crossing | Event flag + age | Event feature | When regime shifts matter more than regime levels |
| **5. Timing** | Book state, toxicity | Entry/exit rule | Execution policy | When you already have directional alpha from Layer 2-3 |
| **6. Integral** | Per-bar flow/pressure | Cumulative sum | Momentum feature | When cumulative pressure matters (demand/supply buildup) |

**Evaluation protocol for bridge patterns:**

1. Compute raw microstructure feature IC at horizons 1, 3, 5, 10 bars. Confirm rapid decay (IC_10bar < 0.3 × IC_1bar).
2. Apply each bridge pattern. Measure IC at 5, 10, 20 bars.
3. A pattern is successful if it shifts the IC peak from 1-3 bars to 5-10+ bars without losing more than 50% of the peak magnitude.
4. Compare bridge-transformed features against simple rolling-mean baselines. Reject if rolling mean matches performance.
5. Test interaction: bridge-transformed microstructure features conditioned on Layer 2 regime should outperform either alone.

---

## 3. Layer 2: Derivatives

Existing proposals cover funding (crowding state), perp-spot (basis dynamics), and options (implied vol surface). One major gap remains.

### 3.1 Funding / Crowding Features (Existing)

**Full proposal:** [`funding-rate-features-mft-research-proposal.md`](funding-rate-features-mft-research-proposal.md)

**Core hypotheses:** Crowding mean reversion (H1), funding surprise information (H2), funding-OI pressure (H3), flow × funding interaction (H4), settlement convexity (H5).

**Feature families:** Level features (funding percentile, annualized carry, cross-venue spread), delta features (funding change, expected-vs-realized gap, OI acceleration), interaction features (funding × flow, funding × vol, funding × OI change), regime features (vol/trend/liquidity/settlement proximity), structure features (persistence of extremes, transition probability).

**Linkages to new families in this proposal:**

| Interacts With | Interaction Logic | Section |
|---------------|-------------------|---------|
| **Liquidation Cascades** | Extreme funding + high OI = pre-cascade state. Funding crowding is the fuel; liquidations are the ignition. Joint feature (funding z-score × OI level × liquidation intensity) should forecast cascade continuation vs exhaustion. | §3.4, H4 |
| **Flow Toxicity** | High VPIN during extreme funding = informed traders positioning ahead of a squeeze. Funding provides the structural context; toxicity measures the execution urgency. Combined signal should outpredict either standalone. | §2.2, H5 |
| **Order Book Shape** | Funding settlement windows thin the book as participants adjust positions. Book depth features should be modulated by settlement proximity (a funding-regime variable). | §2.1, H3 |
| **Temporal/Calendar** | Settlement convexity (H5 in the funding proposal) overlaps with temporal features (§5.3, H2). The funding proposal owns the causal mechanism; the temporal proposal provides the generic time encoding. Avoid double-counting. | §5.3 |
| **Realized Vol** | Funding extremes should precede realized vol spikes (squeeze events). RV conditioned on funding regime should be a stronger volatility predictor than unconditioned RV. | §4.3, H5 |

---

### 3.2 Perp-Spot Basis Features (Existing)

**Full proposal:** [`perp-spot-features-mft-research-proposal.md`](perp-spot-features-mft-research-proposal.md)

**Core hypotheses:** Basis extremes mean-revert conditionally (H1), funding-basis dislocation predicts convergence (H2), perp leads spot in normal regimes (H3), flow divergence signals participant segmentation (H4), volume-asymmetry regime dependence (H5), settlement-window nonlinearity (H6).

**Feature families:** Basis structure (level, z-score, slope, persistence), carry alignment (funding-basis gap, gap velocity), lead-lag (perp-minus-spot return deltas), cross-flow (perp/spot flow divergence, flow-basis interactions), regime (perp/spot activity ratio, liquidity stress, settlement proximity).

**Linkages to new families in this proposal:**

| Interacts With | Interaction Logic | Section |
|---------------|-------------------|---------|
| **Liquidation Cascades** | Wide positive basis + long liquidations = leveraged longs unwinding through perp selling. Basis should converge faster when liquidation intensity is high. Joint feature: basis direction × liquidation direction alignment score. | §3.4, H1-H2 |
| **Cross-Asset Contagion** | Perp-spot basis dynamics may transmit across assets. BTC basis widening may precede ETH basis widening if cross-asset arbitrageurs link the two. Cross-asset basis divergence is a second-order signal. | §4.1, H1 |
| **Cross-Venue Discovery** | Perp-spot lead-lag (H3) depends on which venue leads price discovery. If Binance perp leads while OKX spot lags, the perp-spot signal is really a venue-routing signal. Information share provides the decomposition. | §4.2, H1 |
| **Order Book Shape** | Perp and spot book depth asymmetry reveals where resting interest sits. Thin spot book + wide basis = vulnerability to fast basis convergence if a spot sweep occurs. | §2.1, H4 |
| **Flow Toxicity** | Perp flow toxicity vs spot flow toxicity divergence. If perp VPIN is high but spot VPIN is low, informed traders are operating in the perp venue. This refines the lead-lag hypothesis (H3). | §2.2, H1 |

---

### 3.3 Options / Volatility Surface Features (Existing)

**Full proposal:** [`options-volatility-surface-proposal.md`](options-volatility-surface-proposal.md)

**Core hypotheses:** ATM IV predicts realized volatility, skew is a fear gauge, VRP (IV − RV) mean-reverts, dealer gamma exposure predicts intraday volatility, large options flow leads spot, put-call parity deviations signal arbitrage.

**Feature families:** IV features (ATM IV, IV term structure), skew features (25-delta put-call skew, skew steepness), IV-RV spread (VRP, IV momentum), Greeks features (dealer gamma exposure, vanna exposure), volume/flow features (put-call ratio, large options flow), structural features (put-call parity deviations).

**Data dependency note:** The options proposal uses `options_chain` (Tardis), which provides exchange-computed Greeks (`delta`, `gamma`, `vega`, `theta`) and IVs (`bid_iv`, `ask_iv`, `mark_iv`). This is richer than the raw options trades schema originally proposed — exchange-provided Greeks eliminate the need for Black-Scholes inversion in Phase 1.

**Linkages to new families in this proposal:**

| Interacts With | Interaction Logic | Section |
|---------------|-------------------|---------|
| **Realized Vol Microstructure** | VRP = IV − RV. The options proposal provides IV; the RV proposal (§4.3) provides a better RV denominator via multi-estimator methods. Parkinson or Garman-Klass RV should produce a more accurate VRP than close-to-close RV. This is the strongest cross-layer dependency. | §4.3 |
| **Liquidation Cascades** | Dealer gamma exposure predicts intraday vol; liquidation cascades are the primary vol-generating mechanism in crypto. Negative dealer gamma + active liquidation cascade = amplified move (dealers and liquidation engines both pushing in the same direction). | §3.4, H1 |
| **Funding/Crowding** | Options skew should widen when funding is extreme (market pricing crash risk from leveraged crowding). Skew conditioned on funding regime should be a better fear gauge than unconditional skew. | §3.1 |
| **Cross-Asset Contagion** | BTC implied vol vs ETH implied vol spread. Cross-asset IV divergence may predict rotation or contagion. If BTC IV spikes but ETH IV doesn't, ETH may be lagging and vulnerable. | §4.1, H2 |
| **Temporal/Calendar** | Options expiry cycles (daily, weekly, monthly, quarterly) create predictable vol patterns. "Pinning" effects near large open interest strikes interact with calendar features. | §5.3 |
| **Order Book Shape** | Options market makers hedge by trading spot/futures. When dealer gamma is large, spot book dynamics are partially driven by hedging flow. Book features during high-gamma regimes have different information content. | §2.1 |

### 3.4 Liquidation Cascade Features (NEW)

**Data:** `liquidations` table — schema defined, ingestion supported. Fields: `liquidation_id`, `side`, `price`, `qty`.

**Thesis:** Forced liquidations are the dominant nonlinear mechanism in leveraged crypto markets. They create feedback loops: price moves trigger liquidations, which trigger further price moves. Cascade dynamics are predictable given leverage state (funding + OI), and the aftermath follows characteristic reversal patterns.

**Conceptual model:**
1. **Liquidation state:** current intensity and direction of forced unwinds.
2. **Cascade dynamics:** self-reinforcing liquidation chains that amplify moves.
3. **Exhaustion detection:** when cascade fuel (remaining leveraged positions) is depleted, mean reversion begins.
4. **Pre-condition:** funding crowding + high OI + thin book = high cascade probability.

#### Hypothesis set

**H1: Liquidation intensity predicts short-horizon momentum.**
Active liquidation clusters should produce momentum at 1-5 bar horizons as forced selling/buying continues.

**H2: Liquidation exhaustion predicts reversal.**
After sustained same-direction liquidations, OI declines and the move exhausts. Return should mean-revert once liquidation rate drops below threshold.

**H3: Liquidation asymmetry (long vs short) contains directional information.**
Net liquidation direction (long liquidations minus short liquidations by volume) should predict next-bar returns in the liquidation direction at short horizons, then reverse.

**H4: Cascade probability is forecastable from state variables.**
High funding + high OI + thin book + rising volatility should increase cascade probability in the next N bars. This is a compound interaction hypothesis.

**H5: Liquidation-flow interaction determines continuation vs reversal.**
Liquidations aligned with organic flow = continuation. Liquidations against organic flow = over-extension and reversal.

**H6: Post-liquidation spread widening creates execution risk.**
During cascades, bid-ask spread and slippage increase. Features must be evaluated on executable pricing, not mid-price.

#### Feature families

1. **Liquidation intensity:** volume/count in rolling window, z-score relative to recent distribution.
2. **Liquidation direction:** net signed liquidation volume (long_liq_vol - short_liq_vol), both raw and normalized.
3. **Cascade score:** consecutive same-direction liquidations within time window × average size. Resets on direction change.
4. **Liquidation-OI ratio:** liquidation volume / open interest. Measures how much of the leveraged base is being forcibly unwound.
5. **Pre-cascade state:** composite of funding z-score, OI level, book depth, and short-horizon volatility. A conditional probability feature.
6. **Post-cascade exhaustion:** time since last liquidation cluster, OI drawdown from peak, volatility normalization speed.

#### Key risks

- Liquidation data may be incomplete: not all exchanges report all liquidations.
- Signal is episodic: cascades are infrequent. Must avoid kill criterion #3 (rare stress windows only) by also evaluating "quiet liquidation" regimes.
- Execution during cascades is poor: wide spreads, high slippage. Must discount signal value by execution cost during stress.
- Interaction with funding/OI features may be high: must test incrementality after including funding proposal features.

#### References

- Brunnermeier & Pedersen (2009) — "Market Liquidity and Funding Liquidity"
- Cont & Wagalath (2013) — "Fire Sales Forensics: Measuring Endogenous Risk"
- Cong et al. (2022) — "Crypto Wash Trading" (liquidation data quality concerns)

---

## 4. Layer 3: Cross-Dimensional

These families extract alpha from relationships across assets, venues, and volatility estimators. They cannot be obtained from single-asset, single-venue analysis.

### 4.1 Cross-Asset Contagion & Correlation Dynamics (NEW)

**Data:** Multi-symbol `trades` and `quotes` across BTC, ETH, SOL, etc. Already supported by Pointline's multi-symbol query.

**Thesis:** Crypto assets are highly correlated but the correlation structure is time-varying and regime-dependent. Lead-lag relationships shift, correlation breakdowns signal regime transitions, and beta instability creates idiosyncratic alpha opportunities.

**Conceptual model:**
1. **Static correlation:** baseline co-movement structure.
2. **Dynamic correlation:** time-varying changes in co-movement.
3. **Lead-lag:** which asset's moves precede others, and when.
4. **Breakdown/reconvergence:** sudden decorrelation as a transition signal.

#### Hypothesis set

**H1: BTC leads altcoins in normal regimes; relationship breaks during stress.**
Short-horizon BTC returns should have predictive power for ETH/SOL at 1-5 bar lags in calm markets. During stress, either the lead reverses or the relationship breaks entirely.

**H2: Correlation breakdown precedes volatility expansion.**
Sudden decrease in rolling BTC-ETH correlation should forecast higher realized volatility for both assets in the next N bars.

**H3: Beta regime shifts predict idiosyncratic returns.**
When ETH's rolling beta to BTC drops (decorrelation), ETH's subsequent returns carry more idiosyncratic information. Low-beta regimes may offer higher signal-to-noise for ETH-specific features.

**H4: Cross-asset flow divergence is informative.**
When BTC receives net buying flow and ETH receives net selling flow (or vice versa), the divergence should predict which direction resolves — likely in the direction of BTC flow (larger, more informed market).

**H5: Dispersion is a regime indicator.**
Low cross-asset return dispersion indicates herding/crowding. High dispersion indicates differentiation. Transitions between these states should forecast volatility and signal quality changes.

#### Feature families

1. **Rolling correlation:** Pearson/Spearman correlation of BTC-ETH, BTC-SOL returns at multiple windows (50, 200, 1000 bars).
2. **Lead-lag coefficient:** Hayashi-Yoshida estimator of asynchronous lead-lag. Direction and magnitude.
3. **Rolling beta:** OLS beta of altcoin returns on BTC returns. Level, change, and regime (high/mid/low beta).
4. **Correlation change:** first difference of rolling correlation. Large negative change = breakdown signal.
5. **Cross-asset flow divergence:** signed flow imbalance of asset A minus asset B, normalized.
6. **Return dispersion:** cross-sectional standard deviation of returns across N assets.

#### Key risks

- Requires synchronized multi-asset spines. Clock spine or aligned volume bars needed.
- Lead-lag estimation is noisy at short horizons. Hayashi-Yoshida handles asynchronous data but adds complexity.
- Altcoin liquidity limits capacity: signals may be valid but untradeable at scale.
- Correlation is symmetric; causation is not. Must distinguish leading from lagging.

#### References

- Hayashi & Yoshida (2005) — "On Covariance Estimation of Non-Synchronously Observed Diffusion Processes"
- Forbes & Rigobon (2002) — "No Contagion, Only Interdependence" (correlation bias during stress)
- Barndorff-Nielsen & Shephard (2004) — "Measuring the Impact of Jumps on Multivariate Price Processes"

---

### 4.2 Cross-Venue Price Discovery (NEW)

**Data:** Multi-exchange `trades` and `quotes` for the same asset. Pointline supports 26+ exchanges.

**Thesis:** Price discovery is distributed across venues. The venue that leads price formation varies over time and by regime. Measuring where information originates creates a meta-signal: when venue A leads, features from venue A are more informative. Cross-venue dislocations also create direct arbitrage-style signals.

**Conceptual model:**
1. **Information share:** which venue contributes most to the efficient price.
2. **Dislocation:** transient price differences across venues.
3. **Fragmentation:** how dispersed activity is across venues.
4. **Regime dependence:** leadership shifts during stress, liquidity events, or regulatory changes.

#### Hypothesis set

**H1: The leading venue's flow is more predictive.**
If Binance currently leads price discovery (high information share), then Binance flow imbalance should have higher IC than OKX flow imbalance, and vice versa.

**H2: Cross-venue spread predicts short-horizon returns.**
When venue A trades above venue B for the same asset, subsequent returns should converge (arbitrage closure).

**H3: Fragmentation level modulates signal quality.**
During high fragmentation (activity spread across many venues), microstructure signals are noisier. During low fragmentation (concentrated on one venue), signals from that venue are stronger.

**H4: Information share shifts precede regime changes.**
When leadership migrates from the dominant venue to a secondary venue, it may signal unusual activity (informed trading on the less-monitored venue).

#### Feature families

1. **Hasbrouck information share:** contribution of each venue to efficient price, estimated from VECM.
2. **Cross-venue mid spread:** `(mid_A - mid_B) / mid_A` for venue pairs. Level and z-score.
3. **Fragmentation index:** Herfindahl index of trading volume across venues. Low = concentrated, high = fragmented.
4. **Leadership shift:** change in information share ranking. Binary signal: leadership changed in last N bars.
5. **Cross-venue flow agreement:** correlation of signed flow across venues. High agreement = consensus; low = divergence.

#### Key risks

- Requires extremely tight PIT alignment across venues. Network latency differences between exchanges create measurement error in cross-venue spread.
- Hasbrouck information share estimation requires VECM fitting, which is computationally intensive and sensitive to window choice.
- Arbitrage signals may be consumed by faster participants (HFT) before MFT can react. Must verify signal survival at MFT horizons.
- Some exchanges may have wash trading that distorts volume-based metrics.

#### References

- Hasbrouck (1995) — "One Security, Many Markets: Determining the Contributions to Price Discovery"
- Gonzalo & Granger (1995) — "Estimation of Common Long-Memory Components in Cointegrated Systems"
- Makarov & Schoar (2020) — "Trading and Arbitrage in Cryptocurrency Markets"

---

### 4.3 Realized Volatility Microstructure (NEW)

**Data:** `trades` tick data — already available.

**Thesis:** Different realized volatility estimators extract different information from the price path. The divergence between estimators serves as a jump/discontinuity detector. Volatility roughness, vol-of-vol, and clustering patterns provide standalone regime detection without requiring options data.

**Conceptual model:**
1. **Multi-estimator RV:** each estimator weights different aspects of price path (close-close, range, overnight gap).
2. **Estimator spread:** divergence between estimators proxies for jumps and microstructure noise.
3. **Vol dynamics:** vol-of-vol, roughness, and clustering describe the volatility process itself.
4. **RV vs IV:** realized vol is the denominator of the volatility risk premium (VRP), which is covered in the options proposal.

#### Hypothesis set

**H1: Parkinson (range-based) RV outpredicts close-to-close RV for next-bar returns.**
Range-based estimators use more information from the price path and should produce more accurate vol estimates, improving downstream signal quality.

**H2: Estimator divergence signals jumps.**
When Parkinson RV >> close-to-close RV, the bar contained an intrabar extreme (potential jump or sweep). Jump detection is useful for regime classification.

**H3: Vol-of-vol predicts signal regime quality.**
High vol-of-vol (unstable volatility) should degrade mean-reversion signals and strengthen momentum signals. This is a conditioning variable.

**H4: Roughness (Hurst exponent) of the vol path is informative.**
Low Hurst exponent (rough vol path) indicates anti-persistent vol dynamics — vol is more likely to reverse. High Hurst indicates persistent vol — vol trends continue.

**H5: Volatility clustering structure predicts bar-ahead vol.**
AR/GARCH-style features (lagged RV, lagged squared returns) should have significant IC for forward volatility at MFT horizons.

#### Feature families

1. **Close-to-close RV:** standard deviation of bar returns in rolling window.
2. **Parkinson RV:** range-based estimator using bar high-low. `sqrt(1/(4*N*ln(2)) * sum(ln(H/L)^2))`.
3. **Garman-Klass RV:** uses open, high, low, close. More efficient than Parkinson.
4. **Estimator spread:** `Parkinson_RV - CC_RV`. Jump/noise proxy.
5. **Vol-of-vol:** rolling standard deviation of RV estimates. Regime instability proxy.
6. **Hurst exponent:** estimated from vol path at multiple scales. Roughness indicator.
7. **Vol autocorrelation:** ACF of RV at lags 1, 5, 10. Clustering measure.

#### Key risks

- Microstructure noise (bid-ask bounce) biases RV estimators upward. Must use subsampled or noise-robust estimators at very short horizons.
- Hurst exponent estimation is statistically fragile at short samples. Requires careful window selection.
- Heavy overlap with the options proposal's RV component. Must clearly delineate: this family operates without options data; the options proposal adds IV on top.

#### References

- Parkinson (1980) — "The Extreme Value Method for Estimating the Variance of the Rate of Return"
- Garman & Klass (1980) — "On the Estimation of Security Price Volatilities from Historical Data"
- Gatheral, Jaisson & Rosenbaum (2018) — "Volatility is Rough"
- Andersen & Bollerslev (1998) — "Answering the Skeptics: Yes, Standard Volatility Models Do Provide Accurate Forecasts"

---

## 5. Layer 4: Meta / Conditioning

These families do not generate standalone alpha. They condition, sharpen, and route signals from Layers 1-3.

### 5.1 Multi-Timeframe Features (Existing)

**Full proposal:** [`multitimeframe-features-mft-research-proposal.md`](multitimeframe-features-mft-research-proposal.md)

**Core hypotheses:** Context-conditioned fast signals outperform raw fast signals (H1), divergence predicts regime transition (H2), alignment captures higher-quality flow (H3), ratio-dependent efficacy (H4), staleness-adjusted context improves robustness (H5), incremental value survives baseline controls (H6).

**Feature families:** Fast-layer (short-horizon momentum, flow imbalance, microstructure reversion), slow-layer (trend state, slow volatility, participation proxies), interaction (momentum divergence, flow alignment, trend-confirmation cross terms), structure (timeframe ratio diagnostics, context age/staleness, consistency scores), regime (trending/ranging/transition labels).

**Role in full program:** Multi-timeframe is the primary mechanism for combining Layer 1 (microstructure) short-horizon signals with Layer 2 (derivatives) slower-updating context. Fast spine = volume bars with microstructure features; slow spine = clock or larger volume bars with funding, basis, and vol features. The interaction layer tests whether the combination is more than the sum of parts.

**Linkages:** Applies to every Layer 1-3 family. The multi-timeframe framework is the _how_ for combining signals across layers, not a signal source itself.

---

### 5.2 Adaptive Timeframe Features (Existing)

**Full proposal:** [`adaptive-timeframe-features-mft-research-proposal.md`](adaptive-timeframe-features-mft-research-proposal.md)

**Core hypotheses:** Information-density stabilization (H1), regime-conditional alpha improvement (H2), transition-state predictability (H3), volatility-normalized feature superiority (H4), control-lag tradeoff (H5), symbol-specific calibration necessity (H6).

**Feature families:** State (RV proxies, liquidity stress, bar-formation speed), policy (target granularity, adaptation delta), regime (label, age, transition flags), normalized alpha (vol-scaled flow/momentum/reversion), control-risk (adaptation frequency, threshold drift, stability penalties).

**Role in full program:** Adaptive timeframe controls the sampling policy for the fast spine in the multi-timeframe framework. During high-vol regimes (which the RV microstructure family from §4.3 can detect, or liquidation cascades from §3.4 can trigger), the adaptive layer should widen bars to stabilize information density. This creates a feedback loop: Layer 2/3 signals inform the sampling policy that Layer 1 signals are computed on.

**Linkages:** Depends on RV microstructure (§4.3) for volatility state estimation and on liquidation cascades (§3.4) for stress detection. Temporal features (§5.3) provide the session-regime context that may require different adaptation policies for Asian vs US hours.

### 5.3 Temporal / Calendar Features (NEW)

**Data:** Timestamp metadata only. Zero additional data cost.

**Thesis:** Crypto markets exhibit strong temporal patterns: intraday session effects (Asia/EU/US), funding settlement cycles (8-hour windows), macro event proximity, and weekend liquidity regimes. These patterns are low-cost conditioning variables that modulate every other signal family.

**Conceptual model:**
1. **Session regime:** different participant bases are active at different times. Asian hours have different flow characteristics than US hours.
2. **Settlement proximity:** funding settlement every 8 hours creates predictable behavioral patterns.
3. **Calendar regime:** weekends, month-end, and macro events create liquidity and volatility regime shifts.
4. **Temporal deviation:** actual activity vs expected-at-this-time deviations detect unusual conditions.

#### Hypothesis set

**H1: Intraday session identity modulates signal IC.**
Flow-imbalance features should have different IC during Asian vs European vs US hours, reflecting different participant mixes.

**H2: Funding settlement proximity creates nonlinear behavior.**
Signal behavior should change within 30 minutes of settlement (00:00, 08:00, 16:00 UTC). This partially overlaps with the funding proposal but deserves explicit temporal treatment.

**H3: Weekend liquidity regime degrades microstructure signals.**
Saturday/Sunday have thinner books and wider spreads. Depth-based features should have lower IC during weekends.

**H4: Volume deviation from time-of-day profile detects unusual activity.**
Volume significantly above expected-at-this-time is a soft signal for news or informed flow. This is a conditioning variable for other features.

**H5: Macro event proximity shifts volatility regime.**
Within N hours of FOMC/CPI releases, volatility expectations rise. Features conditioned on macro proximity should improve risk management.

#### Feature families

1. **Session indicator:** categorical encoding of current session (Asian/EU/US/overlap). Binary flags for session transitions.
2. **Funding proximity:** continuous feature: minutes until next funding settlement. Cyclic encoding (sin/cos of 8-hour cycle).
3. **Day-of-week / weekend flag:** binary weekend indicator plus day-of-week cyclical encoding.
4. **Volume Z-score by time-of-day:** actual volume / expected volume at this time. Rolling 7-day profile.
5. **Macro proximity:** distance (hours) to next scheduled macro event (FOMC, CPI, NFP). Requires external calendar.

#### Key risks

- Calendar features are prime overfitting targets. Must use strict out-of-sample validation and avoid in-sample calendar mining.
- Macro event calendars shift dates. Must use a robust external calendar source.
- These features have low standalone IC by design. Value is in interaction. Must test conditioning impact, not standalone IC.

#### References

- Breedon & Ranaldo (2013) — "Intraday Patterns in FX Returns and Order Flow"
- Andersen & Bollerslev (1997) — "Intraday Periodicity and Volatility Persistence in Financial Markets"

---

## 6. Unified Evaluation Framework

All signal families should be evaluated under a single protocol to enable apples-to-apples comparison.

### 6.1 Targets

1. **Primary:** Forward mid-return at 1, 3, 5, 10 bars on volume bars.
2. **Secondary:** Forward executable return (accounting for spread) at same horizons.
3. **Optional:** Event-time returns around settlement, liquidation, and session transitions.

### 6.2 Validation Protocol

1. **Walk-forward:** 60/20/20 train/validate/test splits, expanding window.
2. **Regime slicing:** evaluate separately under HIGH/MED/LOW volatility, TRENDING/RANGING, and LIQUID/ILLIQUID regimes.
3. **Cross-period stability:** rolling IC rank correlation across adjacent evaluation windows.
4. **Cross-symbol:** test on BTC, ETH, and at least one altcoin (SOL or similar).

### 6.3 Metrics

| Metric | Purpose | Target |
|--------|---------|--------|
| Rank IC | Directional predictive power | > 0.03 (standalone), > 0.02 (incremental) |
| IC stability | Cross-period consistency | > 0.60 rank correlation across windows |
| Cost-adjusted Sharpe | Net profitability | > 0.5 after realistic costs |
| Turnover | Capacity constraint | < 200% annualized |
| Regime IC ratio | Signal robustness | Worst regime IC > 50% of best regime IC |
| Incremental IC | Value beyond baseline | > 0.01 after controlling for baseline |

### 6.4 Leakage Controls

1. **PIT alignment:** All features computed from `ts_local_us` (arrival time), not `ts_event_us` (exchange time).
2. **No future information:** Features at time t use only data with `ts_local_us < t`.
3. **Staleness control:** Features from slow-updating streams (funding, OI) must carry age metadata.
4. **Survivorship:** Only symbols active in the evaluation window. No backfill of delisted symbols.

### 6.5 Kill Criteria (Universal)

Reject any feature family if:

1. **Cost death:** IC turns negative after spread + slippage + impact.
2. **Regime concentration:** > 70% of cumulative PnL comes from one regime window.
3. **Sign instability:** feature-return correlation flips sign in > 30% of evaluation windows.
4. **Redundancy:** incremental IC < 0.005 after including simpler baseline features.
5. **Complexity tax:** model with the feature has higher drawdown without proportional Sharpe gain.

---

## 7. Interaction Map

Signal families do not operate in isolation. The following interactions should be explicitly tested.

### 7.1 Cross-Layer Interactions (New × Existing)

These are the highest-priority interactions — they connect the new families in this proposal with the existing derivative-layer proposals.

```
Funding Crowding × Liquidation Cascades  [§3.1 × §3.4]
  → Extreme funding + high OI = pre-cascade state. Funding crowding
     is the fuel; liquidations are the ignition. Joint feature:
     funding z-score × OI level × liquidation intensity should
     forecast cascade continuation vs exhaustion.

Funding Crowding × Flow Toxicity  [§3.1 × §2.2]
  → High VPIN during extreme funding = informed traders front-running
     a squeeze. Funding provides structural context; toxicity measures
     execution urgency. Combined signal should outpredict either alone.

Perp-Spot Basis × Liquidation Direction  [§3.2 × §3.4]
  → Wide positive basis + long liquidations = leveraged longs
     unwinding through perp selling. Basis convergence speed should
     increase when liquidation intensity is high. Joint feature:
     basis direction × liquidation direction alignment score.

Perp-Spot Lead-Lag × Cross-Venue Discovery  [§3.2 × §4.2]
  → Perp-spot lead-lag (H3 of perp-spot proposal) depends on which
     venue leads price discovery. If Binance perp leads while OKX
     spot lags, the perp-spot signal is really a venue-routing signal.
     Information share provides the decomposition.

Perp-Spot Flow Divergence × Flow Toxicity  [§3.2 × §2.2]
  → Perp VPIN vs spot VPIN divergence. If perp flow is toxic but spot
     flow is not, informed traders are operating in the perp venue.
     This refines the perp-leads-spot hypothesis.

Options IV × Realized Vol Microstructure  [§3.3 × §4.3]
  → VRP = IV − RV. The options proposal provides IV; the RV proposal
     provides a better RV denominator via multi-estimator methods.
     Parkinson or Garman-Klass RV should yield more accurate VRP
     than close-to-close RV. Strongest cross-layer dependency.

Options Dealer Gamma × Liquidation Cascades  [§3.3 × §3.4]
  → Negative dealer gamma + active liquidation cascade = amplified
     move (dealers and liquidation engines both pushing same direction).
     Combined regime is the worst-case stress scenario.

Options Skew × Funding Crowding  [§3.3 × §3.1]
  → Skew should widen when funding is extreme (market pricing crash
     risk from leveraged crowding). Skew conditioned on funding regime
     should be a better fear gauge than unconditional skew.

Options Dealer Gamma × Order Book Shape  [§3.3 × §2.1]
  → During high dealer gamma, spot book dynamics are partially driven
     by hedging flow. Book depth features have different information
     content in high-gamma vs low-gamma regimes.
```

### 7.2 Intra-Layer Interactions (New × New)

```
Book Imbalance × Liquidation State  [§2.1 × §3.4]
  → During active cascades, book imbalance is driven by forced flow
     rather than informed positioning. Imbalance signal meaning shifts;
     must adjust interpretation for liquidation context.

Book Depth × Cross-Venue Spread  [§2.1 × §4.2]
  → Thin book on venue A + price premium on venue A = vulnerability
     to arbitrage-driven price correction. Depth × dislocation is a
     compound signal.

Flow Toxicity × Trade Classification  [§2.2 × §2.3]
  → VPIN measures aggregate toxicity; trade classification identifies
     which size bucket drives it. Large-trade toxicity should be more
     informative than small-trade toxicity.

Cross-Asset Lead-Lag × Session Regime  [§4.1 × §5.3]
  → BTC→ETH lead-lag should vary by session (Asian hours: different
     participant mix). Lead-lag conditioned on session should be more
     stable and have higher IC.

Realized Vol × Adaptive Timeframe  [§4.3 × §5.2]
  → RV microstructure provides the volatility state estimate that
     drives the adaptive sampling policy. Multi-estimator RV should
     produce a better state signal than single-estimator RV.

Liquidation Cascades × Temporal/Calendar  [§3.4 × §5.3]
  → Cascade probability may vary with time-of-day (thin liquidity
     during Asian hours → easier to trigger cascades). Settlement
     proximity also increases leverage adjustment activity.
```

### 7.3 Existing × Existing Interactions

These should be tested as part of the re-validation phase (Phase 2).

```
Funding Crowding × Perp-Spot Basis  [§3.1 × §3.2]
  → Funding-basis dislocation (H2 of perp-spot proposal) is itself
     an interaction between these two families. When funding-implied
     carry and observed basis diverge, convergence pressure builds.
     Already hypothesized; needs fresh validation.

Funding Settlement × Multi-Timeframe  [§3.1 × §5.1]
  → Settlement windows create event-time breaks in the slow-context
     layer. Multi-timeframe features should handle settlement as a
     regime transition, not a gradual shift.

Perp-Spot Basis × Options Skew  [§3.2 × §3.3]
  → Wide basis (high perp premium) + steep put skew = market pricing
     both leverage and crash risk. Divergence (wide basis but flat
     skew) may signal complacency.
```

### 7.4 Interaction Testing Protocol

1. **Standalone first:** each family must show IC before interaction testing.
2. **Pairwise interaction:** test each primary interaction pair.
3. **Combined model:** only after pairwise effects are understood, build a combined model.
4. **Complexity budget:** each added interaction must justify its marginal complexity.

---

## 8. Data Dependency Matrix

| Signal Family | Primary Table | Secondary Tables | External Data | Ingested? |
|--------------|---------------|------------------|---------------|-----------|
| Order Book Shape | `orderbook_updates` | `quotes` | — | Yes |
| Flow Toxicity | `trades` | `quotes` | — | Yes |
| Trade Classification | `trades` | — | — | Yes |
| Funding/Crowding | `derivative_ticker` | `trades` | — | Yes |
| Perp-Spot Basis | `trades` (perp + spot) | `derivative_ticker` | — | Yes |
| Options/Vol Surface | `options_chain` | `trades` | — | Yes |
| Liquidation Cascades | `liquidations` | `derivative_ticker` | — | Yes |
| Cross-Asset Contagion | `trades` (multi-symbol) | `quotes` | — | Yes |
| Cross-Venue Discovery | `trades` (multi-exchange) | `quotes` | — | Yes |
| Realized Vol | `trades` | — | — | Yes |
| Multi-Timeframe | (derived) | All Layer 1-3 | — | Yes |
| Adaptive Timeframe | (derived) | All Layer 1-3 | — | Yes |
| Temporal/Calendar | (derived) | — | Macro calendar | Partial |

**Key finding:** All primary data sources are already ingested or have schemas defined. No new data vendor integrations are required except the macro event calendar (for temporal features).

---

## 9. Phased Research Plan

### Phase 1: Microstructure Foundation (Weeks 1-3)

**Objective:** Establish the core short-horizon alpha layer.

| Week | Family | Deliverable |
|------|--------|-------------|
| 1 | Order Book Shape | Book reconstruction verified; depth imbalance and microprice IC baseline |
| 2 | Flow Toxicity | VPIN and Kyle's lambda implemented; standalone IC report |
| 3 | Trade Classification | Size-bucketed flow and clustering metrics; incremental IC vs raw flow |

**Gate 1 exit:** At least one microstructure family has IC > 0.05 at 1-3 bar horizon after costs.

### Phase 2: Derivatives Completion (Weeks 3-5)

**Objective:** Complete the derivatives context layer.

| Week | Family | Deliverable |
|------|--------|-------------|
| 3-4 | Liquidation Cascades | Liquidation intensity, cascade score; interaction with funding |
| 4-5 | Funding + Perp-Spot (existing) | Re-validate existing proposals on fresh data |

**Gate 2 exit:** Liquidation features show incremental IC > 0.02 beyond funding features.

### Phase 3: Cross-Dimensional (Weeks 5-7)

**Objective:** Add cross-asset and cross-venue diversification.

| Week | Family | Deliverable |
|------|--------|-------------|
| 5-6 | Cross-Asset Contagion | BTC-ETH lead-lag, correlation dynamics; IC by regime |
| 6-7 | Realized Vol Microstructure | Multi-estimator RV, estimator spread; vol prediction IC |
| 7 | Cross-Venue Discovery | Information share estimation; cross-venue spread signals |

**Gate 3 exit:** At least one cross-dimensional family adds IC > 0.01 incrementally to Layer 1 + Layer 2 baseline.

### Phase 4: Meta Layer & Integration (Weeks 7-9)

**Objective:** Condition, combine, and validate the full stack.

| Week | Family | Deliverable |
|------|--------|-------------|
| 7-8 | Temporal/Calendar | Session and settlement conditioning; interaction IC tests |
| 8-9 | Multi-TF + Adaptive (existing) | Re-validate with enriched feature set from Layers 1-3 |
| 9 | Full stack integration | Combined model IC, cost-adjusted Sharpe, regime stability |

**Gate 4 exit:** Full stack cost-adjusted Sharpe > 1.0 on out-of-sample period.

---

## 10. Decision Architecture

### 10.1 Per-Family Decision

Each family follows a promote / iterate / archive decision path:

```
         ┌─── Standalone IC > threshold?
         │
    Yes ─┤    ┌─── Survives cost adjustment?
         │    │
         │ Yes┤    ┌─── Incremental IC > 0 after baseline?
         │    │    │
         │    │ Yes┤ → PROMOTE to combined model
         │    │    │
         │    │  No┤ → ARCHIVE (redundant with baseline)
         │    │
         │   No┤ → ITERATE (refine features or change horizons)
         │
     No ─┤ → ITERATE once; if still fails → ARCHIVE
```

### 10.2 Program-Level Decision

After all families are evaluated:

1. **Minimum viable stack:** Layer 1 (microstructure) + at least 2 from Layer 2 (derivatives).
2. **Full stack:** All 4 layers with promoted families.
3. **Kill the program:** If Layer 1 fails to produce any IC > 0.03 after costs, the MFT opportunity is questionable for these instruments.

---

## 11. Risk Register

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Order book reconstruction errors | Invalid depth features | Medium | Verify against periodic snapshots; checksum depth sums |
| Liquidation data incompleteness | Understated cascade signals | Medium | Cross-validate against OI drawdowns; multi-exchange coverage |
| Cross-venue timestamp misalignment | False arbitrage signals | High | Conservative PIT buffers (add latency margin); sensitivity tests |
| Overfitting to calendar patterns | False positive temporal features | High | Strict out-of-sample; no calendar mining; interaction-only evaluation |
| Correlation spuriousness during stress | Cross-asset features unreliable in crises | Medium | Forbes-Rigobon bias correction; regime-conditional evaluation |
| Options data unavailability | Blocks vol surface family | Medium (external) | RV microstructure as fallback; standalone vol estimation |
| Signal decay at MFT horizons | Microstructure signals too fast | Medium | Evaluate at multiple horizons; accept that some features target 1-bar only |
| Compute cost of full stack | Research iteration slows | Low | Prioritize fast-to-compute families first; lazy evaluation |

---

## 12. Success Criteria

### 12.1 Research Phase

- [ ] All 12 families have hypothesis scoreboards (supported / mixed / rejected by regime).
- [ ] At least 6 families promoted to combined model.
- [ ] Full stack IC > 0.06 on out-of-sample (vs 0.04 spot-only baseline from existing proposals).
- [ ] Cost-adjusted Sharpe > 1.0 on out-of-sample.
- [ ] No single family contributes > 40% of combined IC (diversification).

### 12.2 Data Quality

- [ ] Order book reconstruction verified against snapshots (< 0.1% error rate).
- [ ] Liquidation coverage validated against OI changes (> 80% of large OI drops explained).
- [ ] Cross-venue timestamp alignment verified (< 50ms systematic bias between venues).
- [ ] All features pass PIT leakage audit (no future data contamination).

### 12.3 Documentation

- [ ] Per-family hypothesis scoreboard.
- [ ] Regime map: when each family adds value vs fails.
- [ ] Interaction matrix: which pairwise combinations are synergistic.
- [ ] Cost-aware recommendations per family.
- [ ] Handoff notes for implementation phase.

---

## 13. Recommendation

Build the MFT signal program as a **layered architecture**, not a bag of independent features.

**Priority order:**

1. **Microstructure first** (order book, flow toxicity, trade classification) — this is the missing foundation. Without short-horizon alpha, derivatives context has nothing to condition.
2. **Liquidation cascades second** — this is the highest-value gap in the derivatives layer, and it creates the strongest interaction with both microstructure and existing funding/OI features.
3. **Cross-asset and realized vol third** — diversification and standalone vol estimation.
4. **Calendar/temporal last** — conditioning layer that sharpens everything but has no standalone value.

Existing proposals (funding, perp-spot, options, multi-TF, adaptive) should be re-validated on fresh data as part of Gates 2 and 4. Do not assume prior IC estimates are current.

Delay production promotion of any family until the unified evaluation framework (Section 6) is applied to the full stack. Individual family ICs are necessary but not sufficient — the program succeeds or fails as an integrated system.

---

## References (Consolidated)

### Microstructure
- Cont, Stoikov & Talreja (2010) — "A Stochastic Model for Order Book Dynamics"
- Stoikov (2018) — "The Micro-Price"
- Easley, Lopez de Prado & O'Hara (2012) — "Flow Toxicity and Liquidity in a High-Frequency World"
- Kyle (1985) — "Continuous Auctions and Insider Trading"
- Glosten & Milgrom (1985) — "Bid, Ask and Transaction Prices in a Specialist Market"
- Lee & Ready (1991) — "Inferring Trade Direction from Intraday Data"
- Cartea, Jaimungal & Penalva (2015) — "Algorithmic and High-Frequency Trading"

### Derivatives & Liquidations
- Brunnermeier & Pedersen (2009) — "Market Liquidity and Funding Liquidity"
- Cont & Wagalath (2013) — "Fire Sales Forensics: Measuring Endogenous Risk"

### Cross-Dimensional
- Hayashi & Yoshida (2005) — "On Covariance Estimation of Non-Synchronously Observed Diffusion Processes"
- Forbes & Rigobon (2002) — "No Contagion, Only Interdependence"
- Hasbrouck (1995) — "One Security, Many Markets"
- Gonzalo & Granger (1995) — "Estimation of Common Long-Memory Components"
- Makarov & Schoar (2020) — "Trading and Arbitrage in Cryptocurrency Markets"

### Volatility
- Parkinson (1980) — "The Extreme Value Method for Estimating the Variance of the Rate of Return"
- Garman & Klass (1980) — "On the Estimation of Security Price Volatilities from Historical Data"
- Gatheral, Jaisson & Rosenbaum (2018) — "Volatility is Rough"
- Andersen & Bollerslev (1998) — "Answering the Skeptics: Yes, Standard Volatility Models Do Provide Accurate Forecasts"

### Temporal
- Breedon & Ranaldo (2013) — "Intraday Patterns in FX Returns and Order Flow"
- Andersen & Bollerslev (1997) — "Intraday Periodicity and Volatility Persistence in Financial Markets"
