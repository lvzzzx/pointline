# Funding Rate Features for Crypto MFT - Idea-First Research Proposal

**Persona:** Quant Researcher (MFT)
**Status:** Proposed
**Date:** 2026-02-13
**Scope:** Signal research design (not implementation)

---

## Executive Summary

This proposal reframes funding-rate research as an **idea and hypothesis program** rather than an implementation guide. The core thesis remains strong: funding rates are the market-implied price of leveraged directional imbalance in perpetual futures, and that imbalance can help explain short-horizon returns, squeezes, and regime shifts.

The prior guide contains useful intuition, but its concrete IC numbers, "production-ready" framing, and implementation pathways are time-sensitive and should be treated as stale unless re-validated.

This document defines:

1. Which ideas are robust enough to keep.
2. Which claims require fresh evidence.
3. A falsifiable experiment design for MFT horizons (seconds to minutes).
4. Decision gates for promote/iterate/kill outcomes.

---

## 1. Conceptual Model

Funding-rate signals should be viewed as a three-layer system:

1. **State (Crowding):** level of funding and open interest describe directional crowding and leverage inventory.
2. **Change (Shock):** first/second differences in funding and OI capture acceleration/deceleration of crowding.
3. **Interaction (Trigger):** order-flow or return shocks interacting with crowding state determine whether imbalance resolves via continuation or reversal.

Research objective: identify when funding conditions imply **predictable forward return asymmetry** after costs.

---

## 2. Hypothesis Set

### H1: Crowding Mean Reversion
When funding is extreme relative to recent distribution, short-horizon forward returns should mean-revert as crowded positioning unwinds.

### H2: Funding Surprise Information
Unexpected funding updates (realized minus expected) should forecast near-term directional pressure, with stronger effect during high-leverage regimes.

### H3: Funding-OI Pressure
Joint increases in funding and OI indicate unstable leverage expansion and higher squeeze probability; effect direction depends on sign of crowding and concurrent flow.

### H4: Flow x Funding Interaction
Order-flow imbalance aligned with costly crowding indicates informed continuation; opposite alignment suggests exhaustion and reversal.

### H5: Event-Time Convexity Near Settlement
Predictive strength and risk profile should change nonlinearly around funding settlement windows; these windows should be modeled explicitly, not treated as generic intraday time.

---

## 3. Feature Families (Idea Definitions)

1. **Level Features:** funding percentile, annualized carry proxy, cross-venue funding spread.
2. **Delta Features:** funding change, expected-vs-realized gap, OI change and acceleration.
3. **Interaction Features:** funding x flow imbalance, funding x volatility, funding x OI change.
4. **Regime Features:** volatility regime, trend regime, liquidity regime, settlement proximity.
5. **Structure Features:** persistence of extreme funding states, transition probability between funding regimes.

No feature should be considered valid without regime-conditional and cost-adjusted evidence.

---

## 4. Evaluation Design (Falsifiable)

### 5.1 Targets and Horizons

1. Forward mid-return or executable-return over multiple MFT horizons (for example 1, 3, 5, 10 bars).
2. Optional event-based targets around settlement windows.

### 5.2 Validation Protocol

1. Walk-forward or rolling-origin splits.
2. Regime-sliced evaluation (trend, volatility, liquidity, bull/bear proxies).
3. Cross-period stability checks instead of single-window averages.

### 5.3 Metrics

1. Rank IC and Pearson IC by horizon.
2. Hit rate conditioned on signal magnitude buckets.
3. Turnover-adjusted and cost-adjusted Sharpe/IR.
4. Drawdown and tail-risk sensitivity during squeeze events.

### 5.4 Leakage and Bias Controls

1. Strict point-in-time alignment across all streams.
2. Timestamp-latency sensitivity tests (arrival vs exchange time choices).
3. Missingness/staleness stress tests for funding/OI updates.
4. Survivorship and symbol-selection robustness checks.

### 5.5 Kill Criteria

Kill or demote a feature family if any of the following hold after robust testing:

1. Signal disappears after realistic costs/slippage.
2. Performance is concentrated in one short regime window.
3. Direction flips unpredictably across adjacent periods.
4. Effect is explained away by simpler baseline features.

---

## 5. Practical Tradability Lens

All interpretation should be filtered through execution reality:

1. Funding signals are slower than pure microstructure signals; they are context variables, not always direct triggers.
2. Settlement windows may provide edge but also concentrate execution risk.
3. Signals should be assessed jointly with spread, depth, and impact proxies.
4. Position sizing should tighten when funding and volatility both become extreme.

---

## 6. Decision Gates

### Gate 1: Data Reliability
Funding/OI coverage, freshness, and timestamp integrity are sufficient for PIT-safe research.

### Gate 2: Idea Validity
At least one funding feature family shows stable directional effect across regimes before full model integration.

### Gate 3: Economic Viability
Signal retains value after fees, slippage, and conservative impact assumptions.

### Gate 4: Portfolio Incrementality
Funding features improve a baseline MFT stack on risk-adjusted metrics, not just standalone IC.

---

## 7. Expected Deliverables (Idea Phase)

1. Hypothesis scoreboard (supported / mixed / rejected by regime).
2. Regime map of when funding features are additive vs harmful.
3. Cost-aware recommendation: deploy, monitor-only, or archive.
4. Clear handoff notes for a future implementation phase.

---

## 8. Recommendation

Proceed with funding-rate research as a **conditional alpha layer** rather than a universal standalone signal. Treat funding as a crowding-state variable that becomes most useful when combined with flow, OI dynamics, and settlement-event context. Avoid production promotion until the full falsification checklist is passed on recent data.
