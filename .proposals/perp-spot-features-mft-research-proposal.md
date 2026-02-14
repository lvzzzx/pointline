# Perp-Spot Features for Crypto MFT - Idea-First Research Proposal

**Persona:** Quant Researcher (MFT)
**Status:** Proposed
**Date:** 2026-02-13
**Scope:** Signal research design (not implementation)

---

## Executive Summary

This proposal reframes perp-spot feature research as an **idea and falsification program**. The central thesis is that the perp-spot relationship encodes leverage demand, inventory pressure, and price-discovery hierarchy, which can generate tradable short-horizon asymmetries when evaluated with strict PIT controls and realistic execution costs.

The prior guide includes useful intuition, but concrete IC tables, implementation pathways, and "production-ready" framing are time-sensitive and should not be treated as current evidence.

This document defines:

1. Which perp-spot ideas are robust enough to keep.
2. Which claims require fresh evidence.
3. A falsifiable research design for MFT horizons.
4. Decision gates for promote/iterate/kill outcomes.

---

## 1. Conceptual Model

Perp-spot signals can be organized into three linked layers:

1. **State (Relative Pricing):** basis level, basis percentile, and perp/spot volume ratio describe leverage pressure and market structure.
2. **Change (Adjustment):** basis momentum, funding-basis gap dynamics, and flow divergence changes capture convergence/divergence processes.
3. **Interaction (Transmission):** cross-signals (flow x basis, volatility x basis, settlement proximity x dislocation) determine whether moves continue, mean-revert, or regime-shift.

Research objective: identify when perp-spot dislocations produce **cost-adjusted forward return asymmetry**.

---

## 2. Hypothesis Set

### H1: Basis Extremes Mean-Revert Conditionally
Large positive or negative basis should mean-revert, but only under sufficient liquidity and absent shock-regime continuation.

### H2: Funding-Basis Dislocation Predicts Convergence
When funding-implied carry and observed basis diverge materially, subsequent returns should reflect a convergence channel.

### H3: Perp Leads Spot in Normal Regimes
Short-horizon perp moves should contain predictive information for subsequent spot adjustment, with breakdowns during stress or venue fragmentation.

### H4: Flow Divergence Signals Participant Segmentation
Perp and spot flow disagreement should forecast short-term rebalancing pressure and potential reversal/continuation asymmetry.

### H5: Volume-Asymmetry Regime Dependence
Signal behavior should vary with perp/spot activity ratio; extreme asymmetry may increase signal strength for some features and degrade others via execution risk.

### H6: Settlement-Window Nonlinearity
Feature efficacy and risk should change around funding settlement windows, requiring explicit event-time conditioning.

---

## 3. Feature Families (Idea Definitions)

1. **Basis Structure Features:** basis level, z-score/percentile, slope, persistence.
2. **Carry Alignment Features:** funding-basis gap, gap velocity, gap persistence.
3. **Lead-Lag Features:** perp-minus-spot short-horizon return deltas and recovery profiles.
4. **Cross-Flow Features:** perp/spot flow divergence and flow-basis interactions.
5. **Regime Features:** perp/spot activity ratio, liquidity stress proxies, volatility regime, settlement proximity.

No feature family should be accepted without out-of-sample and cost-adjusted robustness.

---

## 4. Evaluation Design (Falsifiable)

### 5.1 Targets and Horizons

1. Forward returns on executable prices over multiple MFT horizons (for example 1, 3, 5, 10 bars).
2. Optional event-time targets around settlement windows and fast dislocation episodes.

### 5.2 Validation Protocol

1. Rolling or walk-forward splits.
2. Regime segmentation by volatility, liquidity, and trend context.
3. Time-of-day and venue-state slices for stability testing.

### 5.3 Metrics

1. Rank IC/Pearson IC by horizon and regime.
2. Magnitude-bucket hit rates and monotonicity checks.
3. Cost-adjusted Sharpe/IR and turnover burden.
4. Drawdown/tail behavior during liquidation-like regimes.

### 5.4 Leakage and Bias Controls

1. Strict point-in-time stream alignment.
2. Timestamp-choice sensitivity (arrival vs exchange-time assumptions).
3. Staleness tests for funding and sparse spot updates.
4. Venue availability and symbol-selection survivorship checks.

### 5.5 Kill Criteria

Demote or reject a feature family if any hold after robust testing:

1. Alpha vanishes under conservative transaction-cost assumptions.
2. Signal sign is unstable across adjacent periods.
3. Performance depends on rare stress windows only.
4. Incremental value disappears once simpler baseline features are included.

---

## 5. Practical Tradability Lens

Interpretation must remain execution-aware:

1. Perp-spot opportunities can be capacity-limited by spot depth and venue frictions.
2. Signals may look strong on mid-prices but fail on executable pricing.
3. Volume-asymmetry regimes can improve predictability while worsening slippage.
4. Settlement windows can increase both edge and tail risk.

---

## 6. Decision Gates

### Gate 1: Data Reliability
Perp, spot, and funding streams have sufficient coverage/freshness for PIT-safe evaluation.

### Gate 2: Idea Validity
At least one perp-spot feature family shows stable directional behavior across regimes.

### Gate 3: Economic Viability
Signal value survives fees, slippage, and conservative impact assumptions.

### Gate 4: Portfolio Incrementality
Perp-spot features improve a baseline MFT stack on risk-adjusted metrics, not only standalone IC.

---

## 7. Expected Deliverables (Idea Phase)

1. Hypothesis scoreboard (supported / mixed / rejected by regime).
2. Regime map showing where perp-spot signals add value vs fail.
3. Cost-aware go/no-go recommendation for each feature family.
4. Clean handoff notes for a future implementation phase.

---

## 8. Recommendation

Proceed with perp-spot research as a **market-structure alpha layer** centered on dislocation dynamics, not as a single static basis signal. Prioritize conditional modeling (regime + execution constraints + settlement context), and delay production promotion until the full falsification checklist is passed on recent data.
