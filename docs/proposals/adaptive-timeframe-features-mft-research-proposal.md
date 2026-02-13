# Adaptive Timeframe Features for Crypto MFT - Idea-First Research Proposal

**Persona:** Quant Researcher (MFT)
**Status:** Proposed
**Date:** 2026-02-13
**Scope:** Signal research design (not implementation)

---

## Executive Summary

This proposal reframes adaptive-timeframe work as an **idea and falsification program**. The core thesis is that fixed sampling granularity causes regime-dependent failure modes, while volatility-aware adaptive sampling can stabilize information density and improve short-horizon signal quality.

The prior guide has strong intuition, but fixed IC tables, implementation pathways, and "production-ready" framing are time-sensitive and should be treated as stale until re-validated.

This document defines:

1. Which adaptive-timeframe ideas are robust enough to keep.
2. Which claims require fresh evidence.
3. A falsifiable research design for MFT horizons.
4. Decision gates for promote/iterate/kill outcomes.

---

## 1. Conceptual Model

Adaptive timeframe research can be framed as a feedback loop:

1. **State Estimation:** infer current market state (volatility, liquidity, transition risk).
2. **Sampling Policy:** map state to sampling granularity (bar size/frequency targets).
3. **Signal Extraction:** compute features on the resulting event stream.
4. **Stability Control:** monitor whether adaptation improves information quality after costs.

Research objective: determine whether adaptive sampling improves **cost-adjusted predictive stability** versus fixed sampling.

---

## 2. Hypothesis Set

### H1: Information-Density Stabilization
Adaptive sampling should reduce regime-driven variance in feature quality by stabilizing information density per bar.

### H2: Regime-Conditional Alpha Improvement
Feature efficacy should improve in at least one regime (for example high-volatility flow features), without severe degradation in others.

### H3: Transition-State Predictability
Signals tied to regime shifts should have incremental value beyond steady-state features.

### H4: Volatility-Normalized Feature Superiority
Volatility-scaled features should outperform raw features on cross-regime robustness.

### H5: Control-Lag Tradeoff
Faster adaptation should improve responsiveness but can increase noise; there should be an optimal adaptation speed.

### H6: Symbol-Specific Calibration Necessity
Adaptive policies should require symbol/venue-specific calibration; global parameters should underperform calibrated ones.

---

## 3. Feature Families (Idea Definitions)

1. **State Features:** realized-vol proxies, liquidity stress, bar-formation speed diagnostics.
2. **Policy Features:** target granularity, adaptation delta, adaptation smoothness.
3. **Regime Features:** regime label, regime age, transition flags, transition direction.
4. **Normalized Alpha Features:** volatility-scaled flow, momentum, and reversion measures.
5. **Control-Risk Features:** adaptation frequency, threshold drift, stability penalties.

No family should be accepted without demonstrating incremental value over fixed-timeframe baselines.

---

## 4. Evaluation Design (Falsifiable)

### 5.1 Targets and Horizons

1. Forward executable returns over multiple MFT horizons (for example 1, 3, 5, 10 bars).
2. Optional transition-event targets around state changes.

### 5.2 Validation Protocol

1. Walk-forward or rolling-origin validation.
2. Regime-sliced and transition-sliced performance evaluation.
3. Cross-symbol and cross-period stability testing.

### 5.3 Metrics

1. Rank IC/Pearson IC by regime and horizon.
2. Cost-adjusted Sharpe/IR and turnover burden.
3. Signal monotonicity by feature magnitude buckets.
4. Sampling-stability metrics (bar duration dispersion, stale-feature rate).

### 5.4 Leakage and Bias Controls

1. Strict PIT state estimation (no future data in adaptation policy).
2. State-estimation latency sensitivity tests.
3. Missingness/staleness stress tests in low-activity periods.
4. Baseline parity checks against fixed-timeframe controls.

### 5.5 Kill Criteria

Demote or reject if any hold after robust testing:

1. Improvement disappears after realistic transaction costs.
2. Gains come only from one short historical window.
3. Adaptation increases instability (higher drawdown/tail risk) without clear alpha gain.
4. Fixed-timeframe baseline matches performance with lower complexity.

---

## 5. Practical Tradability Lens

Interpretation should stay execution-aware:

1. Adaptive sampling can improve signal freshness but may increase turnover.
2. Rapid adaptation can amplify execution noise in stressed conditions.
3. Policy changes should be bounded to avoid unstable live behavior.
4. Complexity cost must be justified by robust incremental PnL after costs.

---

## 6. Decision Gates

### Gate 1: State Reliability
State estimates are PIT-safe and stable enough to drive sampling decisions.

### Gate 2: Idea Validity
At least one adaptive feature family shows robust regime-conditional improvement versus fixed baseline.

### Gate 3: Economic Viability
Adaptive gains survive fees, slippage, and conservative impact assumptions.

### Gate 4: Complexity Justification
Net benefit from adaptation exceeds additional operational/model complexity.

---

## 7. Expected Deliverables (Idea Phase)

1. Hypothesis scoreboard (supported / mixed / rejected by regime).
2. Regime map showing when adaptation helps, is neutral, or harms.
3. Cost-aware recommendation: adaptive deploy, fixed baseline, or hybrid policy.
4. Handoff criteria for future implementation phase.

---

## 8. Recommendation

Proceed with adaptive timeframe research as a **sampling-policy layer** rather than a standalone alpha source. Focus on whether adaptation improves cross-regime robustness after costs, and promote only if the policy shows consistent incremental value versus simpler fixed-timeframe alternatives.
