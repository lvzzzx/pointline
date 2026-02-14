# Multi-Timeframe Features for Crypto MFT - Idea-First Research Proposal

**Persona:** Quant Researcher (MFT)
**Status:** Proposed
**Date:** 2026-02-13
**Scope:** Signal research design (not implementation)

---

## Executive Summary

This proposal reframes multi-timeframe work as an **idea and falsification program**. The core thesis is that combining fast and slow views of market state can separate noise from structure, improving short-horizon decision quality when joins are PIT-safe and evaluated with execution-aware metrics.

The prior guide contains strong intuition, but fixed IC tables, implementation details, and "production-ready" framing are time-sensitive and should be treated as stale until re-validated.

This document defines:

1. Which multi-timeframe ideas are robust enough to keep.
2. Which claims require fresh evidence.
3. A falsifiable research design for MFT horizons.
4. Decision gates for promote/iterate/kill outcomes.

---

## 1. Conceptual Model

Multi-timeframe research can be modeled as hierarchical signal decomposition:

1. **Fast Layer:** short-horizon microstructure and local flow state.
2. **Slow Layer:** trend, volatility, and participation context.
3. **Interaction Layer:** alignment/divergence between layers to detect continuation vs reversal.

Research objective: test whether interaction-layer features provide **incremental, cost-adjusted value** over single-timeframe models.

---

## 2. Hypothesis Set

### H1: Context-Conditioned Fast Signals Outperform Raw Fast Signals
Fast features should perform better when conditioned on slow-layer regime context.

### H2: Divergence Predicts Regime Transition
Large fast-vs-slow divergence should forecast either reversion or breakout depending on slow-layer trend strength.

### H3: Alignment Captures Higher-Quality Flow
When flow and momentum align across timeframes, continuation probability should increase.

### H4: Ratio-Dependent Efficacy
Predictive value should be sensitive to fast/slow granularity ratio; too-close and too-far ratios should degrade performance.

### H5: Staleness-Adjusted Context Improves Robustness
Context features should decay with slow-layer age; stale slow context should weaken model confidence.

### H6: Incremental Value Survives Baseline Controls
Multi-timeframe features should retain value after controlling for strong single-timeframe baselines.

---

## 3. Feature Families (Idea Definitions)

1. **Fast-Layer Features:** short-horizon momentum, flow imbalance, microstructure reversion.
2. **Slow-Layer Features:** trend state, slow volatility, slow participation proxies.
3. **Interaction Features:** momentum divergence, flow alignment/divergence, trend-confirmation cross terms.
4. **Structure Features:** timeframe ratio diagnostics, context age/staleness, layer consistency scores.
5. **Regime Features:** trending/ranging/transition labels and confidence weights.

No family should be accepted without proving incrementality over single-timeframe alternatives.

---

## 4. Evaluation Design (Falsifiable)

### 5.1 Targets and Horizons

1. Forward executable returns over multiple MFT horizons (for example 1, 3, 5, 10 bars).
2. Optional transition-event targets for divergence and alignment events.

### 5.2 Validation Protocol

1. Walk-forward/rolling validation with strict PIT joins.
2. Regime-sliced results (trend, range, transition).
3. Ratio-sweep experiments to test granularity sensitivity.

### 5.3 Metrics

1. Rank IC/Pearson IC by horizon and regime.
2. Cost-adjusted Sharpe/IR and turnover burden.
3. Magnitude-bucket monotonicity for interaction features.
4. Incremental lift versus single-timeframe benchmark models.

### 5.4 Leakage and Bias Controls

1. Backward-only contextual joins (no lookahead).
2. Slow-context staleness limits and age sensitivity checks.
3. Null-handling controls for early/edge bars.
4. Timestamp semantics robustness under alternative latency assumptions.

### 5.5 Kill Criteria

Demote or reject if any hold after robust testing:

1. Apparent gains disappear after costs and slippage.
2. Improvement exists only in one narrow regime.
3. Interaction features are unstable under small ratio changes.
4. Single-timeframe baseline matches performance at lower complexity.

---

## 5. Practical Tradability Lens

Interpretation should remain execution-aware:

1. Multi-timeframe context can reduce false positives but may increase model latency and complexity.
2. Slow-context staleness can create stale conviction if not explicitly penalized.
3. Additional joins and features are justified only if they improve net risk-adjusted returns after costs.
4. Capacity and turnover constraints should be assessed per timeframe design.

---

## 6. Decision Gates

### Gate 1: Data & Join Reliability
Fast and slow streams support PIT-safe context construction with controlled staleness.

### Gate 2: Idea Validity
At least one interaction feature family shows stable directional value across regimes.

### Gate 3: Economic Viability
Incremental lift survives fees, slippage, and conservative impact assumptions.

### Gate 4: Incremental Complexity Test
Added complexity is justified by robust, persistent improvement over single-timeframe baselines.

---

## 7. Expected Deliverables (Idea Phase)

1. Hypothesis scoreboard (supported / mixed / rejected by regime).
2. Ratio-sensitivity map (where multi-timeframe adds value).
3. Cost-aware recommendation by feature family.
4. Handoff criteria for future implementation phase.

---

## 8. Recommendation

Proceed with multi-timeframe research as a **context-and-interaction alpha layer** rather than a broad feature expansion exercise. Prioritize robust incremental lift over single-timeframe baselines, enforce strict PIT context controls, and promote only after regime-stable, cost-adjusted evidence is established.
