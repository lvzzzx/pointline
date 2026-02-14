# Multi-Timeframe Features for Crypto MFT: Research Proposal

## Executive Summary

This proposal evaluates multi-timeframe features as a context-and-interaction layer for crypto MFT. The core thesis is that combining fast microstructure signals with slower regime context can improve signal quality, if joins are PIT-safe and context staleness is controlled.

Success requires incremental net value versus strong single-timeframe baselines.

## Research Objective and Hypothesis

Objective: test whether cross-timeframe interactions provide robust, cost-adjusted value beyond single-timeframe models.

Hypothesis H1: Fast features perform better when conditioned on slow context.

Hypothesis H2: Fast-vs-slow divergence predicts transition states.

Hypothesis H3: Cross-timeframe alignment captures higher-quality continuation.

Hypothesis H4: Performance depends on fast/slow ratio and has an optimal band.

Hypothesis H5: Context staleness penalties improve robustness.

Hypothesis H6: Incremental value survives controls for strong single-timeframe baselines.

## Scope and Non-Goals

In scope:

- Fast/slow feature design and PIT-safe context joins.
- Interaction feature engineering and ratio sweeps.
- Cost-aware incremental evaluation.

Non-goals:

- Blind expansion of feature count without incremental tests.
- Ignoring context age and staleness effects.

## Data and PIT Constraints

Primary inputs:

- Fast streams: `trades`, `quotes`, `orderbook_updates`.
- Slow context: derived trend/volatility/participation states.

PIT constraints:

- Slow context must join backward-only with explicit age features.
- No future bar information in fast/slow alignment.
- Null/staleness handling required at session edges and sparse periods.

## Feature or Model Concept

Feature families:

- Fast-layer: short-horizon flow/momentum/microstructure signals.
- Slow-layer: trend, vol regime, participation context.
- Interaction: alignment/divergence and confirmation terms.
- Structure: ratio diagnostics and context-age metrics.
- Regime: trend/range/transition classification.

## Experiment Design

Phase 1: PIT join and staleness validation.

- Verify context alignment correctness and age decay behavior.

Phase 2: interaction value tests.

- Compare interaction features against single-timeframe baselines.
- Sweep fast/slow ratio grids.

Phase 3: economic and robustness checks.

- Apply execution costs and turnover constraints.
- Evaluate regime stability and sensitivity.

## Evaluation Metrics and Acceptance Criteria

Primary metrics:

- IC metrics by horizon/regime.
- Incremental lift vs single-timeframe baselines.
- Cost-adjusted Sharpe/IR and turnover.
- Ratio-sensitivity stability profile.

Acceptance criteria:

- Consistent incremental value out of sample.
- Benefits survive cost assumptions.
- No severe brittleness to minor ratio changes.

Failure criteria:

- Baseline parity at lower complexity.
- Gains collapse under realistic costs.
- Interaction signal sign instability across windows.

## Risks and Mitigations

Risk: leakage in fast/slow joins.

Mitigation: strict backward joins, timestamp tests, and age features.

Risk: overfitting to one timeframe ratio.

Mitigation: ratio plateau requirement and broad sensitivity analysis.

Risk: added latency/complexity without net benefit.

Mitigation: explicit complexity-adjusted acceptance gates.

## Implementation Readiness

If approved:

- ExecPlan for PIT-safe multi-timeframe feature pipeline.
- Ratio sweep experiment harness.
- Standard reports for incrementality and robustness.

## Related Proposals

- `adaptive-timeframe-features-mft-research-proposal.md`: Adaptive policy can reshape the fast layer used in multi-timeframe joins.
- `funding-rate-features-mft-research-proposal.md`: Key slow-context family for multi-timeframe conditioning.
- `perp-spot-features-mft-research-proposal.md`: Major interaction target for fast/slow dislocation features.
- `crypto-mft-research-program-proposal.md`: Defines program-level role of meta-conditioning layers.

## Clarifying Questions for Requester

- Question: Should we optimize for robustness or peak IC in Phase 1?
  Why it matters: It changes model selection pressure.
  Default if no answer: Optimize for robustness first.

- Question: Which slow context horizon is preferred as default anchor?
  Why it matters: It sets baseline ratio and join design.
  Default if no answer: Use a moderate slow horizon and evaluate ratio sweeps around it.

- Question: Are cross-asset context joins in baseline scope?
  Why it matters: Cross-asset context increases complexity materially.
  Default if no answer: Defer cross-asset context to extension phase.

## Decision Needed

Approve this multi-timeframe proposal rewrite and confirm default clarifying assumptions.

## Decision Log

- Decision: Migrated prior idea-first content into canonical template with explicit PIT and staleness controls.
  Rationale: Multi-timeframe work is leakage-prone and needs standardized gating.
  Date/Author: 2026-02-14 / Codex

## Handoff to ExecPlan

If approved:

- Milestone 1: join correctness and staleness infrastructure.
- Milestone 2: interaction features and ratio experiments.
- Milestone 3: cost-aware incrementality decision.
