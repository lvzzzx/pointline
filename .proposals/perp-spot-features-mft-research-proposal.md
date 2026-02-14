# Perp-Spot Features for Crypto MFT: Research Proposal

## Executive Summary

This proposal evaluates perp-spot dislocation features as a market-structure layer for crypto MFT. The core thesis is that basis dynamics, funding-basis inconsistencies, and perp/spot flow divergences encode leverage pressure and price-discovery hierarchy that can produce tradable asymmetries.

Success requires robust out-of-sample signal value and positive net contribution after realistic costs.

## Research Objective and Hypothesis

Objective: determine when perp-spot relationships provide reliable forward-return asymmetry at MFT horizons.

Hypothesis H1: Basis extremes mean-revert conditionally.

Hypothesis H2: Funding-basis dislocations predict convergence pressure.

Hypothesis H3: Perp often leads spot in normal regimes but fails in stress/fragmented regimes.

Hypothesis H4: Perp-vs-spot flow divergence signals participant segmentation and short-term resolution.

Hypothesis H5: Perp/spot volume asymmetry modulates feature efficacy.

Hypothesis H6: Funding settlement windows create nonlinear behavior.

## Scope and Non-Goals

In scope:

- Basis, carry-alignment, lead-lag, and cross-flow features.
- Regime-conditioned evaluation and settlement-aware tests.
- Cost-aware tradability assessment.

Non-goals:

- Treating static basis level as sufficient standalone signal.
- Ignoring venue/latency effects in lead-lag claims.

## Data and PIT Constraints

Primary inputs:

- `trades` (perp + spot), `quotes`, and `derivative_ticker`.

PIT constraints:

- Strict stream alignment by arrival-time semantics.
- Explicit age/staleness controls for funding/OI-derived terms.
- No future pricing information in spread/dislocation features.

## Feature or Model Concept

Feature families:

- Basis structure: level, percentile, slope, persistence.
- Carry alignment: funding-basis gap and gap velocity.
- Lead-lag: perp-minus-spot short-horizon return deltas.
- Cross-flow: perp/spot flow divergence and flow-basis interactions.
- Regime controls: activity ratio, liquidity stress, settlement proximity.

## Experiment Design

Phase 1: standalone family testing.

- Evaluate each feature family on multiple MFT horizons.
- Segment by volatility/liquidity regimes.

Phase 2: interaction and regime conditioning.

- Add settlement-event and flow context.
- Test stability of lead-lag behavior across regimes.

Phase 3: net tradability.

- Apply realistic fees/slippage and capacity constraints.
- Evaluate net performance and drawdown behavior.

## Evaluation Metrics and Acceptance Criteria

Primary metrics:

- IC metrics by horizon and regime.
- Magnitude-bucket monotonicity and hit rates.
- Cost-adjusted Sharpe/IR and turnover.
- Incremental value over baseline MFT stack.

Acceptance criteria:

- Stable out-of-sample effect across major regimes.
- Net-positive contribution after costs.
- Clear incremental value beyond simpler baselines.

Failure criteria:

- Signal depends on rare stress windows only.
- Sign instability across adjacent periods.
- No incrementality after baseline controls.

## Risks and Mitigations

Risk: venue fragmentation distorts lead-lag interpretation.

Mitigation: regime splits by venue state and robustness checks.

Risk: stale funding updates contaminate carry alignment.

Mitigation: feature age tracking and staleness filters.

Risk: strong gross signals fail under executable pricing.

Mitigation: enforce execution-aware evaluation and cost stress tests.

## Implementation Readiness

If approved:

- ExecPlan for feature extraction and validation phases.
- Reproducible perp-spot benchmark suite.
- PIT/staleness and execution-aware test coverage.

## Related Proposals

- `funding-rate-features-mft-research-proposal.md`: Funding-basis gap is a core interaction in this family.
- `multitimeframe-features-mft-research-proposal.md`: Provides fast/slow interaction framework for lead-lag behavior.
- `adaptive-timeframe-features-mft-research-proposal.md`: Adaptive sampling can improve dislocation-state stability.
- `crypto-mft-research-program-proposal.md`: Program-level sequencing and promotion criteria for derivatives families.

## Clarifying Questions for Requester

- Question: Is mean-reversion or continuation the primary objective in baseline model design?
  Why it matters: It affects label design and feature gating.
  Default if no answer: Evaluate both, promote only regime-conditional strategy.

- Question: Should settlement-window features be mandatory in Phase 1?
  Why it matters: Event-time modeling complexity is nontrivial.
  Default if no answer: Include settlement features in Phase 1.

- Question: Is cross-venue decomposition required before approval?
  Why it matters: Lead-lag claims can be misleading without venue controls.
  Default if no answer: Require at least one venue-robustness check.

## Decision Needed

Approve this perp-spot proposal rewrite and confirm clarifying-question defaults.

## Decision Log

- Decision: Rewrote prior idea-first content into canonical template while preserving hypothesis set and regime emphasis.
  Rationale: Standardized structure improves comparability and implementation handoff quality.
  Date/Author: 2026-02-14 / Codex

## Handoff to ExecPlan

If approved:

- Milestone 1: PIT-safe basis/carry features.
- Milestone 2: lead-lag and cross-flow integration with regime tests.
- Milestone 3: cost-aware net evaluation and go/no-go.
