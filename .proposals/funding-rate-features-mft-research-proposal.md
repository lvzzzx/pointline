# Funding Rate Features for Crypto MFT: Research Proposal

## Executive Summary

This proposal evaluates funding-rate signals as a crowding and leverage-state layer for crypto MFT. Funding is treated as conditional context that interacts with order flow, open interest, and settlement windows rather than as a universal standalone trigger.

Success requires stable, out-of-sample predictive and economic value after costs across multiple regimes.

## Research Objective and Hypothesis

Objective: determine when funding and related leverage-state features provide actionable forward-return asymmetry.

Hypothesis H1: Extreme funding states mean-revert conditionally as crowding unwinds.

Hypothesis H2: Funding surprises (realized vs expected) carry near-term information.

Hypothesis H3: Joint funding and OI expansion predicts unstable squeeze-prone states.

Hypothesis H4: Funding signals become more informative when aligned with flow imbalance.

Hypothesis H5: Settlement proximity introduces nonlinear behavior that must be modeled explicitly.

## Scope and Non-Goals

In scope:

- Funding, OI, and interaction feature engineering.
- Settlement-aware event-time modeling.
- Cost-aware, regime-sliced validation.

Non-goals:

- Assuming static effect direction across all regimes.
- Production deployment decisions without full falsification.

## Data and PIT Constraints

Primary inputs:

- `derivative_ticker` for funding and OI state.
- `trades` and `quotes` for flow and execution context.

PIT constraints:

- Funding/OI snapshots must be aligned to actual arrival availability.
- No use of later settlement outcomes in pre-settlement features.
- Feature age/staleness must be explicit for slow-updating fields.

## Feature or Model Concept

Feature families:

- Level: funding percentile, annualized carry, cross-venue spread.
- Delta: funding/OI change and acceleration.
- Interaction: funding x flow, funding x vol, funding x OI change.
- Regime: trend, volatility, liquidity, settlement proximity.
- Structure: persistence of extreme states and transition probabilities.

## Experiment Design

Phase 1: standalone family validation.

- Test each family on multiple MFT horizons.
- Run regime-stratified diagnostics.

Phase 2: interaction and incremental value.

- Add flow and volatility context.
- Compare against simpler baseline models.

Phase 3: tradability.

- Apply realistic fees/slippage/impact assumptions.
- Evaluate net metrics and drawdown behavior around squeezes.

## Evaluation Metrics and Acceptance Criteria

Primary metrics:

- Rank IC and Pearson IC by horizon/regime.
- Magnitude-bucket hit-rate monotonicity.
- Cost-adjusted Sharpe/IR and drawdown sensitivity.
- Incremental lift over baseline MFT feature set.

Acceptance criteria:

- Stable directional value in multiple out-of-sample windows.
- Net-positive contribution after conservative costs.
- Clear incremental value beyond simpler crowding proxies.

Failure criteria:

- Effect concentrated in one short historical regime.
- Sign instability across adjacent windows.
- Incrementality disappears with baseline controls.

## Risks and Mitigations

Risk: timestamp/staleness mistakes in funding updates.

Mitigation: strict PIT alignment and staleness auditing.

Risk: regime dependence causes brittle signals.

Mitigation: explicit regime conditioning and per-regime evaluation gates.

Risk: execution risk spikes near settlement.

Mitigation: settlement-window stress tests and stricter cost assumptions.

## Implementation Readiness

If approved:

- ExecPlan for feature build and validation pipeline.
- Reproducible funding-state dataset and benchmark reports.
- Tests for PIT correctness and staleness handling.

## Related Proposals

- `perp-spot-features-mft-research-proposal.md`: Funding-basis dislocation hypotheses directly interact with funding state.
- `multitimeframe-features-mft-research-proposal.md`: Defines context-join framework for funding signals with faster layers.
- `adaptive-timeframe-features-mft-research-proposal.md`: Sampling-policy layer for regime-aware funding evaluation.
- `crypto-mft-research-program-proposal.md`: Unified gating and incrementality standards for derivatives features.

## Clarifying Questions for Requester

- Question: Should funding features be treated mainly as directional signals or conditioning signals?
  Why it matters: It changes model architecture and acceptance criteria.
  Default if no answer: Treat funding as conditioning-first, directional-second.

- Question: Is settlement-window behavior a required core result?
  Why it matters: It determines priority of event-time modeling effort.
  Default if no answer: Yes, settlement nonlinearity is required.

- Question: Which assets are mandatory for first validation?
  Why it matters: Cross-asset generalization claims depend on scope.
  Default if no answer: BTC and ETH first, one additional liquid alt as extension.

## Decision Needed

Approve this funding proposal rewrite and confirm the default clarifying-question assumptions.

## Decision Log

- Decision: Converted prior idea-first document into canonical proposal template while preserving hypothesis set.
  Rationale: Enables consistent review and implementation gating across the proposal set.
  Date/Author: 2026-02-14 / Codex

## Handoff to ExecPlan

If approved:

- Milestone 1: PIT-safe funding/OI feature tables.
- Milestone 2: interaction features and regime-sliced validation.
- Milestone 3: cost-aware evaluation and promotion decision.
