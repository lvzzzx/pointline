# Options and Volatility Surface Features for Crypto MFT: Research Proposal

## Executive Summary

This proposal evaluates whether crypto options data (initially Deribit-focused) can provide incremental MFT edge through implied-volatility surface structure, skew, Greeks-related flow context, and IV-versus-RV dislocations.

The key value proposition is forward-looking risk information: options embed market expectations of volatility and tail risk that spot/perp data cannot fully express. The proposal remains conditional on reliable options data availability and PIT-safe integration.

## Research Objective and Hypothesis

Objective: test whether options-derived features improve predictive and risk-adjusted outcomes beyond spot/perp-only baselines.

Hypothesis H1: ATM IV and term-structure features improve forward volatility prediction.

Hypothesis H2: Skew and skew dynamics capture asymmetric tail-risk regime shifts.

Hypothesis H3: IV-RV spread features (VRP variants) contain mean-reverting information.

Hypothesis H4: Dealer-gamma and related Greeks exposure regimes improve intraday risk conditioning.

Hypothesis H5: Large options-flow and put-call structure provide incremental directional context.

Hypothesis H6: Put-call parity and cross-strike consistency deviations identify short-lived dislocation states.

## Scope and Non-Goals

In scope:

- Options data quality validation and PIT-safe feature extraction.
- Vol-surface representation for research (parametric and nonparametric variants).
- Integration tests with realized-vol and derivatives context features.

Non-goals:

- Immediate production options-market-making stack.
- Over-reliance on literature IC magnitudes without fresh validation.
- Full derivatives-pricing library expansion beyond research needs.

## Data and PIT Constraints

Primary inputs:

- Options trades/quotes and chain metadata (Deribit primary).
- Spot/perp prices for moneyness and cross-market context.
- Optional exchange-provided Greeks/IV fields when available.

PIT constraints:

- All features must use arrival-time-consistent data.
- Contract metadata (expiry/strike/type) must be point-in-time correct.
- Expired contracts and roll transitions require explicit handling.
- No future quote states in IV estimation or surface interpolation.

Data quality gates:

- Valid bid/ask relationships and duplicate handling.
- Contract parsing and lifecycle correctness.
- Coverage sufficiency across expiries/strikes for stable surfaces.

## Feature or Model Concept

Feature families:

- IV level and term structure: ATM IV across horizons and slope/curvature.
- Skew structure: put-call skew, skew steepness, skew dynamics.
- IV-RV dislocations: VRP variants using robust RV denominators.
- Greeks context: gamma/vanna-style exposure regime proxies.
- Flow features: put-call volume balance and large-options-flow signals.
- Structural consistency: parity/dislocation indicators.

Surface modeling approach:

- Research-first flexible modeling plus constrained alternatives for robustness.
- Compare model sensitivity and downstream feature stability.

## Experiment Design

Phase 1: Data readiness and surface quality.

- Validate data integrity and coverage.
- Build baseline surface representations and quality diagnostics.

Phase 2: Standalone options feature tests.

- Evaluate each feature family on volatility and directional targets.
- Run horizon and regime segmentation.

Phase 3: Cross-layer integration.

- Combine with funding, perp-spot, and realized-vol features.
- Evaluate incremental value and interaction effects.

Phase 4: Economic evaluation.

- Apply realistic costs and capacity assumptions.
- Assess whether options features improve net risk-adjusted outcomes.

## Evaluation Metrics and Acceptance Criteria

Primary metrics:

- Forecast quality for realized volatility.
- IC/rank-IC for directional and risk-conditioning tasks.
- Net Sharpe/drawdown contribution in combined stack.
- Stability across expiry buckets and market regimes.

Acceptance criteria:

- Options-derived features show out-of-sample incremental value.
- Gains persist under conservative cost assumptions.
- Surface/feature behavior is stable across model variants.

Failure criteria:

- Data sparsity/quality prevents stable feature generation.
- Incremental value vanishes after controls.
- Results are overly dependent on one contract segment or narrow period.

## Risks and Mitigations

Risk: insufficient or uneven options data coverage.

Mitigation: enforce minimum coverage gates and start with liquid underlyings.

Risk: IV/surface model risk dominates signal conclusions.

Mitigation: use multi-model robustness tests and consistency diagnostics.

Risk: executable constraints invalidate gross edge.

Mitigation: evaluate net performance with conservative assumptions.

## Implementation Readiness

If approved:

- ExecPlan for phased data validation, surface modeling, and feature testing.
- Reproducible options research pipelines and diagnostics.
- PIT/data-quality tests for contract lifecycle and quote integrity.

## Related Proposals

- `crypto-mft-research-program-proposal.md`: Defines options family dependencies and cross-layer integration order.
- `funding-rate-features-mft-research-proposal.md`: Funding crowding regimes can condition skew and IV features.
- `perp-spot-features-mft-research-proposal.md`: Perp-spot dislocations interact with IV/RV and risk-pricing context.

## Clarifying Questions for Requester

- Question: Should options work proceed only after data availability is fully confirmed?
  Why it matters: It affects project sequencing and resource allocation.
  Default if no answer: Yes, require data-readiness gate before feature build-out.

- Question: Is the primary objective volatility forecasting or directional alpha lift?
  Why it matters: It changes target design and acceptance thresholds.
  Default if no answer: Prioritize volatility/risk-conditioning first.

- Question: Should exchange-provided Greeks be preferred over computed Greeks in baseline?
  Why it matters: It affects complexity and model-risk exposure.
  Default if no answer: Prefer exchange-provided fields when quality is sufficient.

## Decision Needed

Approve this template-aligned options proposal and confirm clarifying-question defaults.

## Decision Log

- Decision: Replaced implementation-heavy draft with decision-ready research proposal structure.
  Rationale: Current stage needs hypothesis validation and data gating before build detail.
  Date/Author: 2026-02-14 / Codex

## Handoff to ExecPlan

If approved:

- Milestone 1: options data readiness and PIT-quality validation.
- Milestone 2: surface construction and standalone feature evaluation.
- Milestone 3: cross-layer integration and cost-aware go/no-go.
