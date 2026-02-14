# Crypto MFT Signal Research Program: Unified Research Proposal

## Executive Summary

This proposal defines a unified research architecture for crypto MFT signals across four layers: microstructure, derivatives, cross-dimensional relationships, and meta-conditioning. The goal is to replace isolated proposal evaluation with a single falsifiable framework that controls leakage, measures incrementality, and supports phased promotion decisions.

The program objective is not maximum feature count. It is a robust, cost-aware signal stack with diversified sources of edge and clear kill criteria.

## Research Objective and Hypothesis

Objective: determine whether a layered signal program produces more stable and economically meaningful performance than isolated family-level research.

Hypothesis H1: Microstructure families are necessary core alpha sources for short horizons.

Hypothesis H2: Derivatives families provide regime context that amplifies or suppresses microstructure efficacy.

Hypothesis H3: Cross-dimensional families provide incremental diversification and risk information beyond single-asset views.

Hypothesis H4: Meta layers (multi-timeframe, adaptive, temporal) improve conditioning and implementation quality rather than standalone alpha.

Hypothesis H5: A gated, phased program reduces false-positive promotion versus ad hoc family-by-family decisions.

## Scope and Non-Goals

In scope:

- Unified taxonomy and common evaluation framework.
- Cross-family interaction testing and incrementality analysis.
- Phased research plan with explicit decision gates.

Non-goals:

- Immediate productionization of every family.
- Treating literature priors as sufficient evidence.
- Accepting families without cost-aware and robustness validation.

## Data and PIT Constraints

Core data domains:

- Microstructure: `orderbook_updates`, `trades`, `quotes`.
- Derivatives/context: funding/OI, perp-spot structure, options, liquidations.
- Cross-dimensional: multi-asset and multi-venue event streams.
- Meta-conditioning: derived state and temporal context.

PIT constraints:

- Arrival-time-consistent feature construction and joins.
- Backward-only context joins with staleness metadata.
- Deterministic evaluation pipelines and strict leakage tests.
- Explicit handling of sparse updates and asynchronous streams.

## Feature or Model Concept

Layered program structure:

- Layer 1 (Microstructure): order-book shape, toxicity, trade-classification families.
- Layer 2 (Derivatives): funding, perp-spot, options, liquidation-cascade families.
- Layer 3 (Cross-dimensional): cross-asset contagion, cross-venue discovery, realized-vol dynamics.
- Layer 4 (Meta-conditioning): multi-timeframe, adaptive-timeframe, temporal/calendar conditioning.

Interaction priority:

- Evaluate highest-value pairwise interactions first (for example funding x liquidations, options x RV, perp-spot x venue leadership).
- Promote interactions only after standalone family validity is established.

## Experiment Design

Phase 1: Microstructure foundation.

- Validate and benchmark Layer 1 families.
- Require at least one robust promoted family before advancing.

Phase 2: Derivatives completion.

- Re-validate existing derivatives proposals.
- Add liquidation-specific family and key interactions.

Phase 3: Cross-dimensional expansion.

- Test cross-asset/venue and realized-vol families.
- Measure true incrementality versus Layer 1+2 baseline.

Phase 4: Meta-conditioning integration.

- Re-validate multi-timeframe/adaptive with enriched features.
- Add temporal conditioning and integrated stack tests.

Phase 5: Program-level synthesis.

- Build combined stack under unified cost/risk assumptions.
- Issue promote/iterate/archive decisions by family and interaction.

## Evaluation Metrics and Acceptance Criteria

Program-level metrics:

- Out-of-sample IC and stability for promoted families.
- Net Sharpe, drawdown, turnover, and capacity proxies.
- Incremental value versus simpler baselines.
- Regime robustness and sign stability.

Acceptance criteria:

- Multiple families across at least three layers are promoted.
- Combined stack outperforms baseline under conservative costs.
- No dominant single-family dependency and no major leakage findings.

Failure criteria:

- Layer 1 fails to produce robust short-horizon value.
- Incremental gains collapse under costs.
- Program relies on narrow-window or unstable effects.

## Risks and Mitigations

Risk: proposal sprawl without incremental discipline.

Mitigation: enforce per-family and program-level promotion gates.

Risk: hidden leakage in asynchronous multi-stream joins.

Mitigation: strict PIT audits, deterministic replay, and staleness controls.

Risk: interaction explosion and compute burden.

Mitigation: prioritize interaction shortlist and phase-gated expansion.

Risk: overfitting temporal/calendar effects.

Mitigation: out-of-sample rigor and interaction-first interpretation.

## Implementation Readiness

If approved:

- Program ExecPlan with milestone-by-milestone family schedule.
- Standardized evaluation harness reused across families.
- Central reporting templates for hypothesis scoreboards and decision logs.

## Related Proposals

- `funding-rate-features-mft-research-proposal.md`: Derivatives crowding layer in the unified architecture.
- `perp-spot-features-mft-research-proposal.md`: Market-structure dislocation layer in derivatives block.
- `options-volatility-surface-proposal.md`: Options and volatility-surface layer with data-readiness gating.
- `multitimeframe-features-mft-research-proposal.md`: Meta-conditioning layer for cross-horizon integration.
- `adaptive-timeframe-features-mft-research-proposal.md`: Sampling-policy control layer for regime adaptation.

## Clarifying Questions for Requester

- Question: Should the program optimize first for standalone family discovery or integrated stack performance?
  Why it matters: It changes phasing and promotion order.
  Default if no answer: Discovery first, then integrated optimization.

- Question: What is the minimum success bar for program continuation?
  Why it matters: It determines kill/iterate thresholds and resource allocation.
  Default if no answer: Require robust Layer 1 success and positive combined net metrics.

- Question: Are options-dependent families blocked until options data-readiness is confirmed?
  Why it matters: It impacts sequencing and dependency planning.
  Default if no answer: Yes, keep options families gated by data readiness.

## Decision Needed

Approve this unified program proposal rewrite and confirm clarifying-question defaults for sequencing and success thresholds.

## Decision Log

- Decision: Reframed the previous long-form program document into canonical proposal template.
  Rationale: Creates clearer governance for phase gates, incrementality, and implementation handoff.
  Date/Author: 2026-02-14 / Codex

## Handoff to ExecPlan

If approved:

- Milestone 1: build unified evaluation harness and Layer 1 validation.
- Milestone 2: Layer 2 re-validation and key interactions.
- Milestone 3: Layer 3 incremental tests.
- Milestone 4: Layer 4 conditioning integration.
- Milestone 5: full-stack decision report and go/no-go recommendation.
