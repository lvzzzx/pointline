# Chinese Stock L3 Continuous Microstructure Features: Research Proposal

## Executive Summary

This proposal extends Chinese stock L3 research beyond call auctions by focusing on continuous-trading microstructure features (09:30-11:30, 13:00-14:57 CST). The goal is to test whether toxicity, liquidity dynamics, timing patterns, and strategic-order signatures add incremental predictive value to the existing auction-focused proposal.

Success requires PIT-safe, out-of-sample performance that remains positive after Chinese-market cost assumptions.

## Research Objective and Hypothesis

Objective: evaluate whether continuous-session L3 microstructure features provide robust incremental alpha and risk controls for Chinese MFT.

Hypothesis H1: Toxicity proxies (VPIN and cancellation/execution imbalance) predict volatility and directional asymmetry.

Hypothesis H2: L3-enhanced trade classification and size signatures improve flow informativeness versus naive signed volume.

Hypothesis H3: Liquidity resilience and order-book geometry predict continuation vs absorption outcomes.

Hypothesis H4: Strategic-order patterns (iceberg/layering-like behavior) identify high-information periods.

Hypothesis H5: Inter-event timing and burst metrics improve regime detection and execution timing.

Hypothesis H6: Price-impact and adverse-selection proxies improve risk-adjusted signal filtering.

## Scope and Non-Goals

In scope:

- Continuous-session L3 feature engineering and ablation.
- Cross-sectional microstructure factor construction.
- Incremental tests versus auction and baseline flow features.

Non-goals:

- Standalone market-abuse surveillance productization.
- Production execution-system redesign.
- Over-claiming difficult-to-verify manipulative intent labels.

## Data and PIT Constraints

Primary inputs:

- `cn_order_events`, `cn_tick_events`, and symbol metadata.

PIT constraints:

- Use `ts_event_us` for event ordering, with deterministic keys as defined in v2.
- Restrict features to continuous phases; treat auction periods separately.
- Preserve order lifecycle integrity for cancellation and fill-rate features.
- No future information in rolling windows, thresholds, or cross-sectional ranks.

## Feature or Model Concept

Feature families:

- Toxicity: VPIN-like metrics, cancellation/submission pressure, informed-flow proxies.
- Trade classification: L3-enhanced aggressor classification and size-bucket signatures.
- Liquidity dynamics: resilience speed, slope/curvature, depletion-recovery behavior.
- Strategic patterns: iceberg repetition and layering-like episodic signatures.
- Timing: inter-event durations, clustering, and burst indicators.
- Impact/quality: Kyle-lambda-like impact proxies and adverse-selection spread measures.
- Cross-sectional: relative liquidity and informed-trading composite scores.

## Experiment Design

Phase 1: Feature integrity and standalone checks.

- Validate each family for PIT correctness and stability.
- Run univariate predictive tests by horizon.

Phase 2: Incremental integration.

- Combine with auction and baseline features.
- Measure incremental value and redundancy.

Phase 3: Cost-aware and robustness analysis.

- Apply realistic A-share costs and liquidity assumptions.
- Evaluate regime-specific behavior and execution sensitivity.

## Evaluation Metrics and Acceptance Criteria

Primary metrics:

- IC/rank-IC by horizon and session segment.
- Directional hit-rate monotonicity by signal magnitude.
- Cost-adjusted performance and turnover burden.
- Incremental lift beyond auction-focused baseline.

Acceptance criteria:

- At least two feature families show stable incremental value out of sample.
- Net value remains positive under conservative costs.
- Performance is not concentrated in a single regime slice.

Failure criteria:

- Signals collapse after cost or slippage assumptions.
- Features are unstable to small parameter changes.
- Incrementality disappears after baseline controls.

## Risks and Mitigations

Risk: false positives in strategic-pattern detection.

Mitigation: treat these as probabilistic context features, not hard labels.

Risk: microstructure noise and sequencing errors.

Mitigation: strict deterministic replay keys and data-quality checks.

Risk: feature overload and redundancy.

Mitigation: ablations and strict incremental acceptance thresholds.

## Implementation Readiness

If approved:

- ExecPlan for feature-family phased implementation.
- PIT-safe extraction utilities and validation tests.
- Reproducible reporting templates for incremental analysis.

## Related Proposals

- `chinese-stock-l3-mft-features.md`: Primary Chinese L3 auction and T+1 baseline proposal.
- `cotrading-networks-chinese-stocks.md`: Network-level extension for cross-sectional propagation effects.
- `t1-adverse-selection-cotrading-integration.md`: Integration path for T+1 contagion-aware strategy design.

## Clarifying Questions for Requester

- Question: Should this supplement prioritize predictive alpha or execution-risk filtering?
  Why it matters: It changes ranking of feature families and acceptance thresholds.
  Default if no answer: Prioritize alpha first, require risk-filter utility as secondary.

- Question: Should potential manipulation-pattern features stay in baseline scope?
  Why it matters: They are higher-noise and harder to validate causally.
  Default if no answer: Keep as optional extension family.

- Question: What baseline should define incrementality (auction-only or full existing stack)?
  Why it matters: Incremental claim strength depends on baseline choice.
  Default if no answer: Compare against full existing Chinese L3 baseline.

## Decision Needed

Approve this supplement rewrite and confirm the default clarifying assumptions.

## Decision Log

- Decision: Converted implementation-heavy supplement into canonical research proposal structure.
  Rationale: Keeps high-value feature ideas while enabling decision-ready scope and validation.
  Date/Author: 2026-02-14 / Codex

## Handoff to ExecPlan

If approved:

- Milestone 1: toxicity and classification features with PIT validation.
- Milestone 2: liquidity/timing/impact features and ablations.
- Milestone 3: cross-sectional integration and cost-aware go/no-go.
