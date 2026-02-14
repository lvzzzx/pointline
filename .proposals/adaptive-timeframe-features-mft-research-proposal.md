# Adaptive Timeframe Features for Crypto MFT: Research Proposal

## Executive Summary

This proposal evaluates adaptive sampling as a conditioning layer for crypto MFT, not a standalone alpha source. The central thesis is that fixed bar definitions fail across regimes, while volatility/liquidity-aware sampling can stabilize information density and improve cost-adjusted signal reliability.

Success requires measurable out-of-sample improvement over fixed-timeframe baselines after realistic trading costs and complexity penalties.

## Research Objective and Hypothesis

Objective: test whether adaptive timeframe policies improve robustness and tradability of MFT signals across changing market regimes.

Hypothesis H1: Adaptive sampling stabilizes feature quality by reducing regime-driven variance in information density.

Hypothesis H2: At least one regime sees meaningful predictive improvement without severe degradation elsewhere.

Hypothesis H3: Transition-state features become more informative when sampling adapts to state shifts.

Hypothesis H4: Volatility-normalized features are more robust than raw features across regimes.

Hypothesis H5: There is a control-speed optimum between responsiveness and noise.

Hypothesis H6: Symbol/venue-specific calibration outperforms global policy parameters.

## Scope and Non-Goals

In scope:

- Adaptive policy design for bar formation and state-conditioned sampling.
- Regime-aware evaluation of adapted vs fixed feature stacks.
- Cost-aware comparison including turnover and slippage impacts.

Non-goals:

- Live policy auto-tuning in production.
- Full execution engine redesign.
- Treating adaptation itself as a direct directional alpha signal.

## Data and PIT Constraints

Primary inputs:

- `trades`, `quotes`, `orderbook_updates` for state and feature estimation.
- Derived volatility/liquidity state metrics from PIT-aligned streams.

PIT constraints:

- Policy decisions at time `t` may use only information with `ts_local_us <= t`.
- State labels must be computed with backward-only windows.
- Slow/derived context must carry age metadata to avoid stale implicit lookahead.

## Feature or Model Concept

Adaptive framework components:

- State features: realized-vol proxies, liquidity stress, bar-formation speed.
- Policy features: target granularity, adaptation delta, smoothness/penalty terms.
- Regime features: label, age, transition direction.
- Stabilized alpha features: vol-scaled flow/momentum/reversion computed on adaptive bars.
- Control-risk features: adaptation frequency and threshold drift.

## Experiment Design

Phase 1: State reliability and adaptation mechanics.

- Validate state estimation stability and latency sensitivity.
- Compare adaptive bar statistics vs fixed bars (dispersion, stale rate).

Phase 2: Predictive value tests.

- Evaluate adapted vs fixed features on multiple MFT horizons.
- Slice performance by volatility, trend, and liquidity regime.

Phase 3: Economic viability.

- Add realistic costs and turnover constraints.
- Evaluate whether adaptation improves net outcomes, not just gross IC.

## Evaluation Metrics and Acceptance Criteria

Primary metrics:

- Rank IC / Pearson IC by horizon and regime.
- Cost-adjusted Sharpe/IR and turnover burden.
- Bar stability metrics (duration dispersion, stale-feature rate).
- Incremental lift over fixed-timeframe baseline.

Acceptance criteria:

- Positive out-of-sample incremental value in at least one major regime.
- Net improvement survives realistic cost assumptions.
- Gains are stable across multiple periods and symbols.

Failure criteria:

- Fixed baseline matches performance at lower complexity.
- Improvements vanish after costs.
- Policy behavior is unstable under small parameter perturbations.

## Risks and Mitigations

Risk: adaptation noise and control lag produce false transitions.

Mitigation: use bounded policy moves, smoothing, and latency sensitivity tests.

Risk: overfitting policy parameters to one period.

Mitigation: rolling out-of-sample validation and parameter plateau checks.

Risk: higher turnover erases gross signal gains.

Mitigation: enforce turnover budgets and evaluate net implementation metrics.

## Implementation Readiness

If approved:

- ExecPlan for phased policy build-out.
- PIT-safe adaptive bar builder and tests.
- Reproducible benchmark suite (adaptive vs fixed).
- Standardized reporting for regime-level outcomes.

## Related Proposals

- `multitimeframe-features-mft-research-proposal.md`: Defines fast/slow context interactions that adaptive sampling conditions.
- `funding-rate-features-mft-research-proposal.md`: Provides slower crowding-state context that adaptive policies may stabilize.
- `perp-spot-features-mft-research-proposal.md`: Supplies dislocation features that are sensitive to sampling regime.
- `crypto-mft-research-program-proposal.md`: Program-level phasing and promotion criteria for adaptive work.

## Clarifying Questions for Requester

- Question: Should adaptation optimize prediction quality first or net tradability first?
  Why it matters: It changes objective functions and gate ordering.
  Default if no answer: Optimize prediction first, then require net tradability for promotion.

- Question: Is per-symbol calibration allowed in Phase 1?
  Why it matters: It changes complexity and risk of overfitting.
  Default if no answer: Use shared defaults first, then test per-symbol calibration as extension.

- Question: What complexity ceiling is acceptable for policy logic?
  Why it matters: Complex controllers may overfit and slow iteration.
  Default if no answer: Keep policy simple (bounded threshold mapping + smoothing).

## Decision Needed

Approve this adaptive-timeframe proposal as template-aligned and confirm the default clarifying-question assumptions.

## Decision Log

- Decision: Reframed the prior idea-first note into the canonical research proposal template.
  Rationale: Standardized structure improves comparability across proposals and implementation handoff.
  Date/Author: 2026-02-14 / Codex

## Handoff to ExecPlan

If approved:

- Milestone 1: PIT-safe state estimation and adaptive bar policy prototype.
- Milestone 2: Feature recomputation on adaptive bars and baseline comparison.
- Milestone 3: Cost-aware evaluation and go/no-go recommendation.
