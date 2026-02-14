# T+1 Adverse Selection + Co-Trading Networks: Research Proposal

## Executive Summary

This proposal tests whether combining single-stock T+1 adverse-selection signals with co-trading network structure improves cross-sectional prediction and portfolio risk control in Chinese equities.

The core thesis is contagion: high T+1 stress in one stock can propagate through co-trading links, so cluster-aware positioning may outperform single-name signals.

## Research Objective and Hypothesis

Objective: evaluate whether network-aware T+1 features improve predictive and risk-adjusted outcomes versus standalone T+1 factors.

Hypothesis H1: Cluster-aggregated T+1 intensity improves next-day risk prediction versus individual-stock T+1 scores.

Hypothesis H2: Network-informed pressure metrics improve directional signal quality and reduce idiosyncratic noise.

Hypothesis H3: Cluster-level positioning reduces drawdown relative to single-stock expressions at similar expected return.

Hypothesis H4: Network-aware risk constraints improve tail behavior during correlated sell-offs.

## Scope and Non-Goals

In scope:

- Network-augmented T+1 feature definitions.
- Cluster-aware signal and portfolio construction research.
- Comparative backtests versus single-name T+1 baselines.

Non-goals:

- Immediate production portfolio rollout.
- Hard-coding fixed thresholds as universal constants.

## Data and PIT Constraints

Primary inputs:

- T+1 adverse-selection features from Chinese L3 proposal outputs.
- Co-trading similarity/network outputs from co-trading proposal.

PIT constraints:

- Use only network states available before decision time.
- No contamination from future cluster assignments in historical simulation.
- Keep daily boundary semantics explicit for close-to-open features.

## Feature or Model Concept

Core network-aware features:

- Cluster-aggregated T+1 intensity (self + weighted neighbors).
- Network-informed pressure ratio (local pressure blended with neighbors).
- Cluster inventory-pressure metrics and contagion state indicators.

Strategy concepts to evaluate:

- Cluster-short when aggregate T+1 stress is elevated.
- Long/short cluster spread by relative T+1 stress.
- Intraday co-moving fade conditioned on prior-day T+1 stress.

Risk overlays:

- Cluster concentration limits and exposure caps.
- Correlation-aware position normalization.

## Experiment Design

Phase 1: Feature validation.

- Build and test network-aware T+1 features.
- Compare predictive value versus standalone T+1 metrics.

Phase 2: Strategy comparison.

- Evaluate cluster vs single-name expressions.
- Test sensitivity to network construction parameters.

Phase 3: Risk and robustness.

- Assess concentration and drawdown behavior.
- Stress test in high-volatility and broad-selloff regimes.

## Evaluation Metrics and Acceptance Criteria

Primary metrics:

- IC / rank-IC for next-day outcomes.
- Portfolio Sharpe, drawdown, and turnover.
- Cluster concentration and contagion-risk diagnostics.
- Incremental lift vs standalone T+1 baseline.

Acceptance criteria:

- Network-aware features improve predictive metrics out of sample.
- Portfolio-level drawdown improves without unacceptable return decay.
- Results are stable across parameter ranges.

Failure criteria:

- Improvement disappears after costs and turnover controls.
- Results depend on narrow parameter choices.
- Cluster approach increases concentration risk without return benefit.

## Risks and Mitigations

Risk: unstable network topology leads to noisy signals.

Mitigation: use stability filters and rolling consistency checks.

Risk: contagion effects are confounded by market-wide beta.

Mitigation: include beta/market controls and matched baselines.

Risk: cluster trades concentrate hidden factor risk.

Mitigation: enforce cluster exposure limits and cross-cluster diversification.

## Implementation Readiness

If approved:

- ExecPlan for feature integration and cluster strategy tests.
- Reproducible pipeline combining T+1 and co-trading outputs.
- Risk-report templates focused on contagion and concentration.

## Related Proposals

- `chinese-stock-l3-mft-features.md`: Source proposal for base T+1 adverse-selection factors.
- `cotrading-networks-chinese-stocks.md`: Source proposal for dynamic co-trading network construction.
- `chinese-stock-l3-mft-features-supplement.md`: Additional continuous microstructure features for contagion conditioning.

## Clarifying Questions for Requester

- Question: Is the primary objective alpha improvement or risk reduction?
  Why it matters: It changes optimization target and strategy ranking.
  Default if no answer: Optimize for risk-adjusted return improvement.

- Question: Should cluster-based strategies be benchmarked against market-neutral or directional baselines?
  Why it matters: Benchmark choice changes interpretation of improvement.
  Default if no answer: Use both market-neutral and directional baselines.

- Question: What rebalancing frequency should be canonical for first pass?
  Why it matters: Frequency strongly affects turnover and implementation cost.
  Default if no answer: Daily rebalancing at close-related decision point.

## Decision Needed

Approve this integration proposal rewrite and confirm default clarifying assumptions.

## Decision Log

- Decision: Reframed the prior guide into a decision-ready proposal with explicit PIT and robustness gates.
  Rationale: Integration work requires clearer acceptance criteria than code-snippet guidance.
  Date/Author: 2026-02-14 / Codex

## Handoff to ExecPlan

If approved:

- Milestone 1: network-aware T+1 feature build and validation.
- Milestone 2: cluster strategy experiments with risk constraints.
- Milestone 3: cost-aware robustness analysis and go/no-go.
