# Chinese Stock L3 MFT Research Proposal: Auction Microstructure and T+1 Signals

## Executive Summary

This proposal defines a research program for mid-frequency trading (MFT) signals in Chinese A-shares using Pointline Level 3 data. The central idea is that Chinese market microstructure creates predictable state transitions that are hard to capture with Level 1/2 data: the opening call auction (09:15-09:25 CST), the closing call auction (14:57-15:00 CST), and the T+1 settlement constraint.

The expected edge comes from three signal families: auction imbalance and discovery quality, order-flow toxicity during continuous trading, and close-to-open persistence under T+1 constraints. Success is measured by statistically robust predictive power out of sample, meaningful net performance after realistic Chinese trading costs, and stable behavior across liquidity and volatility regimes.

## Research Objective and Hypothesis

The objective is to determine whether L3-only microstructure features can produce economically meaningful, PIT-safe MFT signals for Chinese equities.

Hypothesis H1: Opening auction imbalance and cancellation-phase transitions predict the 09:30-10:00 return distribution better than L1/L2 baselines.

Hypothesis H2: Closing auction imbalance, close-to-open gap structure, and T+1 pressure proxies predict next-day opening and overnight drift.

Hypothesis H3: Order-flow toxicity and queue-dynamics features improve model calibration by filtering low-quality entry periods.

Hypothesis H4: A cost-aware composite model remains positive after stamp duty, commissions, and slippage, and retains performance across regimes.

## Scope and Non-Goals

In scope:

- Research on SZSE/SSE Level 3 event streams using `cn_order_events` and `cn_tick_events`.
- Feature engineering, ablation, and model selection for short-horizon prediction and ranking.
- Cost-aware evaluation including asymmetric sell-side stamp duty.
- Cross-sectional and time-series variants of auction and T+1 factors.

Non-goals:

- Production trading deployment.
- Full execution algorithm design.
- Exchange connectivity changes or ingestion redesign.
- Long-horizon portfolio construction beyond the MFT research horizon.

## Data and PIT Constraints

Primary data tables:

- `cn_order_events`: order submissions/cancellations and order lifecycle details.
- `cn_tick_events`: trades and top-of-book/market state updates.
- `dim_symbol` and metadata tables for symbol lifecycle and instrument filters.

Core PIT constraints:

- Use `ts_event_us` (UTC microseconds) as the event timestamp source of truth.
- Respect session boundaries in China Standard Time (CST, UTC+8):
  - Opening call auction: 09:15-09:25
  - Continuous morning: 09:30-11:30
  - Continuous afternoon: 13:00-14:57
  - Closing call auction: 14:57-15:00
- Model sequence semantics correctly: `channel_seq` is only meaningful with `channel_id` and `trading_date`.
- For intra-channel deterministic replay, use `(trading_date, channel_id, channel_seq)`.
- For cross-channel/cross-table merges, use `(ts_event_us, file_id, file_seq)`.
- Do not use future-day information in features, labels, normalization, or universe selection.

Sampling and horizon assumptions for the initial phase:

- Universe: liquid A-share symbols with sufficient auction participation.
- Frequency: event-derived features aggregated at configurable windows (for example 5s/30s/1m) plus auction snapshot points.
- Initial labels: open-to-interval return, close-to-open return, and directional classification for short horizons.

## Feature or Model Concept

This proposal prioritizes feature families that are uniquely strong in Chinese microstructure.

Feature Family A: Opening call auction discovery.

- Bid/ask pressure ratios and final imbalance measures.
- Cancellable vs non-cancellable phase behavior (09:15-09:20 vs 09:20-09:25).
- Cancellation acceleration and conviction proxies.
- Indicative-price drift, convergence, and velocity.

Feature Family B: Opening transition and information decay.

- 09:25 auction to 09:30 continuous transition gaps.
- Early spread normalization and liquidity recovery.
- Auction-information decay in first continuous-trading minutes.

Feature Family C: Closing auction and overnight linkage.

- Closing imbalance buildup and MOC-like signatures.
- Close-to-open gap and imbalance persistence.
- Auction momentum across previous open, close, and next open.
- Price-discovery efficiency between close and next open.

Feature Family D: Continuous trading toxicity and queue dynamics.

- Submission/cancellation ratio in rolling windows.
- Order-book churn and fleeting-liquidity intensity.
- Large-trade imbalance and informed-flow proxies.
- Fill-rate/queue-position proxies based on L3 order lifecycle.

Feature Family E: T+1 adverse selection proxies.

- T+1 constraint intensity and stuck-inventory pressure.
- End-of-day informed-flow signatures.
- Next-day adverse selection cost proxies.
- Network-aware extensions via co-trading relationships (optional advanced phase).

Candidate model stack:

- Baselines: univariate monotonic buckets and linear/regularized models.
- Mainline: gradient-boosted trees or equivalent tabular model with strict time-aware splits.
- Optional: regime-conditioned ensembles if stability benefits are verified.

## Experiment Design

Phase 1: Feature sanity and univariate signal validity.

- Build PIT-safe feature tables for each family.
- Run single-feature predictive tests with time-aware cross-validation.
- Check monotonicity and stability by liquidity buckets.

Phase 2: Multivariate integration and ablation.

- Train baseline and composite models on harmonized feature sets.
- Run ablations by feature family and by session segment.
- Measure incremental value of L3-only features over L1/L2-like proxies.

Phase 3: Cost-aware backtesting.

- Apply realistic Chinese cost model: commissions, transfer fees, and sell-side stamp duty.
- Include conservative slippage assumptions tied to spread and queue proxies.
- Evaluate turnover and capacity sensitivity.

Phase 4: Robustness and regime analysis.

- Slice by volatility regime, market trend, and liquidity tiers.
- Validate behavior around price-limit proximity and event-heavy days.
- Stress-test with stricter assumptions and reduced universes.

Methodological controls:

- Time-ordered train/validation/test splits only.
- Leakage checks for all rolling features and normalizations.
- Fixed random seeds and deterministic data extraction paths.

## Evaluation Metrics and Acceptance Criteria

Primary predictive metrics:

- Information Coefficient (IC) and rank-IC by horizon.
- Directional accuracy and calibration metrics.
- Stability of signal sign and magnitude across folds/regimes.

Primary economic metrics:

- Net Sharpe and net return after all modeled costs.
- Hit rate, payoff asymmetry, turnover, and drawdown profile.
- Capacity proxy using participation-rate constraints.

Acceptance criteria for this research program:

- Statistical: consistent positive IC/rank-IC out of sample in at least two independent periods.
- Economic: positive net performance after costs with no single-regime dependency.
- Robustness: no material degradation under conservative slippage stress.
- Integrity: zero confirmed PIT leakage and reproducible result generation.

Failure criteria:

- Signal significance disappears outside in-sample periods.
- Net performance turns negative under realistic costs.
- Results depend on unstable preprocessing choices or leakage-prone transformations.

## Risks and Mitigations

Risk: PIT leakage through session boundary mistakes or improper joins.

Mitigation: enforce explicit session clocks, deterministic replay keys, and leakage unit tests for every label horizon.

Risk: Overfitting to auction idiosyncrasies in a narrow sample.

Mitigation: apply rolling out-of-sample windows, cross-period validation, and stricter regularization.

Risk: Cost underestimation due to queue uncertainty and slippage.

Mitigation: use conservative slippage schedules, compare multiple cost assumptions, and report sensitivity bands.

Risk: Feature instability from channel-specific microstructure differences.

Mitigation: include channel-aware controls and validate consistency across exchanges and instrument groups.

Risk: Operational complexity from too many feature families at once.

Mitigation: stage work by phases and require incremental acceptance gates before expansion.

## Implementation Readiness

If approved, expected deliverables are:

- A focused ExecPlan that maps accepted hypotheses to milestone-based implementation.
- PIT-safe feature extraction code paths and tests.
- Reproducible experiment scripts/notebooks with fixed configs.
- Cost-aware evaluation utilities and standardized reporting templates.
- Documentation updates under `.proposals/` and relevant `docs/` paths.

A full formula catalog and extended feature variants can remain in supplementary documents. This proposal defines the canonical decision and validation scaffold.

## Related Proposals

- `chinese-stock-l3-mft-features-supplement.md`: Adds continuous-session microstructure families beyond auction-centric features.
- `cotrading-networks-chinese-stocks.md`: Provides cross-stock dependency and covariance context.
- `t1-adverse-selection-cotrading-integration.md`: Integrates T+1 adverse-selection signals with network contagion logic.

## Clarifying Questions for Requester

- Question: What is the primary target for Phase 1 and Phase 2: pure prediction quality or immediate net tradability?
  Why it matters: This changes label choice, optimization objectives, and ranking of feature families.
  Default if no answer: Prioritize prediction quality first, then gate promotion on cost-aware net tradability in Phase 3.

- Question: Which symbol universe should be canonical for first-pass evaluation?
  Why it matters: Universe choice strongly affects liquidity assumptions, slippage, and robustness interpretation.
  Default if no answer: Use a liquid A-share universe with minimum auction participation and turnover filters.

- Question: Should network-aware T+1 extensions be part of baseline scope or deferred?
  Why it matters: Including network features early increases complexity and may delay core validation.
  Default if no answer: Defer network-aware features to an advanced extension after baseline acceptance.

- Question: What is the acceptance bar for moving from research to implementation?
  Why it matters: Promotion criteria determine whether we optimize for exploration breadth or production readiness.
  Default if no answer: Require positive out-of-sample predictive metrics, positive net performance after conservative costs, and leakage-free reproducibility.

## Decision Needed

Approve this proposal as the canonical research template example for Chinese stock L3 MFT ideas, and confirm or amend the clarifying-question defaults. After approval, the next artifact is an ExecPlan focused on Phase 1-2 implementation.

## Decision Log

- Decision: Reframed the previous feature-engineering guide into the repository's research proposal template.
  Rationale: The prior document mixed proposal, implementation cookbook, and appendix detail; the new structure separates decision-ready research framing from build-out artifacts.
  Date/Author: 2026-02-14 / Codex

- Decision: Kept T+1 adverse selection and auction microstructure as first-class feature families.
  Rationale: These are Chinese-market-specific edge hypotheses and were the strongest differentiated content in the original document.
  Date/Author: 2026-02-14 / Codex

## Handoff to ExecPlan

If approved for build-out, create an ExecPlan that references this file and includes:

- Milestone 1: PIT-safe extraction for Feature Families A and B with validation tests.
- Milestone 2: Family C and D integration with ablations and deterministic reporting.
- Milestone 3: Family E (T+1) integration and cost-aware backtest framework.
- Milestone 4: Robustness/regime evaluation and go/no-go recommendation.

The ExecPlan must resolve each clarifying-question outcome explicitly and define exact commands, expected outputs, and acceptance evidence.
