# Co-Trading Networks for Chinese A-Shares: Research Proposal

## Executive Summary

This proposal evaluates whether co-trading networks built from Chinese A-share Level 3 data can improve dependency modeling, covariance estimation, and portfolio risk control beyond return-correlation baselines.

The core idea is that synchronized trading activity across stocks captures latent information flow earlier than low-frequency correlation estimates. We adapt the Lu et al. (2023) framework to Chinese microstructure by handling auction windows, T+1 settlement behavior, and price-limit regimes.

Success is defined by robust out-of-sample improvements in dependency forecasts and covariance quality, and by measurable portfolio risk-adjusted gains after realistic costs and turnover constraints.

## Research Objective and Hypothesis

Objective: test whether dynamic co-trading networks extracted from `cn_tick_events` provide a superior structure for cross-stock dependency and covariance modeling in Chinese equities.

Hypothesis H1: Co-trading similarity predicts future dependency shifts better than rolling return correlation.

Hypothesis H2: Network-informed covariance shrinkage improves realized risk forecasting versus sample covariance and standard shrinkage baselines.

Hypothesis H3: Dynamic network clusters capture sector rotation and regime transitions better than static industry labels.

Hypothesis H4: Portfolio construction using network-informed covariance reduces realized volatility and drawdowns without unacceptable turnover.

## Scope and Non-Goals

In scope:

- Constructing co-trading similarity matrices from L3 trade events.
- Building dynamic graphs and cluster structures over rolling windows.
- Evaluating covariance estimation methods with network priors.
- Testing portfolio applications for risk and diversification impact.
- Explicit Chinese-market adaptations: auctions, T+1, and price limits.

Non-goals:

- Live trading deployment or execution stack design.
- Fundamental-factor integration as a first-pass dependency signal.
- Ultra-high-frequency market-making strategies.
- Full productionization of a portfolio optimizer service.

## Data and PIT Constraints

Primary data tables:

- `cn_tick_events` for trade timing and trade-size activity.
- `cn_order_events` (optional extension) for order-flow enriched co-trading variants.
- `dim_symbol` for universe filters and symbol lifecycle correctness.

PIT constraints and replay rules:

- Use `ts_event_us` as the event-time anchor.
- Restrict cross-symbol synchronization logic to information available at timestamp `t`.
- Exclude or separately model call auction windows (09:15-09:25, 14:57-15:00 CST) because synchronous batching can induce mechanical co-trading.
- Treat deterministic ordering with v2 keys:
  - intra-channel: `(trading_date, channel_id, channel_seq)`
  - cross-channel/cross-table: `(ts_event_us, file_id, file_seq)`
- Avoid lookahead in rolling normalization, clustering, and covariance estimation windows.

Initial universe and horizon assumptions:

- Universe: liquid SZSE/SSE A-shares passing minimum activity filters.
- Windowing: rolling intraday and multi-day windows for dynamic network updates.
- Evaluation horizons: next-window dependency prediction and daily realized covariance outcomes.

## Feature or Model Concept

Core representation:

- Build pairwise co-trading similarity where two stocks are close when trades occur within a small time window (`delta_t`) and with meaningful matched activity.
- Normalize by stock activity to reduce trivial high-volume dominance.

Key design variants:

- Count-based similarity vs volume-weighted similarity.
- Multiple `delta_t` scales (for example 1ms, 5ms, 10ms equivalents in `ts_event_us`).
- Auction-filtered vs auction-inclusive variants.
- Price-limit-aware weighting to avoid artificial synchrony near limit states.

Network layer:

- Construct dynamic sparse graphs via adaptive thresholding or k-nearest connectivity.
- Derive centrality and community features for dependency-state tracking.
- Apply spectral clustering for data-driven dynamic sectors.

Covariance layer:

- Use network-informed shrinkage targets.
- Compare against sample covariance, Ledoit-Wolf-type shrinkage, and other practical baselines.
- Optional extension: graphical models with network-structured priors.

Portfolio layer (research-only):

- Mean-variance and risk-parity style experiments using each covariance estimator.
- Evaluate diversification quality, realized risk, turnover, and drawdown behavior.

## Experiment Design

Phase 1: Similarity and network construction sanity checks.

- Build reproducible co-trading similarity matrices on rolling windows.
- Validate distributional properties and sparsity stability.
- Compare auction-filtered and non-filtered network behavior.

Phase 2: Dependency prediction tests.

- Test whether current network similarity predicts future return-correlation shifts.
- Benchmark against lagged return-correlation-only models.
- Assess performance by liquidity tiers and market regimes.

Phase 3: Covariance estimation evaluation.

- Forecast realized covariance with network-informed and baseline estimators.
- Score estimation error and downstream portfolio risk outcomes.
- Run ablations: similarity definition, thresholding, and cluster conditioning.

Phase 4: Portfolio impact and robustness.

- Compare realized volatility, drawdown, and risk-adjusted return.
- Include transaction costs and turnover constraints.
- Stress-test around high-volatility sessions and limit-up/limit-down concentration days.

Controls and reproducibility:

- Strictly time-ordered train/validation/test splits.
- Fixed seeds and deterministic extraction configuration.
- Explicit leakage checks for all rolling-window transformations.

## Evaluation Metrics and Acceptance Criteria

Dependency and network metrics:

- Correlation between predicted and realized dependency shifts.
- Cluster persistence/stability scores across windows.
- Network feature stability across exchanges and liquidity buckets.

Covariance quality metrics:

- Forecast error vs realized covariance (multiple norms).
- Portfolio-level risk forecast calibration.
- Robustness under stress windows.

Economic and portfolio metrics:

- Realized volatility reduction vs baselines.
- Net Sharpe and drawdown after cost assumptions.
- Turnover and concentration diagnostics.

Acceptance criteria:

- Network-based dependency predictions outperform correlation baselines in out-of-sample windows.
- Network-informed covariance delivers consistent risk forecast improvement.
- Portfolio experiments show improved risk-adjusted outcomes without excessive turnover.
- Results remain directionally stable under conservative stress assumptions.

Failure criteria:

- Gains vanish after controlling for auctions or regime artifacts.
- Covariance improvements fail to translate to downstream risk outcomes.
- Results are highly sensitive to small parameter changes without robust explanation.

## Risks and Mitigations

Risk: Mechanical synchrony in call auctions creates false dependency links.

Mitigation: separate auction and continuous-session modeling; require robustness when auctions are excluded.

Risk: T+1 settlement and price limits create structural breaks not handled by naive similarity.

Mitigation: include regime flags and interaction features; evaluate separately in high-constraint states.

Risk: Overfitting graph hyperparameters (`delta_t`, thresholds, cluster count).

Mitigation: perform grid sensitivity analysis and require broad-plateau performance, not single-point tuning.

Risk: Capacity and turnover costs offset covariance benefits.

Mitigation: enforce turnover budgets and cost-aware evaluation before accepting any portfolio claim.

Risk: Data-quality drift or symbol lifecycle issues degrade network continuity.

Mitigation: apply strict universe eligibility rules and symbol-lifecycle checks via `dim_symbol`.

## Implementation Readiness

If approved, expected artifacts:

- ExecPlan for phased implementation.
- Reproducible data extraction and similarity-build pipeline.
- Dynamic-network and covariance evaluation scripts with fixed configs.
- Portfolio evaluation notebook/script with standard reporting outputs.
- Tests for PIT safety and deterministic behavior on sampled windows.

## Related Proposals

- `chinese-stock-l3-mft-features.md`: Single-stock microstructure and auction signal foundation.
- `t1-adverse-selection-cotrading-integration.md`: Direct integration path between network structure and T+1 features.
- `chinese-stock-l3-mft-features-supplement.md`: Continuous-session microstructure features that can enrich network models.

## Clarifying Questions for Requester

- Question: Is the primary success target risk forecasting quality or portfolio PnL improvement?
  Why it matters: This changes objective weighting, baseline choices, and promotion criteria.
  Default if no answer: Prioritize risk forecasting quality first, then require portfolio confirmation.

- Question: Should auction windows be excluded by default from similarity construction?
  Why it matters: Auction synchrony can be informative or purely mechanical depending on intent.
  Default if no answer: Exclude auctions by default and treat auction-aware variants as controlled extensions.

- Question: What maximum complexity is acceptable for the first implementation (simple shrinkage only vs graphical-model extension)?
  Why it matters: Scope and runtime differ materially across covariance model families.
  Default if no answer: Start with network-shrunk covariance and defer graphical-model extensions.

- Question: Which benchmark family should be canonical: correlation-only, industry-sector, or both?
  Why it matters: Baseline choice defines how strong the claim of incremental value can be.
  Default if no answer: Use both rolling correlation and sector-based benchmarks.

## Decision Needed

Approve this as the canonical template-aligned proposal for co-trading network research in Chinese stocks, and confirm or revise the default answers in the clarifying-question section.

If approved, the next artifact is an ExecPlan covering Phase 1 and Phase 2 implementation first.

## Decision Log

- Decision: Reframed the prior implementation-heavy guide into the repository's research proposal template.
  Rationale: We need a decision-ready proposal with explicit hypotheses, PIT constraints, and acceptance gates before implementation detail.
  Date/Author: 2026-02-14 / Codex

- Decision: Kept Chinese-market adaptations (auctions, T+1, price limits) as first-class design constraints.
  Rationale: These are the main sources of structural difference from US-market co-trading literature and materially affect validity.
  Date/Author: 2026-02-14 / Codex

## Handoff to ExecPlan

If approved for implementation, create an ExecPlan that includes:

- Milestone 1: PIT-safe data extraction and similarity construction with auction controls.
- Milestone 2: Dynamic graph construction and cluster stability validation.
- Milestone 3: Network-informed covariance estimation and baseline comparisons.
- Milestone 4: Cost-aware portfolio impact evaluation and go/no-go recommendation.

The ExecPlan must resolve each clarifying-question outcome and define exact commands, expected outputs, and acceptance evidence.
