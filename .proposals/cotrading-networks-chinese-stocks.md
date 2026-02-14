# Co-Trading Networks for Chinese A-Shares: Research Proposal

## Executive Summary

This proposal evaluates whether co-trading networks built from Chinese A-share Level 3 data can improve dependency modeling, covariance estimation, and portfolio risk control beyond return-correlation baselines.

The core idea is that correlated institutional order placement — concentrated in call auctions and late-session windows — reveals latent portfolio linkages and information flow earlier than low-frequency correlation estimates. Unlike US markets where algorithmic execution during continuous trading generates sub-second co-trading, CN A-share institutional activity concentrates in auction phases. This structural difference requires a fundamentally different network construction approach: order-flow-profile similarity from auction windows rather than trade-timing proximity during continuous trading.

We define a dual-network architecture: (1) an auction network capturing institutional co-activity from order-level data, and (2) an optional continuous-session network capturing retail herding at minute-scale resolution. The auction network is the primary signal; the continuous network is a controlled extension.

Success is defined by robust out-of-sample improvements in dependency forecasts and covariance quality, and by measurable portfolio risk-adjusted gains after realistic costs and turnover constraints.

## Mechanism and Market Structure Rationale

### Why the US co-trading mechanism does not transfer

Lu et al. (2023) and related US-market work exploit sub-second trade synchrony during continuous trading. The mechanism is algorithmic institutional execution: basket trades, ETF arbitrage, stat-arb, and HFT cross-stock market making generate tight temporal co-activity at millisecond scales. Institutions account for ~90% of US equity volume, so continuous-session co-trading directly captures institutional information flow.

CN A-shares have inverted flow composition:

| Property | US Equities | CN A-Shares |
|---|---|---|
| Institutional share of turnover | ~85-90% | ~30-40% |
| Retail share of turnover | ~10-15% | ~60-70% |
| Dominant continuous-session flow | Algo execution (baskets, arb) | Retail (momentum-chasing, herding) |
| Sub-second cross-stock linking | HFT market makers, ETF arb | Minimal — no maker-taker incentive |
| Timestamp precision | Nanosecond (exchange-reported) | Millisecond (exchange-reported, frequent ties) |

Applying trade-timing co-trading at millisecond scales to CN continuous trading would primarily measure retail herding behavior and timestamp collisions — not the institutional information flow that makes the US version work.

### Where institutional co-activity actually concentrates in CN markets

**Opening call auction (09:15-09:25):** Institutional rebalancing, index-tracking orders, and informed orders from overnight research. The no-cancel phase (09:20-09:25) locks in committed orders, separating conviction from noise. Correlated order placement across stocks reveals latent portfolio linkages.

**Closing call auction (SZSE 14:57-15:00):** Index fund MOC rebalancing, quarter-end window dressing, institutional position squaring. Can be 5-15% of daily volume for index constituents. Correlated closing-auction imbalances reveal institutional linkages.

**Late continuous session (14:30-14:57):** Pre-close institutional VWAP completion and position adjustment. Transition zone where institutional footprint increases relative to retail.

**Continuous session mid-day (10:00-14:30):** Predominantly retail. Institutional TWAP/VWAP algos spread execution over 30-60 minutes, creating diffuse co-activity at minute scales — a different signal (retail herding, sector theme chasing) than the institutional basket execution measured in US co-trading.

### Implication for research design

The primary co-trading signal in CN A-shares must be extracted from **auction-phase order flow**, not continuous-session trade timing. This requires:

- Using `cn_order_events` (order submissions) as the primary data source, not `cn_tick_events` (trade matches).
- Measuring order-flow-profile similarity within auction windows, not sub-second trade-timing proximity.
- Treating continuous-session co-trading as a separate, secondary signal capturing retail herding at minute-to-hour scales.

## Research Objective and Hypothesis

Objective: test whether dynamic co-trading networks — primarily from auction-phase order flow in `cn_order_events`, secondarily from continuous-session activity in `cn_tick_events` — provide a superior structure for cross-stock dependency and covariance modeling in Chinese equities.

Hypothesis H1 (auction network): Order-flow-profile similarity in call auctions predicts future return-dependency shifts better than rolling return correlation.

Hypothesis H2 (covariance): Network-informed covariance shrinkage improves realized risk forecasting versus sample covariance, Ledoit-Wolf shrinkage, and realized covariance (Hayashi-Yoshida) baselines.

Hypothesis H3 (clusters): Dynamic auction-derived network clusters capture sector rotation and regime transitions better than static industry labels.

Hypothesis H4 (portfolio): Portfolio construction using network-informed covariance reduces realized volatility and drawdowns without unacceptable turnover.

Hypothesis H5 (continuous network, secondary): Continuous-session co-trading at minute-scale resolution captures retail herding dynamics that provide incremental information beyond the auction network.

## Scope and Non-Goals

In scope:

- Constructing auction-phase order-flow-profile similarity matrices from `cn_order_events`.
- Building continuous-session co-activity matrices from `cn_tick_events` at minute-scale resolution as a controlled extension.
- Building dynamic graphs and cluster structures over rolling windows.
- Evaluating covariance estimation methods with network priors.
- Testing portfolio applications for risk and diversification impact.
- Explicit Chinese-market adaptations: auction-primary design, T+1, and price limits.

Non-goals:

- Live trading deployment or execution stack design.
- Fundamental-factor integration as a first-pass dependency signal.
- Sub-millisecond trade-timing analysis (not supported by CN L3 timestamp precision).
- Full productionization of a portfolio optimizer service.

## Data and PIT Constraints

Primary data tables:

- `cn_order_events` (primary) for auction-phase order submissions, cancellations, and order lifecycle. Source of auction-network similarity.
- `cn_tick_events` (secondary) for continuous-session trade activity. Source of continuous-network similarity and realized covariance baselines.
- `dim_symbol` for universe filters and symbol lifecycle correctness.

PIT constraints and replay rules:

- Use `ts_event_us` as the event-time anchor.
- Restrict cross-symbol synchronization logic to information available at timestamp `t`.
- Auction-network construction uses only **pre-match order flow** (orders submitted before the auction match). Post-match trade records are mechanical outcomes, not signals of co-activity.
- For the opening auction, the critical window is 09:15-09:25 (order submission). The match result at 09:25 is an outcome, not an input.
- For the SZSE closing auction, the critical window is 14:57-15:00 order flow. SSE has no closing auction; use last-minute (14:59-15:00) VWAP orders as a weaker proxy.
- Continuous-session similarity uses trade events during 09:30-11:30 and 13:00-14:57 only. Never span windows across the 11:30-13:00 lunch break.
- Treat deterministic ordering with v2 keys:
  - intra-channel: `(trading_date, channel_id, channel_seq)`
  - cross-channel/cross-table: `(ts_event_us, file_id, file_seq)`
- Avoid lookahead in rolling normalization, clustering, and covariance estimation windows.

Initial universe and horizon assumptions:

- Universe: liquid SZSE/SSE A-shares passing minimum activity filters. Require minimum auction participation (e.g., >=50 orders in opening auction) for auction-network inclusion.
- Windowing: daily auction windows for auction network; rolling multi-day windows (5-20 trading days) for network smoothing. Rolling intraday windows for continuous network.
- Evaluation horizons: next-day dependency prediction and daily realized covariance outcomes. Intraday dependency prediction as secondary evaluation for continuous network.

Computational feasibility:

- Pairwise similarity across N stocks is O(N^2) per window. For ~2000 liquid stocks, this is ~2M pairs.
- Auction-network computation is bounded: each stock has O(100-10000) orders per auction, and similarity is computed from aggregate order-flow profiles (not pairwise order matching). Per-pair cost is O(1) after per-stock aggregation. Total: O(N) aggregation + O(N^2) pairwise comparison of profiles. Feasible for N=2000.
- Continuous-network computation at minute resolution: pre-aggregate to 1-minute volume/count bars per stock, then compute pairwise correlation of bar series. O(N^2 * T) where T = 240 minutes/day. Feasible with vectorized operations.
- Sparsification: retain only top-k neighbors per stock (k=20-50) or edges above adaptive threshold. Reduces downstream graph operations from O(N^2) to O(N*k).

## Feature or Model Concept

### Network 1: Auction Order-Flow-Profile Similarity (Primary)

The core representation replaces trade-timing proximity with order-flow-profile similarity. Two stocks are similar when institutional participants submit correlated order flow during auction windows.

**Per-stock auction profile (computed per auction window):**

For each stock in each opening auction (09:15-09:25), construct a feature vector from `cn_order_events`:

- Net order imbalance: `(buy_qty - sell_qty) / (buy_qty + sell_qty)`.
- Volume participation: total auction order volume / trailing 5-day average.
- Cancellation rate: cancelled qty / submitted qty (09:15-09:20 cancel-allowed phase).
- Commitment ratio: orders surviving into no-cancel phase (09:20-09:25) / total orders.
- Price aggressiveness: volume-weighted distance of order prices from indicative match price.
- Order-size concentration: Gini coefficient of order sizes. Low entropy = institutional-like.

For SZSE closing auction (14:57-15:00), construct analogous profile from closing-auction order flow.

**Pairwise similarity:**

- Cosine similarity or rank correlation between the auction-profile vectors of stock pairs, computed over a rolling window of D trading days (D=5, 10, 20).
- Normalize by each stock's profile volatility to avoid high-activity stocks dominating.
- Rolling-window smoothing captures persistent co-activity patterns while allowing regime shifts.

**Key design variants:**

- Opening-auction-only vs opening + closing combined profiles.
- Cancel-phase features included vs excluded (cancel behavior may be manipulative noise or genuine signal).
- Profile dimension: minimal (imbalance + volume) vs full (all six features above).
- Window length D: shorter = more responsive, longer = more stable. Test D in {5, 10, 20}.

### Network 2: Continuous-Session Co-Activity (Secondary)

Captures retail herding and sector-theme co-movement during continuous trading. Different mechanism, different time scale, different information content.

**Per-stock activity profile (minute-resolution):**

For each stock, compute 1-minute bars during continuous trading (09:30-11:30, 13:00-14:57):

- Signed volume: net aggressor-signed volume per minute.
- Volume surprise: `(volume_1min - EMA_volume) / std_volume`.
- Return: 1-minute log return.

**Pairwise similarity:**

- Correlation of signed-volume series or volume-surprise series across stocks over a rolling window.
- Time scale is 1-5 minutes — this captures retail herding (sector rotation, theme chasing) not institutional basket execution.
- Explicit lunch-break handling: AM (09:30-11:30) and PM (13:00-14:57) profiles computed separately, then averaged or kept as two sub-networks.

**What this measures vs Network 1:**

| Property | Auction Network | Continuous Network |
|---|---|---|
| Participant type | Institutional (rebalancing, informed) | Retail (herding, momentum) |
| Time resolution | Daily (one profile per auction) | Minute-level |
| Update frequency | Daily | Intraday (rolling) |
| Mechanism | Correlated portfolio decisions | Behavioral contagion, theme-chasing |
| Expected persistence | Days to weeks | Hours to days |

### Price-Limit Handling

Price-limit states create artificial co-trading that must be explicitly handled:

- **Limit-locked stocks:** When a stock is at limit price, all trades execute at a single price, spread = 0, imbalance = max. Exclude limit-locked periods from continuous-network computation entirely. For auction network, flag if the previous close was at a limit (next-day auction behavior is structurally different).
- **Near-limit regime (within 2% of limit):** Trades cluster at the limit price, creating mechanical co-activity with other near-limit stocks. Downweight near-limit periods in continuous-network similarity by a factor proportional to distance from limit.
- **Sector-wide limit events:** When >5 stocks in a sector hit the same limit (e.g., sector-wide limit-up), the co-trading signal is real but mechanically inflated. Flag these events and evaluate network behavior with and without them.
- **Consecutive limit days:** Stocks in multi-day limit-up/down sequences have degenerate auction profiles (only one-sided orders). Exclude from auction-network construction during consecutive-limit periods.

### Network Layer

- Construct dynamic sparse graphs via adaptive thresholding or k-nearest connectivity (k=20-50).
- Derive centrality (degree, betweenness, eigenvector) and community features for dependency-state tracking.
- Apply spectral clustering for data-driven dynamic sectors.
- **Cluster differentiation test:** Compare auction-derived clusters against (a) GICS/SW industry classification, (b) Barra-style factor exposure clusters, (c) return-correlation-based clusters. The auction network adds value only if its clusters capture linkages not already present in these known structures. Track the fraction of cross-industry edges (stocks in different sectors linked by auction co-activity) — these are the novel connections.

### Covariance Layer

- Use network-informed shrinkage targets.
- **Covariance horizon:** Daily realized covariance is the primary estimation target. Aggregate intraday auction and continuous signals into a daily network state. The network structure serves as a prior for which off-diagonal covariance entries to trust vs shrink toward zero.
- Compare against baselines:
  - Sample covariance (rolling window).
  - Ledoit-Wolf shrinkage (linear and non-linear).
  - Realized covariance from intraday returns (Hayashi-Yoshida estimator for asynchronous trading).
  - Industry-block-diagonal shrinkage (standard factor model prior).
- Optional extension: graphical Lasso with network-structured sparsity pattern.

### Portfolio Layer (research-only)

- Mean-variance and risk-parity style experiments using each covariance estimator.
- Evaluate diversification quality, realized risk, turnover, and drawdown behavior.
- Turnover constraint: maximum daily portfolio turnover of 30-50% to ensure cost-feasibility under CN costs.

## Experiment Design

Phase 1: Auction-network construction and sanity checks.

- Build reproducible auction order-flow-profile vectors from `cn_order_events` for opening and closing auctions.
- Compute pairwise similarity matrices on rolling windows (D=5, 10, 20 days).
- Validate distributional properties: similarity distribution, sparsity, and stability across days.
- Sanity check: do auction-network clusters correlate with known industry/factor structure? Measure overlap and divergence.
- Ablation: opening-only vs opening+closing, cancel-phase features included vs excluded.
- Limit-handling validation: verify that limit-locked and consecutive-limit stocks are properly excluded/flagged.

Phase 1b (secondary): Continuous-network construction.

- Build 1-minute signed-volume profiles from `cn_tick_events` during continuous sessions.
- Compute pairwise similarity at minute resolution, with AM/PM separation.
- Compare continuous-network structure against auction-network structure: how much overlap? What is unique to each?
- This phase gates H5 — if the continuous network is redundant with the auction network, drop it.

Phase 2: Dependency prediction tests.

- Test whether current auction-network similarity predicts future return-correlation shifts (next 1, 5, 10 days).
- Benchmark against:
  - Lagged return-correlation-only models.
  - Industry/sector co-membership (binary: same sector or not).
  - Factor-exposure similarity (Barra-style factor loadings).
- Assess performance by liquidity tiers (CSI300/500/1000), board (Main vs ChiNext/STAR), and market regimes (bull/bear/range).
- This phase gates H1 — if auction-network similarity does not predict dependency shifts beyond baselines, stop.

Phase 3: Covariance estimation evaluation.

- Forecast daily realized covariance with network-informed and baseline estimators.
- Baselines: sample covariance, Ledoit-Wolf, Hayashi-Yoshida realized covariance, industry-block shrinkage.
- Score estimation error (Frobenius norm, portfolio-variance-ratio) and downstream portfolio risk outcomes.
- Run ablations: similarity definition, window length, thresholding, cluster conditioning.
- This phase gates H2.

Phase 4: Portfolio impact and robustness.

- Compare realized volatility, drawdown, and risk-adjusted return across covariance estimators.
- Include CN transaction costs (commission ~0.025% each way + stamp duty 0.05% sell-only + spread + slippage).
- Enforce turnover constraints (max 30-50% daily turnover).
- Stress-test around: high-volatility sessions, sector-wide limit-up/down days, index rebalance dates, regime transitions (2020 bull → 2022 bear → 2024 recovery).
- This phase gates H4.

Controls and reproducibility:

- Strictly time-ordered train/validation/test splits. No random splitting.
- Fixed seeds and deterministic extraction configuration.
- Explicit leakage checks for all rolling-window transformations.
- Walk-forward evaluation: rolling train window → predict next window → step forward.

## Evaluation Metrics and Acceptance Criteria

Dependency and network metrics:

- Correlation between predicted and realized dependency shifts (next-day, next-5-day).
- Cluster persistence/stability scores across windows.
- Network feature stability across exchanges (SSE vs SZSE) and liquidity buckets.
- **Novelty score:** fraction of high-similarity pairs that are NOT in the same industry/factor group. Higher = more novel information.

Covariance quality metrics:

- Forecast error vs realized covariance (Frobenius norm, portfolio-variance-ratio).
- Portfolio-level risk forecast calibration: `predicted_variance / realized_variance` should be close to 1.0.
- Improvement over Hayashi-Yoshida realized covariance baseline (the strong baseline for anyone with tick data).
- Robustness under stress windows.

Economic and portfolio metrics:

- Realized volatility reduction vs baselines.
- Net Sharpe and drawdown after CN cost assumptions (commission + stamp duty + spread).
- Turnover and concentration diagnostics.

Acceptance criteria:

- H1 gate: Auction-network similarity predicts next-window dependency shifts with statistically significant improvement over correlation and industry baselines (p < 0.05, bootstrap CI).
- H2 gate: Network-informed covariance improves forecast accuracy over Ledoit-Wolf AND Hayashi-Yoshida realized covariance. Improvement must survive in >60% of walk-forward windows.
- H3 gate: Auction-derived clusters show meaningful divergence from static industry labels (novelty score > 0.2) AND the divergent edges carry predictive value.
- H4 gate: Portfolio experiments show improved risk-adjusted outcomes with turnover below 50% daily.
- Results remain directionally stable across window-length choices (D=5, 10, 20) without requiring narrow parameterization.

Failure criteria:

- Auction-network clusters are redundant with industry/factor structure (novelty score < 0.1).
- Covariance improvements vanish when Hayashi-Yoshida realized covariance is included as baseline.
- Results depend on specific window length or threshold without broad-plateau robustness.
- Continuous network (H5) adds no incremental value over auction network alone — this is an acceptable outcome (drop the extension, keep the auction network).

## Risks and Mitigations

Risk: Auction order-flow profiles reflect mechanical index-tracking rather than informative co-activity.

Mitigation: Compare auction-network clusters against index-constituent membership. If the network merely rediscovers index composition, it adds no value. Require novel cross-industry edges to carry predictive weight.

Risk: Auction manipulation (spoofing, cancel-phase gaming) contaminates order-flow profiles.

Mitigation: Test with and without cancel-phase features (09:15-09:20 vs 09:20-09:25). Require robustness to cancel-phase exclusion. Flag stocks with abnormally high order-to-trade ratios in auctions.

Risk: T+1 settlement and price limits create structural breaks not handled by naive similarity.

Mitigation: Exclude limit-locked and consecutive-limit stocks from network construction. Evaluate separately in high-constraint states. Flag T+1 settlement pressure as a conditioning variable.

Risk: Overfitting graph hyperparameters (window length, thresholds, cluster count, profile dimension).

Mitigation: Perform grid sensitivity analysis across D={5,10,20}, k={20,30,50}, profile variants. Require broad-plateau performance, not single-point tuning.

Risk: Capacity and turnover costs offset covariance benefits.

Mitigation: Enforce turnover budgets (max 30-50% daily) and cost-aware evaluation before accepting any portfolio claim. Use CN cost model from evaluation.md.

Risk: Data-quality drift or symbol lifecycle issues degrade network continuity.

Mitigation: Apply strict universe eligibility rules and symbol-lifecycle checks via `dim_symbol`. Require minimum auction participation threshold for inclusion.

Risk: Continuous-session network captures timestamp collisions rather than meaningful co-activity.

Mitigation: Use minute-resolution aggregation (not sub-second). Validate that continuous-network similarity adds information beyond the auction network before investing further. Accept H5 failure as a valid outcome.

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

- Question: Should the continuous-session network (H5) be included in the initial implementation, or deferred until the auction network is validated?
  Why it matters: The continuous network doubles the implementation scope and may prove redundant.
  Default if no answer: Defer continuous network to a controlled extension after Phase 2 validates the auction network.

- Question: What maximum complexity is acceptable for the first implementation (simple shrinkage only vs graphical-model extension)?
  Why it matters: Scope and runtime differ materially across covariance model families.
  Default if no answer: Start with network-shrunk covariance and defer graphical-model extensions.

- Question: Which benchmark family should be canonical?
  Why it matters: Baseline choice defines how strong the claim of incremental value can be.
  Default if no answer: Use all four baselines: rolling return-correlation, industry-sector co-membership, Ledoit-Wolf shrinkage, and Hayashi-Yoshida realized covariance.

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

- Decision: Restructured from continuous-session trade-timing co-trading to auction-primary order-flow-profile similarity.
  Rationale: The US co-trading mechanism (institutional algo execution during continuous trading) does not transfer to CN A-shares where ~65% of continuous-session volume is retail. Institutional co-activity in CN markets concentrates in call auctions. The similarity measure must match the mechanism: order-flow profiles from auction windows, not sub-millisecond trade-timing proximity. Continuous-session co-trading is retained as a secondary signal capturing retail herding at minute-scale resolution.
  Date/Author: 2026-02-14

## Handoff to ExecPlan

If approved for implementation, create an ExecPlan that includes:

- Milestone 1: PIT-safe auction order-flow extraction from `cn_order_events`, per-stock profile construction, and pairwise similarity computation.
- Milestone 2: Dynamic graph construction, cluster stability validation, and novelty scoring versus industry/factor baselines.
- Milestone 3: Network-informed covariance estimation and comparison against all four baselines (sample, Ledoit-Wolf, Hayashi-Yoshida, industry-block).
- Milestone 4: Cost-aware portfolio impact evaluation with CN cost model and turnover constraints.
- Gate: H1 must pass at end of Milestone 2 before proceeding to Milestones 3-4.

The ExecPlan must resolve each clarifying-question outcome and define exact commands, expected outputs, and acceptance evidence.
