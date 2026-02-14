---
name: cn-ashare-intraday-researcher
description: "ML-driven CN A-share intraday quant research using L2/L3 data. Feature engineering, ML training, and evaluation for 1min-2hr horizons on SSE/SZSE stocks."
---

# CN A-Share Intraday Researcher

ML-driven quant research for Chinese A-share intraday trading. Target: 1min-2hr holding period on SSE/SZSE stocks using L2/L3 data. Key structural constraints: T+1 settlement (cannot sell same-day purchases), price limits (+/-10% Main Board, +/-20% ChiNext/STAR), and 90-minute lunch break.

For L2/L3 data format schemas, field definitions, and exchange-specific parsing rules, see the **quant360** skill.

## Research Workflow

1. **Formulate hypothesis** — What microstructure inefficiency? What mechanism? Why predictive?
2. **Engineer features** — See [references/features.md](references/features.md) for catalog
3. **Design labels** — Forward return, VWAP-benchmarked, alpha (hedged). See [references/ml-models.md](references/ml-models.md#label-design)
4. **Train model** — LightGBM default, walk-forward CV. See [references/ml-models.md](references/ml-models.md)
5. **Evaluate** — IC, decay curve, cost-adjusted Sharpe. See [references/evaluation.md](references/evaluation.md)
6. **Validate** — Hold-out test, deflated Sharpe, T+1-realistic backtest. See [references/evaluation.md](references/evaluation.md#overfitting-detection)

## Feature Categories

| Category | Source | Typical IC | Decay Half-Life | Reference |
|---|---|---|---|---|
| **Book state** | **L2 snapshots, L3-reconstructed** | **0.02-0.06** | **1-10min** | [features.md#book-state](references/features.md#book-state-features) |
| Event stream: order flow | L3 order events | 0.02-0.06 | 1-10min | [features.md#order-flow](references/features.md#order-flow-l3) |
| Event stream: trade flow | Tick-by-tick trades | 0.02-0.05 | 1-15min | [features.md#trade-flow](references/features.md#trade-flow-tick-by-tick) |
| Event stream: order-trade joint | L3 + tick-by-tick | 0.03-0.06 | 1-15min | [features.md#joint](references/features.md#order-trade-joint-features-l3--tick-by-tick) |
| Auction signals | Opening/closing call auction | 0.03-0.06 | 5-30min | [features.md#auction](references/features.md#auction-features) |
| Price limit dynamics | Distance to limit, limit board | 0.02-0.05 | 10min-2hr | [features.md#price-limit](references/features.md#price-limit-features) |
| Volume & liquidity | Intraday volume profile, turnover | 0.01-0.03 | 10min-1hr | [features.md#volume](references/features.md#intraday-volume--liquidity-features) |
| Cross-sectional | Sector flow, relative strength, northbound | 0.02-0.04 | 15min-2hr | [features.md#cross-sectional](references/features.md#cross-sectional-features) |
| Microstructure | Spread, impact, noise | 0.01-0.04 | 1-5min | [features.md#microstructure](references/features.md#microstructure-features) |
| Index futures & options | Basis, put-call ratio, GEX | 0.02-0.04 | 5min-1hr | [features.md#futures](references/features.md#index-futures--etf-options-features) |
| Temporal / calendar | Session, phase, events | 0.01-0.02 | N/A | [features.md#temporal](references/features.md#temporal--calendar-features) |

## Model Selection Quick Guide

- **Start here:** LightGBM with walk-forward CV. `min_child_samples=200-2000`, `num_leaves=31`, `max_depth=6`.
- **Baseline:** Ridge regression on z-scored features. If LightGBM can't beat Ridge, features are weak.
- **Cross-sectional:** Pool across stocks with board/sector/cap as features. Per-stock models only for mega-caps.
- **Scale up:** TCN or Transformer only with >1M samples and pre-validated features.
- **Combine:** Stack Ridge + LightGBM + MLP with Ridge meta-learner on OOS predictions.

Full model details: [references/ml-models.md](references/ml-models.md)

## Evaluation Essentials

**Signal quality thresholds:**
- IC > 0.02 (meaningful), > 0.05 (strong), > 0.08 (check for bugs)
- ICIR > 0.5 (acceptable), > 1.0 (strong)
- OOS/IS Sharpe ratio > 0.5 (not overfit)

**Must-do checks:**
- Signal decay curve across horizons 1m to 2hr
- Cost-adjusted backtest (commission ~0.025% each way + stamp duty 0.05% sell-only + spread + slippage)
- PnL breakdown by session (AM/PM) and market regime
- T+1 realistic backtest (compare to idealized T+0)
- Deflated Sharpe ratio correcting for number of trials
- Shuffled-label sanity check

Full evaluation methodology: [references/evaluation.md](references/evaluation.md)

## CN A-Share Structural Constraints

These constraints fundamentally shape research design. They are not afterthoughts.

- **T+1 settlement** — Cannot sell same-day purchases. Intraday signals must survive overnight holding. Use index futures (IF/IH/IC/IM) to hedge overnight gap risk. Some ETFs allow T+0.
- **Price limits** — Main Board +/-10%, ChiNext/STAR +/-20%. Truncate return distributions. Create magnet effects, limit-up board dynamics, and feature degeneracy at limit.
- **Lunch break (90min)** — Never compute features across 11:30-13:00. Treat AM and PM as quasi-independent sessions. Post-lunch behaves like a mini-open.
- **Retail-dominated (~65% turnover)** — Behavioral alpha from herding, overreaction, momentum-chasing. Different microstructure than institutional markets.
- **No effective short selling** — Most stocks cannot be shorted. Market is structurally long-biased.
- **Session structure** — Opening auction (09:15-09:25), AM continuous (09:30-11:30), PM continuous (13:00-14:57/15:00), closing auction (SZSE 14:57-15:00 only). Each phase has different dynamics.

Full market structure reference: [references/market-structure.md](references/market-structure.md)

## Key Principles

- **T+1 realism first.** No signal matters if it can't survive overnight holding. Always compare T+0 (idealized) vs T+1 (realistic) backtest.
- **Cost-awareness.** CN round-trip costs are ~15-30 bps (commission + stamp duty + spread + slippage). Signal must clear this hurdle.
- **Session-aware.** AM and PM are different markets. Handle lunch break as a discontinuity. Report results by session.
- **Limit-aware.** Price limits create non-linear dynamics. Detect limit-proximity regime and either adapt features or use separate models.
- **Walk-forward only.** CN regimes shift hard (bull 2020 vs bear 2022 vs recovery 2024). Rolling-window CV preferred.
- **Purge and embargo.** Overlapping labels contaminate folds. Purge = label horizon, embargo = max(30min, 1% of sample). Lunch break provides natural embargo.
- **Multiple testing correction.** 100 features tested → need Sharpe ~3.0 for significance. Use deflated Sharpe.
- **Mechanism matters.** "Why does this work?" more important than "does this work?" Behavioral explanation (retail herding, institutional rebalancing) strengthens signal.
