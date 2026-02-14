---
name: crypto-mft-researcher
description: "ML-driven cryptocurrency medium-frequency trading (MFT) research assistant for a perp-dominated market. Covers feature engineering (derivatives positioning, funding rates, OI, liquidations, LOB, trade flow, options IV), ML model training, and evaluation for 10s-1hr holding periods across perps, spot, and options. Use when: (1) designing or reviewing alpha signals for crypto MFT, (2) building ML pipelines using derivatives positioning data (funding, OI, liquidations, long/short ratios), (3) engineering features from order book, trade flow, or options IV surfaces, (4) evaluating signal quality (IC, decay, overfitting), (5) discussing crypto market microstructure for trading research, (6) planning MFT research programs or reviewing research methodology."
---

# Crypto MFT Researcher

ML-driven quant research for crypto medium-frequency trading. Target: 10s-1hr holding period across perps (primary), spot, and options. Crypto is a perp-dominated market — derivatives positioning data (funding rates, OI, liquidations) is often more predictive than LOB features at MFT horizons.

## Research Workflow

1. **Formulate hypothesis** — What market inefficiency? What mechanism? Why predictive?
2. **Engineer features** — See [references/features.md](references/features.md) for catalog
3. **Design labels** — Forward return, triple-barrier, cost-adjusted. See [references/ml-models.md](references/ml-models.md#label-design)
4. **Train model** — LightGBM default, purged CV. See [references/ml-models.md](references/ml-models.md)
5. **Evaluate** — IC, decay curve, cost-adjusted Sharpe. See [references/evaluation.md](references/evaluation.md)
6. **Validate** — Hold-out test, deflated Sharpe, stress test. See [references/evaluation.md](references/evaluation.md#overfitting-detection)

## Feature Categories

| Category | Source | Typical IC | Decay Half-Life | Reference |
|---|---|---|---|---|
| **Derivatives & positioning** | **Funding, OI, liquidations, L/S ratio** | **0.03-0.08** | **5min-1hr** | [features.md#derivatives](references/features.md#derivatives--positioning-features) |
| Order book / LOB | L2 snapshots, book updates | 0.02-0.08 | 30s-5min | [features.md#order-book](references/features.md#order-book--lob-features) |
| Trade flow | Tick trades, aggressor side | 0.02-0.06 | 1-10min | [features.md#trade-flow](references/features.md#trade-flow-features) |
| Microstructure | Spread, impact, noise | 0.01-0.04 | 1-5min | [features.md#microstructure](references/features.md#microstructure-features) |
| Cross-asset | Cross-exchange, beta, sector | 0.02-0.05 | 5-30min | [features.md#cross-asset](references/features.md#cross-asset-features) |
| Volatility | Realized vol, range estimators | 0.01-0.03 | 10min-1hr | [features.md#volatility](references/features.md#volatility-features) |
| Options-derived | IV surface, Greeks, GEX, flow | 0.02-0.05 | 10min-4hr | [features.md#options](references/features.md#options-derived-features) |
| Temporal | Time-of-day, funding countdown | 0.01-0.02 | N/A | [features.md#temporal](references/features.md#temporal--calendar-features) |

## Model Selection Quick Guide

- **Start here:** LightGBM with purged K-fold. `min_child_samples=100-1000`, `num_leaves=31`, `max_depth=6`.
- **Baseline:** Ridge regression on z-scored features. If LightGBM can't beat Ridge, features are weak.
- **Scale up:** TCN or Transformer only with >1M samples and pre-validated features.
- **Combine:** Stack Ridge + LightGBM + MLP with Ridge meta-learner on OOS predictions.
- **Adapt:** Incremental LightGBM (`init_model`) for daily/hourly warm-start retraining.

Full model details, architectures, and training methodology: [references/ml-models.md](references/ml-models.md)

## Evaluation Essentials

**Signal quality thresholds:**
- IC > 0.02 (meaningful), > 0.05 (strong), > 0.10 (check for bugs)
- ICIR > 0.5 (acceptable), > 1.0 (strong)
- OOS/IS Sharpe ratio > 0.5 (not overfit)

**Must-do checks:**
- Signal decay curve across horizons 10s to 4hr
- Cost-adjusted backtest (taker fees + median spread + slippage)
- PnL breakdown by session (Asia/EU/US) and vol regime
- Deflated Sharpe ratio correcting for number of trials
- Shuffled-label sanity check

Full evaluation methodology and checklist: [references/evaluation.md](references/evaluation.md)

## Crypto Market Structure

Crypto is perp-dominated: perp volume is 5-20x spot on most pairs. This means derivatives data (funding, OI, liquidations, positioning) is the primary signal source, not an afterthought.

Key structural properties:
- **Perp dominance** — Perps lead price discovery on most pairs. Spot follows. Perp-specific data (funding, OI, liquidations) carries unique alpha not available in TradFi.
- **Funding rate** — 8h settlement (00/08/16 UTC). Predicted rate available real-time. Pre-settlement drift, post-settlement reversal, extreme funding mean-reversion.
- **Open interest dynamics** — OI-price divergence is a key regime signal. Rising price + falling OI = short covering (weak). OI/market-cap ratio measures leverage fragility.
- **Liquidation cascades** — Leveraged perps create cascading forced liquidations. Non-Gaussian tails. Detectable and partially predictable from OI + funding extremes.
- **24/7 trading** — No open/close auctions. Session effects from participant geography (Asia/EU/US).
- **Fragmented liquidity** — Same pair on 5+ exchanges. Cross-exchange funding/OI divergence is a signal.
- **Options on Deribit** — IV surface, GEX, dealer positioning provide forward-looking signals.

Full market structure reference: [references/market-structure.md](references/market-structure.md)

## Key Principles

- **Cost-awareness first.** No signal matters if it can't survive `2 * (fee + half_spread + slippage)` per round-trip.
- **Walk-forward only.** Crypto regimes shift hard. Expanding-window or rolling-window CV. Never random split.
- **Purge and embargo.** Overlapping labels contaminate folds. Purge = label horizon, embargo = 1% of sample.
- **Multiple testing correction.** 100 features tested -> need Sharpe ~3.0 for significance. Use deflated Sharpe.
- **Mechanism matters.** "Why does this work?" is more important than "does this work?" Statistical arb without mechanism is fragile.
- **Decay determines horizon.** Measure signal half-life. Optimal holding ~ half-life adjusted for costs.
