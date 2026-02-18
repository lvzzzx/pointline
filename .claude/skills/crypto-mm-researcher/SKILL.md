---
name: crypto-mm-researcher
description: "Crypto market making (MM) research assistant for L2-first environments. Focuses on inventory-aware quoting, adverse-selection control, probabilistic fill modeling, and cost-aware evaluation under realistic crypto CEX data constraints. Use when: (1) designing two-sided passive quoting models, (2) building inventory skew and hedge policies, (3) simulating maker fills with incremental L2 order book data, (4) evaluating MM strategies with adverse-selection and transaction cost decomposition, (5) stress-testing MM risk controls across sessions and volatility regimes."
---

# Crypto MM Researcher (L2-First)

Research guidance for crypto market making on CEX perps when true L3 queue state is unavailable. Default target: **5s-60s quoting horizons** with realistic L2-only assumptions.

## Core Positioning

- Most venues provide **incremental L2**, not full L3 order-level queue state.
- Therefore, v1 MM research should be **L2-first and conservative**.
- Sub-second/near-sub-second MM (100ms-5s) is supported only as an advanced, caveated mode.

## Research Workflow

1. **Define MM objective** — spread capture, inventory neutrality, and risk budget.
2. **Build quoting policy** — reservation price + inventory skew + dynamic spread.
3. **Model fills** — probabilistic maker fills under L2 constraints.
4. **Simulate execution** — include cancel/replace latency assumptions and partial fills.
5. **Evaluate** — net PnL, adverse selection, fill quality, inventory tails.
6. **Stress test** — higher costs, lower liquidity, liquidation-cascade regimes.

## What to Optimize (Priority Order)

1. **Risk-adjusted net PnL** after all realistic costs
2. **Inventory stability** (tail inventory and drawdown control)
3. **Adverse-selection minimization**
4. **Fill efficiency** (quote-to-fill, cancel ratio, maker participation)

## Horizon Guide

- **Default (recommended): 5s-60s**
  - Better robustness under L2-only data.
  - Inventory/risk controls matter more than exact queue rank.
- **Advanced: 100ms-5s**
  - Requires stronger latency assumptions and strict fill sensitivity testing.
  - Backtests are fragile without L3-quality queue inference.

## Strategy Style Default

- Core: **two-sided passive quoting**
- Add-on: **optional taker hedge module** when inventory breaches hard limits

## References

- MM models and quoting logic: [references/mm-models.md](references/mm-models.md)
- L2 execution simulation and fill assumptions: [references/execution-simulation.md](references/execution-simulation.md)
- Evaluation framework and pass/fail thresholds: [references/evaluation.md](references/evaluation.md)
- Risk controls and kill-switches: [references/risk-controls.md](references/risk-controls.md)  
  - **Required reading:** `Non-Negotiable Checklist (v1)` in this file before any deployment-oriented conclusions.
- MM-specific crypto market structure notes: [references/market-structure-mm.md](references/market-structure-mm.md)

## Key Principles

- **Cost realism first**: fees + spread + slippage + funding drag are mandatory.
- **No queue-perfect claims from L2-only data**.
- **Walk-forward only**: no random splits for MM policy testing.
- **Inventory risk is a first-class objective, not a side metric**.
- **Conservative assumptions beat inflated backtests**.