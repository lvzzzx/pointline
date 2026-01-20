# exp_2026-01-19_binance_taker_flow

Goal
Test whether size-segmented taker flow predicts 3h forward returns on Binance.

Hypothesis
Large taker flow (aggressive market orders with high notional) reflects informed
pressure and should have positive predictive power for 3h returns. Small taker
flow is more noise-driven and may show weaker or mean-reverting effects. The
predictive edge should strengthen when liquidity is thin or when perp
positioning is crowded.

Data
- lake_root:
- tables:
  - trades (perp)
  - quotes or L2 snapshots (for mid/VWAP and liquidity controls)
  - derivative tickers (OI, funding, mark/index if available)
- symbol_ids:
- date_range:
- ts_col: ts_local_us
- venue: binance-futures (USDT-M)

Definitions
- Taker buy/sell classification:
  - Use trade-side fields to identify aggressor. A taker buy is a market buy
    that lifts the ask; a taker sell hits the bid.
- Bar size:
  - 5m bars for feature construction.
- Target:
  - 3h forward log return on perp mid or VWAP.

Feature construction (bar-level)
1) Size segmentation
   - Compute rolling notional distribution per symbol (72h trailing).
   - Define thresholds:
     - Large: top 15% of trade notionals
     - Small: bottom 50% of trade notionals

2) Taker flow metrics (5m bar)
   - buy_notional_large, sell_notional_large
   - buy_notional_small, sell_notional_small
   - flow_imbalance_large = (buy_large - sell_large) / (buy_large + sell_large)
   - flow_imbalance_small = (buy_small - sell_small) / (buy_small + sell_small)
   - flow_share_large = (buy_large + sell_large) / total_notional

3) Controls (optional but recommended)
   - Liquidity: spread, top-of-book depth, short-horizon volatility
   - Perp state: OI level, dOI z-score, funding z-score
   - Market beta: BTC/ETH return over same window

Modeling and tests
- Baselines:
  - Correlation/IC of flow_imbalance_large vs 3h forward return
  - Compare to flow_imbalance_small
  - Compare to total flow imbalance (no size segmentation)
- Regression:
  - Per-symbol regressions with and without controls
  - Interaction terms: flow_imbalance_large x low_liquidity
- Regime slice (optional):
  - Condition on high funding or large dOI for perp-dominant states

Evaluation
- Predictive:
  - 1h/3h IC and rank IC
  - Quantile return spreads (top vs bottom decile)
- Economic:
  - Simple long/short signal with transaction cost assumptions
- Robustness:
  - Stability across symbols, volatility regimes, and liquidity buckets

Outputs
- logs/runs.jsonl (one JSONL line per run)
- results/features.parquet (or CSV)
- plots/ (IC plots, quantile returns, regime breakdowns)

Notes / assumptions
- Size thresholds are per-symbol and recomputed daily (72h lookback).
- Use ts_local_us consistently for PIT correctness.
- Funding PnL is not included in the target unless explicitly specified.

Next steps
- Fill symbol_ids and date_range for the first run
- Implement the bar builder + taker flow segmentation
- Run baseline IC + decile tests, then add controls
