# exp_2026-01-19_binance_taker_flow

Goal: test whether size-segmented taker flow predicts 3h forward returns on Binance.

## Baseline parameters

- bars: 5m
- large threshold: top 15% rolling notional (72h trailing)
- small threshold: bottom 50% rolling notional (72h trailing)
- target: 3h forward log return

## Suggested run steps

1) Run QA on raw trades
2) Build bar-level factors
3) Save features + target and log the run

## Outputs

- logs/runs.jsonl
- results/features.parquet (or CSV)
- plots/ (IC plots, quantile returns, regime breakdowns)
