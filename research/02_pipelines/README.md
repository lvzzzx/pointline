# Research Pipelines (Script-First)

These scripts are lightweight, CLI-first building blocks for research runs.
They avoid extra dependencies and focus on PIT-correct access patterns.

## Pipelines

- extract/qa_trades.py
  - Basic QA: duplicates, timestamp monotonicity, side sanity checks

- features/large_small_taker_flow.py
  - Builds 5m bars, rolling size thresholds, large/small aggregates, and 3h targets

## Conventions

- Inputs are symbol_id + [start_ts_us, end_ts_us)
- Use ts_local_us for replay-safe timing
- Decode price/qty only when needed for notional
- Append run metadata to logs/runs.jsonl in the experiment folder
