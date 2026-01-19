# Research Workspace (Script-First)

This folder is the home for quantitative research using the Pointline data lake.
Default mode: scripts + reusable pipelines, notebooks only for quick exploration.

## Layout

- _config/            Global configs (lake root, defaults)
- 01_explore/         Quick checks and one-off exploration
- 02_pipelines/       Reusable code (extract, features, backtests)
- 03_experiments/     Self-contained experiment runs
- 04_reports/         Final writeups, summaries, dashboards
- 99_archive/         Old or abandoned work

## Core research rules

- Resolve symbol_id once and store it in the experiment config.
- Use ts_local_us for any backtest or replay timeline (PIT-correct).
- Keep price_int/qty_int as integers until the final decode step.
- Prefer pointline.research helpers for partition pruning.

See 02_pipelines/README.md for available scripts.

## Experiment template

Copy: 03_experiments/_template -> 03_experiments/exp_YYYY-MM-DD_name

Each experiment should include:
- README.md     Hypothesis + method + result summary
- config.yaml   All parameters used for the run
- queries/      SQL or query notes
- logs/         JSONL run logs (one line per run)
- results/      Small derived artifacts (metrics, CSVs)
- plots/        Figures

## Run logging (JSONL)

Each run must append a single line JSON object to logs/runs.jsonl.
Suggested schema:

{"run_id":"2026-01-19T18:02:11Z","git_commit":"abc1234","lake_root":"/mnt/pointline/lake","symbol_ids":[101],"tables":["silver.trades","silver.quotes"],"start_ts_us":1700000000000000,"end_ts_us":1700003600000000,"ts_col":"ts_local_us","decode":"pointline.tables.trades.decode_fixed_point","params":{"lookback_s":60},"metrics":{"sharpe":1.12,"turnover":3.4}}

## Repro checklist

- symbol_id(s) and resolution window recorded
- ts_col recorded (default ts_local_us)
- lake_root recorded
- git commit recorded
- parameters recorded
- metrics recorded
