# Feature Pipeline Modes: Decision Matrix and PIT-Safe Templates

This guide defines when to use each feature-construction mode and provides PIT-safe templates.

## Status: Current

Current first-class production path:
- `research.pipeline(request)` with mode-first execution:
  - `event_joined`
  - `tick_then_bar`
  - `bar_then_feature`
- Strict PIT and determinism gates are evaluated in every run.

## Modes

1. `event_joined`
- Build features on an event timeline after PIT as-of joins across streams (e.g., TAQ).

2. `tick_then_bar`
- Build microfeatures at tick/event level, then aggregate to bars.

3. `bar_then_feature`
- Resample raw streams to bars first, then compute features on bar data.

## Decision Matrix

| Criterion | `event_joined` | `tick_then_bar` | `bar_then_feature` |
|---|---|---|---|
| Best for | Cross-stream state features | Microstructure alpha | Fast, scalable MFT baselines |
| Signal fidelity | High | Highest | Medium |
| Compute cost | High | Highest | Lowest |
| Implementation complexity | Medium-High | High | Low-Medium |
| PIT leakage risk | Medium (join misuse) | Medium-High (bar boundary mistakes) | Low-Medium |
| Interpretability | High | Medium | High |
| Recommended horizon | Sub-second to minutes | Tick to seconds/minutes | Minutes to hours/days |
| Default join style | As-of backward on event time | As-of backward before/after rollup | Optional as-of after resample |
| Good first production mode | Sometimes | Rarely | Yes |

## Mode Selection Rules

Use `event_joined` when:
- feature depends on interaction between asynchronous streams (trade vs quote state)
- event ordering is part of the hypothesis

Use `tick_then_bar` when:
- alpha is genuinely microstructure-native and weak after direct resampling
- you need burst/toxicity/queue-like event signatures before compression

Use `bar_then_feature` when:
- objective is medium-horizon MFT robustness and scale
- you need quick iteration and stable batch compute

## Global PIT and Determinism Rules

Apply to all modes:
- Primary timeline: `ts_local_us`
- Deterministic order before time operations: `ts_local_us`, `file_id`, `file_line_number`
- As-of joins must default to `strategy="backward"`
- Forward-looking transforms are labels only, never features
- Keep fixed-point integers until final decode/normalization where practical

---

## Canonical Template 1: `event_joined` (TAQ backbone) - Current

```python
from datetime import datetime, timezone
from pointline.research import features

start = datetime(2024, 5, 1, tzinfo=timezone.utc)
end = datetime(2024, 5, 2, tzinfo=timezone.utc)

# Current first-class pipeline: event spine + PIT as-of alignment + feature families
cfg = features.FeatureRunConfig(
    include_microstructure=True,
    include_trade_flow=True,
    include_flow_rolling=True,
)

lf = features.build_feature_frame(
    symbol_id=12345,
    start_ts_us=start,
    end_ts_us=end,
    config=cfg,
)

df = lf.collect()
```

Use for:
- spread dynamics, execution context, cross-stream interaction features
- production-aligned PIT-safe feature generation in current codebase

---

## Canonical Template 2: `tick_then_bar` (micro -> aggregate)

```python
import polars as pl
from pointline.research import query

# 1) Load tick stream
trades = (
    query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
    .sort(["ts_local_us", "file_id", "file_line_number"])
)

# 2) Compute microfeatures at tick level
micro = trades.with_columns([
    pl.when(pl.col("side") == 0).then(-pl.col("qty")).otherwise(pl.col("qty")).alias("signed_qty"),
    (pl.col("price") * pl.col("qty")).alias("notional"),
])

# 3) Aggregate to bars (example: 1m)
bars = (
    micro.group_by_dynamic("ts_local_us", every="1m", period="1m", closed="left", label="left")
    .agg([
        pl.col("signed_qty").sum().alias("signed_flow_1m"),
        pl.col("notional").sum().alias("notional_1m"),
        pl.len().alias("trade_count_1m"),
    ])
    .sort("ts_local_us")
)
```

Critical caution:
- define bucket boundaries once (`closed`, `label`) and keep consistent across train/test.

---

## Canonical Template 3: `bar_then_feature` (resample -> feature)

```python
import polars as pl
from pointline.research import query

# 1) Resample raw stream(s) first
quotes = (
    query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
    .sort(["ts_local_us", "file_id", "file_line_number"])
)

quote_bars = (
    quotes.group_by_dynamic("ts_local_us", every="5m", period="5m", closed="left", label="left")
    .agg([
        pl.col("bid_px").last().alias("bid_px_close"),
        pl.col("ask_px").last().alias("ask_px_close"),
    ])
    .with_columns([
        (pl.col("ask_px_close") - pl.col("bid_px_close")).alias("spread_close"),
        ((pl.col("ask_px_close") + pl.col("bid_px_close")) / 2.0).alias("mid_close"),
    ])
)

# 2) Feature engineering on bars
features = quote_bars.with_columns([
    pl.col("mid_close").pct_change().alias("ret_5m"),
    pl.col("spread_close").rolling_mean(window_size=12).alias("spread_ma_1h"),
])
```

Use for: scalable baseline modeling, regime studies, robust MFT experiments.

---

## Recommended Defaults by Persona (Current Repo)

- HFT-heavy research: start with first-class `event_joined` via `features.build_feature_frame`.
- MFT baseline and broad scans: use current feature framework first; use manual resample patterns only when needed.
- MFT with microstructure edge hypothesis: use manual `tick_then_bar` only with explicit leakage checks and documented bucket policy.

## Validation Checklist (before trusting results)

- Are all feature joins backward as-of?
- Are tie-break sort keys present before time ops?
- Are feature windows backward-only?
- Are label definitions explicitly separated and (if needed) forward-looking?
- Are bucket boundaries (`every/period/closed/label`) fixed and documented?
- Does rerunning produce identical outputs on same inputs?
