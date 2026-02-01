# exp_YYYY-MM-DD_name

## Hypothesis

[State your hypothesis here]

Example: "Large trades (>1 BTC) cause short-term price impact of >0.1% on average"

---

## Data

- **Exchange:** binance-futures
- **Symbol:** BTCUSDT
- **Date range:** 2024-05-01 to 2024-05-31
- **Tables:** trades, quotes
- **Timestamp column:** ts_local_us (default)

**Note:** Using query API for automatic symbol resolution. For production research requiring explicit symbol_id control, see [Researcher Guide - Core API](../../docs/guides/researcher_guide.md#7-advanced-topics-core-api).

---

## Method

### Data Loading

```python
from pointline.research import query

trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-31",
    decoded=True,
    lazy=True,
)
```

### Analysis Steps

1. [Step 1: e.g., Filter large trades]
2. [Step 2: e.g., Measure price impact]
3. [Step 3: e.g., Calculate statistics]

---

## Results

### Metrics

| Metric | Value | Notes |
|--------|-------|-------|
| Example metric | 0.00 | Description |

### Key Findings

- [Finding 1]
- [Finding 2]
- [Finding 3]

### Visualizations

See `plots/` directory:
- `plot_1.png` - [Description]
- `plot_2.png` - [Description]

---

## Next Steps

- [ ] [Next iteration or follow-up experiment]
- [ ] [Additional analysis needed]
- [ ] [Questions to investigate]

---

## Reproducibility

**Run ID:** [Auto-logged in logs/runs.jsonl]
**Git commit:** [Run `git rev-parse HEAD` to get commit hash]

To reproduce this experiment:
1. Checkout the git commit above
2. Run `python experiment.py`
3. Results will be logged to `logs/runs.jsonl`
