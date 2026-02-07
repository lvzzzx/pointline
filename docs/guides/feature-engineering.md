# Feature Engineering Framework (MFT)

This guide describes the PIT-correct feature engineering framework built on Pointline's research APIs. It is designed for MFT workflows that require deterministic ordering, reproducibility, and offline scale.

## Goals
- Deterministic event spine for feature computation
- PIT-aligned joins across trades, book snapshots, and derivative ticker
- Modular feature families (microstructure, flow, funding, regime)

## Core Concepts

### 1) Event Spine
A spine defines the timeline for feature computation. The framework supports:
- `clock` spine: fixed interval timeline (e.g., 1s)
- `trades` spine: event-driven timeline using trade timestamps

### 2) PIT Alignment
All inputs are as-of joined onto the spine using `ts_local_us`, with deterministic ordering enforced by `file_id` and `file_line_number`.

### 3) Feature Families
- **Microstructure**: top-of-book, spread, mid, depth imbalance
- **Trade Flow**: last-trade and signed flow proxies
- **Rolling Flow**: windowed imbalance and trade intensity
- **Funding/Basis**: mark-index basis, funding and OI deltas
- **Book Shape**: depth-weighted VWAP and slope
- **Execution Cost**: spread, half-spread, depth pressure
- **Spread Dynamics**: mid and spread changes
- **Liquidity Shock**: depth z-score vs rolling mean
- **Basis Momentum**: rolling basis z-score and momentum
- **Trade Burst**: inter-arrival timing and burstiness
- **Cross-Venue**: perp-spot basis (requires pre-joined mid prices)
- **Regime**: short-horizon returns and rolling volatility

## Quick Start

```python
from datetime import datetime, timezone
from pointline.research import features

start = datetime(2024, 5, 1, tzinfo=timezone.utc)
end = datetime(2024, 5, 2, tzinfo=timezone.utc)

config = features.FeatureRunConfig(
    spine=features.EventSpineConfig(mode="clock", step_ms=1000),
    include_regime=True,
    regime_window_rows=30,
)

lf = features.build_feature_frame(
    symbol_id=12345,
    start_ts_us=start,
    end_ts_us=end,
    config=config,
)

features_df = lf.collect()
print(features_df.head())
```

## Customization

### Use a trade-driven spine
```python
config = features.FeatureRunConfig(
    spine=features.EventSpineConfig(mode="trades"),
)
```

### Disable a feature family
```python
config = features.FeatureRunConfig(include_funding=False)
```

### Cross-venue mid join helper
```python
from pointline.research import features

cross_mid = features.build_cross_venue_mid_frame(
    spot_symbol_id=111,
    perp_symbol_id=222,
    start_ts_us="2024-05-01",
    end_ts_us="2024-05-02",
    step_ms=1000,
)

config = features.FeatureRunConfig(
    include_cross_venue=True,
    cross_venue_spot_symbol_id=111,
    cross_venue_perp_symbol_id=222,
)
```

## Notes
- Feature outputs use fixed-point inputs (e.g., `px_int`) to keep precision deterministic.
- You can write the resulting DataFrame to a Delta table or Parquet file using Polars helpers.
- For large spans, increase `step_ms` or set a higher `max_rows` on the event spine.
