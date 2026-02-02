# Machine Learning Feature Engineering

Guide for generating predictive features from Pointline data using Polars.

## Order Book Imbalance (OBI)

**Concept:** Measures the pressure difference between buy and sell sides of the order book.
**Predictive Power:** High correlation with short-term price direction (1-10s horizon).

```python
# Polars Implementation
# Assumes 'book' DataFrame from query.book_snapshot_25(..., decoded=True)

book = book.with_columns([
    # Level 1 Imbalance
    ((pl.col("bid_qty_0") - pl.col("ask_qty_0")) /
     (pl.col("bid_qty_0") + pl.col("ask_qty_0"))).alias("obi_l1"),

    # Deep Imbalance (Top 5 Levels)
    ((pl.sum_horizontal([f"bid_qty_{i}" for i in range(5)]) -
      pl.sum_horizontal([f"ask_qty_{i}" for i in range(5)])) /
     (pl.sum_horizontal([f"bid_qty_{i}" for i in range(5)]) +
      pl.sum_horizontal([f"ask_qty_{i}" for i in range(5)]))).alias("obi_l5")
])
```

## Trade Flow Imbalance (TFI)

**Concept:** The net volume of buyer-initiated vs seller-initiated trades over a window.
**Predictive Power:** Indicates aggressive market participation.

```python
# Polars Implementation
# Assumes 'trades' DataFrame with 'side' (0=buy, 1=sell)

# Signed Volume
trades = trades.with_columns(
    pl.when(pl.col("side") == 0).then(pl.col("qty"))
      .otherwise(-pl.col("qty")).alias("signed_qty")
)

# Rolling Imbalance (e.g., 1-minute window)
# Note: Requires time-based rolling, usually done after set_sorted("ts_local_us")
tfi = trades.rolling(
    index_column="ts_local_us",
    period="1m"
).agg([
    pl.col("signed_qty").sum().alias("tfi_1m")
])
```

## VPIN (Volume-Synchronized Probability of Informed Trading)

**Concept:** A proxy for toxicity or adverse selection risk. High VPIN suggests informed traders are active.
**Approximation:** Absolute order flow imbalance normalized by total volume.

```python
# Simple VPIN-like proxy
vpin_proxy = trades.rolling(
    index_column="ts_local_us",
    period="1m"
).agg([
    (pl.col("signed_qty").sum().abs() / pl.col("qty").sum()).alias("vpin_proxy_1m")
])
```

## Realized Volatility (Rv)

**Concept:** Standard deviation of returns, scaled to a time period.
**Usage:** Normalization factor for other features or target variable.

```python
# Assumes 'klines' or resampled trades
returns = klines.with_columns(
    (pl.col("close") / pl.col("close").shift(1) - 1).alias("ret")
)

rv = returns.rolling(
    index_column="timestamp",
    period="1h"
).agg([
    (pl.col("ret").std() * (24 * 365)**0.5).alias("annualized_rv")
])
```

## Feature Engineering Best Practices

1.  **Stationarity:** Most raw prices/volumes are non-stationary. Always use ratios (imbalance), differences (returns), or normalized values (z-scores).
2.  **Lookahead Bias:** NEVER calculate statistics using the *current* row if that row is the target. Use `shift(1)` to ensure features are strictly based on past data.
3.  **PIT Correctness:** Ensure all joins between features (e.g., Book features joining to Trades) use `join_asof` with `strategy="backward"`.
