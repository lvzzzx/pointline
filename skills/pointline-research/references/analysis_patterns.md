# Common Analysis Patterns

This guide provides reference implementations for common quantitative analysis patterns.

## Table of Contents

- [Spread Analysis](#spread-analysis)
- [Volume Profiling](#volume-profiling)
- [Order Flow Metrics](#order-flow-metrics)
- [Market Microstructure](#market-microstructure)
- [Execution Quality](#execution-quality)

---

## Spread Analysis

### Quoted Spread (Bid-Ask Spread)

The difference between best ask and best bid at any point in time.

```python
from pointline.research import query
import polars as pl

# Load quotes
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Calculate quoted spread
quotes_with_spread = quotes.with_columns([
    # Absolute spread
    (pl.col("ask_price") - pl.col("bid_price")).alias("quoted_spread"),

    # Relative spread (basis points)
    ((pl.col("ask_price") - pl.col("bid_price")) / pl.col("mid_price") * 10000).alias("quoted_spread_bps"),

    # Mid price
    ((pl.col("ask_price") + pl.col("bid_price")) / 2).alias("mid_price"),
])

# Summary statistics
spread_stats = quotes_with_spread.select([
    pl.col("quoted_spread_bps").mean().alias("mean_spread_bps"),
    pl.col("quoted_spread_bps").median().alias("median_spread_bps"),
    pl.col("quoted_spread_bps").std().alias("std_spread_bps"),
    pl.col("quoted_spread_bps").min().alias("min_spread_bps"),
    pl.col("quoted_spread_bps").max().alias("max_spread_bps"),
])

print(spread_stats)
```

### Effective Spread

The difference between trade price and mid-quote at time of trade.

```python
from pointline.research import query
import polars as pl

# Load trades and quotes
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Calculate mid price for quotes
quotes = quotes.with_columns(
    ((pl.col("ask_price") + pl.col("bid_price")) / 2).alias("mid_price")
)

# As-of join: match each trade with most recent quote
trades_with_quotes = trades.join_asof(
    quotes.select(["ts_local_us", "mid_price"]),
    on="ts_local_us",
    strategy="backward",  # Use most recent quote BEFORE trade
)

# Calculate effective spread
trades_with_spread = trades_with_quotes.with_columns([
    # Signed effective spread (positive for buys, negative for sells)
    pl.when(pl.col("side") == 0)  # Buy
      .then((pl.col("price") - pl.col("mid_price")) * 2)
      .otherwise((pl.col("mid_price") - pl.col("price")) * 2)
      .alias("effective_spread"),

    # Effective spread in basis points
    pl.when(pl.col("side") == 0)
      .then((pl.col("price") - pl.col("mid_price")) / pl.col("mid_price") * 20000)
      .otherwise((pl.col("mid_price") - pl.col("price")) / pl.col("mid_price") * 20000)
      .alias("effective_spread_bps"),
])

# Average effective spread (should be positive)
avg_effective_spread = trades_with_spread.select(
    pl.col("effective_spread_bps").mean().alias("avg_effective_spread_bps")
)

print(avg_effective_spread)
```

### Realized Spread

Measures adverse selection: effective spread minus price change after trade.

```python
# Calculate mid price change after each trade (5-second window)
window_us = 5_000_000  # 5 seconds

# Self as-of join to get future mid price
trades_future = trades_with_quotes.select([
    (pl.col("ts_local_us") + window_us).alias("ts_future"),
    pl.col("mid_price").alias("mid_future"),
])

trades_with_future = trades_with_quotes.join_asof(
    trades_future,
    left_on="ts_local_us",
    right_on="ts_future",
    strategy="backward",
)

# Calculate realized spread
trades_with_realized = trades_with_future.with_columns([
    # Realized spread = effective spread - adverse selection
    pl.when(pl.col("side") == 0)  # Buy
      .then((pl.col("price") - pl.col("mid_future")) * 2)
      .otherwise((pl.col("mid_future") - pl.col("price")) * 2)
      .alias("realized_spread"),
])

# Adverse selection cost
trades_with_realized = trades_with_realized.with_columns(
    (pl.col("effective_spread") - pl.col("realized_spread")).alias("adverse_selection")
)

print(trades_with_realized.select([
    pl.col("effective_spread").mean().alias("avg_effective_spread"),
    pl.col("realized_spread").mean().alias("avg_realized_spread"),
    pl.col("adverse_selection").mean().alias("avg_adverse_selection"),
]))
```

---

## Volume Profiling

### VWAP (Volume-Weighted Average Price)

```python
from pointline.research import query
import polars as pl

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Calculate VWAP (cumulative)
trades_with_vwap = trades.with_columns([
    # Cumulative VWAP
    ((pl.col("price") * pl.col("qty")).cum_sum() / pl.col("qty").cum_sum()).alias("vwap_cumulative"),
])

# Calculate VWAP over sliding window (e.g., 100 trades)
trades_with_vwap = trades_with_vwap.with_columns([
    # Rolling VWAP (100 trades)
    ((pl.col("price") * pl.col("qty")).rolling_sum(window_size=100) /
     pl.col("qty").rolling_sum(window_size=100)).alias("vwap_rolling_100"),
])

# VWAP by hour
trades_with_hour = trades.with_columns([
    # Convert ts_local_us to datetime
    (pl.col("ts_local_us") / 1_000_000).cast(pl.Int64).cast(pl.Datetime("us")).alias("datetime"),
])

hourly_vwap = trades_with_hour.group_by_dynamic(
    "datetime",
    every="1h",
).agg([
    (pl.col("price") * pl.col("qty")).sum().alias("price_qty_sum"),
    pl.col("qty").sum().alias("total_qty"),
]).with_columns(
    (pl.col("price_qty_sum") / pl.col("total_qty")).alias("vwap")
)

print(hourly_vwap)
```

### TWAP (Time-Weighted Average Price)

```python
from pointline.research import query
import polars as pl

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# TWAP requires time-weighted calculation
# Approach: Sample price at regular intervals (e.g., every second)

# Create time grid (1-second intervals)
start_ts = trades["ts_local_us"].min()
end_ts = trades["ts_local_us"].max()
time_grid = pl.DataFrame({
    "ts_local_us": pl.int_range(start_ts, end_ts, step=1_000_000, eager=True)  # 1-second steps
})

# As-of join to get price at each timestamp
prices_sampled = time_grid.join_asof(
    trades.select(["ts_local_us", "price"]),
    on="ts_local_us",
    strategy="backward",
)

# TWAP = simple average of sampled prices
twap = prices_sampled.select(
    pl.col("price").mean().alias("twap")
)

print(twap)

# TWAP by hour
prices_sampled = prices_sampled.with_columns([
    (pl.col("ts_local_us") / 1_000_000).cast(pl.Int64).cast(pl.Datetime("us")).alias("datetime"),
])

hourly_twap = prices_sampled.group_by_dynamic(
    "datetime",
    every="1h",
).agg([
    pl.col("price").mean().alias("twap"),
])

print(hourly_twap)
```

### Volume Distribution

```python
from pointline.research import query
import polars as pl

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Volume by price level (volume profile)
volume_profile = trades.group_by(
    # Round price to nearest tick (e.g., $10 increments)
    (pl.col("price") / 10).floor() * 10
).agg([
    pl.col("qty").sum().alias("total_volume"),
    pl.count().alias("num_trades"),
]).sort("price")

print(volume_profile)

# Volume by side
volume_by_side = trades.group_by("side").agg([
    pl.col("qty").sum().alias("total_volume"),
    pl.count().alias("num_trades"),
])

print(volume_by_side)
```

---

## Order Flow Metrics

### Trade Imbalance

```python
from pointline.research import query
import polars as pl

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Calculate signed volume (buy volume - sell volume)
trades_with_signed = trades.with_columns([
    pl.when(pl.col("side") == 0)  # Buy
      .then(pl.col("qty"))
      .otherwise(-pl.col("qty"))
      .alias("signed_volume"),
])

# Cumulative order flow imbalance
trades_with_imbalance = trades_with_signed.with_columns([
    pl.col("signed_volume").cum_sum().alias("cumulative_imbalance"),
])

# Rolling imbalance (e.g., last 100 trades)
trades_with_imbalance = trades_with_imbalance.with_columns([
    pl.col("signed_volume").rolling_sum(window_size=100).alias("imbalance_rolling_100"),
])

# Imbalance ratio (buy volume / total volume)
trades_with_imbalance = trades_with_imbalance.with_columns([
    (pl.col("imbalance_rolling_100") / pl.col("qty").rolling_sum(window_size=100)).alias("imbalance_ratio"),
])

print(trades_with_imbalance.select([
    "ts_local_us", "price", "qty", "side", "imbalance_rolling_100", "imbalance_ratio"
]))
```

### Order Book Imbalance

```python
from pointline.research import query
import polars as pl

# Load order book snapshots
book = query.book_snapshot_25("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Calculate order book imbalance (top of book)
book_with_imbalance = book.with_columns([
    # Bid-ask imbalance
    ((pl.col("bid_qty_0") - pl.col("ask_qty_0")) /
     (pl.col("bid_qty_0") + pl.col("ask_qty_0"))).alias("imbalance_top"),

    # Imbalance over top 5 levels
    ((pl.sum_horizontal([f"bid_qty_{i}" for i in range(5)]) -
      pl.sum_horizontal([f"ask_qty_{i}" for i in range(5)])) /
     (pl.sum_horizontal([f"bid_qty_{i}" for i in range(5)]) +
      pl.sum_horizontal([f"ask_qty_{i}" for i in range(5)]))).alias("imbalance_top5"),
])

print(book_with_imbalance.select([
    "ts_local_us", "imbalance_top", "imbalance_top5"
]))
```

### Tick Rule (Trade Classification)

```python
from pointline.research import query
import polars as pl

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Classify trades as buyer-initiated or seller-initiated
# Tick rule: compare price to previous trade price
trades_with_tick = trades.with_columns([
    # Previous trade price
    pl.col("price").shift(1).alias("prev_price"),
]).with_columns([
    # Tick rule classification
    pl.when(pl.col("price") > pl.col("prev_price"))
      .then(pl.lit("buyer_initiated"))
      .when(pl.col("price") < pl.col("prev_price"))
      .then(pl.lit("seller_initiated"))
      .otherwise(pl.lit("mid_price"))
      .alias("tick_rule"),
])

# Aggregate by classification
tick_rule_summary = trades_with_tick.group_by("tick_rule").agg([
    pl.col("qty").sum().alias("total_volume"),
    pl.count().alias("num_trades"),
])

print(tick_rule_summary)
```

---

## Market Microstructure

### Price Impact

Measures how much a trade moves the market.

```python
from pointline.research import query
import polars as pl

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Calculate mid price
quotes = quotes.with_columns(
    ((pl.col("ask_price") + pl.col("bid_price")) / 2).alias("mid_price")
)

# Match trades with pre-trade mid price
trades_with_pre_mid = trades.join_asof(
    quotes.select(["ts_local_us", "mid_price"]),
    on="ts_local_us",
    strategy="backward",
).rename({"mid_price": "mid_pre"})

# Match trades with post-trade mid price (e.g., 1 second later)
window_us = 1_000_000  # 1 second

quotes_future = quotes.select([
    (pl.col("ts_local_us") - window_us).alias("ts_past"),
    pl.col("mid_price").alias("mid_post"),
])

trades_with_post_mid = trades_with_pre_mid.join_asof(
    quotes_future,
    left_on="ts_local_us",
    right_on="ts_past",
    strategy="forward",
)

# Calculate price impact
trades_with_impact = trades_with_post_mid.with_columns([
    # Absolute price impact
    (pl.col("mid_post") - pl.col("mid_pre")).alias("price_impact"),

    # Price impact in basis points
    ((pl.col("mid_post") - pl.col("mid_pre")) / pl.col("mid_pre") * 10000).alias("price_impact_bps"),

    # Signed price impact (buy should increase price)
    pl.when(pl.col("side") == 0)  # Buy
      .then((pl.col("mid_post") - pl.col("mid_pre")) / pl.col("mid_pre") * 10000)
      .otherwise((pl.col("mid_pre") - pl.col("mid_post")) / pl.col("mid_pre") * 10000)
      .alias("signed_impact_bps"),
])

# Price impact by trade size
impact_by_size = trades_with_impact.with_columns([
    # Categorize by trade size
    pl.when(pl.col("qty") < 1.0)
      .then(pl.lit("small"))
      .when(pl.col("qty") < 10.0)
      .then(pl.lit("medium"))
      .otherwise(pl.lit("large"))
      .alias("size_category"),
]).group_by("size_category").agg([
    pl.col("signed_impact_bps").mean().alias("avg_impact_bps"),
    pl.count().alias("num_trades"),
])

print(impact_by_size)
```

### Quote Stability

Measures how long best quotes stay stable.

```python
from pointline.research import query
import polars as pl

quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Detect quote changes
quotes_with_changes = quotes.with_columns([
    # Previous bid/ask
    pl.col("bid_price").shift(1).alias("prev_bid"),
    pl.col("ask_price").shift(1).alias("prev_ask"),
]).with_columns([
    # Did quote change?
    ((pl.col("bid_price") != pl.col("prev_bid")) |
     (pl.col("ask_price") != pl.col("prev_ask"))).alias("quote_changed"),

    # Time since last quote update
    (pl.col("ts_local_us") - pl.col("ts_local_us").shift(1)).alias("time_since_update_us"),
])

# Average time between quote updates
avg_update_interval = quotes_with_changes.select(
    (pl.col("time_since_update_us").mean() / 1_000_000).alias("avg_update_interval_sec")
)

print(avg_update_interval)

# Distribution of update intervals
update_intervals = quotes_with_changes.filter(
    pl.col("quote_changed")
).with_columns([
    (pl.col("time_since_update_us") / 1000).alias("interval_ms")
]).group_by(
    # Bin by milliseconds
    (pl.col("interval_ms") / 10).floor() * 10
).agg([
    pl.count().alias("count")
]).sort("interval_ms")

print(update_intervals)
```

---

## Execution Quality

### Slippage Analysis

```python
from pointline.research import query
import polars as pl

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Match trades with quotes (to get best bid/ask at trade time)
trades_with_quotes = trades.join_asof(
    quotes.select(["ts_local_us", "bid_price", "ask_price"]),
    on="ts_local_us",
    strategy="backward",
)

# Calculate slippage (distance from best quote)
trades_with_slippage = trades_with_quotes.with_columns([
    # For buys: slippage = price - ask_price (positive = worse execution)
    # For sells: slippage = bid_price - price (positive = worse execution)
    pl.when(pl.col("side") == 0)  # Buy
      .then(pl.col("price") - pl.col("ask_price"))
      .otherwise(pl.col("bid_price") - pl.col("price"))
      .alias("slippage"),

    # Slippage in basis points
    pl.when(pl.col("side") == 0)
      .then((pl.col("price") - pl.col("ask_price")) / pl.col("ask_price") * 10000)
      .otherwise((pl.col("bid_price") - pl.col("price")) / pl.col("bid_price") * 10000)
      .alias("slippage_bps"),
])

# Slippage statistics
slippage_stats = trades_with_slippage.select([
    pl.col("slippage_bps").mean().alias("mean_slippage_bps"),
    pl.col("slippage_bps").median().alias("median_slippage_bps"),
    pl.col("slippage_bps").quantile(0.95).alias("p95_slippage_bps"),
    pl.col("slippage_bps").max().alias("max_slippage_bps"),
])

print(slippage_stats)

# Slippage by trade size
slippage_by_size = trades_with_slippage.with_columns([
    pl.when(pl.col("qty") < 1.0)
      .then(pl.lit("small"))
      .when(pl.col("qty") < 10.0)
      .then(pl.lit("medium"))
      .otherwise(pl.lit("large"))
      .alias("size_category"),
]).group_by("size_category").agg([
    pl.col("slippage_bps").mean().alias("avg_slippage_bps"),
    pl.count().alias("num_trades"),
])

print(slippage_by_size)
```

### Fill Rate Analysis

For strategies with hypothetical orders, analyze fill probability.

```python
from pointline.research import query
import polars as pl

# Hypothetical scenario: limit orders at best bid/ask
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# For each quote, check if a limit buy at bid would have been filled
quotes_with_id = quotes.with_row_index("quote_id")

# Match each quote with future trades
trades_future = trades.select([
    pl.col("ts_local_us").alias("trade_ts"),
    pl.col("price").alias("trade_price"),
    pl.col("side").alias("trade_side"),
])

# For each quote, find next trade within 1 second
window_us = 1_000_000  # 1 second

quotes_with_next_trade = quotes_with_id.join_asof(
    trades_future,
    left_on="ts_local_us",
    right_on="trade_ts",
    strategy="forward",
).with_columns([
    (pl.col("trade_ts") - pl.col("ts_local_us")).alias("time_to_trade_us"),
])

# Check if limit buy at bid would have been filled
# (requires trade price <= bid_price and trade_side == sell)
quotes_with_fill = quotes_with_next_trade.with_columns([
    (
        (pl.col("time_to_trade_us") <= window_us) &
        (pl.col("trade_price") <= pl.col("bid_price")) &
        (pl.col("trade_side") == 1)  # Sell trade
    ).alias("buy_filled"),

    (
        (pl.col("time_to_trade_us") <= window_us) &
        (pl.col("trade_price") >= pl.col("ask_price")) &
        (pl.col("trade_side") == 0)  # Buy trade
    ).alias("sell_filled"),
])

# Fill rate statistics
fill_rate = quotes_with_fill.select([
    pl.col("buy_filled").mean().alias("buy_fill_rate"),
    pl.col("sell_filled").mean().alias("sell_fill_rate"),
])

print(fill_rate)
```

---

## Additional Patterns

For more advanced patterns, see:
- **Correlation analysis:** Cross-exchange arbitrage, lead-lag relationships
- **Volatility metrics:** Realized volatility, Garman-Klass estimator
- **Liquidity metrics:** Amihud illiquidity, Kyle's lambda
- **Market regime detection:** Volatility clustering, trend identification
