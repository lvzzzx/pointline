# Quickstart: 5 Minutes to Your First Analysis

Get started with Pointline's data lake in 5 minutes. This guide shows you how to discover, load, explore, and visualize market data.

---

## Prerequisites

```bash
# Install dependencies
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
```

---

## Step 1: Discover Available Data (1 minute)

Before loading data, find out what's available:

```python
from pointline import research

# What exchanges have data?
exchanges = research.list_exchanges(asset_class="crypto-derivatives")
print(exchanges)

# Output:
# ┌──────────────────┬─────────────┬───────────────────────┬────────────┐
# │ exchange         │ exchange_id │ asset_class           │ is_active  │
# ├──────────────────┼─────────────┼───────────────────────┼────────────┤
# │ binance-futures  │ 1           │ crypto-derivatives    │ true       │
# │ deribit          │ 2           │ crypto-derivatives    │ true       │
# │ bybit            │ 3           │ crypto-derivatives    │ true       │
# └──────────────────┴─────────────┴───────────────────────┴────────────┘
```

```python
# What symbols are available on Binance?
symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")
print(f"Found {symbols.height} BTC symbols")
print(symbols.head())

# Output:
# Found 5 BTC symbols
# ┌───────────┬─────────────────┬────────────┬─────────────┬─────────────┐
# │ symbol_id │ exchange_symbol │ base_asset │ quote_asset │ asset_type  │
# ├───────────┼─────────────────┼────────────┼─────────────┼─────────────┤
# │ 12345     │ BTCUSDT         │ BTC        │ USDT        │ perpetual   │
# │ 12346     │ BTCUSD_PERP     │ BTC        │ USD         │ perpetual   │
# └───────────┴─────────────────┴────────────┴─────────────┴─────────────┘
```

```python
# Check data coverage for BTCUSDT
coverage = research.data_coverage("binance-futures", "BTCUSDT")
print(f"Trades available: {coverage['trades']['available']}")
print(f"Quotes available: {coverage['quotes']['available']}")

# Output:
# Trades available: True
# Quotes available: True
```

**What you learned:** How to discover exchanges, symbols, and data availability.

---

## Step 2: Load Data (1 minute)

Load data using the **query API** - the simplest way to get market data:

```python
from pointline.research import query

# Load 1 day of trades (automatic symbol resolution)
trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,  # Returns human-readable float prices
)

print(f"Loaded {trades.height:,} trades")
print(trades.head(5))

# Output:
# Loaded 1,234,567 trades
# ┌──────────────────┬───────────┬──────────┬──────┬──────┐
# │ ts_local_us      │ symbol_id │ price    │ qty  │ side │
# ├──────────────────┼───────────┼──────────┼──────┼──────┤
# │ 1714521600000000 │ 12345     │ 67123.45 │ 0.12 │ 0    │
# │ 1714521600123456 │ 12345     │ 67123.50 │ 0.45 │ 1    │
# └──────────────────┴───────────┴──────────┴──────┴──────┘
```

**What just happened?**
- ✅ Automatically resolved "BTCUSDT" → symbol_id
- ✅ Converted ISO date strings → microsecond timestamps
- ✅ Decoded fixed-point integers → human-readable floats
- ✅ Used lazy evaluation for memory efficiency

**Timestamp flexibility:**
```python
# ISO strings (simplest)
trades = query.trades(..., start="2024-05-01", end="2024-05-02")

# Datetime objects
from datetime import datetime, timezone
trades = query.trades(
    ...,
    start=datetime(2024, 5, 1, tzinfo=timezone.utc),
    end=datetime(2024, 5, 2, tzinfo=timezone.utc),
)
```

---

## Step 3: Explore Data (2 minutes)

Analyze the data using Polars:

```python
import polars as pl

# View sample
print(trades.head(5))

# Summary statistics
print(trades.select(["price", "qty"]).describe())

# Output:
# ┌────────┬──────────┬──────────┐
# │ stat   │ price    │ qty      │
# ├────────┼──────────┼──────────┤
# │ mean   │ 67123.45 │ 0.234    │
# │ std    │ 234.56   │ 0.456    │
# │ min    │ 66500.00 │ 0.001    │
# │ max    │ 67800.00 │ 10.5     │
# └────────┴──────────┴──────────┘
```

```python
# Filter large trades
large_trades = trades.filter(pl.col("qty") > 1.0)
print(f"Large trades: {large_trades.height:,}")

# Calculate VWAP (Volume-Weighted Average Price)
vwap = trades.select([
    (pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()
]).item()
print(f"VWAP: ${vwap:.2f}")

# Output:
# Large trades: 12,345
# VWAP: $67,123.45
```

```python
# Group by side (buy/sell)
by_side = trades.group_by("side").agg([
    pl.col("qty").sum().alias("total_volume"),
    pl.col("price").mean().alias("avg_price"),
    pl.count().alias("trade_count"),
])
print(by_side)

# Output:
# ┌──────┬──────────────┬───────────┬─────────────┐
# │ side │ total_volume │ avg_price │ trade_count │
# ├──────┼──────────────┼───────────┼─────────────┤
# │ 0    │ 1234.56      │ 67120.12  │ 645,234     │
# │ 1    │ 1245.67      │ 67126.78  │ 589,333     │
# └──────┴──────────────┴───────────┴─────────────┘
```

**What you learned:** How to filter, aggregate, and calculate metrics.

---

## Step 4: Visualize (1 minute)

Create a quick plot:

```python
import matplotlib.pyplot as plt

# For plotting, we need to convert LazyFrame to DataFrame
if hasattr(trades, 'collect'):
    trades_df = trades.collect()
else:
    trades_df = trades

# Convert timestamps to datetime
trades_df = trades_df.with_columns(
    pl.from_epoch("ts_local_us", time_unit="us").alias("timestamp")
)

# Plot price over time
plt.figure(figsize=(12, 6))
plt.plot(trades_df["timestamp"], trades_df["price"], linewidth=0.5)
plt.xlabel("Time")
plt.ylabel("Price (USD)")
plt.title("BTC-USDT Price on 2024-05-01")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
```

**Advanced: Resample to 1-minute bars**
```python
# Aggregate to 1-minute OHLCV bars
bars = trades.group_by_dynamic(
    "ts_local_us",
    every="1m",
).agg([
    pl.col("price").first().alias("open"),
    pl.col("price").max().alias("high"),
    pl.col("price").min().alias("low"),
    pl.col("price").last().alias("close"),
    pl.col("qty").sum().alias("volume"),
])

print(f"1-minute bars: {bars.height:,}")

# Output:
# 1-minute bars: 1,440
```

---

## Next Steps

### 🎯 Continue Exploring (Query API)

You've been using the **query API** - perfect for exploration and prototyping:

```python
# Load quotes (bid/ask)
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Load order book snapshots
book = query.book_snapshot_25("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Load derivative funding data
funding = query.derivative_ticker("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02")
```

### 📚 Learn More

- **[Common Recipes](guides/researcher_guide.md#6-common-workflows)** - Copy-paste examples for common tasks
- **[Choosing an API](guides/choosing-an-api.md)** - When to use query API vs core API
- **[Research API Guide](research_api_guide.md)** - Complete API reference

### 🔬 Production Research (Core API)

For production workflows requiring reproducibility and explicit control, use the **core API**:

```python
from pointline import research, registry

# Explicit symbol resolution
symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]

# Load with explicit control
trades = research.load_trades(
    symbol_id=symbol_id,
    start_ts_us=1714521600000000,
    end_ts_us=1714608000000000,
)
```

**When to use core API:**
- Production research requiring reproducibility
- Explicit symbol_id control needed
- Performance-critical queries
- Handling SCD Type 2 symbol changes explicitly

See [Researcher's Guide](guides/researcher_guide.md) for details.

---

## Troubleshooting

### "No symbols found for exchange='binance-futures', symbol='BTCUSD'"

Use discovery API to search:
```python
symbols = research.list_symbols(search="BTC", exchange="binance-futures")
print(symbols)
```

### "No data found for date range"

Check coverage:
```python
coverage = research.data_coverage("binance-futures", "BTCUSDT")
print(coverage)
```

### "Memory error when loading large date ranges"

Use lazy evaluation and filter before collecting:
```python
# Load as LazyFrame (default)
trades_lf = query.trades(..., lazy=True)

# Filter while still lazy
large_trades = trades_lf.filter(pl.col("qty") > 1.0)

# Only materialize filtered data
result = large_trades.collect()
```

---

## Summary

**In 5 minutes you learned:**
1. ✅ How to discover available data
2. ✅ How to load data with the query API
3. ✅ How to explore and analyze data
4. ✅ How to visualize market data

**Key takeaway:** The query API makes it easy to explore data. Use it for 90% of your work. Switch to the core API only when you need explicit control for production research.

Happy researching! 🚀
