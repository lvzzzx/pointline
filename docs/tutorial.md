# Tutorial: Your First Analysis (30 Minutes)

This hands-on tutorial walks you through a complete workflow from setup to results. You'll discover data, load trades, perform analysis, and create visualizations.

**Time:** ~30 minutes
**Prerequisites:** Python 3.10+, basic Python knowledge

---

## 📋 What You'll Learn

By the end of this tutorial, you'll be able to:
- ✅ Set up your Pointline environment
- ✅ Discover available data
- ✅ Load and explore market data
- ✅ Perform quantitative analysis (VWAP, aggregations)
- ✅ Create visualizations
- ✅ Understand when to use Query API vs Core API

---

## Step 1: Setup Your Environment (5 minutes)

### Install Dependencies

```bash
# Navigate to the pointline directory
cd /path/to/pointline

# Create and activate virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install pointline in development mode
uv pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install

# Verify installation
pointline --help
python -c "from pointline import research; print('Success!')"
```

### Configure Lake Root

Pointline needs to know where your data lake is located:

```bash
# Option 1: Use CLI (recommended)
pointline config set --lake-root ~/data/lake

# Option 2: Environment variable
export LAKE_ROOT=~/data/lake

# Verify configuration
pointline config show
```

**Expected output:**
```
Lake root: /Users/you/data/lake
Bronze root: /Users/you/data/lake/bronze
```

---

## Step 2: Understand the Data Lake Structure (5 minutes)

Pointline uses a **Bronze → Silver → Gold** architecture:

```
~/data/lake/
├── bronze/              # Raw vendor data (immutable)
│   └── tardis/
│       └── exchange=binance-futures/
│           └── type=trades/
│               └── date=2024-05-01/
│                   └── symbol=BTCUSDT/
│                       └── BTCUSDT.csv.gz
├── silver/              # Canonical research tables (Delta Lake)
│   ├── trades/
│   ├── quotes/
│   ├── book_snapshot_25/
│   └── dim_symbol/      # Symbol master table
└── gold/                # Derived tables (optional)
```

**Key concepts:**
- **Bronze:** Raw files exactly as downloaded from vendors
- **Silver:** Cleaned, validated, research-ready tables (you'll use these!)
- **Gold:** Pre-computed aggregations (advanced, optional)

---

## Step 3: Discover Available Data (5 minutes)

Before loading data, let's explore what's available using the **Discovery API**.

### What Exchanges Have Data?

```python
from pointline import research

# List all exchanges
exchanges = research.list_exchanges()
print(exchanges)

# Filter by asset class
crypto_exchanges = research.list_exchanges(asset_class="crypto-derivatives")
print(f"\nFound {crypto_exchanges.height} crypto derivatives exchanges")
print(crypto_exchanges.select(["exchange", "exchange_id", "is_active"]))
```

**Expected output:**
```
┌──────────────────┬─────────────┬───────────┐
│ exchange         │ exchange_id │ is_active │
├──────────────────┼─────────────┼───────────┤
│ binance-futures  │ 1           │ true      │
│ deribit          │ 2           │ true      │
│ bybit            │ 3           │ true      │
└──────────────────┴─────────────┴───────────┘
```

### What Symbols Are Available?

```python
# Find BTC symbols on Binance Futures
btc_symbols = research.list_symbols(
    exchange="binance-futures",
    base_asset="BTC"
)

print(f"Found {btc_symbols.height} BTC symbols")
print(btc_symbols.select([
    "symbol_id", "exchange_symbol", "tick_size", "asset_type"
]).head())
```

**Expected output:**
```
┌───────────┬─────────────────┬───────────┬────────────┐
│ symbol_id │ exchange_symbol │ tick_size │ asset_type │
├───────────┼─────────────────┼───────────┼────────────┤
│ 12345     │ BTCUSDT         │ 0.1       │ perpetual  │
└───────────┴─────────────────┴───────────┴────────────┘
```

### Check Data Coverage

```python
# Check what data exists for BTCUSDT
coverage = research.data_coverage("binance-futures", "BTCUSDT")

print("Data availability for BTCUSDT:")
for table_name, info in coverage.items():
    status = "✓" if info["available"] else "✗"
    print(f"  {status} {table_name}")
```

**Expected output:**
```
Data availability for BTCUSDT:
  ✓ trades
  ✓ quotes
  ✓ book_snapshot_25
```

**💡 Pro Tip:** Always use `data_coverage()` before loading data to avoid errors!

---

## Step 4: Load Your First Dataset (5 minutes)

Now let's load some actual market data using the **Query API**.

### Load Trades Data

```python
from pointline.research import query
import polars as pl

# Load 1 day of trades
trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,  # Get human-readable float prices
    lazy=True,     # Don't load all data into memory yet
)

# Collect to DataFrame
trades_df = trades.collect()

print(f"Loaded {trades_df.height:,} trades")
print("\nFirst 5 trades:")
print(trades_df.select(["ts_local_us", "price", "qty", "side"]).head())
```

**Expected output:**
```
Loaded 1,234,567 trades

First 5 trades:
┌──────────────────┬──────────┬──────┬──────┐
│ ts_local_us      │ price    │ qty  │ side │
├──────────────────┼──────────┼──────┼──────┤
│ 1714521600000000 │ 67123.4  │ 0.12 │ 0    │
│ 1714521600123456 │ 67123.5  │ 0.45 │ 1    │
│ 1714521600234567 │ 67123.3  │ 0.23 │ 0    │
└──────────────────┴──────────┴──────┴──────┘
```

**Understanding the columns:**
- `ts_local_us`: Arrival time in microseconds (UTC)
- `price`: Trade price (decoded from fixed-point)
- `qty`: Trade quantity
- `side`: 0 = buy, 1 = sell

---

## Step 5: Perform Analysis (5 minutes)

Let's calculate some basic metrics using Polars.

### Calculate VWAP (Volume-Weighted Average Price)

```python
# VWAP = sum(price × quantity) / sum(quantity)
vwap = trades_df.select([
    (pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()
]).item()

print(f"VWAP: ${vwap:,.2f}")
```

### Basic Statistics

```python
stats = trades_df.select([
    pl.col("price").min().alias("min_price"),
    pl.col("price").max().alias("max_price"),
    pl.col("price").mean().alias("avg_price"),
    pl.col("price").std().alias("price_std"),
    pl.col("qty").sum().alias("total_volume"),
    pl.len().alias("trade_count"),
])

print("\nTrade Statistics:")
print(stats)
```

**Expected output:**
```
Trade Statistics:
┌───────────┬───────────┬───────────┬───────────┬──────────────┬─────────────┐
│ min_price │ max_price │ avg_price │ price_std │ total_volume │ trade_count │
├───────────┼───────────┼───────────┼───────────┼──────────────┼─────────────┤
│ 66500.0   │ 67800.0   │ 67123.4   │ 234.5     │ 12345.67     │ 1234567     │
└───────────┴───────────┴───────────┴───────────┴──────────────┴─────────────┘
```

### Group by Buy/Sell

```python
by_side = trades_df.group_by("side").agg([
    pl.col("qty").sum().alias("volume"),
    pl.col("price").mean().alias("avg_price"),
    pl.len().alias("count"),
])

print("\nBy Side (0=buy, 1=sell):")
print(by_side)
```

---

## Step 6: Create Aggregations (5 minutes)

Let's create 1-minute OHLCV bars from tick data.

```python
# Add datetime column for grouping
trades_with_dt = trades_df.with_columns(
    pl.from_epoch("ts_local_us", time_unit="us").alias("ts_dt")
)

# Aggregate to 1-minute bars
bars = (
    trades_with_dt
    .sort("ts_dt")
    .group_by_dynamic("ts_dt", every="1m")
    .agg([
        pl.col("price").first().alias("open"),
        pl.col("price").max().alias("high"),
        pl.col("price").min().alias("low"),
        pl.col("price").last().alias("close"),
        pl.col("qty").sum().alias("volume"),
        pl.len().alias("trade_count"),
    ])
)

print(f"Created {bars.height:,} 1-minute bars")
print("\nFirst 5 bars:")
print(bars.head())
```

**Expected output:**
```
Created 1,440 1-minute bars

First 5 bars:
┌─────────────────────┬──────────┬──────────┬──────────┬──────────┬────────┬─────────────┐
│ ts_dt               │ open     │ high     │ low      │ close    │ volume │ trade_count │
├─────────────────────┼──────────┼──────────┼──────────┼──────────┼────────┼─────────────┤
│ 2024-05-01 00:00:00 │ 67123.4  │ 67145.2  │ 67110.3  │ 67132.1  │ 45.23  │ 856         │
│ 2024-05-01 00:01:00 │ 67132.1  │ 67150.0  │ 67125.0  │ 67148.5  │ 52.34  │ 923         │
└─────────────────────┴──────────┴──────────┴──────────┴──────────┴────────┴─────────────┘
```

---

## Step 7: Work with Multiple Data Sources (Optional)

Let's combine trades with quotes to calculate effective spread.

### Load Quotes

```python
quotes = query.quotes(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,
    lazy=True,
).collect()

print(f"Loaded {quotes.height:,} quotes")
print(quotes.select(["ts_local_us", "bid_price", "ask_price"]).head())
```

### Join Trades with Quotes (As-Of Join)

```python
# Sort both DataFrames
trades_sorted = trades_df.sort("ts_local_us")
quotes_sorted = quotes.sort("ts_local_us")

# Point-in-time join: match each trade with the last known quote
trades_with_quotes = trades_sorted.join_asof(
    quotes_sorted,
    on="ts_local_us",
    by="symbol_id",
    strategy="backward",  # Get last quote BEFORE or AT trade time
)

print(f"Joined {trades_with_quotes.height:,} trades with quotes")

# Calculate effective spread
trades_with_quotes = trades_with_quotes.with_columns([
    (pl.col("ask_price") - pl.col("bid_price")).alias("spread"),
    ((pl.col("bid_price") + pl.col("ask_price")) / 2).alias("mid_price"),
])

print("\nSample with spreads:")
print(trades_with_quotes.select([
    "ts_local_us", "price", "bid_price", "ask_price", "spread"
]).head())
```

---

## Step 8: Visualization (Optional)

Create a simple price chart using matplotlib.

```python
import matplotlib.pyplot as plt

# Use the 1-minute bars we created earlier
bars_df = bars.with_columns(
    pl.col("ts_dt").cast(pl.Datetime)
)

# Plot candlestick (simplified as line chart)
plt.figure(figsize=(14, 7))
plt.plot(bars_df["ts_dt"], bars_df["close"], linewidth=1, label="Close")
plt.fill_between(
    bars_df["ts_dt"],
    bars_df["low"],
    bars_df["high"],
    alpha=0.3,
    label="High-Low Range"
)
plt.xlabel("Time")
plt.ylabel("Price (USD)")
plt.title("BTC-USDT Price on 2024-05-01 (1-minute bars)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("btc_price.png", dpi=150)
print("Chart saved to btc_price.png")
```

---

## Step 9: Understanding APIs (Conceptual)

You've been using the **Query API** - perfect for exploration!

### Query API (What You've Been Using)

```python
# Simple, automatic symbol resolution
from pointline.research import query

trades = query.trades(
    "binance-futures", "BTCUSDT",
    "2024-05-01", "2024-05-02",
    decoded=True
)
```

**When to use:**
- ✅ Exploration and prototyping
- ✅ Jupyter notebooks
- ✅ Quick analysis
- ✅ **90% of your work!**

### Core API (For Production Research)

```python
# Explicit control, reproducible
from pointline import research, registry

# Step 1: Resolve symbol_id explicitly
symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]

# Step 2: Load with explicit symbol_id
trades = research.load_trades(
    symbol_id=symbol_id,
    start_ts_us=1714521600000000,
    end_ts_us=1714608000000000,
)

# Step 3: Decode manually if needed
from pointline.tables.trades import decode_fixed_point
from pointline.dim_symbol import read_dim_symbol_table
dim_symbol = read_dim_symbol_table()
trades = decode_fixed_point(trades, dim_symbol)
```

**When to use:**
- ✅ Production research requiring reproducibility
- ✅ Need to log exact symbol_ids
- ✅ Handling symbol metadata changes explicitly

See [Choosing an API](guides/choosing-an-api.md) for detailed comparison.

---

## Step 10: Best Practices (Key Takeaways)

### 1. Always Start with Discovery

```python
# Before loading data
coverage = research.data_coverage("binance-futures", "BTCUSDT")
```

### 2. Use Lazy Evaluation for Large Datasets

```python
# Load as LazyFrame
trades_lf = query.trades(..., lazy=True)

# Filter and aggregate before collecting
result = trades_lf.filter(...).group_by(...).agg(...).collect()

# NOT this:
trades_df = trades_lf.collect()  # Loads everything!
result = trades_df.filter(...)   # Too late
```

### 3. Use decoded=True for Human Interaction

```python
# Always decode for analysis
trades = query.trades(..., decoded=True)
```

### 4. Join with join_asof for Point-in-Time Correctness

```python
# Use backward strategy for as-of joins
trades.join_asof(quotes, on="ts_local_us", strategy="backward")
```

---

## 🎉 Congratulations!

You've completed your first Pointline analysis! You now know how to:

- ✅ Set up and configure Pointline
- ✅ Discover available data
- ✅ Load and analyze market data
- ✅ Create aggregations (OHLCV bars)
- ✅ Join multiple data sources
- ✅ Use the Query API effectively

---

## Next Steps

### Continue Learning

1. **[Common Recipes](guides/researcher-guide.md#6-common-workflows)** - Copy-paste examples for common tasks
2. **[Choosing an API](guides/choosing-an-api.md)** - When to use Query API vs Core API
3. **[API Reference](reference/api-reference.md)** - Complete function documentation

### Explore More Data Sources

```python
# Load order book snapshots
book = query.book_snapshot_25(
    "binance-futures", "BTCUSDT",
    "2024-05-01", "2024-05-02",
    decoded=True
)

# Load derivative ticker (funding, OI)
ticker = query.derivative_ticker(
    "binance-futures", "BTCUSDT",
    "2024-05-01", "2024-05-02"
)
```

### Try Advanced Analysis

```python
# Multi-symbol analysis
btc_trades = query.trades("binance-futures", "BTCUSDT", ...)
eth_trades = query.trades("binance-futures", "ETHUSDT", ...)

# Cross-symbol correlation
# Lead-lag analysis
# Pair trading signals
```

---

## 🆘 Troubleshooting

**Ran into issues?** See the [Troubleshooting Guide](troubleshooting.md) for solutions to common problems.

**Common issues during tutorial:**
- "Lake root not found" → Run `pointline config set --lake-root ~/data/lake`
- "Symbol not found" → Use `research.list_symbols()` to find correct symbol name
- "No data found" → Check `research.data_coverage()` to verify data exists

---

## 📚 Additional Resources

- **[Quickstart](quickstart.md)** - 5-minute overview
- **[Researcher's Guide](guides/researcher-guide.md)** - Comprehensive reference
- **[Examples](../examples/)** - More code examples
  - [discovery_example.py](../examples/discovery_example.py)
  - [query_api_example.py](../examples/query_api_example.py)

Happy researching! 🚀
