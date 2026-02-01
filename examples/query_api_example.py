#!/usr/bin/env python
"""Example: Complete Query API Workflow

This script demonstrates the recommended workflow for exploring and analyzing
market data using the query API. It covers the full pipeline:
  1. Discovery - Find available data
  2. Loading - Load data with automatic symbol resolution
  3. Analysis - Calculate metrics and features
  4. Visualization - Plot results

This is the recommended approach for 90% of research tasks.
For production research requiring explicit symbol_id control, see the core API.
"""

import polars as pl

from pointline import research
from pointline.research import query

# =============================================================================
# STEP 1: DISCOVER AVAILABLE DATA
# =============================================================================

print("=" * 80)
print("STEP 1: DISCOVER AVAILABLE DATA")
print("=" * 80)
print()

# What exchanges have data?
print("Available exchanges:")
exchanges = research.list_exchanges(asset_class="crypto-derivatives", include_stats=False)
print(exchanges.select(["exchange", "asset_class", "is_active"]).head(3))
print()

# What symbols are available on Binance?
print("BTC symbols on Binance Futures:")
symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")
if not symbols.is_empty():
    print(symbols.select(["symbol_id", "exchange_symbol", "tick_size"]).head(3))
else:
    print("No symbols found (dim_symbol may be empty)")
print()

# Check data coverage for BTCUSDT
print("Data coverage for BTCUSDT:")
coverage = research.data_coverage("binance-futures", "BTCUSDT")
for table_name, info in coverage.items():
    status = "✓" if info["available"] else "✗"
    print(f"  {status} {table_name}")
print()

# =============================================================================
# STEP 2: LOAD DATA (QUERY API - AUTOMATIC SYMBOL RESOLUTION)
# =============================================================================

print("=" * 80)
print("STEP 2: LOAD DATA WITH QUERY API")
print("=" * 80)
print()

# Define time range (multiple formats supported)
# Option 1: ISO strings (simplest)
start = "2024-05-01"
end = "2024-05-02"

# Option 2: datetime objects
# start = datetime(2024, 5, 1, tzinfo=timezone.utc)
# end = datetime(2024, 5, 2, tzinfo=timezone.utc)

# Option 3: Microsecond timestamps
# start = 1714521600000000
# end = 1714608000000000

print(f"Loading trades for BTCUSDT from {start} to {end}...")

try:
    # Load trades with automatic symbol resolution and decoding
    trades = query.trades(
        exchange="binance-futures",
        symbol="BTCUSDT",
        start=start,
        end=end,
        decoded=True,  # Returns human-readable float prices
        lazy=True,  # Returns LazyFrame for memory efficiency
    )

    # Collect to DataFrame (only do this after filtering/aggregating for large datasets)
    trades_df = trades.collect()
    print(f"✓ Loaded {trades_df.height:,} trades")
    print()
    print("Sample data:")
    print(trades_df.select(["ts_local_us", "price", "qty", "side"]).head(5))
    print()

except Exception as e:
    print(f"✗ Could not load trades: {e}")
    print("This is expected if you don't have data for this date range.")
    print("Try adjusting the date range or checking data_coverage() output.")
    print()
    print("Continuing with synthetic example data for demonstration...")
    # Create synthetic data for demonstration
    trades_df = pl.DataFrame(
        {
            "ts_local_us": range(1714521600000000, 1714521600000000 + 1000000, 1000),
            "price": [67000.0 + i * 0.1 for i in range(1000)],
            "qty": [0.1 + (i % 10) * 0.01 for i in range(1000)],
            "side": [i % 2 for i in range(1000)],
        }
    )
    print(f"✓ Created {trades_df.height:,} synthetic trades for demonstration")
    print()

# =============================================================================
# STEP 3: ANALYZE DATA
# =============================================================================

print("=" * 80)
print("STEP 3: ANALYZE DATA")
print("=" * 80)
print()

# Calculate VWAP (Volume-Weighted Average Price)
vwap = trades_df.select([(pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()]).item()
print(f"VWAP: ${vwap:,.2f}")

# Calculate basic statistics
stats = trades_df.select(
    [
        pl.col("price").min().alias("min_price"),
        pl.col("price").max().alias("max_price"),
        pl.col("price").mean().alias("avg_price"),
        pl.col("qty").sum().alias("total_volume"),
        pl.count().alias("trade_count"),
    ]
)
print("\nTrade Statistics:")
print(stats)
print()

# Group by side (buy vs sell)
by_side = trades_df.group_by("side").agg(
    [
        pl.col("qty").sum().alias("volume"),
        pl.col("price").mean().alias("avg_price"),
        pl.count().alias("count"),
    ]
)
print("By Side (0=buy, 1=sell):")
print(by_side)
print()

# =============================================================================
# STEP 4: ADVANCED ANALYSIS - MULTIPLE DATA SOURCES
# =============================================================================

print("=" * 80)
print("STEP 4: MULTI-TABLE ANALYSIS (OPTIONAL)")
print("=" * 80)
print()

print("Loading quotes and book snapshots...")
try:
    # Load quotes (bid/ask)
    quotes = query.quotes(
        exchange="binance-futures",
        symbol="BTCUSDT",
        start=start,
        end=end,
        decoded=True,
        lazy=True,
    ).collect()
    print(f"✓ Loaded {quotes.height:,} quotes")

    # Load order book snapshots
    book = query.book_snapshot_25(
        exchange="binance-futures",
        symbol="BTCUSDT",
        start=start,
        end=end,
        decoded=True,
        lazy=True,
    ).collect()
    print(f"✓ Loaded {book.height:,} book snapshots")
    print()

    # Join trades with quotes (as-of join for point-in-time correctness)
    print("Performing as-of join (trades + quotes)...")
    trades_sorted = trades_df.sort("ts_local_us")
    quotes_sorted = quotes.sort("ts_local_us")

    pit_data = trades_sorted.join_asof(
        quotes_sorted,
        on="ts_local_us",
        by=["exchange_id", "symbol_id"],
        strategy="backward",  # Get last quote known BEFORE or AT trade time
    )
    print(f"✓ Joined {pit_data.height:,} rows")
    print("\nSample joined data:")
    print(pit_data.select(["ts_local_us", "price", "bid_px", "ask_px"]).head(3))
    print()

except Exception as e:
    print(f"✗ Could not load additional data: {e}")
    print("Skipping multi-table analysis.")
    print()

# =============================================================================
# STEP 5: AGGREGATION - CREATE BARS
# =============================================================================

print("=" * 80)
print("STEP 5: AGGREGATE TO BARS")
print("=" * 80)
print()

# Create 1-minute OHLCV bars
print("Creating 1-minute OHLCV bars...")
bars = (
    trades_df.sort("ts_local_us")
    .group_by_dynamic(
        "ts_local_us",
        every="1m",
    )
    .agg(
        [
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("qty").sum().alias("volume"),
            pl.count().alias("trade_count"),
        ]
    )
)

print(f"✓ Created {bars.height:,} 1-minute bars")
print("\nSample bars:")
print(bars.head(5))
print()

# =============================================================================
# STEP 6: VISUALIZATION (OPTIONAL)
# =============================================================================

print("=" * 80)
print("STEP 6: VISUALIZATION")
print("=" * 80)
print()

print("To visualize the data, you can use matplotlib:")
print()
print("  import matplotlib.pyplot as plt")
print()
print("  # Convert timestamps to datetime")
print("  trades_df = trades_df.with_columns(")
print('      pl.from_epoch("ts_local_us", time_unit="us").alias("timestamp")')
print("  )")
print()
print("  # Plot price over time")
print("  plt.figure(figsize=(12, 6))")
print('  plt.plot(trades_df["timestamp"], trades_df["price"], linewidth=0.5)')
print('  plt.xlabel("Time")')
print('  plt.ylabel("Price (USD)")')
print('  plt.title("BTC-USDT Price")')
print("  plt.grid(True, alpha=0.3)")
print("  plt.show()")
print()

# Uncomment to actually plot (requires matplotlib)
# import matplotlib.pyplot as plt
# trades_plot = trades_df.with_columns(
#     pl.from_epoch("ts_local_us", time_unit="us").alias("timestamp")
# )
# plt.figure(figsize=(12, 6))
# plt.plot(trades_plot["timestamp"], trades_plot["price"], linewidth=0.5)
# plt.xlabel("Time")
# plt.ylabel("Price (USD)")
# plt.title("BTC-USDT Price")
# plt.grid(True, alpha=0.3)
# plt.show()

# =============================================================================
# SUMMARY AND NEXT STEPS
# =============================================================================

print("=" * 80)
print("SUMMARY")
print("=" * 80)
print()
print("✓ Discovered available exchanges and symbols")
print("✓ Loaded data with query API (automatic symbol resolution)")
print("✓ Calculated VWAP and statistics")
print("✓ Aggregated trades to 1-minute bars")
print()
print("=" * 80)
print("NEXT STEPS")
print("=" * 80)
print()
print("1. Explore more data sources:")
print("   - query.quotes() - bid/ask data")
print("   - query.book_snapshot_25() - order book snapshots")
print("   - query.derivative_ticker() - funding rates, open interest")
print()
print("2. Try lazy evaluation for large datasets:")
print("   - Use lazy=True (default) to return LazyFrame")
print("   - Filter and aggregate before calling .collect()")
print()
print("3. For production research:")
print("   - See docs/guides/researcher_guide.md Section 7")
print("   - Use core API with explicit symbol_id control")
print("   - Log symbol_ids for reproducibility")
print()
print("4. Learn more:")
print("   - Quickstart: docs/quickstart.md")
print("   - Choosing an API: docs/guides/choosing-an-api.md")
print("   - Full guide: docs/guides/researcher_guide.md")
print()
print("=" * 80)
