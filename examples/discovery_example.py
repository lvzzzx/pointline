#!/usr/bin/env python
"""Example: Using the Discovery API to explore the data lake.

This script demonstrates how to discover available data without prior knowledge.
"""

from pointline import research

print("=" * 70)
print("POINTLINE DATA DISCOVERY EXAMPLE")
print("=" * 70)
print()

# Step 1: Discover what exchanges have data
print("Step 1: List available exchanges")
print("-" * 70)
exchanges = research.list_exchanges(asset_class="crypto-derivatives", include_stats=False)
print(f"Found {exchanges.height} crypto derivatives exchanges:\n")
print(exchanges.head(5))
print()

# Step 2: List symbols on a specific exchange
print("Step 2: Find BTC symbols on Binance Futures")
print("-" * 70)
symbols = research.list_symbols(
    exchange="binance-futures",
    base_asset="BTC",
    asset_type="perpetual",
)
if not symbols.is_empty():
    print(f"Found {symbols.height} BTC perpetuals:\n")
    print(symbols.select(["symbol_id", "exchange_symbol", "tick_size", "is_current"]).head(3))
else:
    print("No BTC perpetuals found (dim_symbol may be empty)")
print()

# Step 3: List available tables
print("Step 3: What tables are available?")
print("-" * 70)
tables = research.list_tables()
print(f"Found {tables.height} tables:\n")
print(tables.select(["table_name", "layer", "has_date_partition", "description"]))
print()

# Step 4: Check coverage for a specific symbol
print("Step 4: Check data coverage for BTCUSDT")
print("-" * 70)
coverage = research.data_coverage("binance-futures", "BTCUSDT")
print("Coverage by table:")
for table_name, info in coverage.items():
    status = "✓" if info["available"] else "✗"
    print(f"  {status} {table_name:<20} ", end="")
    if info["available"]:
        print(f"(symbol_id: {info['symbol_id']})")
    else:
        print(f"({info['reason']})")
print()

# Step 5: Rich summary (optional - requires symbol to exist)
print("Step 5: Get detailed symbol summary")
print("-" * 70)
try:
    research.summarize_symbol("BTCUSDT", exchange="binance-futures")
except Exception as e:
    print(f"Could not generate summary: {e}")
    print("(This is expected if dim_symbol is empty)")
print()

print("=" * 70)
print("NEXT STEPS:")
print("=" * 70)
print("Now that you know what data is available, you can load it:")
print()
print("  from pointline.research import query")
print()
print("  trades = query.trades(")
print('      exchange="binance-futures",')
print('      symbol="BTCUSDT",')
print('      start="2024-05-01",')
print('      end="2024-05-02",')
print("      decoded=True,")
print("  )")
print()
print("=" * 70)
