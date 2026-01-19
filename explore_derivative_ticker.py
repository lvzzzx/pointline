#!/usr/bin/env python3
"""Explore the derivative_ticker table in the Pointline data lake.

This script demonstrates how to:
- Query the derivative_ticker table using the research module
- Explore available data (exchanges, symbols, date ranges)
- Analyze funding rates, open interest, and price metrics
- Join with dim_symbol for symbol resolution
"""

from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
from pointline import research
from pointline.config import LAKE_ROOT, get_exchange_id, get_exchange_name


def print_section(title: str) -> None:
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def explore_table_overview() -> None:
    """Show basic overview of the derivative_ticker table."""
    print_section("Table Overview")
    
    print(f"\nTable path: {research.table_path('derivative_ticker')}")
    print(f"Lake root: {LAKE_ROOT}")
    
    # Check if table exists
    table_path = research.table_path("derivative_ticker")
    if not table_path.exists():
        print(f"\n⚠️  Table does not exist at {table_path}")
        print("   Make sure you have ingested derivative_ticker data first.")
        return
    
    print("\n✅ Table exists")
    
    # Schema information
    print("\n📋 Schema (from docs/schemas.md):")
    schema_info = [
        ("date", "date", "Partition column"),
        ("exchange", "string", "Partition column"),
        ("exchange_id", "i16", "Dictionary ID"),
        ("symbol_id", "i64", "Stable identifier from dim_symbol"),
        ("ts_local_us", "i64", "Primary replay timeline (arrival time)"),
        ("ts_exch_us", "i64", "Exchange timestamp"),
        ("ingest_seq", "i32", "Stable ordering within file"),
        ("mark_px", "f64", "Mark price (float for precision)"),
        ("index_px", "f64", "Index price (float for precision)"),
        ("last_px", "f64", "Last trade price (float for precision)"),
        ("funding_rate", "f64", "Funding rate (nullable)"),
        ("predicted_funding_rate", "f64", "Predicted funding rate (nullable)"),
        ("funding_ts_us", "i64", "Next funding event timestamp (nullable)"),
        ("open_interest", "f64", "Open interest in base asset units (nullable)"),
        ("file_id", "i32", "Lineage tracking"),
        ("file_line_number", "i32", "Lineage tracking"),
    ]
    
    for col, dtype, desc in schema_info:
        print(f"  • {col:20s} {dtype:8s} - {desc}")


def explore_available_data() -> None:
    """Explore what data is available in the table."""
    print_section("Available Data")
    
    try:
        # Get a sample to see what's available
        sample = research.scan_table(
            "derivative_ticker",
            columns=["date", "exchange", "exchange_id", "symbol_id"],
        ).limit(1000).collect()
        
        if sample.is_empty():
            print("\n⚠️  Table is empty - no data available")
            return
        
        print(f"\n📊 Sample size: {sample.height:,} rows (first 1000)")
        
        # Date range
        date_range = sample.select([
            pl.col("date").min().alias("min_date"),
            pl.col("date").max().alias("max_date"),
        ]).row(0)
        print(f"\n📅 Date range: {date_range[0]} to {date_range[1]}")
        
        # Exchanges
        exchanges = sample.select("exchange").unique().sort("exchange")
        print(f"\n🏦 Exchanges ({exchanges.height}):")
        for row in exchanges.iter_rows(named=True):
            print(f"  • {row['exchange']}")
        
        # Exchange IDs
        exchange_ids = sample.select("exchange_id").unique().sort("exchange_id")
        print(f"\n🔢 Exchange IDs ({exchange_ids.height}):")
        for row in exchange_ids.iter_rows(named=True):
            try:
                exchange_name = get_exchange_name(row['exchange_id'])
                print(f"  • {row['exchange_id']:2d} = {exchange_name}")
            except ValueError:
                print(f"  • {row['exchange_id']:2d} = (unknown)")
        
        # Symbol IDs
        symbol_ids = sample.select("symbol_id").unique()
        print(f"\n📈 Unique symbol_ids in sample: {symbol_ids.height:,}")
        
        # Get full statistics
        print("\n🔍 Getting full statistics (this may take a moment)...")
        full_stats = research.scan_table(
            "derivative_ticker",
            columns=["date", "exchange", "symbol_id"],
        ).select([
            pl.col("date").min().alias("min_date"),
            pl.col("date").max().alias("max_date"),
            pl.col("exchange").n_unique().alias("num_exchanges"),
            pl.col("symbol_id").n_unique().alias("num_symbols"),
            pl.len().alias("total_rows"),
        ]).collect()
        
        if not full_stats.is_empty():
            stats = full_stats.row(0)
            print(f"\n📊 Full Statistics:")
            print(f"  • Total rows: {stats[4]:,}")
            print(f"  • Date range: {stats[0]} to {stats[1]}")
            print(f"  • Unique exchanges: {stats[2]}")
            print(f"  • Unique symbols: {stats[3]:,}")
        
    except Exception as e:
        print(f"\n❌ Error exploring data: {e}")
        import traceback
        traceback.print_exc()


def explore_by_exchange(exchange: str = "binance-futures") -> None:
    """Explore data for a specific exchange."""
    print_section(f"Exchange: {exchange}")
    
    try:
        exchange_id = get_exchange_id(exchange)
        print(f"Exchange ID: {exchange_id}")
        
        # Get date range for this exchange
        df = research.scan_table(
            "derivative_ticker",
            exchange=exchange,
            columns=["date", "symbol_id", "ts_local_us"],
        ).select([
            pl.col("date").min().alias("min_date"),
            pl.col("date").max().alias("max_date"),
            pl.col("symbol_id").n_unique().alias("num_symbols"),
            pl.len().alias("total_rows"),
        ]).collect()
        
        if df.is_empty():
            print(f"\n⚠️  No data found for exchange: {exchange}")
            return
        
        stats = df.row(0)
        print(f"\n📊 Statistics:")
        print(f"  • Total rows: {stats[3]:,}")
        print(f"  • Date range: {stats[0]} to {stats[1]}")
        print(f"  • Unique symbols: {stats[2]:,}")
        
        # Get symbol breakdown
        symbol_counts = research.scan_table(
            "derivative_ticker",
            exchange=exchange,
            columns=["symbol_id"],
        ).group_by("symbol_id").agg([
            pl.len().alias("row_count"),
        ]).sort("row_count", descending=True).limit(10).collect()
        
        if not symbol_counts.is_empty():
            print(f"\n📈 Top 10 symbols by row count:")
            for row in symbol_counts.iter_rows(named=True):
                print(f"  • symbol_id {row['symbol_id']:6d}: {row['row_count']:,} rows")
        
    except ValueError as e:
        print(f"\n❌ Error: {e}")
    except Exception as e:
        print(f"\n❌ Error exploring exchange: {e}")
        import traceback
        traceback.print_exc()


def explore_funding_rates(exchange: str = "binance-futures", start_date: str = None) -> None:
    """Explore funding rate data."""
    print_section(f"Funding Rates - {exchange}")
    
    try:
        # Get funding rate data
        df = research.scan_table(
            "derivative_ticker",
            exchange=exchange,
            start_date=start_date,
            columns=[
                "date",
                "symbol_id",
                "ts_local_us",
                "funding_rate",
                "predicted_funding_rate",
                "funding_ts_us",
                "mark_px",
            ],
        ).filter(
            pl.col("funding_rate").is_not_null()
        ).collect()
        
        if df.is_empty():
            print(f"\n⚠️  No funding rate data found for {exchange}")
            return
        
        print(f"\n📊 Funding Rate Statistics:")
        print(f"  • Total rows with funding_rate: {df.height:,}")
        
        stats = df.select([
            pl.col("funding_rate").min().alias("min_funding"),
            pl.col("funding_rate").max().alias("max_funding"),
            pl.col("funding_rate").mean().alias("mean_funding"),
            pl.col("funding_rate").std().alias("std_funding"),
            pl.col("predicted_funding_rate").min().alias("min_predicted"),
            pl.col("predicted_funding_rate").max().alias("max_predicted"),
        ]).row(0)
        
        print(f"\n  Funding Rate:")
        print(f"    • Min:  {stats[0]:.6f}")
        print(f"    • Max:  {stats[1]:.6f}")
        print(f"    • Mean: {stats[2]:.6f}")
        print(f"    • Std:  {stats[3]:.6f}")
        
        if stats[4] is not None:
            print(f"\n  Predicted Funding Rate:")
            print(f"    • Min:  {stats[4]:.6f}")
            print(f"    • Max:  {stats[5]:.6f}")
        
        # Sample of recent funding rates
        recent = df.sort("ts_local_us", descending=True).limit(5)
        print(f"\n📅 Recent funding rates (last 5):")
        for row in recent.iter_rows(named=True):
            dt = datetime.fromtimestamp(row['ts_local_us'] / 1_000_000, tz=timezone.utc)
            print(f"  • {dt.strftime('%Y-%m-%d %H:%M:%S')} UTC - "
                  f"funding_rate: {row['funding_rate']:.6f}, "
                  f"mark_px: {row['mark_px']:.2f}")
        
    except Exception as e:
        print(f"\n❌ Error exploring funding rates: {e}")
        import traceback
        traceback.print_exc()


def explore_open_interest(exchange: str = "binance-futures", start_date: str = None) -> None:
    """Explore open interest data."""
    print_section(f"Open Interest - {exchange}")
    
    try:
        # Get open interest data
        df = research.scan_table(
            "derivative_ticker",
            exchange=exchange,
            start_date=start_date,
            columns=[
                "date",
                "symbol_id",
                "ts_local_us",
                "open_interest",
                "mark_px",
            ],
        ).filter(
            pl.col("open_interest").is_not_null()
        ).collect()
        
        if df.is_empty():
            print(f"\n⚠️  No open interest data found for {exchange}")
            return
        
        print(f"\n📊 Open Interest Statistics:")
        print(f"  • Total rows with open_interest: {df.height:,}")
        
        stats = df.select([
            pl.col("open_interest").min().alias("min_oi"),
            pl.col("open_interest").max().alias("max_oi"),
            pl.col("open_interest").mean().alias("mean_oi"),
            pl.col("open_interest").sum().alias("total_oi"),
        ]).row(0)
        
        print(f"\n  Open Interest (base asset units):")
        print(f"    • Min:  {stats[0]:,.2f}")
        print(f"    • Max:  {stats[1]:,.2f}")
        print(f"    • Mean: {stats[2]:,.2f}")
        print(f"    • Total: {stats[3]:,.2f}")
        
        # Open interest by symbol
        oi_by_symbol = df.group_by("symbol_id").agg([
            pl.col("open_interest").max().alias("max_oi"),
            pl.col("open_interest").mean().alias("mean_oi"),
        ]).sort("max_oi", descending=True).limit(10)
        
        print(f"\n📈 Top 10 symbols by max open interest:")
        for row in oi_by_symbol.iter_rows(named=True):
            print(f"  • symbol_id {row['symbol_id']:6d}: "
                  f"max OI: {row['max_oi']:,.2f}, "
                  f"mean OI: {row['mean_oi']:,.2f}")
        
    except Exception as e:
        print(f"\n❌ Error exploring open interest: {e}")
        import traceback
        traceback.print_exc()


def explore_price_metrics(exchange: str = "binance-futures", start_date: str = None) -> None:
    """Explore mark/index/last price metrics."""
    print_section(f"Price Metrics - {exchange}")
    
    try:
        # Get price data
        df = research.scan_table(
            "derivative_ticker",
            exchange=exchange,
            start_date=start_date,
            columns=[
                "date",
                "symbol_id",
                "ts_local_us",
                "mark_px",
                "index_px",
                "last_px",
            ],
        ).collect()
        
        if df.is_empty():
            print(f"\n⚠️  No price data found for {exchange}")
            return
        
        print(f"\n📊 Price Statistics:")
        print(f"  • Total rows: {df.height:,}")
        
        # Mark price stats
        mark_stats = df.filter(pl.col("mark_px").is_not_null()).select([
            pl.col("mark_px").min().alias("min"),
            pl.col("mark_px").max().alias("max"),
            pl.col("mark_px").mean().alias("mean"),
        ]).row(0)
        
        if mark_stats[0] is not None:
            print(f"\n  Mark Price:")
            print(f"    • Min:  ${mark_stats[0]:,.2f}")
            print(f"    • Max:  ${mark_stats[1]:,.2f}")
            print(f"    • Mean: ${mark_stats[2]:,.2f}")
        
        # Index price stats
        index_stats = df.filter(pl.col("index_px").is_not_null()).select([
            pl.col("index_px").min().alias("min"),
            pl.col("index_px").max().alias("max"),
            pl.col("index_px").mean().alias("mean"),
        ]).row(0)
        
        if index_stats[0] is not None:
            print(f"\n  Index Price:")
            print(f"    • Min:  ${index_stats[0]:,.2f}")
            print(f"    • Max:  ${index_stats[1]:,.2f}")
            print(f"    • Mean: ${index_stats[2]:,.2f}")
        
        # Last price stats
        last_stats = df.filter(pl.col("last_px").is_not_null()).select([
            pl.col("last_px").min().alias("min"),
            pl.col("last_px").max().alias("max"),
            pl.col("last_px").mean().alias("mean"),
        ]).row(0)
        
        if last_stats[0] is not None:
            print(f"\n  Last Price:")
            print(f"    • Min:  ${last_stats[0]:,.2f}")
            print(f"    • Max:  ${last_stats[1]:,.2f}")
            print(f"    • Mean: ${last_stats[2]:,.2f}")
        
        # Price spread (mark vs index)
        spread_df = df.filter(
            pl.col("mark_px").is_not_null() & pl.col("index_px").is_not_null()
        ).with_columns([
            (pl.col("mark_px") - pl.col("index_px")).alias("mark_index_spread"),
            ((pl.col("mark_px") - pl.col("index_px")) / pl.col("index_px") * 100).alias("spread_pct"),
        ])
        
        if not spread_df.is_empty():
            spread_stats = spread_df.select([
                pl.col("mark_index_spread").mean().alias("mean_spread"),
                pl.col("spread_pct").mean().alias("mean_spread_pct"),
            ]).row(0)
            
            print(f"\n  Mark-Index Spread:")
            print(f"    • Mean spread: ${spread_stats[0]:,.2f}")
            print(f"    • Mean spread %: {spread_stats[1]:.4f}%")
        
    except Exception as e:
        print(f"\n❌ Error exploring price metrics: {e}")
        import traceback
        traceback.print_exc()


def example_queries() -> None:
    """Show example queries for common use cases."""
    print_section("Example Queries")
    
    print("\n1️⃣  Query by exchange and date range:")
    print("""
    from pointline import research
    
    df = research.read_table(
        "derivative_ticker",
        exchange="binance-futures",
        start_date="2024-01-01",
        end_date="2024-01-31",
    )
    """)
    
    print("\n2️⃣  Query by symbol (requires symbol resolution):")
    print("""
    from pointline import research
    from pointline.config import get_exchange_id
    from datetime import datetime, timezone
    
    exchange_id = get_exchange_id("binance-futures")
    symbol = "BTCUSDT"
    
    # First resolve symbol_id
    dim_symbol = research.read_table("dim_symbol")
    symbol_id = (
        dim_symbol.filter(
            (pl.col("exchange_id") == exchange_id)
            & (pl.col("exchange_symbol") == symbol)
            & (pl.col("is_current") == True)
        )
        .select("symbol_id")
        .item()
    )
    
    # Then query derivative_ticker
    df = research.read_table(
        "derivative_ticker",
        symbol_id=symbol_id,
        start_date="2024-01-01",
    )
    """)
    
    print("\n3️⃣  Analyze funding rates over time:")
    print("""
    from pointline import research
    
    df = research.scan_table(
        "derivative_ticker",
        exchange="binance-futures",
        start_date="2024-01-01",
        columns=["date", "ts_local_us", "funding_rate", "symbol_id"],
    ).filter(
        pl.col("funding_rate").is_not_null()
    ).group_by("date").agg([
        pl.col("funding_rate").mean().alias("mean_funding"),
        pl.col("funding_rate").std().alias("std_funding"),
    ]).sort("date").collect()
    """)
    
    print("\n4️⃣  Track open interest changes:")
    print("""
    from pointline import research
    
    df = research.scan_table(
        "derivative_ticker",
        exchange="binance-futures",
        start_date="2024-01-01",
        columns=["date", "symbol_id", "open_interest", "ts_local_us"],
    ).filter(
        pl.col("open_interest").is_not_null()
    ).group_by(["date", "symbol_id"]).agg([
        pl.col("open_interest").max().alias("max_oi"),
        pl.col("open_interest").min().alias("min_oi"),
    ]).sort(["symbol_id", "date"]).collect()
    """)


def main() -> None:
    """Main exploration function."""
    print("\n" + "=" * 80)
    print("  DERIVATIVE_TICKER TABLE EXPLORATION")
    print("=" * 80)
    
    # Overview
    explore_table_overview()
    
    # Available data
    explore_available_data()
    
    # Exchange-specific exploration
    explore_by_exchange("binance-futures")
    
    # Funding rates
    explore_funding_rates("binance-futures")
    
    # Open interest
    explore_open_interest("binance-futures")
    
    # Price metrics
    explore_price_metrics("binance-futures")
    
    # Example queries
    example_queries()
    
    print("\n" + "=" * 80)
    print("  Exploration Complete")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
