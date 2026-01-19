#!/usr/bin/env python3
"""Quick query helper for derivative_ticker table.

Usage examples:
    # Query by exchange and date
    python query_derivative_ticker.py --exchange binance-futures --date 2024-08-31
    
    # Query specific symbol
    python query_derivative_ticker.py --exchange binance-futures --symbol BTCUSDT --date 2024-08-31
    
    # Show funding rates
    python query_derivative_ticker.py --exchange binance-futures --date 2024-08-31 --show-funding
    
    # Show open interest
    python query_derivative_ticker.py --exchange binance-futures --date 2024-08-31 --show-oi
"""

import argparse
from datetime import date

import polars as pl
from pointline import research
from pointline.config import get_exchange_id


def query_derivative_ticker(
    exchange: str,
    date_str: str | None = None,
    symbol: str | None = None,
    limit: int = 100,
    show_funding: bool = False,
    show_oi: bool = False,
) -> None:
    """Query and display derivative_ticker data."""
    
    print(f"\n🔍 Querying derivative_ticker for exchange: {exchange}")
    if date_str:
        print(f"   Date: {date_str}")
    if symbol:
        print(f"   Symbol: {symbol}")
    print()
    
    # Resolve symbol_id if symbol is provided
    symbol_id = None
    if symbol:
        exchange_id = get_exchange_id(exchange)
        dim_symbol = research.read_table("dim_symbol")
        symbol_match = dim_symbol.filter(
            (pl.col("exchange_id") == exchange_id)
            & (pl.col("exchange_symbol") == symbol)
            & (pl.col("is_current") == True)
        )
        
        if symbol_match.is_empty():
            print(f"❌ Symbol '{symbol}' not found for exchange '{exchange}'")
            return
        
        symbol_id = symbol_match.select("symbol_id").item()
        print(f"   Resolved symbol_id: {symbol_id}\n")
    
    # Build query
    columns = [
        "date",
        "symbol_id",
        "ts_local_us",
        "mark_px",
        "index_px",
        "last_px",
    ]
    
    if show_funding or show_oi:
        if show_funding:
            columns.extend(["funding_rate", "predicted_funding_rate", "funding_ts_us"])
        if show_oi:
            columns.append("open_interest")
    
    df = research.scan_table(
        "derivative_ticker",
        exchange=exchange,
        symbol_id=symbol_id,
        start_date=date_str,
        end_date=date_str,
        columns=columns,
    ).sort("ts_local_us", descending=True).limit(limit).collect()
    
    if df.is_empty():
        print("⚠️  No data found matching the criteria")
        return
    
    print(f"📊 Found {df.height:,} rows (showing first {limit})")
    print()
    
    # Display summary stats
    print("📈 Summary Statistics:")
    if "mark_px" in df.columns:
        mark_stats = df.filter(pl.col("mark_px").is_not_null()).select([
            pl.col("mark_px").min().alias("min"),
            pl.col("mark_px").max().alias("max"),
            pl.col("mark_px").mean().alias("mean"),
        ])
        if not mark_stats.is_empty():
            stats = mark_stats.row(0)
            if stats[0] is not None:
                print(f"   Mark Price: min=${stats[0]:,.2f}, max=${stats[1]:,.2f}, mean=${stats[2]:,.2f}")
    
    if show_funding and "funding_rate" in df.columns:
        funding_stats = df.filter(pl.col("funding_rate").is_not_null()).select([
            pl.col("funding_rate").min().alias("min"),
            pl.col("funding_rate").max().alias("max"),
            pl.col("funding_rate").mean().alias("mean"),
        ])
        if not funding_stats.is_empty():
            stats = funding_stats.row(0)
            if stats[0] is not None:
                print(f"   Funding Rate: min={stats[0]:.6f}, max={stats[1]:.6f}, mean={stats[2]:.6f}")
    
    if show_oi and "open_interest" in df.columns:
        oi_stats = df.filter(pl.col("open_interest").is_not_null()).select([
            pl.col("open_interest").min().alias("min"),
            pl.col("open_interest").max().alias("max"),
            pl.col("open_interest").mean().alias("mean"),
        ])
        if not oi_stats.is_empty():
            stats = oi_stats.row(0)
            if stats[0] is not None:
                print(f"   Open Interest: min={stats[0]:,.2f}, max={stats[1]:,.2f}, mean={stats[2]:,.2f}")
    
    print()
    print("📋 Sample Data (most recent first):")
    print(df.head(10))
    
    if df.height > 10:
        print(f"\n   ... and {df.height - 10:,} more rows")


def main():
    parser = argparse.ArgumentParser(
        description="Query the derivative_ticker table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--exchange",
        required=True,
        help="Exchange name (e.g., binance-futures)",
    )
    parser.add_argument(
        "--date",
        help="Date to query (YYYY-MM-DD format)",
    )
    parser.add_argument(
        "--symbol",
        help="Symbol to query (e.g., BTCUSDT)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of rows to return (default: 100)",
    )
    parser.add_argument(
        "--show-funding",
        action="store_true",
        help="Include funding rate columns",
    )
    parser.add_argument(
        "--show-oi",
        action="store_true",
        help="Include open interest column",
    )
    
    args = parser.parse_args()
    
    query_derivative_ticker(
        exchange=args.exchange,
        date_str=args.date,
        symbol=args.symbol,
        limit=args.limit,
        show_funding=args.show_funding,
        show_oi=args.show_oi,
    )


if __name__ == "__main__":
    main()
