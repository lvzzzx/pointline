#!/usr/bin/env python3
"""Download trades data for 5 symbols from 2024-05-01 to 2025-12-31.

This script downloads Tardis trades data for:
- BNBUSDT
- BTCUSDT
- ETHUSDT
- SOLUSDT
- TRXUSDT
"""

import os
from pathlib import Path

from pointline.config import LAKE_ROOT
from pointline.io.vendor.tardis import download_tardis_datasets


def main():
    # Configuration
    exchange = "binance-futures"
    data_types = ["trades"]
    symbols = ["BNBUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "TRXUSDT"]
    from_date = "2024-05-01"
    to_date = "2026-01-01"  # Non-inclusive, so use 2026-01-01 to include 2025-12-31
    
    filename_template = (
        "tardis/exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/"
        "{exchange}_{data_type}_{date}_{symbol}.{format}"
    )
    
    # Check API key
    api_key = os.getenv("TARDIS_API_KEY")
    if not api_key:
        print("Error: TARDIS_API_KEY environment variable is not set")
        print("Please set it with: export TARDIS_API_KEY=your_api_key")
        return 1
    
    print("=" * 60)
    print("Downloading Trades Data")
    print("=" * 60)
    print(f"Exchange: {exchange}")
    print(f"Data Type: {data_types[0]}")
    print(f"Symbols: {', '.join(symbols)}")
    print(f"Date Range: {from_date} to {to_date} (non-inclusive)")
    print(f"  (This covers: {from_date} to 2025-12-31)")
    print(f"Download Directory: {LAKE_ROOT}")
    print(f"Filename Template: {filename_template}")
    print()
    print("⚠️  Note: Trades files can be large. This may take a while...")
    print()
    
    try:
        download_tardis_datasets(
            exchange=exchange,
            data_types=data_types,
            symbols=symbols,
            from_date=from_date,
            to_date=to_date,
            filename_template=filename_template,
            download_dir=LAKE_ROOT,
            format="csv",
            api_key=api_key,
            concurrency=3,  # Moderate concurrency for large files
        )
        print()
        print("✅ Download complete!")
        return 0
    except ValueError as e:
        print(f"❌ Error: {e}")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    exit(main())
