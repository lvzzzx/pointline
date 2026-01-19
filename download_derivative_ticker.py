#!/usr/bin/env python3
"""Download derivative_ticker data for existing symbols from 2024-09-01 to now.

This script downloads Tardis derivative_ticker data for the 5 symbols that are
already present in the existing derivative_ticker table:
- TRXUSDT
- BTCUSDT
- SOLUSDT
- BNBUSDT
- ETHUSDT
"""

import os
from datetime import date, timedelta
from pathlib import Path

from pointline.config import LAKE_ROOT
from pointline.io.vendor.tardis import download_tardis_datasets


def main():
    # Configuration
    exchange = "binance-futures"
    data_types = ["derivative_ticker"]
    symbols = ["TRXUSDT", "BTCUSDT", "SOLUSDT", "BNBUSDT", "ETHUSDT"]
    from_date = "2024-09-01"
    
    # Calculate end date (tomorrow to include today, as to_date is non-inclusive)
    today = date.today()
    to_date = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    
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
    print("Downloading Tardis derivative_ticker data")
    print("=" * 60)
    print(f"Exchange: {exchange}")
    print(f"Data Type: {data_types[0]}")
    print(f"Symbols: {', '.join(symbols)}")
    print(f"Date Range: {from_date} to {to_date} (non-inclusive)")
    print(f"Download Directory: {LAKE_ROOT}")
    print(f"Filename Template: {filename_template}")
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
            concurrency=3,  # Reduced to be gentler on server
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
