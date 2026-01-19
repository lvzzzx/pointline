#!/bin/bash
# Download derivative_ticker data for existing 5 symbols from 2024-09-01 to now

set -e

# Activate virtual environment
source .venv/bin/activate

# Configuration
EXCHANGE="binance-futures"
DATA_TYPE="derivative_ticker"
SYMBOLS="TRXUSDT,BTCUSDT,SOLUSDT,BNBUSDT,ETHUSDT"
FROM_DATE="2024-09-01"
TO_DATE="2026-01-19"  # Non-inclusive, so use tomorrow to include today
FILENAME_TEMPLATE="tardis/exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/{exchange}_{data_type}_{date}_{symbol}.{format}"

# Check if TARDIS_API_KEY is set
if [ -z "$TARDIS_API_KEY" ]; then
    echo "Error: TARDIS_API_KEY environment variable is not set"
    echo "Please set it with: export TARDIS_API_KEY=your_api_key"
    exit 1
fi

echo "=========================================="
echo "Downloading Tardis derivative_ticker data"
echo "=========================================="
echo "Exchange: $EXCHANGE"
echo "Data Type: $DATA_TYPE"
echo "Symbols: $SYMBOLS"
echo "Date Range: $FROM_DATE to $TO_DATE (non-inclusive)"
echo "Filename Template: $FILENAME_TEMPLATE"
echo ""

# Run the download command
pointline bronze download \
    --exchange "$EXCHANGE" \
    --data-types "$DATA_TYPE" \
    --symbols "$SYMBOLS" \
    --start-date "$FROM_DATE" \
    --end-date "$TO_DATE" \
    --format csv \
    --filename-template "$FILENAME_TEMPLATE" \
    --concurrency 5

echo ""
echo "Download complete!"
