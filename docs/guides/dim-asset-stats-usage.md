# dim_asset_stats Usage Guide

## Overview

The `dim_asset_stats` table tracks daily asset-level statistics (circulating supply, market cap, etc.) from CoinGecko API. This guide shows how to sync and backfill historical data.

## Commands

### 1. Sync Single Date

Sync asset stats for a specific date:

```bash
pointline silver dim-asset-stats sync --date 2024-01-15
```

**Options:**
- `--date YYYY-MM-DD`: Date to sync (required)
- `--base-assets BTC,ETH,SOL`: Optional comma-separated list of assets (default: all from dim_symbol)
- `--table-path PATH`: Custom table path (default: `silver/dim_asset_stats`)
- `--api-key KEY`: CoinGecko API key (optional, for higher rate limits)
- `--provider coingecko|coinmarketcap`: Data source provider (default: coingecko)

**Example:**
```bash
# Sync all assets for a specific date
pointline silver dim-asset-stats sync --date 2024-01-15

# Sync only specific assets
pointline silver dim-asset-stats sync --date 2024-01-15 --base-assets BTC,ETH,SOL
```

### 2. Backfill Historical Data (Date Range)

Download historical data for a date range:

```bash
pointline silver dim-asset-stats backfill --start-date 2024-01-01 --end-date 2024-12-31
```

**Options:**
- `--start-date YYYY-MM-DD`: Start date, inclusive (required)
- `--end-date YYYY-MM-DD`: End date, inclusive (required)
- `--base-assets BTC,ETH,SOL`: Optional comma-separated list of assets (default: all from dim_symbol)
- `--table-path PATH`: Custom table path (default: `silver/dim_asset_stats`)
- `--api-key KEY`: CoinGecko API key (optional, for higher rate limits)
- `--provider coingecko|coinmarketcap`: Data source provider (default: coingecko)

**Examples:**

```bash
# Backfill entire year 2024 for all assets
pointline silver dim-asset-stats backfill --start-date 2024-01-01 --end-date 2024-12-31

# Backfill last 30 days for specific assets
pointline silver dim-asset-stats backfill \
  --start-date 2024-12-01 \
  --end-date 2024-12-31 \
  --base-assets BTC,ETH,SOL,BNB

# Backfill with API key (for higher rate limits)
pointline silver dim-asset-stats backfill \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --api-key YOUR_COINGECKO_API_KEY
```

## Important Notes

### Rate Limiting

- **Free tier:** ~10 requests/minute (very conservative to avoid 429 errors)
- **With API key (Pro/Enterprise):** Much higher limits + access to chart endpoints
- The service automatically adds delays between requests (~7 seconds for free tier)

### Using Chart Endpoint (Recommended for Historical Backfills)

If you have a CoinGecko Pro/Enterprise API key, the service automatically uses the efficient **circulating_supply_chart** endpoint for historical backfills:

**Benefits:**
- ✅ One API call per asset (instead of one per day per asset)
- ✅ Backfill 1 year of data: **24 calls** (one per asset) vs **8,784 calls** (daily method)
- ✅ Completes in minutes instead of hours
- ✅ Avoids rate limit issues

**Example with API key:**
```bash
# This will use the efficient chart endpoint automatically
pointline silver dim-asset-stats backfill \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --api-key YOUR_COINGECKO_API_KEY
```

**Without API key:** Falls back to daily sync method (slower but works on free tier)

### Data Availability

- CoinGecko updates data approximately once per 24 hours
- Historical data may not be available for very old dates
- Some assets may not exist in CoinGecko (warnings will be logged)

### Asset Mapping

The service automatically maps `base_asset` (from `dim_symbol`) to CoinGecko `coin_id`:
- `BTC` → `bitcoin`
- `ETH` → `ethereum`
- `SOL` → `solana`
- etc.

If an asset doesn't have a mapping, it will be skipped with a warning.

### Idempotency

- Running the same command multiple times is safe
- The service uses MERGE operations on `(base_asset, date)` key
- Existing data for a date will be updated if CoinGecko data changed

## Example Workflows

### Initial Setup: Backfill Last Year

```bash
# Get data for the past year
pointline silver dim-asset-stats backfill \
  --start-date 2023-01-01 \
  --end-date 2023-12-31
```

### Daily Sync (Cron Job)

```bash
# Sync yesterday's data (run daily at 02:00 UTC)
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
pointline silver dim-asset-stats sync --date $YESTERDAY
```

### Selective Asset Backfill

```bash
# Backfill only major assets for a specific period
pointline silver dim-asset-stats backfill \
  --start-date 2024-05-01 \
  --end-date 2024-08-31 \
  --base-assets BTC,ETH,SOL,BNB,TRX,UNI
```

## Querying the Data

After syncing, you can query the data:

```python
import polars as pl
from pointline.config import get_table_path

# Read the table
df = pl.read_delta(str(get_table_path("dim_asset_stats")))

# Filter by date range
recent = df.filter(
    (pl.col("date") >= "2024-01-01") &
    (pl.col("date") <= "2024-12-31")
)

# Join with dim_symbol
symbols = pl.read_delta(str(get_table_path("dim_symbol")))
joined = symbols.join(
    df,
    left_on="base_asset",
    right_on="base_asset",
    how="left"
).filter(pl.col("is_current") == True)
```

## Troubleshooting

### "No CoinGecko mapping for base_asset"

- The asset doesn't have a mapping in `ASSET_TO_COINGECKO_MAP`
- Add the mapping to `pointline/config.py` or skip that asset

### Rate Limit Errors

- Use `--api-key` for higher rate limits
- Reduce the date range and run in smaller batches
- The service automatically retries with backoff

### Missing Historical Data

- CoinGecko may not have data for very old dates
- Some assets may not have been tracked historically
- Check CoinGecko's data availability for specific assets
