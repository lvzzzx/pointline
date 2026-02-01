# CLI Reference

Complete command-line interface reference for Pointline.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Configuration](#configuration)
- [Data Discovery](#data-discovery)
- [Symbol Management](#symbol-management)
- [Bronze Layer](#bronze-layer)
- [Ingestion (Silver Layer)](#ingestion-silver-layer)
- [Manifest Management](#manifest-management)
- [Data Quality](#data-quality)
- [Validation](#validation)
- [Delta Lake Maintenance](#delta-lake-maintenance)
- [Stock Data (Chinese Markets)](#stock-data-chinese-markets)

---

## Overview

The `pointline` CLI provides tools for managing the data lake, from bronze layer organization to silver layer ingestion and maintenance.

**General usage:**
```bash
pointline <command> <subcommand> [options]
```

**Get help:**
```bash
pointline --help
pointline <command> --help
pointline <command> <subcommand> --help
```

---

## Configuration

### `pointline config show`

Display current configuration (lake_root, bronze_root, etc.).

**Usage:**
```bash
pointline config show
```

**Example output:**
```
Lake root: /Users/you/data/lake
Bronze root: /Users/you/data/lake/bronze
Table paths:
  dim_symbol: /Users/you/data/lake/silver/dim_symbol
  trades: /Users/you/data/lake/silver/trades
  ...
```

**Use this to:**
- Verify your configuration
- Troubleshoot path issues
- Check which lake you're working with

---

### `pointline config set`

Set configuration values persistently.

**Usage:**
```bash
pointline config set --lake-root <path>
```

**Options:**
- `--lake-root`: Path to the data lake root directory (required)

**Example:**
```bash
# Set lake root
pointline config set --lake-root ~/data/lake

# Verify
pointline config show
```

**Creates:** `~/.config/pointline/config.toml`

---

## Data Discovery

### `pointline data list-exchanges` (planned)

List all exchanges with data in the lake.

**Planned usage:**
```bash
pointline data list-exchanges [--asset-class <class>]
```

**Python equivalent (use this for now):**
```python
from pointline import research
exchanges = research.list_exchanges(asset_class="crypto-derivatives")
print(exchanges)
```

---

### `pointline data list-symbols` (planned)

List symbols available on an exchange.

**Planned usage:**
```bash
pointline data list-symbols --exchange <name> [--base-asset <asset>]
```

**Python equivalent (use this for now):**
```python
from pointline import research
symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")
print(symbols)
```

---

### `pointline data coverage` (planned)

Check data coverage for a specific symbol.

**Planned usage:**
```bash
pointline data coverage --exchange <name> --symbol <symbol>
```

**Python equivalent (use this for now):**
```python
from pointline import research
coverage = research.data_coverage("binance-futures", "BTCUSDT")
print(coverage)
```

---

## Symbol Management

### `pointline symbol search`

Search for symbols in dim_symbol.

**Usage:**
```bash
pointline symbol search [query] [options]
```

**Options:**
- `query`: Fuzzy search term (optional)
- `--exchange`: Filter by exchange
- `--base-asset`: Filter by base asset
- `--quote-asset`: Filter by quote asset

**Examples:**
```bash
# Find all symbols
pointline symbol search

# Search for BTC symbols
pointline symbol search BTC

# Search on specific exchange
pointline symbol search --exchange binance-futures

# Search for BTC perpetuals on Binance
pointline symbol search BTC --exchange binance-futures
```

---

### `pointline symbol sync`

Sync dim_symbol from Tardis API or a file.

**Usage:**
```bash
pointline symbol sync --source <api|file> [options]
```

**Options:**
- `--source`: Metadata source (`api` for Tardis API, or file path)
- `--exchange`: Exchange name (required for API source)
- `--symbol`: Instrument symbol/ID (optional, for API source)
- `--filter`: JSON filter payload for Tardis API
- `--api-key`: Tardis API key (or set `TARDIS_API_KEY` env var)
- `--effective-ts`: Unix timestamp in microseconds (default: now)
- `--table-path`: Path to dim_symbol table (default: auto-detected)
- `--rebuild`: Perform full history rebuild

**Examples:**

**Sync from Tardis API:**
```bash
# Sync all symbols for an exchange
pointline symbol sync \
  --source api \
  --exchange binance-futures \
  --api-key YOUR_TARDIS_API_KEY

# Sync specific symbol
pointline symbol sync \
  --source api \
  --exchange binance-futures \
  --symbol BTCUSDT \
  --api-key YOUR_TARDIS_API_KEY
```

**Sync from CSV file:**
```bash
pointline symbol sync --source ./symbols.csv
```

**Full rebuild:**
```bash
pointline symbol sync \
  --source api \
  --exchange binance-futures \
  --api-key YOUR_KEY \
  --rebuild
```

---

### `pointline symbol sync-tushare`

Sync Chinese stock symbols from Tushare API to dim_symbol.

**Usage:**
```bash
pointline symbol sync-tushare --exchange <szse|sse|all> [options]
```

**Options:**
- `--exchange`: Exchange to sync (szse=Shenzhen, sse=Shanghai, all=both) (required)
- `--include-delisted`: Include delisted stocks
- `--token`: Tushare API token (or set `TUSHARE_TOKEN` env var)
- `--table-path`: Path to dim_symbol table

**Example:**
```bash
# Sync SZSE stocks
pointline symbol sync-tushare \
  --exchange szse \
  --token YOUR_TUSHARE_TOKEN

# Sync all exchanges including delisted
pointline symbol sync-tushare \
  --exchange all \
  --include-delisted \
  --token YOUR_TOKEN
```

---

### `pointline symbol sync-from-stock-basic-cn`

Sync dim_symbol from silver.stock_basic_cn snapshot.

**Usage:**
```bash
pointline symbol sync-from-stock-basic-cn [options]
```

**Options:**
- `--stock-basic-path`: Path to stock_basic_cn table (default: auto-detected)
- `--table-path`: Path to dim_symbol table (default: auto-detected)
- `--rebuild`: Perform full history rebuild

**Example:**
```bash
pointline symbol sync-from-stock-basic-cn
```

---

## Bronze Layer

### `pointline bronze download`

Download datasets from Tardis to bronze layer.

**Usage:**
```bash
pointline bronze download \
  --exchange <name> \
  --data-types <types> \
  --symbols <symbols> \
  --start-date <date> \
  --end-date <date> \
  --filename-template <template> \
  [options]
```

**Options:**
- `--exchange`: Exchange name (e.g., binance-futures) (required)
- `--data-types`: Comma-separated data types (e.g., trades,quotes,book_snapshot_25) (required)
- `--symbols`: Comma-separated symbols (e.g., BTCUSDT,ETHUSDT) (required)
- `--start-date`: Start date YYYY-MM-DD (inclusive) (required)
- `--end-date`: End date YYYY-MM-DD (exclusive) (required)
- `--filename-template`: Template with `{exchange},{data_type},{date},{symbol},{format}` (required)
- `--format`: Dataset format (default: csv)
- `--download-dir`: Root directory for downloads (default: LAKE_ROOT/bronze/tardis)
- `--api-key`: Tardis API key (or set `TARDIS_API_KEY`)
- `--concurrency`: Number of concurrent downloads (default: 5)
- `--http-proxy`: HTTP proxy URL (optional)

**Example:**
```bash
pointline bronze download \
  --exchange binance-futures \
  --data-types trades,quotes \
  --symbols BTCUSDT,ETHUSDT \
  --start-date 2024-05-01 \
  --end-date 2024-05-02 \
  --filename-template "exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/{symbol}.csv.gz" \
  --api-key YOUR_TARDIS_API_KEY \
  --concurrency 10
```

---

### `pointline bronze reorganize`

Reorganize vendor archives into Hive-partitioned bronze layout.

**Usage:**
```bash
pointline bronze reorganize \
  --source-dir <dir> \
  --bronze-root <dir> \
  [options]
```

**Options:**
- `--source-dir`: Source directory containing vendor archives (required)
- `--bronze-root`: Bronze root directory (target) (required)
- `--vendor`: Vendor name (default: quant360)
- `--dry-run`: Print actions without executing

**Example:**

**Reorganize Quant360 archives:**
```bash
# Dry run first
pointline bronze reorganize \
  --source-dir ~/downloads/quant360 \
  --bronze-root ~/data/lake/bronze \
  --vendor quant360 \
  --dry-run

# Actually reorganize
pointline bronze reorganize \
  --source-dir ~/downloads/quant360 \
  --bronze-root ~/data/lake/bronze \
  --vendor quant360
```

**What it does:**
- Extracts `.7z` archives
- Organizes files into `exchange={ex}/type={type}/date={date}/symbol={sym}/`
- Compresses to `.csv.gz`

---

### `pointline bronze discover`

Discover bronze files ready for ingestion (replaced by `pointline ingest discover`).

**Usage:**
```bash
pointline ingest discover [--pending-only]
```

See [Ingestion](#ingestion-silver-layer) section.

---

## Ingestion (Silver Layer)

### `pointline ingest discover`

Discover bronze files and show ingestion status.

**Usage:**
```bash
pointline ingest discover [options]
```

**Options:**
- `--bronze-root`: Bronze root path (default: LAKE_ROOT/bronze)
- `--vendor`: Vendor name (optional, constructs bronze-root as LAKE_ROOT/bronze/{vendor})
- `--manifest-path`: Path to ingest_manifest (default: auto-detected)
- `--glob`: Glob pattern for bronze files (default: `**/*.csv.gz`)
- `--data-type`: Filter by data type (e.g., trades, quotes)
- `--pending-only`: Show only files not yet ingested
- `--limit`: Maximum number of files to show

**Examples:**
```bash
# Show all discovered files
pointline ingest discover

# Show only pending (not yet ingested)
pointline ingest discover --pending-only

# Discover Quant360 files
pointline ingest discover --vendor quant360 --pending-only

# Filter by data type
pointline ingest discover --data-type trades --pending-only

# Limit output
pointline ingest discover --pending-only --limit 10
```

**Sample output:**
```
Discovered 150 bronze files, 47 pending ingestion.

Pending files:
  bronze/tardis/exchange=binance-futures/type=trades/date=2024-05-01/symbol=BTCUSDT/BTCUSDT.csv.gz
  bronze/tardis/exchange=binance-futures/type=trades/date=2024-05-02/symbol=BTCUSDT/BTCUSDT.csv.gz
  ...
```

---

### `pointline ingest run`

Run ETL ingestion for a specific table/exchange/date.

**Usage:**
```bash
pointline ingest run --table <name> --exchange <name> --date <date> [options]
```

**Options:**
- `--table`: Target table (e.g., trades, quotes, book_snapshot_25) (required)
- `--exchange`: Exchange name (e.g., binance-futures) (required)
- `--date`: Date YYYY-MM-DD (required)
- `--bronze-root`: Bronze root path (default: auto-detected)
- `--vendor`: Vendor name (default: auto-detect)
- `--table-path`: Path to silver table (default: auto-detected)
- `--manifest-path`: Path to ingest_manifest (default: auto-detected)
- `--force`: Force re-ingestion even if already completed

**Examples:**

**Basic ingestion:**
```bash
# Ingest trades for 2024-05-01
pointline ingest run \
  --table trades \
  --exchange binance-futures \
  --date 2024-05-01

# Ingest quotes
pointline ingest run \
  --table quotes \
  --exchange binance-futures \
  --date 2024-05-01
```

**Force re-ingestion:**
```bash
pointline ingest run \
  --table trades \
  --exchange binance-futures \
  --date 2024-05-01 \
  --force
```

**Process multiple dates:**
```bash
# Use a loop
for date in 2024-05-{01..31}; do
  pointline ingest run --table trades --exchange binance-futures --date $date
done
```

---

## Manifest Management

### `pointline manifest show`

Show manifest status and counts.

**Usage:**
```bash
pointline manifest show [options]
```

**Options:**
- `--manifest-path`: Path to ingest_manifest (default: auto-detected)
- `--vendor`: Filter by vendor
- `--exchange`: Filter by exchange
- `--data-type`: Filter by data type
- `--status`: Filter by status (pending, completed, failed, quarantined)

**Examples:**
```bash
# Show all manifest entries
pointline manifest show

# Filter by status
pointline manifest show --status pending
pointline manifest show --status failed

# Filter by exchange
pointline manifest show --exchange binance-futures

# Combined filters
pointline manifest show --exchange binance-futures --status completed
```

**Sample output:**
```
Manifest Summary:
  Total entries: 500
  Completed: 450
  Pending: 47
  Failed: 3
  Quarantined: 0

Recent entries:
  vendor=tardis, exchange=binance-futures, type=trades, date=2024-05-01, symbol=BTCUSDT → completed
  vendor=tardis, exchange=binance-futures, type=trades, date=2024-05-02, symbol=BTCUSDT → pending
  ...
```

---

### `pointline manifest backfill-sha256`

Backfill missing SHA256 checksums in manifest.

**Usage:**
```bash
pointline manifest backfill-sha256 [options]
```

**Options:**
- `--manifest-path`: Path to ingest_manifest (default: auto-detected)
- `--bronze-root`: Bronze root path (default: auto-detected)

**Example:**
```bash
pointline manifest backfill-sha256
```

---

## Data Quality

### `pointline dq run`

Run data quality checks on a table partition.

**Usage:**
```bash
pointline dq run --table <name> --exchange <name> --date <date>
```

**Options:**
- `--table`: Table name (required)
- `--exchange`: Exchange name (required)
- `--date`: Date YYYY-MM-DD (required)

**Example:**
```bash
pointline dq run --table trades --exchange binance-futures --date 2024-05-01
```

**Checks performed:**
- Schema validation
- Null checks
- Range checks
- Monotonicity checks (timestamps)
- Crossed book detection (for quotes/book data)

---

### `pointline dq summary`

Show summary of data quality check results.

**Usage:**
```bash
pointline dq summary [options]
```

**Options:**
- `--table`: Filter by table
- `--exchange`: Filter by exchange
- `--status`: Filter by status (passed, failed)

**Example:**
```bash
# Show all DQ results
pointline dq summary

# Filter by table
pointline dq summary --table trades

# Show only failures
pointline dq summary --status failed
```

---

### `pointline dq report`

Generate detailed data quality report.

**Usage:**
```bash
pointline dq report --table <name> --exchange <name> --date <date>
```

**Options:**
- `--table`: Table name (required)
- `--exchange`: Exchange name (required)
- `--date`: Date YYYY-MM-DD (required)
- `--output`: Output file path (optional)

**Example:**
```bash
pointline dq report \
  --table trades \
  --exchange binance-futures \
  --date 2024-05-01 \
  --output dq_report_trades_20240501.txt
```

---

## Validation

### `pointline validate trades`

Validate trades table partition.

**Usage:**
```bash
pointline validate trades --exchange <name> --date <date>
```

**Options:**
- `--exchange`: Exchange name (required)
- `--date`: Date YYYY-MM-DD (required)

**Example:**
```bash
pointline validate trades --exchange binance-futures --date 2024-05-01
```

---

### `pointline validate quotes`

Validate quotes table partition.

**Usage:**
```bash
pointline validate quotes --exchange <name> --date <date>
```

**Options:**
- `--exchange`: Exchange name (required)
- `--date`: Date YYYY-MM-DD (required)

**Example:**
```bash
pointline validate quotes --exchange binance-futures --date 2024-05-01
```

---

### `pointline validation show`

Show validation results.

**Usage:**
```bash
pointline validation show [options]
```

**Options:**
- `--table`: Filter by table
- `--exchange`: Filter by exchange
- `--status`: Filter by status

**Example:**
```bash
pointline validation show
pointline validation show --status failed
```

---

### `pointline validation stats`

Show validation statistics.

**Usage:**
```bash
pointline validation stats
```

---

## Delta Lake Maintenance

### `pointline delta optimize`

Optimize Delta Lake table partition (Z-ordering).

**Usage:**
```bash
pointline delta optimize --table <name> --partition <spec>
```

**Options:**
- `--table`: Table name (required)
- `--partition`: Partition spec (e.g., `exchange=binance-futures/date=2024-05-01`) (required)

**Example:**
```bash
# Optimize specific partition
pointline delta optimize \
  --table trades \
  --partition exchange=binance-futures/date=2024-05-01

# Optimize multiple partitions
for date in 2024-05-{01..31}; do
  pointline delta optimize \
    --table trades \
    --partition exchange=binance-futures/date=$date
done
```

**What it does:**
- Compacts small Parquet files
- Applies Z-ordering on `(symbol_id, ts_local_us)`
- Improves query performance

**When to run:**
- After ingesting new data
- When queries are slow
- Weekly/monthly maintenance

---

### `pointline delta vacuum`

Vacuum old Parquet files from Delta Lake table.

**Usage:**
```bash
pointline delta vacuum --table <name> --retention-hours <hours>
```

**Options:**
- `--table`: Table name (required)
- `--retention-hours`: Retention period in hours (required)

**Example:**
```bash
# Vacuum files older than 7 days
pointline delta vacuum --table trades --retention-hours 168

# Vacuum with 30-day retention
pointline delta vacuum --table quotes --retention-hours 720
```

**⚠️ Warning:**
- This permanently deletes old Parquet files
- You won't be able to time-travel to versions before vacuum
- Recommended: retention >= 168 hours (7 days)

---

## Stock Data (Chinese Markets)

### `pointline stock-basic-cn sync`

Sync stock_basic_cn table from Tushare.

**Usage:**
```bash
pointline stock-basic-cn sync [options]
```

**Options:**
- `--token`: Tushare API token (or set `TUSHARE_TOKEN`)
- `--table-path`: Path to stock_basic_cn table (default: auto-detected)

**Example:**
```bash
pointline stock-basic-cn sync --token YOUR_TUSHARE_TOKEN
```

---

### `pointline dim-asset-stats sync`

Sync dim_asset_stats from CoinGecko API.

**Usage:**
```bash
pointline dim-asset-stats sync [options]
```

**Options:**
- `--api-key`: CoinGecko API key (or set `COINGECKO_API_KEY`)
- `--table-path`: Path to dim_asset_stats table (default: auto-detected)

**Example:**
```bash
pointline dim-asset-stats sync --api-key YOUR_COINGECKO_API_KEY
```

---

### `pointline dim-asset-stats backfill`

Backfill historical dim_asset_stats data.

**Usage:**
```bash
pointline dim-asset-stats backfill --start-date <date> --end-date <date>
```

**Options:**
- `--start-date`: Start date YYYY-MM-DD (required)
- `--end-date`: End date YYYY-MM-DD (required)
- `--api-key`: CoinGecko API key
- `--table-path`: Path to dim_asset_stats table

**Example:**
```bash
pointline dim-asset-stats backfill \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --api-key YOUR_KEY
```

---

## Common Workflows

### Complete Ingestion Workflow

```bash
# 1. Download data to bronze
pointline bronze download \
  --exchange binance-futures \
  --data-types trades,quotes \
  --symbols BTCUSDT \
  --start-date 2024-05-01 \
  --end-date 2024-05-02 \
  --filename-template "exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/{symbol}.csv.gz" \
  --api-key $TARDIS_API_KEY

# 2. Discover pending files
pointline ingest discover --pending-only

# 3. Ingest to silver
pointline ingest run --table trades --exchange binance-futures --date 2024-05-01
pointline ingest run --table quotes --exchange binance-futures --date 2024-05-01

# 4. Validate
pointline validate trades --exchange binance-futures --date 2024-05-01

# 5. Check manifest
pointline manifest show --exchange binance-futures

# 6. Optimize (optional)
pointline delta optimize --table trades --partition exchange=binance-futures/date=2024-05-01
```

---

### Quant360 Workflow (Chinese Stocks)

```bash
# 1. Reorganize archives
pointline bronze reorganize \
  --source-dir ~/downloads/quant360 \
  --bronze-root ~/data/lake/bronze \
  --vendor quant360

# 2. Discover files
pointline ingest discover --vendor quant360 --pending-only

# 3. Ingest L3 data
pointline ingest run --table l3_orders --exchange szse --date 2024-09-30
pointline ingest run --table l3_ticks --exchange szse --date 2024-09-30

# 4. Validate
pointline validate l3_orders --exchange szse --date 2024-09-30
```

---

## Tips & Best Practices

### 1. Always check config first

```bash
pointline config show
```

### 2. Use dry-run for destructive operations

```bash
pointline bronze reorganize --dry-run ...
```

### 3. Check pending files before ingestion

```bash
pointline ingest discover --pending-only
```

### 4. Monitor manifest status

```bash
pointline manifest show --status failed
```

### 5. Optimize after ingestion

```bash
pointline delta optimize --table trades --partition ...
```

### 6. Use environment variables for API keys

```bash
export TARDIS_API_KEY=your_key
export TUSHARE_TOKEN=your_token
export COINGECKO_API_KEY=your_key
```

---

## See Also

- [Troubleshooting](troubleshooting.md) - Common CLI errors
- [Tutorial](tutorial.md) - End-to-end workflow
- [API Reference](reference/api-reference.md) - Python API equivalent commands
