# Troubleshooting Guide

Common errors and how to fix them when using Pointline.

---

## 📋 Table of Contents

- [Installation & Setup](#installation--setup)
- [Configuration](#configuration)
- [Data Loading Errors](#data-loading-errors)
- [Symbol Resolution](#symbol-resolution)
- [CLI Errors](#cli-errors)
- [Performance Issues](#performance-issues)
- [Advanced Debugging](#advanced-debugging)

---

## Installation & Setup

### "Command not found: pointline"

**Error:**
```bash
$ pointline
zsh: command not found: pointline
```

**Cause:** Package not installed or virtual environment not activated.

**Solution:**
```bash
# Ensure virtual environment is activated
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in editable mode
uv pip install -e ".[dev]"

# Verify installation
pointline --help
```

---

### "ModuleNotFoundError: No module named 'pointline'"

**Error:**
```python
>>> from pointline import research
ModuleNotFoundError: No module named 'pointline'
```

**Cause:** Python can't find the pointline package.

**Solution:**
```bash
# Make sure you're in the project directory
cd /path/to/pointline

# Activate virtual environment
source .venv/bin/activate

# Install in editable mode
uv pip install -e ".[dev]"

# Verify in Python
python -c "from pointline import research; print('Success!')"
```

---

### "Pre-commit hooks failing"

**Error:**
```bash
$ git commit -m "fix: something"
ruff....................................................................Failed
```

**Cause:** Code doesn't meet style requirements.

**Solution:**
```bash
# Run ruff to see specific errors
ruff check .

# Auto-fix what can be fixed
ruff check --fix .

# Format code
ruff format .

# Try commit again
git commit -m "fix: something"

# OR skip hooks for emergency commits (use sparingly!)
git commit --no-verify -m "fix: something"
```

---

## Configuration

### "Lake root not found"

**Error:**
```
FileNotFoundError: Lake root not found: /Users/username/data/lake
```

**Cause:** `LAKE_ROOT` not configured or pointing to non-existent directory.

**Solution:**

**Option 1: Environment variable (temporary)**
```bash
export LAKE_ROOT=/path/to/your/lake
```

**Option 2: Config file (permanent)**
```bash
# Create config directory
mkdir -p ~/.config/pointline

# Create config file
cat > ~/.config/pointline/config.toml <<EOF
lake_root = "/path/to/your/lake"
EOF

# Verify
pointline config show
```

**Option 3: Use CLI to set config**
```bash
pointline config set --lake-root /path/to/your/lake
pointline config show  # Verify
```

---

### "Permission denied when writing to lake"

**Error:**
```
PermissionError: [Errno 13] Permission denied: '/lake/silver/trades/...'
```

**Cause:** Insufficient permissions on lake directory.

**Solution:**
```bash
# Check current permissions
ls -la /path/to/lake

# Fix permissions (adjust as needed)
chmod -R u+w /path/to/lake

# Verify you can write
touch /path/to/lake/test.txt && rm /path/to/lake/test.txt
```

---

## Data Loading Errors

### "No data found for date range"

**Error:**
```python
>>> trades = query.trades("binance-futures", "BTCUSDT", "2024-01-01", "2024-01-02")
FileNotFoundError: No such file or directory:
  '/lake/silver/trades/exchange=binance-futures/date=2024-01-01'
```

**Cause:** Data doesn't exist for the requested time range.

**Solution:**

**Step 1: Check data coverage**
```python
from pointline import research

coverage = research.data_coverage("binance-futures", "BTCUSDT")
print(coverage)

# Output shows which tables have data
# {'trades': {'available': False, 'reason': 'no partitions found'}, ...}
```

**Step 2: Check if symbol exists**
```python
symbols = research.list_symbols(exchange="binance-futures", search="BTCUSDT")
print(symbols)

# If empty, symbol isn't in dim_symbol
```

**Step 3: Check what data IS available**
```python
# List all exchanges with data
exchanges = research.list_exchanges()
print(exchanges)

# List all symbols on the exchange
symbols = research.list_symbols(exchange="binance-futures")
print(symbols.head())
```

**Step 4: If data should exist, check ingestion**
```bash
# Check manifest status
pointline manifest show

# Discover pending bronze files
pointline ingest discover --pending-only

# Run ingestion for missing data
pointline ingest run --table trades --exchange binance-futures --date 2024-01-01
```

---

### "Symbol not found"

**Error:**
```python
>>> trades = query.trades("binance-futures", "BTCUSD", "2024-05-01", "2024-05-02")
PointlineError: No symbols found for exchange='binance-futures', symbol='BTCUSD'
```

**Cause:** Symbol doesn't exist in `dim_symbol` table, or incorrect symbol name.

**Solution:**

**Step 1: Search for similar symbols**
```python
from pointline import research

# Fuzzy search
symbols = research.list_symbols(search="BTCUSD", exchange="binance-futures")
print(symbols)

# List all BTC symbols
symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")
print(symbols.select(["exchange_symbol", "quote_asset", "asset_type"]))

# Output might show:
# exchange_symbol  quote_asset  asset_type
# BTCUSDT          USDT         perpetual
# BTCUSD_PERP      USD          perpetual
```

**Step 2: Use correct symbol name**
```python
# It's BTCUSDT, not BTCUSD
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02")
```

**Step 3: If symbol should exist, sync dim_symbol**
```bash
# Sync from Tardis API
pointline symbol sync --source api --exchange binance-futures --api-key YOUR_KEY

# Or sync from a CSV file
pointline symbol sync --source ./symbols.csv
```

---

### "Memory error when loading large datasets"

**Error:**
```python
>>> trades = query.trades("binance-futures", "BTCUSDT", "2024-01-01", "2024-12-31")
MemoryError: Unable to allocate ... GB
```

**Cause:** Trying to load too much data into memory at once.

**Solution:**

**Use lazy evaluation and filter before collecting**
```python
from pointline.research import query
import polars as pl

# Load as LazyFrame (default)
trades_lf = query.trades(
    "binance-futures",
    "BTCUSDT",
    "2024-01-01",
    "2024-12-31",
    lazy=True,  # Default
    decoded=True,
)

# Filter while still lazy
large_trades = trades_lf.filter(pl.col("qty") > 1.0)

# Aggregate before collecting
hourly = large_trades.with_columns(
    pl.from_epoch("ts_local_us", time_unit="us").alias("ts_dt")
).group_by_dynamic("ts_dt", every="1h").agg([
    pl.col("price").mean(),
    pl.col("qty").sum(),
])

# Only materialize aggregated results (much smaller!)
df = hourly.collect()
print(f"Loaded {df.height} hourly bars instead of millions of trades")
```

**Process data in chunks**
```python
from datetime import datetime, timedelta

start = datetime(2024, 1, 1)
results = []

# Process one month at a time
for month in range(12):
    month_start = start + timedelta(days=30 * month)
    month_end = month_start + timedelta(days=30)

    trades = query.trades(
        "binance-futures", "BTCUSDT",
        month_start, month_end,
        decoded=True, lazy=True
    )

    # Aggregate within the month
    monthly_vwap = trades.select([
        (pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()
    ]).collect().item()

    results.append({"month": month, "vwap": monthly_vwap})

print(results)
```

---

## Symbol Resolution

### "Multiple symbol versions found"

**Warning:**
```
⚠️  Warning: Symbol metadata changed during query period:
    Exchange: binance-futures
    Symbol: SOLUSDT
    Symbol IDs: [12345, 12346]

    Found 2 versions:
      - symbol_id=12345: valid_from_ts=..., tick_size=0.001
      - symbol_id=12346: valid_from_ts=..., tick_size=0.0001
```

**Cause:** Symbol metadata changed (tick size, lot size, etc.) during your query period.

**What it means:** The query API automatically handled this and queried both symbol versions.

**Action needed:**

**For exploration:** No action needed, data is correct.

**For production research:**
```python
from pointline import research, registry

# Find all symbol versions
symbols = registry.find_symbol("SOLUSDT", exchange="binance-futures")
print(symbols[["symbol_id", "valid_from_ts", "valid_until_ts", "tick_size"]])

# Option 1: Use latest version only
latest = symbols.sort("valid_from_ts", descending=True)[0]
trades = research.load_trades(symbol_id=latest["symbol_id"], ...)

# Option 2: Use specific version for time period
trades = research.load_trades(symbol_id=[12345], ...)  # Old tick size
trades = research.load_trades(symbol_id=[12346], ...)  # New tick size

# Option 3: Accept both (query API behavior)
trades = research.load_trades(symbol_id=[12345, 12346], ...)
```

---

## CLI Errors

### "No bronze files found"

**Error:**
```bash
$ pointline ingest discover --pending-only
No bronze files discovered.
```

**Cause:** No data in bronze layer, or incorrect bronze-root path.

**Solution:**

**Step 1: Check bronze-root configuration**
```bash
pointline config show

# Look for:
# bronze_root: /path/to/lake/bronze
```

**Step 2: Verify bronze files exist**
```bash
# Check bronze directory
ls -la $LAKE_ROOT/bronze/

# Should see vendor directories like:
# tardis/
# quant360/

# Check specific vendor
ls -la $LAKE_ROOT/bronze/tardis/exchange=binance-futures/type=trades/
```

**Step 3: If empty, download data**
```bash
# Download from Tardis
pointline bronze download \
  --exchange binance-futures \
  --data-types trades \
  --symbols BTCUSDT \
  --start-date 2024-05-01 \
  --end-date 2024-05-02 \
  --filename-template "..." \
  --api-key YOUR_KEY
```

---

### "Manifest table not found"

**Error:**
```bash
$ pointline manifest show
FileNotFoundError: Manifest table not found at /lake/silver/ingest_manifest
```

**Cause:** First-time setup - manifest table doesn't exist yet.

**Solution:**

The manifest table is created automatically on first ingestion:
```bash
# Run your first ingestion
pointline ingest run --table trades --exchange binance-futures --date 2024-05-01

# Manifest will be created automatically
# Verify
pointline manifest show
```

---

## Performance Issues

### "Queries are slow"

**Symptom:** Queries take minutes instead of seconds.

**Common causes and solutions:**

**1. Not using partition pruning**
```python
# ❌ Slow: scans entire table
trades = research.scan_table("trades")  # Don't do this!

# ✅ Fast: uses partition pruning
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02")
```

**2. Collecting too early**
```python
# ❌ Slow: materializes all data then filters
trades = query.trades(..., lazy=True).collect()
large_trades = trades.filter(pl.col("qty") > 1.0)

# ✅ Fast: filters before materializing
trades_lf = query.trades(..., lazy=True)
large_trades = trades_lf.filter(pl.col("qty") > 1.0).collect()
```

**3. Not using Delta Lake optimization**
```bash
# Optimize table (Z-ordering)
pointline delta optimize --table trades --partition exchange=binance-futures/date=2024-05-01

# Vacuum old files
pointline delta vacuum --table trades --retention-hours 168
```

---

### "Disk space filling up"

**Symptom:** Lake directory growing too large.

**Solution:**

**1. Check table sizes**
```bash
du -sh $LAKE_ROOT/silver/*

# Example output:
# 50G  trades
# 100G book_snapshot_25
# 10G  quotes
```

**2. Vacuum old files**
```bash
# Remove old Parquet files (after Delta Lake OPTIMIZE)
pointline delta vacuum --table trades --retention-hours 168  # 7 days

# Be careful: this deletes old data versions!
```

**3. Check for duplicate data**
```bash
# Check manifest for duplicates
pointline manifest show

# Look for duplicate entries with same (vendor, exchange, data_type, date, symbol)
```

---

## Advanced Debugging

### Enable verbose logging

```python
import logging

logging.basicConfig(level=logging.DEBUG)

# Now run your code
from pointline.research import query
trades = query.trades(...)
```

---

### Check Delta Lake table metadata

```python
import polars as pl
from pointline.config import get_table_path

# Read Delta table metadata
path = get_table_path("trades")
df = pl.read_delta(str(path), version=0)  # Read specific version

# Check partition info
print(df.select(["exchange", "date"]).unique())
```

---

### Inspect dim_symbol table

```python
from pointline import registry
import polars as pl

# Find all symbols for an exchange
symbols = registry.find_symbol(exchange="binance-futures")
print(symbols.select([
    "symbol_id", "exchange_symbol", "tick_size",
    "valid_from_ts", "valid_until_ts"
]))

# Check for expired symbols
current_symbols = symbols.filter(pl.col("is_current") == True)
print(f"{current_symbols.height} active symbols")
```

---

## 🆘 Still Stuck?

If you've tried the solutions above and are still having issues:

1. **Check examples:**
   - [examples/discovery_example.py](../examples/discovery_example.py)
   - [examples/query_api_example.py](../examples/query_api_example.py)

2. **Review documentation:**
   - [Quickstart](quickstart.md) - Basic usage
   - [Researcher's Guide](guides/researcher-guide.md) - Complete guide
   - [API Reference](reference/api-reference.md) - Function details

3. **Check for known issues:**
   - [GitHub Issues](https://github.com/pointline/pointline/issues)

4. **Ask for help:**
   - Open a new issue with:
     - Error message (full traceback)
     - Minimal code to reproduce
     - Your environment (OS, Python version, pointline version)
     - Output of `pointline config show`

---

## 💡 Tips for Avoiding Common Issues

1. **Always start with discovery API**
   ```python
   # Before loading data, check if it exists
   coverage = research.data_coverage("binance-futures", "BTCUSDT")
   ```

2. **Use lazy evaluation for large datasets**
   ```python
   # Default is lazy=True - use it!
   trades = query.trades(..., lazy=True)
   ```

3. **Filter and aggregate before collecting**
   ```python
   # Do this
   result = trades_lf.filter(...).group_by(...).agg(...).collect()

   # Not this
   trades_df = trades_lf.collect()
   result = trades_df.filter(...).group_by(...).agg(...)
   ```

4. **Check manifest status regularly**
   ```bash
   pointline manifest show
   ```

5. **Use the query API for exploration**
   ```python
   # Simple and automatic
   from pointline.research import query
   trades = query.trades(..., decoded=True)
   ```
