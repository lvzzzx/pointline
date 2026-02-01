# Exchange-Local Date Partitioning Implementation

## Summary

Successfully implemented exchange-local date partitioning to resolve timezone issues when extending the data lake from crypto-only to support Chinese stock exchanges.

## Problem Solved

**Before:** All partitions used UTC date, causing Chinese trading days to split across multiple partitions:
- Trading day 2024-09-30 CST (00:00-23:59) split across:
  - `date=2024-09-29` UTC (for early morning CST times)
  - `date=2024-09-30` UTC (for late morning/afternoon CST times)

**After:** Partitions use exchange-local date, ensuring one trading day = one partition:
- Trading day 2024-09-30 CST maps entirely to `date=2024-09-30`
- Trading day 2024-09-30 UTC maps entirely to `date=2024-09-30`

## Changes Made

### 1. Configuration (`pointline/config.py`)

Added `EXCHANGE_TIMEZONES` registry and `get_exchange_timezone()` function:

```python
EXCHANGE_TIMEZONES = {
    # Crypto (24/7, use UTC)
    "binance-futures": "UTC",
    "coinbase": "UTC",
    # ... other crypto exchanges

    # Chinese Stock Exchanges (CST, UTC+8)
    "szse": "Asia/Shanghai",
    "sse": "Asia/Shanghai",
}

def get_exchange_timezone(exchange: str) -> str:
    """Get timezone for exchange-local date partitioning."""
    normalized = normalize_exchange(exchange)
    return EXCHANGE_TIMEZONES.get(normalized, "UTC")
```

### 2. Updated All Ingestion Services

Modified `_add_metadata()` in all services to use exchange-local timezone:

**Before:**
```python
# Derive date from ts_local_us in UTC
pl.from_epoch(pl.col("ts_local_us"), time_unit="us").cast(pl.Date).alias("date")
```

**After:**
```python
# Derive date from ts_local_us in exchange-local timezone
exchange_tz = get_exchange_timezone(exchange)
pl.from_epoch(pl.col("ts_local_us"), time_unit="us")
  .dt.replace_time_zone("UTC")           # Mark epoch as UTC
  .dt.convert_time_zone(exchange_tz)     # Convert to exchange timezone
  .dt.date()                              # Extract date in local timezone
  .alias("date")
```

**Critical:** Must use `.dt.date()` instead of `.cast(pl.Date)` to preserve timezone during date extraction.

**Services updated:**
- `szse_l3_orders_service.py`
- `szse_l3_ticks_service.py`
- `trades_service.py`
- `quotes_service.py`
- `book_snapshots_service.py`
- `derivative_ticker_service.py`
- `klines_service.py`

### 3. Documentation Updates

**`CLAUDE.md`:**
- Added "Timezone Handling" section explaining:
  - Timestamp storage (always UTC)
  - Partition date semantics (exchange-local)
  - Rationale and cross-exchange query guidance
- Updated `config.py` module description to mention timezone registry

**`docs/schemas.md`:**
- Updated `date` column description to clarify exchange-local semantics

### 4. Test Coverage

Created `tests/test_timezone_partitioning.py` with comprehensive tests:
- `TestExchangeTimezoneRegistry`: Verify timezone configuration
- `TestExchangeLocalDatePartitioning`: Verify date derivation logic
  - CST boundary conditions (early morning, late night)
  - Full trading day mapping to single partition
  - Crypto UTC partitioning unchanged
  - Same instant → different dates for different exchanges

## Benefits

1. **Query Efficiency**: One trading day = one partition scan
2. **Bronze Alignment**: Matches existing bronze structure (`date=YYYY-MM-DD` in exchange-local time)
3. **Researcher UX**: Query by trading day (intuitive), not UTC fragments
4. **No Ambiguity**: Clear semantic meaning per exchange partition
5. **Extensible**: Easy to add new regional exchanges (NYSE, LSE, etc.)

## Example Usage

### SZSE Trading Day Query
```python
# Get all SZSE data for CST trading day 2024-09-30
# Single partition scan: exchange=szse/date=2024-09-30
scan_table("szse_l3_orders",
           exchange="szse",
           date="2024-09-30")
```

### Cross-Exchange Query (Use Timestamps)
```python
# For precise cross-exchange alignment, use ts_local_us
scan_table("trades",
           start_ts_us=start_ts,
           end_ts_us=end_ts)
```

## Important Notes

1. **Timestamp Storage Still UTC**: All `ts_local_us`, `ts_exch_us` remain in UTC
2. **Only Partition Date Changed**: Just the `date` partition column uses exchange-local timezone
3. **No Backward Compatibility Needed**: User confirmed no existing data to migrate
4. **Default Timezone**: Unknown exchanges default to UTC

## Testing Checklist

To verify the implementation works:

```bash
# Install dependencies
pip install -e .

# Run timezone tests
pytest tests/test_timezone_partitioning.py -v

# Verify SZSE ingestion creates correct partitions
pointline bronze discover --vendor quant360 --pending-only
pointline ingest run --table szse_l3_orders --exchange szse --date 2024-09-30

# Check partition structure
ls -la ~/data/lake/silver/szse_l3_orders/exchange=szse/
# Should see: date=2024-09-30/ (CST trading day, not split across UTC dates)
```

## Future Extensibility

To add new regional exchanges:

1. Add to `EXCHANGE_MAP` in `config.py`:
   ```python
   "nyse": 40,
   "nasdaq": 41,
   ```

2. Add to `EXCHANGE_TIMEZONES`:
   ```python
   "nyse": "America/New_York",
   "nasdaq": "America/New_York",
   ```

3. Services automatically use correct timezone via `get_exchange_timezone()`

No other code changes needed!
