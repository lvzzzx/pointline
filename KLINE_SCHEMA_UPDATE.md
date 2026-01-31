# Kline Schema Update: Fixed-Point for Quote Volumes

## Summary

Updated the kline schema to use **fixed-point integer encoding for ALL numeric fields**, including quote volumes. This ensures complete determinism and consistency across the data lake.

## Changes Applied

### 1. Schema Update (`KLINE_SCHEMA`)
**Before:**
```python
"quote_volume": pl.Float64,
"taker_buy_quote_qty": pl.Float64,
```

**After:**
```python
"quote_volume_int": pl.Int64,
"taker_buy_quote_qty_int": pl.Int64,
```

### 1.5. Header Detection (`parse_binance_klines_csv`)
**Added robust header handling** for inconsistent Binance CSV formats:
- **Problem**: Some Binance CSV files have headers, some don't
- **Solution**: Added `_filter_header_row()` function that:
  - Detects if first row is a header (checks if `open_time` column contains string "open_time")
  - Automatically filters out header row if present
  - Handles both formats seamlessly

This prevents header rows from being treated as data and causing parsing errors.

### 2. Encoding Logic (`encode_fixed_point`)
- **Added computed `quote_increment`**: `price_increment × amount_increment`
- **Encodes quote volumes** using the computed increment:
  - `quote_volume → quote_volume_int`
  - `taker_buy_quote_volume → taker_buy_quote_qty_int`
- **Added validation**: Checks for invalid increments (≤0) and Int64 overflow
- **Overflow protection**: Raises error if quote values exceed Int64 range

### 3. Decoding Logic (`decode_fixed_point`)
- **Decodes quote volumes** back to Float64:
  - `quote_volume_int × quote_increment → quote_volume`
  - `taker_buy_quote_qty_int × quote_increment → taker_buy_quote_volume`
- Updated docstring to reflect new columns

### 4. Validation (`validate_klines`)
- **Added required columns**: `quote_volume_int`, `taker_buy_quote_qty_int`
- **Added validation rules**:
  - Both fields must be non-null and >= 0
  - Included in `required_columns_validation_expr`

### 5. Completeness Check (`check_kline_completeness`)
**New data quality function** to detect missing or duplicate klines:
- **Validates row counts** per (date, symbol_id) match expected interval
- **Expected counts**:
  - 1h klines: 24 rows/day
  - 4h klines: 6 rows/day
  - 1d klines: 1 row/day
  - (etc. for all standard intervals)
- **Logs warnings** for incomplete days (data gaps, new listings, exchange downtime)
- **Returns DataFrame** with completeness statistics for monitoring

**Usage:**
```python
from pointline.tables.klines import check_kline_completeness

# After ingestion, check completeness
completeness = check_kline_completeness(klines_df, interval="1h")

# Find incomplete days
incomplete = completeness.filter(~pl.col("is_complete"))
```

## Technical Details

### Quote Increment Calculation
```python
quote_increment = price_increment × amount_increment
```

**Example (BTCUSDT):**
- `price_increment = 0.01` USDT (tick size)
- `amount_increment = 0.00001` BTC (lot size)
- `quote_increment = 0.0000001` USDT

**Why this formula?**
Each trade contributes `price × quantity` to quote_volume:
- Price is in multiples of `price_increment`
- Quantity is in multiples of `amount_increment`
- Therefore, minimum quote_volume increment = `price_increment × amount_increment`

### Int64 Range Safety
- **Max Int64**: 9.2 × 10^18
- **BTCUSDT example**: $50B daily volume with increment 0.0000001 → 5 × 10^17 ✓
- Validation will catch overflow if extreme volumes are encountered

## Migration Requirements

### ⚠️ BREAKING CHANGE
This is a **schema-breaking change** that requires data migration:

1. **Bronze layer**: No changes needed (immutable vendor data)
2. **Silver layer** (`lake/silver/kline_*`): Requires re-ingestion or migration
3. **dim_symbol**: Already has `price_increment` and `amount_increment` (no changes needed)

### Migration Options

**Option A: Re-ingest from Bronze (Recommended)**
```bash
# Drop existing kline tables
delta vacuum --table kline_1h --retention-hours 0

# Re-run ingestion with new schema
pointline ingest run --table klines --exchange binance-futures --date-range 2024-01-01:2024-12-31
```

**Option B: In-place Migration Script**
```python
# Read old data with Float64 quote columns
old_df = pl.read_parquet("lake/silver/kline_1h/**/*.parquet")

# Load dim_symbol
dim_symbol = pl.read_parquet("lake/silver/dim_symbol/**/*.parquet")

# Re-encode quote volumes
# (Need to join with dim_symbol to get increments, then encode)
```

## Validation Checklist

- [x] Update `KLINE_SCHEMA` to use `*_int` columns
- [x] Update `encode_fixed_point` to encode quote volumes
- [x] Update `decode_fixed_point` to decode quote volumes
- [x] Update `validate_klines` to validate quote volume fields
- [x] Add overflow protection in encoding
- [ ] **TODO**: Add unit tests for quote volume encoding/decoding
- [ ] **TODO**: Test with real Binance data to verify no overflow
- [ ] **TODO**: Update any documentation referencing kline schema
- [ ] **TODO**: Run migration on existing data

## Testing Recommendations

### Unit Tests Needed
```python
def test_encode_quote_volume_with_computed_increment():
    """Verify quote_volume is encoded using price_increment × amount_increment."""
    # Setup test data with known increments
    # Verify encoded value matches expected

def test_decode_quote_volume_roundtrip():
    """Verify encode → decode produces original values."""
    # Test round-trip with various quote_volume values

def test_quote_volume_overflow_detection():
    """Verify Int64 overflow is caught and raises error."""
    # Create quote_volume that exceeds Int64 range
    # Verify error is raised

def test_quote_increment_calculation():
    """Verify quote_increment = price_increment × amount_increment."""
    # Test with various increment combinations

def test_parse_klines_with_header():
    """Verify CSV with header row is parsed correctly."""
    # Create DataFrame with header row as first row
    # Verify header is filtered out and data is parsed

def test_parse_klines_without_header():
    """Verify CSV without header row is parsed correctly."""
    # Create DataFrame with only data rows
    # Verify all rows are parsed (no data lost)

def test_kline_completeness_full_day():
    """Verify completeness check for full day of 1h klines (24 rows)."""
    # Create 24 klines for a single (date, symbol_id)
    # Verify is_complete = True

def test_kline_completeness_partial_day():
    """Verify completeness check detects gaps."""
    # Create 20 klines for a single (date, symbol_id) (missing 4)
    # Verify is_complete = False, row_count = 20

def test_kline_completeness_multiple_symbols():
    """Verify completeness check per symbol."""
    # Create complete data for symbol_id=1, incomplete for symbol_id=2
    # Verify correct per-symbol completeness
```

### Integration Test
```bash
# Test with a small real dataset
pointline ingest run --table klines --exchange binance-futures --symbol BTCUSDT --date 2024-01-01

# Verify no errors and check sample data
pointline validate klines --exchange binance-futures --date 2024-01-01
```

## Benefits of This Design

1. **Complete Determinism**: No floating-point arithmetic in storage/retrieval
2. **Storage Efficiency**: Int64 compresses ~20-30% better than Float64
3. **Consistency**: All numeric fields use same encoding strategy
4. **Mathematical Correctness**: Uses proper minimum increment for quote volumes
5. **Precision Guarantee**: No loss of precision from source data

## Files Modified

- `pointline/tables/klines.py`: Schema, encode/decode, validation

## Files Using Klines (No changes needed)

- `pointline/services/klines_service.py`: Already uses updated functions ✓
- `pointline/config.py`: Table registry (no schema details) ✓
- `pointline/cli/ingestion_factory.py`: CLI wiring (no schema details) ✓

## Next Steps

1. **Write tests** for the new encoding/decoding logic
2. **Test with real data** to verify no overflow issues
3. **Plan migration** for existing silver layer data
4. **Update documentation** (schemas.md, architecture docs)
5. **Consider adding quote_increment** to dim_symbol as a pre-computed column (optimization)
6. **Integrate completeness checks** into data quality monitoring

## Optional: Integrate Completeness Check into Service

You can add completeness checking to `KlinesIngestionService` post-write:

```python
# In pointline/services/klines_service.py

from pointline.tables.klines import check_kline_completeness

class KlinesIngestionService(BaseService):
    def write(self, result: pl.DataFrame) -> None:
        if result.is_empty():
            logger.warning("write: skipping empty DataFrame")
            return

        # Write data
        if hasattr(self.repo, "append"):
            self.repo.append(result)
        else:
            raise NotImplementedError("Repository must support append() for klines")

        # Optional: Check completeness after write
        try:
            completeness = check_kline_completeness(result, interval="1h", warn_on_gaps=True)
            incomplete_count = completeness.filter(~pl.col("is_complete")).height
            if incomplete_count > 0:
                logger.info(f"Ingested data has {incomplete_count} incomplete day(s)")
        except Exception as e:
            logger.warning(f"Completeness check failed: {e}")
```

Or run as a separate validation command:
```bash
# After ingestion, check data quality
pointline validate klines --exchange binance-futures --date 2024-01-01 --check-completeness
```
