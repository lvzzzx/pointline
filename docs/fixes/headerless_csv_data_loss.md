# Critical Bug Fix: Headerless CSV Data Loss

## Issue
The generic ingestion service was consuming the first data row of headerless CSVs (e.g., Binance klines) as column headers, causing:
1. **Data loss**: First record completely lost
2. **Malformed column names**: Data values used as column names
3. **Silent failure**: No error, just wrong results

## Root Cause
`pl.read_csv()` defaults to `has_header=True`. For headerless files, this treats the first data row as the header.

## Solution
Added `HEADERLESS_FORMATS` registry in `generic_ingestion_service.py`:

```python
HEADERLESS_FORMATS: dict[tuple[str, str], list[str]] = {
    ("binance", "klines"): [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trade_count",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ],
}
```

Modified `_read_bronze_csv()` to:
1. Check if `(vendor, data_type)` is in `HEADERLESS_FORMATS`
2. Set `has_header=False` and `new_columns=...` for headerless formats
3. Adjust `row_index_offset` accordingly (1 for headerless, 2 for header)

## Testing
Added `tests/test_headerless_csv.py` to verify:
- ✅ Headerless CSVs preserve all rows (no data loss)
- ✅ Column names are correct (not data values)
- ✅ CSVs with headers still work correctly

## Adding New Headerless Formats
To support a new headerless format:

1. Add entry to `HEADERLESS_FORMATS` in `generic_ingestion_service.py`:
```python
("vendor_name", "data_type"): ["col1", "col2", "col3"],
```

2. Column names must match what the parser expects

3. Add test case to verify no data loss

## References
- Fixed in: `pointline/services/generic_ingestion_service.py`
- Tests: `tests/test_headerless_csv.py`
- Related: Binance klines parser (`pointline/io/parsers/binance/klines.py`)
