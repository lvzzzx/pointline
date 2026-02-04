# Vendor Read+Parse Refactoring Plan

**Date**: 2026-02-04
**Status**: Draft (Updated with Column-Based Metadata)
**Authors**: Architecture Discussion

## Executive Summary

Refactor the ingestion pipeline to move file reading and parsing responsibilities entirely to vendor plugins. Vendors output **self-contained DataFrames** with metadata columns (exchange, exchange_symbol, date), eliminating the need for redundant metadata fields.

**Key Innovation**: Metadata lives in DataFrame columns, not in `BronzeFileMetadata`. Single source of truth.

This enables:
- **Simpler architecture**: No meta.symbol/meta.exchange redundancy
- **True vendor encapsulation**: Vendors own format AND metadata extraction
- **Natural multi-symbol/multi-exchange**: DataFrame columns support any cardinality
- **Delta Lake native**: Partitions by DataFrame columns automatically
- **Better separation of concerns**: Vendor format vs. domain logic

## Current Architecture (Problems)

### Current Flow
```
GenericIngestionService.ingest_file():
  1. _read_bronze_csv(path, meta)           ← Service knows vendor formats!
     - Handles gzip, ZIP compression
     - Checks HEADERLESS_FORMATS registry  ← Vendor-specific knowledge!
     - Returns raw_df

  2. parser = get_parser(vendor, data_type) ← Vendor only transforms columns
     parsed_df = parser(raw_df)

  3. Quarantine check (assumes meta.symbol is scalar)
  4. Resolve symbol_ids (assumes ONE symbol per file)
  5. Encode fixed-point
  6. Validate
  7. Write to Delta
```

### Key Problems

1. **Leaky Abstraction**: Service has vendor-specific knowledge (`HEADERLESS_FORMATS`)
2. **Split Responsibility**: Reading and parsing are coupled but separated
3. **Limited Flexibility**: Only supports CSV-like formats
4. **Single-Symbol Constraint**: `meta.symbol` is scalar, files must contain one symbol
5. **Normalization Location**: Symbol normalization happens in factory, not vendor
6. **Redundant Metadata**: `meta.symbol` and `meta.exchange` duplicate what's in data
7. **Two Sources of Truth**: Metadata in both `BronzeFileMetadata` AND DataFrame (ambiguous)

## Core Innovation: Column-Based Metadata

**Problem**: Current architecture has `meta.symbol` and `meta.exchange` in `BronzeFileMetadata`, creating:
- Redundancy (metadata in two places)
- Ambiguity (what if meta.symbol ≠ df["exchange_symbol"]?)
- Complexity (single-symbol vs multi-symbol branching logic)
- Limited flexibility (multi-exchange not naturally supported)

**Solution**: **Metadata lives ONLY in DataFrame columns**

```python
# OLD: Metadata in BronzeFileMetadata
meta = BronzeFileMetadata(
    exchange="binance",   # ← Redundant
    symbol="BTCUSDT",     # ← Redundant
    ...
)
df = vendor.read_and_parse(path, meta)
# Returns: [ts_local_us, price_px, qty, side]  ← No metadata!

# NEW: Metadata in DataFrame columns
meta = BronzeFileMetadata(
    vendor="tardis",
    data_type="trades",
    bronze_file_path="...",
    sha256="...",
    # NO symbol/exchange fields!
)
df = vendor.read_and_parse(path, meta)
# Returns: [exchange, exchange_symbol, date, ts_local_us, price_px, qty, side]
#          ↑        ↑               ↑
#          Metadata columns from vendor
```

**Benefits**:
1. ✅ **Single source of truth**: Metadata in data, not separate structure
2. ✅ **Simpler**: No meta.symbol/meta.exchange fields needed
3. ✅ **Flexible**: Multi-symbol AND multi-exchange natural (just more rows)
4. ✅ **Delta Lake native**: Partitions by DataFrame columns
5. ✅ **Vendor freedom**: Extract metadata however they want (path, filename, CSV data)

### Visual Comparison

```
┌─────────────────────────────────────────────────────────────────────┐
│ OLD APPROACH: Metadata in BronzeFileMetadata                        │
└─────────────────────────────────────────────────────────────────────┘

BronzeFileMetadata {                    DataFrame from vendor
  vendor: "tardis"                      ┌─────────────────────────┐
  exchange: "binance"  ← REDUNDANT      │ ts_local_us | price_px  │
  symbol: "BTCUSDT"    ← REDUNDANT      │ 1714521600  | 60000.0   │
  data_type: "trades"                   │ 1714521601  | 60001.0   │
}                                       └─────────────────────────┘
        ↓                                        ↓
Service uses meta.symbol                 Service broadcasts meta.symbol to all rows
        ↓
❌ Two sources of truth (meta vs. data)
❌ Multi-symbol requires special logic (optional column)
❌ Multi-exchange not supported


┌─────────────────────────────────────────────────────────────────────┐
│ NEW APPROACH: Metadata in DataFrame Columns                         │
└─────────────────────────────────────────────────────────────────────┘

BronzeFileMetadata {                    DataFrame from vendor
  vendor: "tardis"                      ┌──────────────────────────────────────────────────┐
  data_type: "trades"                   │ exchange | exchange_symbol | date | ts_local_us  │
  bronze_file_path: "..."               │ "binance"| "BTCUSDT"       | ...  | 1714521600   │
  sha256: "..."                         │ "binance"| "BTCUSDT"       | ...  | 1714521601   │
}                                       │ "binance"| "ETHUSDT"       | ...  | 1714521602   │
  ↑                                     │ "coinbase"| "BTC-USD"      | ...  | 1714521603   │
  NO symbol/exchange fields!            └──────────────────────────────────────────────────┘
                                                 ↑
                                        Metadata already in data!
                                                 ↓
✅ Single source of truth (DataFrame only)
✅ Multi-symbol natural (multiple exchange_symbol values)
✅ Multi-exchange natural (multiple exchange values)
✅ Delta Lake partitions by df["exchange"], df["date"]
```

### Flow Comparison

```
OLD FLOW:                           NEW FLOW:
┌───────────────┐                  ┌───────────────┐
│ Bronze File   │                  │ Bronze File   │
└───────┬───────┘                  └───────┬───────┘
        │                                  │
        ↓                                  ↓
┌───────────────────────────┐      ┌──────────────────────────────────┐
│ Extract metadata from path│      │ Vendor extracts AND adds columns │
│ meta.symbol = "BTCUSDT"   │      │ df = df.with_columns([           │
│ meta.exchange = "binance" │      │   pl.lit("binance")              │
└───────┬───────────────────┘      │     .alias("exchange"),          │
        │                          │   pl.lit("BTCUSDT")              │
        ↓                          │     .alias("exchange_symbol")    │
┌───────────────────────────┐      │ ])                               │
│ Vendor parses CSV         │      └──────────────┬───────────────────┘
│ Returns: [ts, price, qty] │                     │
│ (NO metadata columns)     │                     ↓
└───────┬───────────────────┘      ┌──────────────────────────────────┐
        │                          │ Service validates columns present│
        ↓                          │ if "exchange" not in df.columns: │
┌───────────────────────────┐      │   raise ValueError               │
│ Service broadcasts meta:  │      └──────────────┬───────────────────┘
│ df = df.with_columns(     │                     │
│   pl.lit(meta.symbol)     │                     ↓
│     .alias("exchange_..") │      ┌──────────────────────────────────┐
│ )                         │      │ Service uses DataFrame columns:  │
└───────┬───────────────────┘      │ - Map df["exchange"] → ID        │
        │                          │ - Join on df["exchange_symbol"]  │
        ↓                          │ - Partition by df["exchange"]    │
❌ Two paths for metadata    │      └──────────────┬───────────────────┘
❌ Complex branching logic   │                     │
❌ Ambiguity possible        │                     ↓
                                   ✅ One clear path
                                   ✅ No branching
                                   ✅ No ambiguity
```

## Proposed Architecture (Solution)

### New Flow
```
GenericIngestionService.ingest_file():
  1. vendor = get_vendor(meta.vendor)
     df = vendor.read_and_parse(path, meta)  ← Vendor owns everything!
     # Returns: DataFrame WITH metadata columns:
     #   - exchange (str): normalized exchange name
     #   - exchange_symbol (str): normalized symbol
     #   - date (date): trading date
     #   - file_line_number (i32): lineage
     #   + table-specific data columns

  2. Validate required columns present (exchange, exchange_symbol, date)
  3. Resolve exchange_id (map df["exchange"] → exchange_id)
  4. Quarantine check (use df["exchange_id", "exchange_symbol"])
  5. Resolve symbol_ids (batch join on df["exchange_id", "exchange_symbol"])
  6. Encode fixed-point (per-row join on symbol_id)
  7. Validate data quality
  8. Write to Delta (partitions by df["exchange", "date"] automatically)
```

### Key Improvements

1. ✅ **Single Source of Truth**: Metadata in DataFrame columns only (no redundancy)
2. ✅ **Simpler Metadata**: `BronzeFileMetadata` has no symbol/exchange fields
3. ✅ **True Vendor Encapsulation**: Vendor knows format AND metadata extraction
4. ✅ **Format Flexibility**: Vendors can handle CSV, Parquet, binary, archives
5. ✅ **Natural Multi-Symbol/Multi-Exchange**: DataFrame columns support any cardinality
6. ✅ **Delta Lake Native**: Partitions by DataFrame columns (not metadata fields)
7. ✅ **Better Performance**: Batch processing, single Delta write
8. ✅ **Cleaner Separation**: Vendor format vs. domain logic

## Design Decisions

### Decision 1: Single DataFrame vs. Dict of DataFrames

**Question**: For multi-symbol files, should vendors return:
- Option A: `dict[tuple[exchange, symbol], DataFrame]` (split early)
- Option B: Single `DataFrame` with `exchange_symbol` column (split late)

**Decision**: **Option B - Single DataFrame** ✅

**Rationale**:
| Aspect | Dict (Split Early) | Single DF (Split Late) |
|--------|-------------------|------------------------|
| Vendor complexity | Must split data | Just return flat DataFrame |
| Service complexity | Loop over dict | Single batch operation |
| Performance | Multiple writes | Single write (better!) |
| Symbol resolution | N lookups | 1 batch join |
| Fixed-point encoding | N encodes | 1 batch encode |
| Delta partitioning | Already split | Let Delta handle it ✅ |
| Current architecture | Major changes | Minimal changes |
| Memory footprint | N smaller frames | 1 larger frame (watch limits) |

**Key Insight**: Current `encode_fixed_point()` ALREADY supports multi-symbol via per-row join on `symbol_id`.

**Performance Consideration**: For very large multi-symbol files (>10M rows), consider chunked processing in future iterations. Document memory limits and add streaming support as needed.

### Decision 2: Metadata Source of Truth

**Question**: Should metadata (symbol, exchange) live in `BronzeFileMetadata` or DataFrame columns?

**Decision**: **DataFrame columns only** ✅

**Schema**:
```python
@dataclass
class BronzeFileMetadata:
    # Required fields (truly file-level metadata)
    vendor: str
    data_type: str
    bronze_file_path: str
    file_size_bytes: int
    last_modified_ts: int
    sha256: str

    # Optional fields (convenience for discovery, NOT used by service)
    date: date | None = None      # For filtering during bronze discovery
    interval: str | None = None   # For klines (1h, 4h, 1d)
    extra: dict[str, Any] | None = None

    # REMOVED: exchange, symbol (redundant - in DataFrame columns instead!)
```

**Rationale**:
1. **Single source of truth**: Metadata lives in data, not separate structure
2. **No ambiguity**: Can't have conflicting meta.symbol vs df["exchange_symbol"]
3. **Simpler**: Fewer optional fields, fewer edge cases
4. **Flexible**: Naturally supports single-symbol, multi-symbol, multi-exchange
5. **Delta Lake native**: Partitions by DataFrame columns anyway

**Required Metadata Fields**:
`REQUIRED_METADATA_FIELDS` should include **only file-level fields** (e.g., vendor, data_type, bronze_file_path, file_size_bytes, last_modified_ts, sha256, interval/date when needed for discovery). It must **not** include `exchange` or `symbol`.

**Vendor Contract**: Vendors MUST output DataFrames with metadata columns:
```python
def read_and_parse(self, path: Path, meta: BronzeFileMetadata) -> pl.DataFrame:
    """Return DataFrame with REQUIRED metadata columns:

    - exchange (Utf8): Normalized exchange name
    - exchange_symbol (Utf8): Normalized symbol
    - date (Date): Trading date in exchange timezone
    - file_line_number (Int32): Lineage tracking

    Plus table-specific data columns (ts_local_us, price_px, qty, side, etc.)
    """
    # Extract metadata (from path, filename, or CSV data)
    exchange = ...  # Vendor-specific extraction
    symbol = ...    # Vendor-specific extraction

    df = read_csv_with_lineage(path)

    # Add metadata columns
    return df.with_columns([
        pl.lit(self.normalize_exchange(exchange)).alias("exchange"),
        pl.lit(self.normalize_symbol(symbol, exchange)).alias("exchange_symbol"),
        pl.lit(trading_date).alias("date"),
    ])
```

**Multi-Symbol/Multi-Exchange**:
- **Single-symbol**: DataFrame has one unique value in `exchange_symbol` column
- **Multi-symbol**: DataFrame has multiple unique values in `exchange_symbol` column
- **Multi-exchange**: DataFrame has multiple unique values in `exchange` AND `exchange_symbol` columns
- **All supported naturally** - no special cases needed!

### Decision 3: Lineage Semantics

**Question**: How should `file_line_number` behave for different file formats?

**Decision**: **1-indexed line numbers matching original file structure** ✅

**Semantics**:
- **CSV with header**: `file_line_number` starts at 2 (first data row after header)
  - Line 1 = header row (not in DataFrame)
  - Line 2 = first data row
- **Headerless CSV**: `file_line_number` starts at 1 (first data row)
  - Line 1 = first data row
- **Gzipped files**: Line numbers refer to uncompressed content
- **ZIP archives**: Line numbers refer to content of the extracted CSV
  - **v1 constraint**: Only single-CSV ZIPs supported; multi-member ZIPs raise error

**Rationale**: Lineage enables debugging by mapping back to exact line in bronze file. Line numbers must be consistent with manual inspection of bronze files.

**Test Coverage**: Each format must have tests asserting correct line numbering:
```python
def test_csv_with_header_line_numbers():
    # CSV: header + 3 data rows
    # Expected: file_line_number = [2, 3, 4]
    ...

def test_headerless_csv_line_numbers():
    # CSV: 3 data rows (no header)
    # Expected: file_line_number = [1, 2, 3]
    ...
```

### Decision 4: Symbol Normalization Location

**Question**: Where should symbol normalization happen?

**Decision**: **Vendor does it during `read_and_parse()`** ✅

**Rationale**: Vendor knows its data format AND how to normalize it. Service shouldn't care about vendor-specific formats.

**Implementation**:
```python
class TardisVendor:
    def read_and_parse(self, path, meta):
        # Extract from Hive-style path
        # bronze/tardis/exchange=binance-futures/type=trades/symbol=BTCUSDT/file.csv.gz
        exchange_raw = extract_from_path(path, "exchange")  # "binance-futures"
        symbol_raw = extract_from_path(path, "symbol")      # "BTCUSDT"

        df = read_csv_with_lineage(path)

        # Normalize and add as columns
        return df.with_columns([
            pl.lit(self.normalize_exchange(exchange_raw)).alias("exchange"),
            pl.lit(self.normalize_symbol(symbol_raw, exchange_raw)).alias("exchange_symbol"),
        ])
        # ✅ Service receives already-normalized columns

class VendorXVendor:
    def read_and_parse(self, path, meta):
        # CSV has: timestamp,exchange,symbol,price,qty
        df = pl.read_csv(path)

        # Normalize per-row values (vectorized)
        return df.with_columns([
            pl.col("exchange").map_elements(self.normalize_exchange).alias("exchange"),
            pl.struct(["symbol", "exchange"]).map_elements(
                lambda row: self.normalize_symbol(row["symbol"], row["exchange"])
            ).alias("exchange_symbol"),
        ])
        # ✅ Multi-symbol AND multi-exchange supported!
```

**Service doesn't need normalization logic** - it just uses the columns vendor provides.

## Schema Contracts

### Per-Table Required Columns from `read_and_parse()`

All vendors MUST output DataFrames with these columns:

**Common Metadata Columns** (all tables):
```python
REQUIRED_METADATA_COLUMNS = {
    "exchange": pl.Utf8,          # Normalized exchange name
    "exchange_symbol": pl.Utf8,   # Normalized symbol
    "date": pl.Date,              # Trading date (exchange timezone)
    "file_line_number": pl.Int32, # Lineage tracking
}
```

**Trades** (in addition to metadata):
```python
REQUIRED_DATA_COLUMNS = {
    "ts_local_us": pl.Int64,      # Arrival timestamp (microseconds)
    "price_px": pl.Float64,       # Trade price
    "qty": pl.Float64,            # Trade quantity
    "side": pl.UInt8,             # 0=buy, 1=sell, 2=unknown
}
OPTIONAL_DATA_COLUMNS = {
    "ts_exch_us": pl.Int64,       # Exchange timestamp (optional)
    "trade_id": pl.Utf8,          # Trade identifier (optional)
}
```

**Quotes**:
```python
REQUIRED_PARSED_COLUMNS = {
    "file_line_number": pl.Int32,
    "ts_local_us": pl.Int64,
    "bid_px": pl.Float64,
    "bid_qty": pl.Float64,
    "ask_px": pl.Float64,
    "ask_qty": pl.Float64,
}
OPTIONAL_PARSED_COLUMNS = {
    "ts_exch_us": pl.Int64,
}
```

**SZSE L3 Orders**:
```python
REQUIRED_PARSED_COLUMNS = {
    "file_line_number": pl.Int32,
    "ts_local_us": pl.Int64,
    "appl_seq_num": pl.Int64,
    "side": pl.UInt8,
    "ord_type": pl.UInt8,
    "price_px": pl.Float64,
    "order_qty": pl.Int64,
    "channel_no": pl.Int32,
}
# Metadata columns are REQUIRED for all tables, including single-symbol files.
# SZSE vendors MUST add exchange + exchange_symbol + date columns.
```

### Empty File Handling

**Contract**: Vendors MAY return empty DataFrame for empty/invalid files.

**Service Behavior**:
```python
if parsed_df.is_empty():
    logger.info(f"Empty file: {bronze_path}")
    return IngestionResult(row_count=0, error_message=None)
```

**Rationale**: Empty files are valid (e.g., no trades in a time window). Service logs and skips without error.

**Vendor Responsibility**: Vendors SHOULD log reason for empty DataFrame (e.g., "no data rows", "invalid format").

### IngestionResult Schema

**Updated to track partial ingestion**:

```python
@dataclass
class IngestionResult:
    """Result of ingesting a single bronze file to silver.

    Attributes:
        row_count: Number of rows successfully ingested
        ts_local_min_us: Minimum timestamp in ingested data
        ts_local_max_us: Maximum timestamp in ingested data
        error_message: Error description if ingestion failed (None on success)
        partial_ingestion: True if some symbols/rows were filtered (quarantine, validation)
        filtered_symbol_count: Number of symbols filtered out (for multi-symbol files)
        filtered_row_count: Number of rows filtered out (total)
    """
    row_count: int
    ts_local_min_us: int
    ts_local_max_us: int
    error_message: str | None = None
    partial_ingestion: bool = False  # NEW: track partial ingestion
    filtered_symbol_count: int = 0   # NEW: track filtered symbols
    filtered_row_count: int = 0      # NEW: track filtered rows
```

**Usage**: Downstream audit systems can detect partial ingestion and alert if thresholds exceeded.

## Implementation Plan

### Phase 1: Update VendorPlugin Protocol

**File**: `pointline/io/vendors/base.py`

```python
class VendorPlugin(Protocol):
    # ... existing methods ...

    def read_and_parse(
        self,
        path: Path,
        meta: BronzeFileMetadata,
    ) -> pl.DataFrame:
        """Read bronze file and return self-contained DataFrame with metadata columns.

        Vendor responsibilities:
        - File format detection (CSV, Parquet, binary, archives)
        - Compression handling (gzip, ZIP, 7z, etc.)
        - Header detection (headerless CSV, custom schemas)
        - Metadata extraction (from path, filename, or file contents)
        - Normalization (exchange names, symbol names)
        - Column transformation (vendor format → canonical schema)
        - Lineage tracking (file_line_number column)

        Args:
            path: Absolute path to bronze file
            meta: Bronze file metadata (does NOT include symbol/exchange)

        Returns:
            Self-contained DataFrame with:

            **Required metadata columns:**
            - exchange (Utf8): Normalized exchange name
            - exchange_symbol (Utf8): Normalized symbol
            - date (Date): Trading date in exchange local timezone
            - file_line_number (Int32): Line number for lineage

            **Table-specific data columns** (e.g., for trades):
            - ts_local_us (Int64): Arrival timestamp
            - price_px (Float64): Trade price
            - qty (Float64): Trade quantity
            - side (UInt8): 0=buy, 1=sell, 2=unknown
            - ts_exch_us (Int64, optional): Exchange timestamp
            - trade_id (Utf8, optional): Trade identifier

        Examples:
            **Single-symbol file** (Tardis Hive-style):
            ```python
            # Path: bronze/tardis/exchange=binance-futures/symbol=BTCUSDT/trades.csv.gz
            df = vendor.read_and_parse(path, meta)
            # Returns:
            # exchange         | exchange_symbol | date       | ts_local_us | price_px | qty | side
            # "binance-futures"| "BTCUSDT"       | 2024-05-01 | 1714521600  | 60000.0  | 1.5 | 0
            # "binance-futures"| "BTCUSDT"       | 2024-05-01 | 1714521601  | 60001.0  | 2.0 | 1
            # ✅ All rows have same exchange + symbol (single-symbol file)
            ```

            **Multi-symbol file** (VendorX CSV):
            ```python
            # Path: bronze/vendorx/all_trades_2024-05-01.csv
            # CSV: timestamp,exchange,symbol,price,quantity,side
            df = vendor.read_and_parse(path, meta)
            # Returns:
            # exchange  | exchange_symbol | date       | ts_local_us | price_px | qty  | side
            # "binance" | "BTCUSDT"       | 2024-05-01 | 1714521600  | 60000.0  | 1.5  | 0
            # "binance" | "ETHUSDT"       | 2024-05-01 | 1714521601  | 3000.0   | 10.0 | 1
            # "coinbase"| "BTC-USD"       | 2024-05-01 | 1714521602  | 60001.0  | 2.0  | 0
            # ✅ Multiple symbols AND exchanges (multi-everything supported!)
            ```

        Raises:
            ValueError: If file format invalid or cannot extract required metadata

        Contract:
            - MUST be deterministic (same inputs → identical output)
            - MUST NOT mutate meta parameter
            - MUST normalize exchange and symbol names
            - MUST add metadata columns (exchange, exchange_symbol, date)
        """
        ...

    def normalize_exchange(self, exchange: str) -> str:
        """Normalize vendor-specific exchange name to canonical format.

        Args:
            exchange: Raw exchange name from vendor data

        Returns:
            Normalized exchange name (lowercase, canonical)

        Examples:
            >>> vendor.normalize_exchange("Binance-Futures")
            "binance-futures"

            >>> vendor.normalize_exchange("COINBASE_PRO")
            "coinbase"
        """
        ...

    def normalize_symbol(self, symbol: str, exchange: str) -> str:
        """Normalize vendor-specific symbol format for dim_symbol matching.

        Different vendors use different formats:
        - Tardis: "BTCUSDT" (uppercase, no separator)
        - Others might use: "BTC-USDT", "btc-usdt", "BTC/USDT"

        This method converts vendor format → canonical format for dim_symbol lookup.

        Args:
            symbol: Raw symbol from vendor data
            exchange: Exchange name (normalization may be exchange-specific)

        Returns:
            Normalized symbol for dim_symbol matching

        Examples:
            >>> tardis.normalize_symbol("BTCUSDT", "binance")
            "BTCUSDT"  # Identity (already normalized)

            >>> some_vendor.normalize_symbol("btc-usdt", "binance")
            "BTCUSDT"  # Uppercase + remove separator

        Contract:
            - MUST be deterministic (same inputs → same output)
            - MUST NOT mutate global state
        """
        ...
```

### Phase 2: Provide Shared Utilities

**File**: `pointline/io/vendors/utils.py` (new)

```python
"""Shared utilities for vendor plugins."""

from pathlib import Path
import gzip
import zipfile

import polars as pl


def read_csv_with_lineage(
    path: Path,
    *,
    has_header: bool = True,
    columns: list[str] | None = None,
    **read_options,
) -> pl.DataFrame:
    """Standard CSV reader with lineage tracking and compression handling.

    Handles:
    - Gzip compression (.gz, .csv.gz)
    - ZIP archives (.zip - single CSV only; warns if multiple)
    - Headerless CSVs (has_header=False with explicit columns)
    - Row numbering (file_line_number column)

    Args:
        path: Path to CSV file
        has_header: Whether CSV has header row
        columns: Column names (required if has_header=False)
        **read_options: Additional options passed to pl.read_csv()

    Returns:
        DataFrame with file_line_number column (Int32, 1-indexed for headerless, 2-indexed for headered)

    Raises:
        ValueError: If headerless CSV without columns specified
        pl.exceptions.NoDataError: If file is empty (returns empty DataFrame)

    Line Numbering:
        - Headered CSV: line 1 is header (not in DataFrame), data starts at line 2
        - Headerless CSV: data starts at line 1
        - This matches line numbers when inspecting bronze files manually

    ZIP Archives (v1 constraint):
        - Only single-CSV ZIPs supported
        - Multi-member ZIPs: extracts first CSV only (logs warning)
        - Future: support multi-member ZIPs by treating each as separate file

    Determinism:
        - MUST return identical DataFrame for same inputs
        - Row order preserved from file

    Examples:
        >>> # CSV with header
        >>> df = read_csv_with_lineage(Path("trades.csv"))
        >>> df["file_line_number"].to_list()
        [2, 3, 4, 5]  # Lines 2-5 in original file

        >>> # Headerless CSV
        >>> df = read_csv_with_lineage(
        ...     Path("data.csv.gz"),
        ...     has_header=False,
        ...     columns=["col1", "col2", "col3"]
        ... )
        >>> df["file_line_number"].to_list()
        [1, 2, 3]  # Lines 1-3 in original file
    """
    if not has_header and columns is None:
        raise ValueError("Headerless CSV requires explicit column names")

    # Configure read options
    options = {
        "infer_schema_length": 10000,
        "try_parse_dates": False,
        **read_options,
    }

    if not has_header:
        options["has_header"] = False
        options["new_columns"] = columns

    # Determine if we need to add row index after reading
    # (for headerless CSVs to avoid column name conflicts)
    add_row_index_after = not has_header

    if not add_row_index_after:
        # Add row index during read for CSVs with headers
        import inspect
        if "row_index_name" in inspect.signature(pl.read_csv).parameters:
            options["row_index_name"] = "file_line_number"
            options["row_index_offset"] = 2  # Skip header row
        else:
            options["row_count_name"] = "file_line_number"
            options["row_count_offset"] = 2

    # Read file with compression handling
    try:
        if path.suffix == ".zip":
            with zipfile.ZipFile(path) as zf:
                csv_name = next(
                    (name for name in zf.namelist() if name.endswith(".csv")),
                    None,
                )
                if csv_name is None:
                    return pl.DataFrame()
                with zf.open(csv_name) as handle:
                    df = pl.read_csv(handle, **options)
        elif path.suffix == ".gz" or str(path).endswith(".csv.gz"):
            with gzip.open(path, "rt", encoding="utf-8") as f:
                df = pl.read_csv(f, **options)
        else:
            df = pl.read_csv(path, **options)

        # Add row index after reading for headerless CSVs
        if add_row_index_after and not df.is_empty():
            df = df.with_row_index(name="file_line_number", offset=1)

        return df

    except pl.exceptions.NoDataError:
        return pl.DataFrame()


def detect_compression(path: Path) -> str | None:
    """Detect compression format from file extension.

    Returns:
        "gzip", "zip", "7z", or None for uncompressed
    """
    suffix = path.suffix.lower()
    if suffix == ".gz" or str(path).endswith(".csv.gz"):
        return "gzip"
    elif suffix == ".zip":
        return "zip"
    elif suffix == ".7z":
        return "7z"
    return None
```

### Phase 3: Update Vendor Plugins

**Example: Tardis** (`pointline/io/vendors/tardis/plugin.py`)

```python
class TardisVendor:
    # ... existing methods ...

    def read_and_parse(
        self,
        path: Path,
        meta: BronzeFileMetadata,
    ) -> pl.DataFrame:
        """Read Tardis CSV and return normalized trades/quotes/etc.

        Tardis delivers:
        - CSV with headers (gzip compressed)
        - One file per symbol
        - Timestamps in microseconds
        """
        from pointline.io.vendors.utils import read_csv_with_lineage

        # Read CSV (handles gzip automatically)
        df = read_csv_with_lineage(path, has_header=True)

        if df.is_empty():
            return df

        # Get parser for this data type
        parser = self.get_parsers()[meta.data_type]

        # Parse columns (existing parser logic)
        parsed_df = parser(df)

        # Add metadata columns (single-symbol still included as columns)
        exchange_raw = extract_from_path(path, "exchange")
        symbol_raw = extract_from_path(path, "symbol")
        trading_date = extract_from_path(path, "date")

        return parsed_df.with_columns([
            pl.lit(self.normalize_exchange(exchange_raw)).alias("exchange"),
            pl.lit(self.normalize_symbol(symbol_raw, exchange_raw)).alias("exchange_symbol"),
            pl.lit(trading_date).alias("date"),
        ])

    def normalize_symbol(self, symbol: str, exchange: str) -> str:
        """Tardis symbols already normalized (uppercase, no separator)."""
        return symbol
```

**Example: Binance Vision** (`pointline/io/vendors/binance_vision/plugin.py`)

```python
class BinanceVisionVendor:
    # ... existing methods ...

    def read_and_parse(
        self,
        path: Path,
        meta: BronzeFileMetadata,
    ) -> pl.DataFrame:
        """Read Binance Vision CSV (may be headerless for klines).

        Binance Vision delivers:
        - Klines: Headerless CSV with 12 columns
        - Other data: CSV with headers
        - One file per symbol
        """
        from pointline.io.vendors.utils import read_csv_with_lineage

        # Check if headerless format
        if meta.data_type == "klines":
            # Headerless klines CSV
            columns = [
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "trade_count",
                "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
            ]
            df = read_csv_with_lineage(path, has_header=False, columns=columns)
        else:
            # Normal CSV with headers
            df = read_csv_with_lineage(path, has_header=True)

        if df.is_empty():
            return df

        # Parse columns
        parser = self.get_parsers()[meta.data_type]
        parsed_df = parser(df)

        exchange_raw = extract_from_path(path, "exchange")
        symbol_raw = extract_from_path(path, "symbol")
        trading_date = extract_from_path(path, "date")

        return parsed_df.with_columns([
            pl.lit(self.normalize_exchange(exchange_raw)).alias("exchange"),
            pl.lit(self.normalize_symbol(symbol_raw, exchange_raw)).alias("exchange_symbol"),
            pl.lit(trading_date).alias("date"),
        ])

    def normalize_symbol(self, symbol: str, exchange: str) -> str:
        """Binance Vision symbols already normalized."""
        return symbol
```

**Example: Future Multi-Symbol Vendor** (`pointline/io/vendors/vendorx/plugin.py`)

```python
class VendorXVendor:
    """Example vendor that delivers multi-symbol files."""

    def read_and_parse(
        self,
        path: Path,
        meta: BronzeFileMetadata,
    ) -> pl.DataFrame:
        """Read multi-symbol CSV file.

        VendorX delivers:
        - One CSV file per day with ALL symbols
        - Columns: timestamp,exchange,symbol,price,quantity,side
        - Multi-symbol (data contains multiple symbols)
        """
        from pointline.io.vendors.utils import read_csv_with_lineage

        df = read_csv_with_lineage(path, has_header=True)

        if df.is_empty():
            return df

        # Parse and normalize
        parsed = df.select([
            pl.col("timestamp").cast(pl.Int64).alias("ts_local_us"),
            pl.col("price").cast(pl.Float64).alias("price_px"),
            pl.col("quantity").cast(pl.Float64).alias("qty"),
            self._parse_side(pl.col("side")).alias("side"),
            pl.col("symbol").alias("exchange_symbol"),
            pl.col("exchange").alias("exchange"),
        ])

        # Normalize exchange + symbols (vectorized)
        parsed = parsed.with_columns(
            pl.col("exchange").map_elements(self.normalize_exchange).alias("exchange"),
            pl.struct(["exchange_symbol", "exchange"]).map_elements(
                lambda row: self.normalize_symbol(row["exchange_symbol"], row["exchange"])
            ).alias("exchange_symbol"),
        )

        return parsed

    def normalize_symbol(self, symbol: str, exchange: str) -> str:
        """VendorX uses lowercase with hyphens."""
        return symbol.upper().replace("-", "")  # "btc-usdt" → "BTCUSDT"
```

### Phase 4: Update GenericIngestionService

**File**: `pointline/services/generic_ingestion_service.py`

**Changes**:

1. **Remove `_read_bronze_csv()` method**
2. **Remove `HEADERLESS_FORMATS` registry**
3. **Update `ingest_file()` to call `vendor.read_and_parse()`**
4. **Remove all `meta.symbol` / `meta.exchange` branching**

```python
def ingest_file(
    self,
    meta: BronzeFileMetadata,
    file_id: int,
    *,
    bronze_root: Path | None = None,
) -> IngestionResult:
    """Ingest a single file from Bronze to Silver."""

    # Validate required metadata
    table_module = importlib.import_module(f"pointline.tables.{self.table_name}")
    required_fields = getattr(table_module, "REQUIRED_METADATA_FIELDS", set())

    # Metadata columns are required in DataFrame; meta contains only file-level fields.
    required_fields_strict = required_fields

    missing_fields = []
    for field in required_fields_strict:
        if getattr(meta, field, None) is None:
            missing_fields.append(field)

    if missing_fields:
        error_msg = f"Missing required metadata: {missing_fields}"
        logger.error(error_msg)
        return IngestionResult(row_count=0, error_message=error_msg)

    # Get vendor plugin
    vendor = get_vendor(meta.vendor)

    # Resolve bronze path
    if bronze_root is None:
        bronze_root = get_bronze_root(meta.vendor)
    bronze_path = bronze_root / meta.bronze_file_path

    if not bronze_path.exists():
        error_msg = f"Bronze file not found: {bronze_path}"
        logger.error(error_msg)
        return IngestionResult(row_count=0, error_message=error_msg)

    try:
        # 1. Vendor reads and parses file (returns DataFrame with metadata columns)
        df = vendor.read_and_parse(bronze_path, meta)

        if df.is_empty():
            logger.info(f"Empty file (no data): {bronze_path}")
            return IngestionResult(row_count=0, error_message=None)

        # 2. Validate required metadata columns present
        required_cols = ["exchange", "exchange_symbol", "date", "file_line_number"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            error_msg = (
                f"Vendor output missing required columns: {missing}. "
                f"Vendor must add metadata columns during read_and_parse()."
            )
            logger.error(error_msg)
            return IngestionResult(row_count=0, error_message=error_msg)

        # 3. Resolve exchange_id from exchange column (vectorized)
        df = df.with_columns([
            pl.col("exchange").map_dict(EXCHANGE_MAP, default=None).alias("exchange_id")
        ])

        # Check for unmapped exchanges
        unmapped = df.filter(pl.col("exchange_id").is_null())
        if not unmapped.is_empty():
            invalid_exchanges = unmapped["exchange"].unique().to_list()
            error_msg = f"Unknown exchanges: {invalid_exchanges}"
            logger.error(error_msg)
            return IngestionResult(row_count=0, error_message=error_msg)

        # Cast exchange_id to Int16
        df = df.with_columns(pl.col("exchange_id").cast(pl.Int16))

        # 4. Load dim_symbol for symbol resolution
        dim_symbol = self.dim_symbol_repo.read_all()

        # 5. Quarantine check for all unique (exchange_id, symbol) pairs
        unique_symbols = df.select(["exchange_id", "exchange_symbol"]).unique()
        quarantined_pairs = []
        original_row_count = df.height

        for row in unique_symbols.iter_rows(named=True):
            exchange_id = row["exchange_id"]
            symbol = row["exchange_symbol"]

            is_valid, error_msg = self._check_quarantine_symbol(
                meta, dim_symbol, exchange_id, symbol
            )
            if not is_valid:
                logger.warning(
                    f"Symbol quarantined: exchange_id={exchange_id}, "
                    f"symbol={symbol}, reason={error_msg}"
                )
                quarantined_pairs.append((exchange_id, symbol))
                # Filter out this symbol's rows
                df = df.filter(
                    ~((pl.col("exchange_id") == exchange_id) &
                      (pl.col("exchange_symbol") == symbol))
                )

        filtered_row_count = original_row_count - df.height
        partial_ingestion = len(quarantined_pairs) > 0

        if df.is_empty():
            return IngestionResult(
                row_count=0,
                error_message="All symbols quarantined",
                partial_ingestion=True,
                filtered_symbol_count=len(quarantined_pairs),
                filtered_row_count=filtered_row_count,
            )

        if partial_ingestion:
            logger.warning(
                f"Partial ingestion: {len(quarantined_pairs)} symbols filtered, "
                f"{filtered_row_count} rows dropped from {meta.bronze_file_path}"
            )

        # 6. Resolve symbol_ids (batch join on exchange_id + exchange_symbol)
        resolved_df = self.strategy.resolve_symbol_ids(
            df,
            dim_symbol,
            exchange_id=None,       # exchange_id already present as column
            exchange_symbol=None,   # use column
            ts_col=self.strategy.ts_col,
        )

        # 7. Encode fixed-point (works for both single and multi-symbol)
        encoded_df = self.strategy.encode_fixed_point(resolved_df, dim_symbol)

        # 8. Add lineage columns
        lineage_df = self._add_lineage(encoded_df, file_id)

        # 9. Normalize schema
        normalized_df = self.strategy.normalize_schema(lineage_df)

        # 10. Validate
        validated_df = self.validate(normalized_df)

        if validated_df.is_empty():
            logger.warning(f"No valid rows after validation: {bronze_path}")
            return IngestionResult(
                row_count=0,
                error_message="All rows filtered by validation"
            )

        # 11. Write to Delta (single write, Delta handles partitioning)
        self.write(validated_df)

        # 12. Compute result stats
        ts_min = validated_df[self.strategy.ts_col].min()
        ts_max = validated_df[self.strategy.ts_col].max()

        symbol_info = f"{unique_symbols.height} symbol pairs"

        logger.info(
            f"Ingested {validated_df.height} rows from {meta.bronze_file_path} "
            f"[vendor={meta.vendor}, symbols={symbol_info}] "
            f"(ts range: {ts_min} - {ts_max})"
        )

        return IngestionResult(
            row_count=validated_df.height,
            ts_local_min_us=ts_min,
            ts_local_max_us=ts_max,
            error_message=None,
        )

    except Exception as e:
        error_msg = f"Error ingesting {bronze_path}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return IngestionResult(row_count=0, error_message=error_msg)

def _check_quarantine_symbol(
    self,
    meta: BronzeFileMetadata,
    dim_symbol: pl.DataFrame,
    exchange_id: int,
    symbol: str,
) -> tuple[bool, str]:
    """Check if a single symbol should be quarantined."""
    # Extract to method for reuse in multi-symbol case
    # ... (existing _check_quarantine logic but for one symbol)
```

### Phase 5: Update Symbol Resolution

**File**: `pointline/tables/_base.py`

```python
def generic_resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int | None = None,
    exchange_symbol: str | None = None,  # ← Make optional
    *,
    ts_col: str = "ts_local_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for data using as-of join with dim_symbol.

    Supports both single-symbol and multi-symbol DataFrames:
    - Single-symbol: exchange_symbol parameter provided (broadcasted to all rows)
    - Multi-symbol: exchange_symbol=None (uses existing column in DataFrame)

    Args:
        data: DataFrame with timestamp column
        dim_symbol: dim_symbol table in canonical schema
        exchange_id: Exchange ID (scalar for single-exchange, or use column)
        exchange_symbol: Exchange symbol (scalar) or None (use column)
        ts_col: Timestamp column name (default: ts_local_us)

    Returns:
        DataFrame with symbol_id column added

    Raises:
        ValueError: If exchange_symbol=None and DataFrame missing exchange_symbol column

    Warnings:
        If both exchange_symbol parameter and column exist, logs warning about ambiguity
        (parameter takes precedence but this may indicate vendor bug)
    """
    from pointline.dim_symbol import resolve_symbol_ids as _resolve_symbol_ids
    import logging

    logger = logging.getLogger(__name__)
    result = data.clone()

    # Add exchange_id if not present
    if "exchange_id" not in result.columns:
        if exchange_id is None:
            raise ValueError(
                "exchange_id is required when DataFrame lacks exchange_id column. "
                "With column-based metadata, vendors must include exchange_id "
                "or service must map exchange column first."
            )
        result = result.with_columns(pl.lit(exchange_id, dtype=pl.Int16).alias("exchange_id"))
    else:
        result = result.with_columns(pl.col("exchange_id").cast(pl.Int16))

    # Add exchange_symbol if not present
    if "exchange_symbol" not in result.columns:
        if exchange_symbol is None:
            raise ValueError(
                "exchange_symbol parameter is None but DataFrame missing exchange_symbol column. "
                "For multi-symbol files, ensure vendor.read_and_parse() includes exchange_symbol column."
            )
        # Broadcast scalar to all rows
        result = result.with_columns(pl.lit(exchange_symbol).alias("exchange_symbol"))
    elif exchange_symbol is not None:
        # Column exists AND parameter provided - AMBIGUOUS!
        # Warn: this may indicate vendor bug (vendor should NOT include column for single-symbol)
        logger.warning(
            f"Ambiguous symbol resolution: DataFrame has exchange_symbol column "
            f"but parameter exchange_symbol={exchange_symbol} also provided. "
            f"Parameter takes precedence, overwriting column. "
            f"This may indicate a vendor bug (single-symbol files should not include exchange_symbol column)."
        )
        # Override column with parameter
        result = result.with_columns(pl.lit(exchange_symbol).alias("exchange_symbol"))
    # else: use existing column values

    return _resolve_symbol_ids(result, dim_symbol, ts_col=ts_col)
```

### Phase 6: Update TableStrategy

**File**: `pointline/services/generic_ingestion_service.py`

**Remove `normalize_symbol` from TableStrategy**:

```python
@dataclass
class TableStrategy:
    """Table-specific functions (encoding, validation, normalization, resolution).

    This encapsulates all the table-specific domain logic while keeping the
    ingestion pipeline vendor-agnostic.
    """

    encode_fixed_point: Callable[[pl.DataFrame, pl.DataFrame], pl.DataFrame]
    validate: Callable[[pl.DataFrame], pl.DataFrame]
    normalize_schema: Callable[[pl.DataFrame], pl.DataFrame]
    resolve_symbol_ids: Callable[
        [pl.DataFrame, pl.DataFrame, int, str | None, str], pl.DataFrame
    ]  # Note: exchange_symbol is now optional
    ts_col: str = "ts_local_us"

    # REMOVED: normalize_symbol (moved to VendorPlugin)
```

### Phase 7: Update Ingestion Factory

**File**: `pointline/cli/ingestion_factory.py`

**Remove all `normalize_symbol` assignments**:

```python
def create_ingestion_service(data_type: str, manifest_repo, *, interval: str | None = None):
    """Create ingestion service for data type."""

    dim_symbol_repo = BaseDeltaRepository(get_table_path("dim_symbol"))

    # Trades
    if data_type == "trades":
        repo = BaseDeltaRepository(
            get_table_path("trades"),
            partition_by=["exchange", "date"],
        )
        strategy = TableStrategy(
            encode_fixed_point=tables.trades.encode_fixed_point,
            validate=tables.trades.validate_trades,
            normalize_schema=tables.trades.normalize_trades_schema,
            resolve_symbol_ids=tables.trades.resolve_symbol_ids,
            # REMOVED: normalize_symbol=no_normalization
        )
        return GenericIngestionService("trades", strategy, repo, dim_symbol_repo, manifest_repo)

    # ... similar for other tables
```

### Phase 8: Cleanup

**Files to delete**:
- `pointline/symbol_normalization.py` (functionality moved to VendorPlugin)

**Files to update**:
- Remove `normalize_binance_symbol` and `no_normalization` imports from factory

## Migration Path

### Step 1: Add New Methods (Non-Breaking)
- Add `read_and_parse()` to VendorPlugin protocol
- Add `normalize_symbol()` to VendorPlugin protocol
- Implement in all vendor plugins
- Keep existing parser methods

### Step 2: Add Utilities (Non-Breaking)
- Create `pointline/io/vendors/utils.py`
- Implement `read_csv_with_lineage()`
- Vendor plugins can start using utilities

### Step 3: Update Service (Breaking)
- Switch `GenericIngestionService.ingest_file()` to use `vendor.read_and_parse()`
- Remove `_read_bronze_csv()` method
- Remove `HEADERLESS_FORMATS` registry
- Remove all `meta.symbol` / `meta.exchange` branching

### Step 4: Update Symbol Resolution (Non-Breaking)
- Make `exchange_symbol` parameter optional in `generic_resolve_symbol_ids()`
- Backwards compatible (existing scalar usage still works)

### Step 5: Cleanup (Breaking)
- Remove `normalize_symbol` from TableStrategy
- Remove symbol_normalization.py
- Update factory to not assign normalize_symbol

## Testing Strategy

### Unit Tests

#### Test `read_csv_with_lineage()` Utility

**File format tests**:
```python
def test_read_csv_with_header():
    """CSV with header: line numbers start at 2."""
    # File: header + 3 data rows
    df = read_csv_with_lineage(csv_path, has_header=True)
    assert df["file_line_number"].to_list() == [2, 3, 4]

def test_read_csv_headerless():
    """Headerless CSV: line numbers start at 1."""
    # File: 3 data rows (no header)
    df = read_csv_with_lineage(
        csv_path,
        has_header=False,
        columns=["col1", "col2", "col3"]
    )
    assert df["file_line_number"].to_list() == [1, 2, 3]

def test_read_csv_gz():
    """Gzipped CSV: line numbers refer to uncompressed content."""
    df = read_csv_with_lineage(Path("data.csv.gz"), has_header=True)
    assert "file_line_number" in df.columns
    assert df["file_line_number"][0] == 2  # First data row

def test_read_zip_single_csv():
    """ZIP with single CSV: extracts and numbers correctly."""
    # ZIP contains: trades.csv (header + 5 rows)
    df = read_csv_with_lineage(Path("trades.zip"), has_header=True)
    assert df.height == 5
    assert df["file_line_number"].to_list() == [2, 3, 4, 5, 6]

def test_read_zip_multiple_csvs_raises():
    """ZIP with multiple CSVs: should raise or pick first (document behavior)."""
    # ZIP contains: trades.csv, quotes.csv
    # v1: picks first CSV only
    df = read_csv_with_lineage(Path("multi.zip"), has_header=True)
    # Should log warning about multiple files
    assert not df.is_empty()

def test_read_headerless_without_columns_raises():
    """Headerless CSV without column names: raises ValueError."""
    with pytest.raises(ValueError, match="requires explicit column names"):
        read_csv_with_lineage(csv_path, has_header=False)

def test_read_empty_file():
    """Empty CSV: returns empty DataFrame (no error)."""
    # File: 0 bytes or only header
    df = read_csv_with_lineage(empty_csv_path, has_header=True)
    assert df.is_empty()
```

#### Test Vendor `read_and_parse()`
```python
def test_tardis_read_and_parse_single_symbol(tmp_path):
    """Test Tardis vendor reads single-symbol CSV."""
    # Create test CSV
    csv_path = tmp_path / "trades.csv.gz"
    # ... write test data ...

    meta = BronzeFileMetadata(
        vendor="tardis",
        data_type="trades",
        # ...
    )

    vendor = TardisVendor()
    df = vendor.read_and_parse(csv_path, meta)

    assert "file_line_number" in df.columns
    assert "ts_local_us" in df.columns
    assert "exchange_symbol" in df.columns  # Single-symbol still includes metadata
    assert df["exchange_symbol"].n_unique() == 1

def test_vendorx_read_and_parse_multi_symbol(tmp_path):
    """Test VendorX reads multi-symbol CSV."""
    csv_path = tmp_path / "all_trades.csv"
    # ... write CSV with multiple symbols ...

    meta = BronzeFileMetadata(
        vendor="vendorx",
        data_type="trades",
        # ...
    )

    vendor = VendorXVendor()
    df = vendor.read_and_parse(csv_path, meta)

    assert "exchange_symbol" in df.columns  # Multi-symbol
    assert df["exchange_symbol"].unique().len() > 1
```

**Test symbol resolution with optional parameter**:
```python
def test_resolve_symbol_ids_scalar(sample_dim_symbol):
    """Test symbol resolution with scalar exchange_symbol."""
    data = pl.DataFrame({
        "ts_local_us": [100, 200, 300],
        "price_px": [1.0, 2.0, 3.0],
    })

    resolved = generic_resolve_symbol_ids(
        data,
        sample_dim_symbol,
        exchange_id=1,
        exchange_symbol="BTCUSDT",  # Scalar
    )

    assert "symbol_id" in resolved.columns
    assert resolved["exchange_symbol"].unique().len() == 1

def test_resolve_symbol_ids_column(sample_dim_symbol):
    """Test symbol resolution with exchange_symbol column."""
    data = pl.DataFrame({
        "ts_local_us": [100, 200, 300],
        "exchange_symbol": ["BTCUSDT", "ETHUSDT", "BTCUSDT"],
        "price_px": [1.0, 2.0, 3.0],
    })

    resolved = generic_resolve_symbol_ids(
        data,
        sample_dim_symbol,
        exchange_id=1,
        exchange_symbol=None,  # Use column
    )

    assert "symbol_id" in resolved.columns
    assert resolved["exchange_symbol"].unique().len() == 2

def test_resolve_symbol_ids_ambiguous_warns(sample_dim_symbol, caplog):
    """Test warning when both parameter and column present."""
    data = pl.DataFrame({
        "ts_local_us": [100, 200],
        "exchange_symbol": ["ETHUSDT", "BTCUSDT"],  # Column present
    })

    # Also pass parameter (ambiguous!)
    resolved = generic_resolve_symbol_ids(
        data,
        sample_dim_symbol,
        exchange_id=1,
        exchange_symbol="BTCUSDT",  # Parameter overrides
    )

    # Should warn
    assert "Ambiguous symbol resolution" in caplog.text
    # Parameter wins
    assert resolved["exchange_symbol"].unique().len() == 1
    assert resolved["exchange_symbol"][0] == "BTCUSDT"
```

#### Test Schema Validation

```python
def test_validate_parsed_columns_trades():
    """Validate trades DataFrame has required columns."""
    from pointline.tables.trades import REQUIRED_PARSED_COLUMNS

    # Valid DataFrame
    df = pl.DataFrame({
        "file_line_number": [1, 2],
        "ts_local_us": [100, 200],
        "price_px": [1.0, 2.0],
        "qty": [0.5, 1.0],
        "side": [0, 1],
    })

    # Should pass validation
    missing = [col for col in REQUIRED_PARSED_COLUMNS if col not in df.columns]
    assert missing == []

    # Invalid DataFrame (missing qty)
    df_invalid = df.drop("qty")
    missing = [col for col in REQUIRED_PARSED_COLUMNS if col not in df_invalid.columns]
    assert "qty" in missing
```

#### Test Metadata Column Requirements

```python
def test_missing_metadata_columns_fails_service(test_lake, caplog):
    """Service rejects vendor output missing required metadata columns."""
    df = pl.DataFrame({
        "ts_local_us": [100, 200],
        "price_px": [1.0, 2.0],
        "qty": [0.5, 1.0],
        "side": [0, 1],
        # Missing: exchange, exchange_symbol, date, file_line_number
    })

    # Simulate vendor returning df without metadata
    with pytest.raises(ValueError, match="missing required columns"):
        validate_required_columns(df)

def test_exchange_id_mapping_unknown_exchange_fails(caplog):
    """Unknown exchange values should fail before symbol resolution."""
    df = pl.DataFrame({
        "exchange": ["unknown-exchange"],
        "exchange_symbol": ["BTCUSDT"],
        "date": [date(2024, 5, 1)],
        "file_line_number": [2],
        "ts_local_us": [100],
        "price_px": [1.0],
        "qty": [0.5],
        "side": [0],
    })

    with pytest.raises(ValueError, match="Unknown exchanges"):
        map_exchange_id(df)  # service helper
```

### Integration Tests

**Test end-to-end ingestion**:
```python
def test_ingest_single_symbol_file(test_lake):
    """Test ingesting single-symbol file."""
    # Setup bronze file
    # Run ingestion
    # Verify silver table has correct data

def test_ingest_multi_symbol_file(test_lake):
    """Test ingesting multi-symbol file."""
    # Setup bronze file with multiple symbols
    # Run ingestion
    # Verify silver table partitioned correctly by symbol

def test_ingest_partial_quarantine(test_lake):
    """Test ingesting file where one symbol is quarantined."""
    # Setup:
    # - Bronze file with 3 symbols: BTCUSDT, ETHUSDT, XRPUSDT
    # - dim_symbol has: BTCUSDT, ETHUSDT (XRPUSDT missing)

    result = ingest_file(meta, file_id)

    # Should succeed with partial ingestion
    assert result.row_count > 0
    assert result.partial_ingestion is True
    assert result.filtered_symbol_count == 1  # XRPUSDT filtered
    assert result.error_message is None

    # Silver table should have only BTCUSDT and ETHUSDT
    silver_df = read_table("trades", exchange="binance", date="2024-05-01")
    symbols = silver_df["exchange_symbol"].unique().to_list()
    assert "XRPUSDT" not in symbols
    assert "BTCUSDT" in symbols
    assert "ETHUSDT" in symbols

def test_ingest_all_symbols_quarantined(test_lake):
    """Test ingesting file where all symbols are quarantined."""
    # Setup: file with symbols not in dim_symbol

    result = ingest_file(meta, file_id)

    # Should fail with quarantine error
    assert result.row_count == 0
    assert result.error_message == "All symbols quarantined"
    assert result.partial_ingestion is True
    assert result.filtered_symbol_count > 0

def test_ingest_multi_symbol_performance(test_lake, benchmark):
    """Benchmark: multi-symbol batch processing vs N single-symbol files."""
    # Compare:
    # 1. One file with 100 symbols → single batch ingestion
    # 2. 100 files with 1 symbol each → 100 individual ingestions

    # Hypothesis: batch processing is 5-10x faster
    ...
```

### Performance Tests

```python
def test_batch_symbol_resolution_performance():
    """Verify batch join is faster than N individual lookups."""
    # Generate data: 1M rows, 100 unique symbols
    data = generate_multi_symbol_data(rows=1_000_000, symbols=100)

    # Batch approach (single join)
    start = time.time()
    resolved_batch = generic_resolve_symbol_ids(
        data, dim_symbol, 1, exchange_symbol=None
    )
    batch_time = time.time() - start

    # Naive approach (100 individual joins)
    start = time.time()
    resolved_individual = []
    for symbol in data["exchange_symbol"].unique():
        subset = data.filter(pl.col("exchange_symbol") == symbol)
        resolved = generic_resolve_symbol_ids(subset, dim_symbol, 1, symbol)
        resolved_individual.append(resolved)
    resolved_individual = pl.concat(resolved_individual)
    individual_time = time.time() - start

    # Batch should be 5-10x faster
    assert batch_time < individual_time / 5

    # Results should be identical
    assert_frame_equal(
        resolved_batch.sort("ts_local_us"),
        resolved_individual.sort("ts_local_us")
    )

def test_memory_footprint_large_file():
    """Test memory usage for large multi-symbol file."""
    # Generate 10M row file with 1000 symbols
    # Monitor memory during ingestion
    # Ensure peak memory < 2GB
    ...
```

## Rollout Plan

### Phase 0: Documentation (Week 1)
- [x] Write architecture plan (this document)
- [ ] Review with team
- [ ] Update CLAUDE.md

### Phase 1: Foundation (Week 2)
- [ ] Add `read_and_parse()` to VendorPlugin protocol
- [ ] Add `normalize_symbol()` to VendorPlugin protocol
- [ ] Create `pointline/io/vendors/utils.py`
- [ ] Write unit tests for utilities

### Phase 2: Vendor Implementation (Week 3)
- [ ] Implement `read_and_parse()` in Tardis vendor
- [ ] Implement `read_and_parse()` in Binance Vision vendor
- [ ] Implement `read_and_parse()` in Quant360 vendor
- [ ] Write tests for each vendor

### Phase 3: Service Refactor (Week 4)
- [ ] Update `GenericIngestionService.ingest_file()`
- [ ] Remove all `meta.symbol` / `meta.exchange` branching
- [ ] Remove `_read_bronze_csv()`
- [ ] Remove `HEADERLESS_FORMATS`
- [ ] Write integration tests

### Phase 4: Symbol Resolution (Week 5)
- [ ] Make `exchange_symbol` optional in `generic_resolve_symbol_ids()`
- [ ] Update all `resolve_symbol_ids()` wrappers
- [ ] Write tests for both scalar and column modes

### Phase 5: Cleanup (Week 6)
- [ ] Remove `normalize_symbol` from TableStrategy
- [ ] Update ingestion factory
- [ ] Delete `symbol_normalization.py`
- [ ] Update all documentation

### Phase 6: Validation (Week 7)
- [ ] Run full test suite
- [ ] Test on production-like data
- [ ] Performance benchmarks
- [ ] Fix any issues

## Success Criteria

- [ ] All existing tests pass
- [ ] New tests for multi-symbol support pass
- [ ] All vendors implement `read_and_parse()`
- [ ] Service successfully ingests both single and multi-symbol files
- [ ] Performance: No regression vs. current implementation
- [ ] Documentation updated (CLAUDE.md, vendor guides)
- [ ] Code cleanup complete (dead code removed)

## Resolved Questions (From Review)

### 1. Meta.exchange Ambiguity for Multi-Symbol Files

**Question**: The schema allows `exchange=None` but multi-symbol branch requires it. Is this consistent?

**Resolution**: ✅ **Eliminate ambiguity**:
- `meta.exchange` and `meta.symbol` are removed entirely.
- Vendors MUST populate `exchange` and `exchange_symbol` columns for all rows.
- Multi-exchange support is automatic when rows contain multiple exchanges.

### 2. Lineage Column Behavior

**Question**: Are file_line_number offsets consistent for headerless vs. headered CSVs?

**Resolution**: ✅ **Documented in Decision 3**:
- Headered CSV: offset=2 (line 1 is header)
- Headerless CSV: offset=1 (line 1 is data)
- **Test requirement**: Every format has assertion tests for line numbering

### 3. normalize_symbol Contract for exchange=None

**Question**: What if exchange context is missing?

**Resolution**: ✅ **Exchange comes from data**:
- `normalize_symbol(symbol, exchange)` MUST receive non-None exchange
- Vendors MUST extract exchange per-row (or per-file) and add it as a column

### 4. Symbol Resolution Column Precedence

**Question**: When both parameter and column exist, silently override is risky.

**Resolution**: ✅ **Add warning log**:
```python
if "exchange_symbol" in df.columns and exchange_symbol is not None:
    logger.warning("Ambiguous symbol resolution: parameter overrides column (vendor bug?)")
```

**Rationale**: Helps debug vendor issues where single-symbol file incorrectly includes column.

### 5. Quarantine Handling for Multi-Symbol

**Question**: How to track partial ingestion when some symbols are filtered?

**Resolution**: ✅ **Enhanced IngestionResult**:
```python
@dataclass
class IngestionResult:
    partial_ingestion: bool = False
    filtered_symbol_count: int = 0
    filtered_row_count: int = 0
```

**Benefit**: Downstream audits can detect if >50% of symbols dropped (data quality issue).

### 6. ZIP Archive Multi-File Handling

**Question**: What if ZIP contains multiple CSVs?

**Resolution**: ✅ **v1 constraint**:
- Only single-CSV ZIPs supported
- Multi-member ZIPs: extract first CSV only (with warning log)
- **Future v2**: Support multi-member ZIPs by treating each CSV as separate file

**Implementation**:
```python
def read_csv_with_lineage(path: Path, ...):
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as zf:
            csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
            if len(csv_files) > 1:
                logger.warning(
                    f"ZIP contains {len(csv_files)} CSVs, using first only: {path}"
                )
            csv_name = csv_files[0]
            # ... extract first CSV
```

### 7. Empty File Expectations

**Question**: Should vendors raise or return empty DataFrame for empty files?

**Resolution**: ✅ **Return empty DataFrame**:
- Empty files are valid (e.g., no trades in time window)
- Vendors return `pl.DataFrame()` (empty, no error)
- Service logs `INFO` and returns `IngestionResult(row_count=0, error_message=None)`

**Vendor best practice**: Log reason for empty result:
```python
def read_and_parse(self, path, meta):
    df = read_csv_with_lineage(path)
    if df.is_empty():
        logger.info(f"Empty file (no data rows): {path}")
    return df
```

### 8. Determinism and Immutability

**Question**: Should `read_and_parse()` have explicit contracts?

**Resolution**: ✅ **Document in protocol**:

**Contracts**:
1. **Deterministic**: Same inputs (path, meta) → identical output DataFrame
2. **Idempotent**: Can be called multiple times without side effects
3. **No metadata mutation**: MUST NOT modify `meta` parameter
4. **No global state**: MUST NOT depend on or mutate global variables
5. **Thread-safe**: Callable concurrently (for parallel ingestion)

**Rationale**: Enables reproducible ETL, parallel processing, and testing.

## Open Questions (Remaining)

1. **Memory overhead for large files**: Single DataFrame approach may use more memory than chunked processing
   - **Mitigation**: Monitor peak memory in tests; add streaming API in v2 if needed
   - **Threshold**: If file >10M rows, consider chunking (deferred to future)

2. **Vendor capability flags**: Should vendors declare `supports_multi_symbol()` upfront?
   - **Decision**: ✅ Add to protocol (helps service validate assumptions)

3. **Schema validation helper**: Should service validate parsed DataFrame columns?
   - **Decision**: ✅ Add per-table `REQUIRED_PARSED_COLUMNS` and validation helper

4. **Backwards compatibility timeline**: How long to keep old parsers during migration?
   - **Decision**: Remove after Phase 3 complete (all vendors migrated)

## Review Feedback Summary

This plan was reviewed with the following key refinements incorporated:

### Addressed Risks

1. ✅ **Meta.exchange ambiguity**: Explicit v1 constraint (multi-symbol requires single exchange)
2. ✅ **Lineage semantics**: Documented offset behavior and test requirements
3. ✅ **Normalization contract**: Exchange parameter required, validation added
4. ✅ **Symbol resolution precedence**: Warning logs for ambiguous cases
5. ✅ **Partial quarantine tracking**: Enhanced IngestionResult with metrics

### Added Clarifications

1. ✅ **Per-table schema contracts**: `REQUIRED_PARSED_COLUMNS` for each table
2. ✅ **ZIP file handling**: Single-CSV only (v1), multi-member deferred
3. ✅ **Empty file expectations**: Return empty DataFrame, service logs INFO
4. ✅ **Determinism contract**: Explicit requirements in protocol docstrings

### Enhanced Testing

1. ✅ **Utility tests**: Line numbering for all formats (header, headerless, gzip, ZIP)
2. ✅ **Multi-symbol tests**: Exchange requirement, partial quarantine, ambiguity warnings
3. ✅ **Integration tests**: End-to-end with partial ingestion tracking
4. ✅ **Performance tests**: Batch vs. individual, memory footprint

### API Improvements

1. ✅ **Vendor capability flag**: `supports_multi_symbol()` method
2. ✅ **Schema validation**: Per-table required columns with validation helpers
3. ✅ **Partial ingestion tracking**: `IngestionResult` fields for audit
4. ✅ **Warning logs**: Ambiguous parameter/column precedence

### Deferred to Future

1. ⏭️ **Multi-exchange files**: Deferred to v2 (requires parser to return both columns)
2. ⏭️ **Streaming/chunking**: Deferred until memory pressure observed (>10M rows)
3. ⏭️ **Multi-member ZIPs**: Deferred to v2 (treat each CSV as separate file)

## References

- [Vendor Plugin System](../vendor-plugin-system.md)
- [CLAUDE.md](../../CLAUDE.md)
- [Architecture Design](./design.md)
- [Storage IO Design](./storage-io-design.md)

## Appendix A: Example Multi-Symbol File

**Bronze File**: `/lake/bronze/vendorx/all_trades_2024-05-01.csv`

```csv
timestamp,exchange,symbol,price,quantity,side
1714521600000000,binance,BTCUSDT,60000.0,1.5,buy
1714521601000000,binance,ETHUSDT,3000.0,10.0,sell
1714521602000000,binance,BTCUSDT,60001.0,2.0,buy
1714521603000000,coinbase,BTC-USD,60002.0,0.5,sell
```

**After `read_and_parse()`**:

```python
pl.DataFrame({
    "file_line_number": [2, 3, 4, 5],  # 1-indexed (row 1 is header)
    "exchange_symbol": ["BTCUSDT", "ETHUSDT", "BTCUSDT", "BTCUSD"],  # Normalized
    "ts_local_us": [1714521600000000, 1714521601000000, ...],
    "price_px": [60000.0, 3000.0, 60001.0, 60002.0],
    "qty": [1.5, 10.0, 2.0, 0.5],
    "side": [0, 1, 0, 1],  # 0=buy, 1=sell
})
```

**After Symbol Resolution**:

```python
# Joined with dim_symbol on (exchange_id, exchange_symbol, ts_local_us)
pl.DataFrame({
    "file_line_number": [2, 3, 4, 5],
    "exchange_symbol": ["BTCUSDT", "ETHUSDT", "BTCUSDT", "BTCUSD"],
    "symbol_id": [12345, 12346, 12345, 12347],  # Per-row resolution
    "ts_local_us": [...],
    "price_px": [...],
    "qty": [...],
    "side": [...],
})
```

**After Fixed-Point Encoding**:

```python
# Each row uses its symbol's tick_size/lot_size
pl.DataFrame({
    "symbol_id": [12345, 12346, 12345, 12347],
    "px_int": [60000000, 3000000, 60001000, 60002000],  # Different increments!
    "qty_int": [15000, 100000, 20000, 5000],             # Different increments!
    # ... other columns
})
```

**Written to Delta Lake**:

```
/lake/silver/trades/
  exchange=binance/
    date=2024-05-01/
      part-00000.parquet  # Contains BTCUSDT and ETHUSDT rows
  exchange=coinbase/
    date=2024-05-01/
      part-00001.parquet  # Contains BTCUSD rows
```

Delta Lake automatically partitions by `(exchange, date)` even though all data came from one file!
