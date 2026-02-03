# Vendor Plugin System

**Last Updated:** 2026-02-03
**Status:** Production Ready

## Overview

The vendor plugin system provides a unified, extensible architecture for integrating data from multiple vendors. Each vendor is a self-contained plugin that declares its capabilities and provides the necessary code to acquire and parse data.

**Key Design Principles:**
- **Separation of Concerns:** Data acquisition (Bronze) vs data processing (Silver)
- **Runtime Dispatch:** Services are vendor-agnostic, plugins are vendor-specific
- **Capability-Based:** Vendors declare what they support (download, parsers, prehooks)
- **Auto-Discovery:** Plugins register automatically on import

## Architecture

### Directory Structure

```
pointline/io/vendors/
├── __init__.py              # Auto-discovery and exports
├── base.py                  # VendorPlugin protocol (interface)
├── registry.py              # Vendor and parser registry
├── tardis/                  # Tardis.dev plugin
│   ├── __init__.py          # Auto-registration
│   ├── plugin.py            # TardisVendor class
│   ├── client.py            # Download client
│   ├── datasets.py          # Dataset download utilities
│   ├── mapper.py            # dim_symbol mapping
│   └── parsers/             # Data format parsers
│       ├── trades.py
│       ├── quotes.py
│       ├── book_snapshots.py
│       └── derivative_ticker.py
├── binance_vision/          # Binance historical data plugin
│   ├── __init__.py
│   ├── plugin.py
│   ├── aliases.py           # Symbol normalization
│   ├── datasets.py          # Public data downloads
│   └── parsers/
│       └── klines.py
├── quant360/                # Chinese stock L3 data plugin
│   ├── __init__.py
│   ├── plugin.py
│   ├── reorganize.py        # Archive preprocessing
│   └── parsers/
│       ├── l3_orders.py
│       ├── l3_ticks.py
│       └── utils.py
├── coingecko/               # Market data API plugin
│   ├── __init__.py
│   ├── plugin.py
│   └── client.py
└── tushare/                 # Chinese stock API plugin
    ├── __init__.py
    ├── plugin.py
    ├── client.py
    └── stock_basic_cn.py
```

### VendorPlugin Protocol

Every vendor plugin must implement the `VendorPlugin` protocol:

```python
from typing import Protocol, Callable, Any
from pathlib import Path
import polars as pl

class VendorPlugin(Protocol):
    """Protocol defining the interface for vendor plugins."""

    # Identity
    name: str                    # "tardis", "binance_vision", etc.
    display_name: str            # "Tardis.dev", "Binance Vision", etc.

    # Capabilities (what this vendor can do)
    supports_parsers: bool       # Can parse data files?
    supports_download: bool      # Can download data from API?
    supports_prehooks: bool      # Needs preprocessing (e.g., unzip)?

    # Methods
    def get_parsers(self) -> dict[str, Callable[[pl.DataFrame], pl.DataFrame]]:
        """Return {data_type: parser_function}"""
        ...

    def get_download_client(self) -> Any:
        """Return download client (e.g., TardisClient)"""
        ...

    def run_prehook(self, bronze_root: Path) -> None:
        """Run preprocessing before ingestion"""
        ...
```

## Vendor Capability Matrix

| Vendor         | Download | Prehooks | Parsers | Bronze Source         | Primary Use Case            |
|----------------|----------|----------|---------|----------------------|-----------------------------|
| **tardis**     | ✅       | ❌       | ✅ (4)  | We download          | Crypto trades/quotes/books  |
| **binance_vision** | ✅   | ❌       | ✅ (1)  | We download          | Historical OHLCV klines     |
| **quant360**   | ❌       | ✅       | ✅ (2)  | External delivery    | SZSE/SSE Level 3 orderbook  |
| **coingecko**  | ✅       | ❌       | ❌      | We fetch API         | Market cap, price metadata  |
| **tushare**    | ✅       | ❌       | ❌      | We fetch API         | Chinese stock fundamentals  |

**Legend:**
- ✅ Supported
- ❌ Not supported
- (N) = Number of parsers

## Pipeline Workflows

### Type 1: Self-Download Vendors (Tardis, Binance Vision)

**Assumption:** We actively download data from vendor APIs to Bronze layer.

#### Workflow Diagram

```
┌──────────────┐
│  Vendor API  │
└──────┬───────┘
       │ download_tardis_datasets()
       │ (or download_binance_klines)
       ↓
┌──────────────────────────────────────────────┐
│  Bronze Layer                                 │
│  bronze/tardis/exchange=X/type=Y/date=Z/     │
│  *.csv.gz (immutable, checksummed)           │
└──────────────────┬───────────────────────────┘
                   │ parse_tardis_trades_csv()
                   │ (runtime dispatch via get_parser)
                   ↓
┌──────────────────────────────────────────────┐
│  Silver Layer                                 │
│  silver/trades/exchange=X/date=Z/            │
│  Delta Lake (typed, encoded, validated)      │
└──────────────────────────────────────────────┘
```

#### CLI Commands

```bash
# Step 1: Download from vendor API → Bronze
pointline download --exchange binance-futures \
  --data-types trades,quotes \
  --symbols BTCUSDT,ETHUSDT \
  --from-date 2024-05-01 --to-date 2024-05-31 \
  --api-key YOUR_KEY

# Downloads to:
# bronze/tardis/exchange=binance-futures/type=trades/date=2024-05-01/symbol=BTCUSDT/*.csv.gz

# Step 2: Discover pending files
pointline bronze discover --vendor tardis --pending-only

# Step 3: Ingest Bronze → Silver
pointline ingest run --table trades --exchange binance-futures --date 2024-05-01

# Step 4: Validate (optional)
pointline validate trades --exchange binance-futures --date 2024-05-01
```

#### Code Example

```python
from pointline.io.vendors.registry import get_vendor

# Get vendor plugin
tardis = get_vendor("tardis")

# Check capabilities
assert tardis.supports_download == True
assert tardis.supports_parsers == True
assert tardis.supports_prehooks == False

# Get download client
client = tardis.get_download_client()  # Returns TardisClient()

# Get parsers
parsers = tardis.get_parsers()
# Returns {
#   "trades": parse_tardis_trades_csv,
#   "quotes": parse_tardis_quotes_csv,
#   "book_snapshot_25": parse_tardis_book_snapshots_csv,
#   "derivative_ticker": parse_tardis_derivative_ticker_csv
# }

# Use parser
parse_trades = parsers["trades"]
parsed_df = parse_trades(raw_csv_df)
```

---

### Type 2: External Bronze Vendors (Quant360)

**Assumption:** Vendor delivers data externally (archives, hard drives, cloud storage). We must reorganize into Hive-partitioned structure before ingestion.

#### Workflow Diagram

```
┌──────────────────────────────┐
│  External Archive             │
│  order_new_STK_SZ_20240930.7z│
│  (delivered by vendor)        │
└──────────────┬────────────────┘
               │ reorganize_quant360_archives()
               │ (PREHOOK: extract + partition)
               ↓
┌──────────────────────────────────────────────┐
│  Bronze Layer                                 │
│  bronze/quant360/exchange=szse/type=l3_orders│
│  /date=2024-09-30/symbol=000001/*.csv.gz     │
└──────────────────┬───────────────────────────┘
                   │ parse_quant360_orders_csv()
                   ↓
┌──────────────────────────────────────────────┐
│  Silver Layer                                 │
│  silver/szse_l3_orders/exchange=szse/date=Z/ │
└──────────────────────────────────────────────┘
```

#### CLI Commands

```bash
# Step 1: Vendor delivers archives to staging area (manual)
# User places files in: ~/data/archives/quant360/
# Files like: order_new_STK_SZ_20240930.7z, tick_STK_SZ_20240930.7z

# Step 2: Reorganize archives → Bronze (Prehook)
pointline bronze reorganize \
  --vendor quant360 \
  --source-dir ~/data/archives/quant360 \
  --bronze-root ~/data/lake/bronze

# Extracts and organizes to:
# bronze/quant360/exchange=szse/type=l3_orders/date=2024-09-30/symbol=000001/000001.csv.gz

# Step 3: Discover reorganized files
pointline bronze discover --vendor quant360 --pending-only

# Step 4: Ingest Bronze → Silver
pointline ingest run --table l3_orders --exchange szse --date 2024-09-30
pointline ingest run --table l3_ticks --exchange szse --date 2024-09-30
```

#### Code Example

```python
from pointline.io.vendors.registry import get_vendor
from pathlib import Path

# Get vendor plugin
quant360 = get_vendor("quant360")

# Check capabilities
assert quant360.supports_download == False  # No API access
assert quant360.supports_parsers == True
assert quant360.supports_prehooks == True   # Needs archive extraction!

# Run prehook (reorganize archives)
bronze_root = Path("/data/lake/bronze")
quant360.run_prehook(bronze_root)
# This extracts .7z archives into Hive-partitioned structure

# Get parsers
parsers = quant360.get_parsers()
# Returns {
#   "l3_orders": parse_quant360_orders_csv,
#   "l3_ticks": parse_quant360_ticks_csv
# }

# Use parser
parse_orders = parsers["l3_orders"]
parsed_df = parse_orders(raw_csv_df)
```

---

### Type 3: API-Only Vendors (CoinGecko, Tushare)

**Assumption:** We fetch metadata from APIs directly to dimension tables. No Bronze layer, no Silver layer—API responses go straight to dimension tables.

#### Workflow Diagram

```
┌──────────────────┐
│   Vendor API     │
│   (CoinGecko,    │
│    Tushare)      │
└────────┬─────────┘
         │ Direct API calls
         │ (no file downloads)
         ↓
┌──────────────────────────────┐
│  Dimension Tables             │
│  - dim_symbol                 │
│  - dim_asset_stats            │
│  (no Bronze, no Silver)       │
└──────────────────────────────┘
```

#### CLI Commands

```bash
# CoinGecko: Fetch market data → dim_asset_stats
pointline dim-asset-stats sync --provider coingecko

# Tushare: Fetch Chinese stock info → dim_symbol
pointline dim-symbol sync-tushare --exchange szse --date 2024-09-30
```

#### Code Example

```python
from pointline.io.vendors.registry import get_vendor

# Get CoinGecko plugin
coingecko = get_vendor("coingecko")

# Check capabilities
assert coingecko.supports_download == True   # Has API client
assert coingecko.supports_parsers == False   # No file parsing
assert coingecko.supports_prehooks == False

# Get API client
client = coingecko.get_download_client()  # Returns CoinGeckoClient

# Use client directly for API calls
coin_data = client.get_coin_by_id("bitcoin")
market_data = client.get_market_chart("bitcoin", vs_currency="usd", days=30)

# No parsers available (raises NotImplementedError)
# coingecko.get_parsers()  # ❌ Error!
```

---

## Generic Ingestion Pipeline (Bronze → Silver)

**Key Insight:** Regardless of how Bronze is populated, the ingestion pipeline is **identical** for all vendors.

### Ingestion Flow

```python
from pointline.services.generic_ingestion_service import GenericIngestionService
from pointline.io.vendors import get_parser

class GenericIngestionService:
    def ingest_file(self, meta: BronzeFileMetadata) -> IngestionResult:
        """Vendor-agnostic ingestion with runtime parser dispatch."""

        # 1. Read Bronze CSV file
        raw_df = self._read_bronze_csv(bronze_path)

        # 2. Get vendor-specific parser (RUNTIME DISPATCH)
        parser = get_parser(meta.vendor, meta.data_type)
        parsed_df = parser(raw_df)

        # 3-12. Standard pipeline (SAME FOR ALL VENDORS)
        dim_symbol = self.dim_symbol_repo.read_all()
        exchange_id = self._resolve_exchange_id(meta.exchange)

        # Check quarantine (symbol coverage validation)
        is_valid, error_msg = self._check_quarantine(meta, dim_symbol, exchange_id, parsed_df)
        if not is_valid:
            return IngestionResult(..., error_message=error_msg)

        # Resolve symbol IDs (SCD Type 2 as-of join)
        resolved_df = self.strategy.resolve_symbol_ids(parsed_df, dim_symbol, exchange_id, ...)

        # Encode fixed-point (price/qty → integers)
        encoded_df = self.strategy.encode_fixed_point(resolved_df, dim_symbol)

        # Add lineage (file_id, file_line_number)
        lineage_df = self._add_lineage(encoded_df, file_id)

        # Add metadata (exchange, exchange_id, date)
        final_df = self._add_metadata(lineage_df, meta.exchange, exchange_id)

        # Normalize schema (enforce canonical types)
        normalized_df = self.strategy.normalize_schema(final_df)

        # Validate (quality checks)
        validated_df = self.strategy.validate(normalized_df)

        # Write to Delta Lake
        self.repo.append(validated_df)

        return IngestionResult(row_count=validated_df.height, ...)
```

**Important:** Only Step 2 (parsing) varies by vendor. Steps 1, 3-12 are identical for all vendors.

---

## How to Add a New Vendor

### Step 1: Create Plugin Directory

```bash
mkdir -p pointline/io/vendors/my_vendor/parsers
touch pointline/io/vendors/my_vendor/__init__.py
touch pointline/io/vendors/my_vendor/plugin.py
touch pointline/io/vendors/my_vendor/parsers/__init__.py
```

### Step 2: Implement Plugin Class

**File:** `pointline/io/vendors/my_vendor/plugin.py`

```python
"""My Vendor plugin for XYZ data."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl


class MyVendorPlugin:
    """My Vendor plugin for market data."""

    # Identity
    name = "my_vendor"
    display_name = "My Data Vendor"

    # Capabilities (declare what you support)
    supports_parsers = True      # We parse CSV files
    supports_download = True     # We have download API
    supports_prehooks = False    # Files already in Hive structure

    def get_parsers(self) -> dict[str, Callable[[pl.DataFrame], pl.DataFrame]]:
        """Get all parsers provided by this vendor."""
        from pointline.io.vendors.my_vendor.parsers import (
            parse_my_vendor_trades_csv,
            parse_my_vendor_quotes_csv,
        )

        return {
            "trades": parse_my_vendor_trades_csv,
            "quotes": parse_my_vendor_quotes_csv,
        }

    def get_download_client(self) -> Any:
        """Get download client for this vendor."""
        from pointline.io.vendors.my_vendor.client import MyVendorClient
        return MyVendorClient()

    def run_prehook(self, bronze_root: Path) -> None:
        """Not supported for this vendor."""
        raise NotImplementedError(f"{self.name} does not support prehooks")
```

### Step 3: Implement Parsers

**File:** `pointline/io/vendors/my_vendor/parsers/trades.py`

```python
"""My Vendor trades parser."""

import polars as pl
from pointline.io.vendors.registry import register_parser


@register_parser(vendor="my_vendor", data_type="trades")
def parse_my_vendor_trades_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse My Vendor trades CSV into normalized format.

    My Vendor CSV format:
    - timestamp_ms: milliseconds since epoch
    - symbol: trading pair (e.g., "BTC-USD")
    - price: decimal price
    - quantity: decimal quantity
    - side: "buy" or "sell"

    Returns:
        DataFrame with normalized columns:
        - ts_local_us (i64): timestamp in microseconds
        - ts_exch_us (i64): exchange timestamp (nullable)
        - trade_id (str): trade identifier (nullable)
        - side (u8): 0=buy, 1=sell, 2=unknown
        - price (f64): trade price
        - qty (f64): trade quantity
    """
    # Parse timestamp (convert ms → us)
    result = df.with_columns([
        (pl.col("timestamp_ms") * 1000).cast(pl.Int64).alias("ts_local_us"),
        pl.lit(None, dtype=pl.Int64).alias("ts_exch_us"),
        pl.lit(None, dtype=pl.Utf8).alias("trade_id"),
    ])

    # Parse side
    result = result.with_columns([
        pl.when(pl.col("side") == "buy")
          .then(pl.lit(0, dtype=pl.UInt8))
          .when(pl.col("side") == "sell")
          .then(pl.lit(1, dtype=pl.UInt8))
          .otherwise(pl.lit(2, dtype=pl.UInt8))
          .alias("side")
    ])

    # Parse price and quantity
    result = result.with_columns([
        pl.col("price").cast(pl.Float64).alias("price"),
        pl.col("quantity").cast(pl.Float64).alias("qty"),
    ])

    # Select canonical columns
    return result.select([
        "ts_local_us",
        "ts_exch_us",
        "trade_id",
        "side",
        "price",
        "qty",
    ])
```

### Step 4: Export Parsers

**File:** `pointline/io/vendors/my_vendor/parsers/__init__.py`

```python
"""My Vendor parsers."""

from pointline.io.vendors.my_vendor.parsers.trades import parse_my_vendor_trades_csv
from pointline.io.vendors.my_vendor.parsers.quotes import parse_my_vendor_quotes_csv

__all__ = [
    "parse_my_vendor_trades_csv",
    "parse_my_vendor_quotes_csv",
]
```

### Step 5: Auto-Register Plugin

**File:** `pointline/io/vendors/my_vendor/__init__.py`

```python
"""My Vendor plugin package."""

# Import plugin class
from pointline.io.vendors.my_vendor.plugin import MyVendorPlugin

# Import parsers (triggers registration)
from pointline.io.vendors.my_vendor.parsers import (
    parse_my_vendor_trades_csv,
    parse_my_vendor_quotes_csv,
)

# Register plugin
from pointline.io.vendors.registry import register_vendor

register_vendor(MyVendorPlugin())

__all__ = [
    "MyVendorPlugin",
    "parse_my_vendor_trades_csv",
    "parse_my_vendor_quotes_csv",
]
```

### Step 6: Update Top-Level Vendors Package

**File:** `pointline/io/vendors/__init__.py`

```python
# Add new vendor import
from pointline.io.vendors.my_vendor import MyVendorPlugin

__all__ = [
    # ... existing vendors ...
    "MyVendorPlugin",  # Add here
]
```

### Step 7: Test Your Plugin

```python
from pointline.io.vendors.registry import get_vendor, list_vendors

# Verify registration
assert "my_vendor" in list_vendors()

# Get plugin
my_vendor = get_vendor("my_vendor")

# Check capabilities
assert my_vendor.supports_parsers == True
assert my_vendor.supports_download == True

# Get parsers
parsers = my_vendor.get_parsers()
assert "trades" in parsers
assert "quotes" in parsers

# Test parser
import polars as pl
sample_df = pl.DataFrame({
    "timestamp_ms": [1620000000000, 1620000001000],
    "symbol": ["BTC-USD", "BTC-USD"],
    "price": [50000.0, 50001.0],
    "quantity": [0.1, 0.2],
    "side": ["buy", "sell"],
})

parse_trades = parsers["trades"]
parsed = parse_trades(sample_df)

assert "ts_local_us" in parsed.columns
assert "price" in parsed.columns
assert parsed.height == 2
```

### Step 8: Use via CLI

```bash
# Download (if supports_download=True)
pointline download --vendor my_vendor \
  --exchange my_exchange \
  --data-types trades,quotes \
  --symbols BTC-USD,ETH-USD \
  --from-date 2024-05-01

# Discover bronze files
pointline bronze discover --vendor my_vendor --pending-only

# Ingest to silver
pointline ingest run --table trades --exchange my_exchange --date 2024-05-01
```

---

## Best Practices

### 1. Parser Implementation

**DO:**
- ✅ Handle missing columns gracefully (use nullable types)
- ✅ Parse timestamps as `Int64` microseconds since epoch
- ✅ Use vectorized operations (avoid `map_elements`)
- ✅ Return only canonical columns
- ✅ Include docstrings with CSV format description

**DON'T:**
- ❌ Assume all columns exist
- ❌ Use string timestamps (always convert to `Int64` microseconds)
- ❌ Return extra columns (breaks schema validation)
- ❌ Use Python loops (use Polars expressions)

### 2. Client Implementation

**DO:**
- ✅ Implement retries with exponential backoff
- ✅ Respect vendor rate limits
- ✅ Log download progress
- ✅ Verify checksums when provided
- ✅ Write to Bronze in Hive-partitioned structure

**Example Hive Structure:**
```
bronze/{vendor}/exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/*.csv.gz
```

### 3. Prehook Implementation

**DO:**
- ✅ Support `--dry-run` mode
- ✅ Skip already-processed files (idempotent)
- ✅ Log progress (files processed, errors)
- ✅ Validate archive integrity before extraction
- ✅ Write to temp location, then atomic move

**DON'T:**
- ❌ Modify existing Bronze files
- ❌ Fail silently on errors
- ❌ Leave partial extractions

### 4. Capability Flags

Choose capability flags carefully:

| Scenario | Download | Prehooks | Parsers |
|----------|----------|----------|---------|
| We download from API | ✅ | ❌ | ✅ |
| External delivers Hive files | ❌ | ❌ | ✅ |
| External delivers archives | ❌ | ✅ | ✅ |
| API-only (no files) | ✅ | ❌ | ❌ |

---

## Troubleshooting

### Plugin Not Discovered

**Symptom:**
```python
>>> list_vendors()
['tardis', 'binance_vision']  # my_vendor missing!
```

**Solution:**
1. Check plugin is imported in `pointline/io/vendors/__init__.py`
2. Verify `register_vendor()` is called in plugin's `__init__.py`
3. Check for import errors: `python -c "import pointline.io.vendors.my_vendor"`

### Parser Not Found

**Symptom:**
```python
>>> get_parser("my_vendor", "trades")
KeyError: "No parser registered for vendor=my_vendor, data_type=trades"
```

**Solution:**
1. Check `@register_parser` decorator is present
2. Verify parser is imported in plugin's `get_parsers()` method
3. Check data_type matches exactly (case-sensitive)

### Ingestion Fails with "No parser registered"

**Symptom:**
```bash
$ pointline ingest run --table trades --vendor my_vendor --date 2024-05-01
Error: No parser registered for vendor=my_vendor, data_type=trades
```

**Solution:**
1. Ensure `supports_parsers = True` in plugin
2. Verify `get_parsers()` returns correct data_type
3. Check Bronze file metadata has correct vendor name

---

## Related Documentation

- [Architecture Overview](./architecture/design.md)
- [Schema Reference](./schemas.md)
- [Bronze Layer Spec](./bronze-layer.md)
- [Silver Layer Spec](./silver-layer.md)
- [CLI Reference](./cli-reference.md)

---

## Changelog

### 2026-02-03: Initial Release
- Vendor plugin protocol defined
- 5 vendors migrated (tardis, binance_vision, quant360, coingecko, tushare)
- Generic ingestion service with runtime dispatch
- Auto-discovery and registration system
- ~3,750 lines of duplicated code removed
