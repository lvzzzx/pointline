# Vendor Plugin Quick Reference

**For:** Developers adding new vendors or working with existing ones

## Vendor Discovery

```python
from pointline.io.vendors.registry import list_vendors, get_vendor

# List all vendors
vendors = list_vendors()
# → ['binance_vision', 'coingecko', 'quant360', 'tardis', 'tushare']

# Get specific vendor
tardis = get_vendor("tardis")

# Check capabilities
print(f"Parsers: {tardis.supports_parsers}")      # True
print(f"Download: {tardis.supports_download}")    # True
print(f"Prehooks: {tardis.supports_prehooks}")    # False
```

## Parser Discovery

```python
from pointline.io.vendors import get_parser, list_supported_combinations

# List all (vendor, data_type) combinations
combos = list_supported_combinations()
# → [('tardis', 'trades'), ('tardis', 'quotes'), ...]

# Get specific parser
parse_trades = get_parser("tardis", "trades")
parsed_df = parse_trades(raw_csv_df)
```

## Vendor Capability Matrix

| Vendor | Download | Prehooks | Parsers | Use Case |
|--------|----------|----------|---------|----------|
| tardis | ✅ | ❌ | ✅ (4) | Crypto trades/quotes/books |
| binance_vision | ✅ | ❌ | ✅ (1) | Historical OHLCV |
| quant360 | ❌ | ✅ | ✅ (2) | SZSE/SSE L3 orderbook |
| coingecko | ✅ | ❌ | ❌ | Market data API |
| tushare | ✅ | ❌ | ❌ | Chinese stock API |

## Common Workflows

### Workflow 1: Self-Download Vendors (Tardis, Binance)

```bash
# Download → Bronze → Silver
pointline download --exchange binance-futures --data-types trades --symbols BTCUSDT --from-date 2024-05-01
pointline bronze discover --vendor tardis --pending-only
pointline ingest run --table trades --exchange binance-futures --date 2024-05-01
```

### Workflow 2: External Bronze Vendors (Quant360)

```bash
# Reorganize → Bronze → Silver
pointline bronze reorganize --vendor quant360 --source-dir ~/archives --bronze-root ~/data/bronze
pointline bronze discover --vendor quant360 --pending-only
pointline ingest run --table l3_orders --exchange szse --date 2024-09-30
```

### Workflow 3: API-Only Vendors (CoinGecko, Tushare)

```bash
# Direct API → Dimension Tables (no Bronze/Silver)
pointline dim-asset-stats sync --provider coingecko
pointline dim-symbol sync-tushare --exchange szse
```

## Adding a New Vendor (Minimal Example)

### 1. Create Directory

```bash
mkdir -p pointline/io/vendors/my_vendor/parsers
```

### 2. Create Plugin (`plugin.py`)

```python
class MyVendorPlugin:
    name = "my_vendor"
    display_name = "My Vendor"
    supports_parsers = True
    supports_download = True
    supports_prehooks = False

    def get_parsers(self):
        from pointline.io.vendors.my_vendor.parsers import parse_my_vendor_trades_csv
        return {"trades": parse_my_vendor_trades_csv}

    def get_download_client(self):
        from pointline.io.vendors.my_vendor.client import MyVendorClient
        return MyVendorClient()

    def run_prehook(self, bronze_root):
        raise NotImplementedError(f"{self.name} does not support prehooks")
```

### 3. Create Parser (`parsers/trades.py`)

```python
import polars as pl
from pointline.io.vendors.registry import register_parser

@register_parser(vendor="my_vendor", data_type="trades")
def parse_my_vendor_trades_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse My Vendor trades CSV."""
    return df.select([
        (pl.col("timestamp_ms") * 1000).cast(pl.Int64).alias("ts_local_us"),
        pl.lit(None, dtype=pl.Int64).alias("ts_exch_us"),
        pl.lit(None, dtype=pl.Utf8).alias("trade_id"),
        pl.when(pl.col("side") == "buy").then(0).otherwise(1).cast(pl.UInt8).alias("side"),
        pl.col("price").cast(pl.Float64).alias("price"),
        pl.col("quantity").cast(pl.Float64).alias("qty"),
    ])
```

### 4. Auto-Register (`__init__.py`)

```python
from pointline.io.vendors.my_vendor.plugin import MyVendorPlugin
from pointline.io.vendors.my_vendor.parsers import parse_my_vendor_trades_csv
from pointline.io.vendors.registry import register_vendor

register_vendor(MyVendorPlugin())
```

### 5. Test

```python
from pointline.io.vendors import get_vendor, get_parser

# Verify registration
assert "my_vendor" in list_vendors()

# Get plugin
vendor = get_vendor("my_vendor")
assert vendor.supports_parsers == True

# Get parser
parse_trades = get_parser("my_vendor", "trades")
```

## Parser Requirements

All parsers must return a DataFrame with these canonical columns:

### Trades
```python
{
    "ts_local_us": pl.Int64,      # Local timestamp (microseconds)
    "ts_exch_us": pl.Int64,       # Exchange timestamp (nullable)
    "trade_id": pl.Utf8,          # Trade ID (nullable)
    "side": pl.UInt8,             # 0=buy, 1=sell, 2=unknown
    "price": pl.Float64,          # Trade price
    "qty": pl.Float64,            # Trade quantity
}
```

### Quotes
```python
{
    "ts_local_us": pl.Int64,      # Local timestamp
    "ts_exch_us": pl.Int64,       # Exchange timestamp
    "bid_price": pl.Float64,      # Best bid price (nullable)
    "bid_amount": pl.Float64,     # Best bid amount (nullable)
    "ask_price": pl.Float64,      # Best ask price (nullable)
    "ask_amount": pl.Float64,     # Best ask amount (nullable)
}
```

## CLI Commands

```bash
# Download (if vendor supports it)
pointline download --vendor {vendor} --exchange {exchange} --data-types {types} --symbols {symbols} --from-date {date}

# Reorganize archives (if vendor needs prehook)
pointline bronze reorganize --vendor {vendor} --source-dir {dir} --bronze-root {root}

# Discover bronze files
pointline bronze discover --vendor {vendor} --pending-only

# Ingest bronze → silver
pointline ingest run --table {table} --exchange {exchange} --date {date}

# Validate silver data
pointline validate {table} --exchange {exchange} --date {date}
```

## Full Documentation

See [Vendor Plugin System](./vendor-plugin-system.md) for complete documentation including:
- Architecture details
- All workflow types with diagrams
- Complete implementation guide
- Best practices
- Troubleshooting
