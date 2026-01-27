# Implementation Plan: Quant360 SZSE Level 3 Order Book Support

**Status:** Planning
**Created:** 2026-01-27
**Target:** Add full Level 3 (order-by-order) order book reconstruction for Shenzhen Stock Exchange

## Overview

Add support for Quant360 SZSE Level 2 data, which provides **Level 3 order book** reconstruction capability through two event streams:
- **Order Stream:** New order placements (limit and market orders)
- **Tick Stream:** Executions (fills) and cancellations

This is fundamentally different from the existing Tardis L2 updates (which are aggregated price-level snapshots). SZSE L3 tracks individual orders by ID.

## Architecture Decision

### Level 3 vs Level 2 Storage

**Key Difference:**
- **L2 (existing Tardis):** Aggregated by price level: `{price_int: 50000, size_int: 1000}` (all orders at this price)
- **L3 (SZSE):** Order-by-order: `{order_id: 12345, price_int: 50000, qty_int: 100}` (individual orders tracked)

**Storage Strategy:**
Store raw events in Silver layer, reconstruct book in application layer (not in Gold). Rationale:
- L3 books can be huge (thousands of orders per symbol)
- Applications need different views (some need full L3, others just L2 aggregation, others just trades)
- Raw events are compact and allow flexible reconstruction
- Follows the existing pattern: store canonical events, derive on-demand

## Phase 0: Pre-requisite - Populate dim_symbol with SZSE Symbols (via Tushare)

**Critical Dependency:** L3 ingestion requires symbol_id resolution via dim_symbol.

**Data Source:** [Tushare Pro API](https://tushare.pro/document/2?doc_id=25) - Chinese financial data provider

### 0.1 Tushare Integration Overview

**API:** `stock_basic` endpoint
- Provides stock code, name, listing date, delisting date
- Requires: 2000 points minimum (basic tier)
- Recommended: Call once and cache locally

**Authentication:**
```python
import tushare as ts
ts.set_token('your_token_here')
pro = ts.pro_api()
```

**Coverage:**
- **SZSE (Shenzhen Stock Exchange):** .SZ suffix
- **SSE (Shanghai Stock Exchange):** .SH suffix (bonus - can support both!)
- **BSE (Beijing Stock Exchange):** .BJ suffix (optional)

### 0.2 SZSE Symbol Characteristics

**Exchange Symbol Format:**
- Tushare format: `000001.SZ` (ts_code)
- Pointline format: `000001` (exchange_symbol, without suffix)
- 6-digit numeric codes (e.g., `000001`, `000002`, `300750`)
- Prefix indicates board:
  - `000xxx` - Main Board
  - `001xxx` - Main Board (new)
  - `002xxx` - SME Board (Small and Medium Enterprise)
  - `300xxx` - ChiNext (Growth Enterprise Market)
  - `301xxx` - ChiNext (new)

**Example Symbols:**
- `000001.SZ` → Ping An Bank (平安银行)
- `000002.SZ` → Vanke A (万科A)
- `300750.SZ` → Contemporary Amperex Technology (宁德时代)

### 0.3 Tushare to dim_symbol Field Mapping

| Tushare Field | Type | dim_symbol Field | Transformation |
|---|---|---|---|
| `ts_code` | str | `exchange_symbol` | Remove `.SZ` suffix: `"000001.SZ"` → `"000001"` |
| `exchange` | str | `exchange`, `exchange_id` | Map: `"SZSE"` → `"szse"` (30), `"SSE"` → `"sse"` (31) |
| `name` | str | `base_asset` | Use as-is (Chinese name) |
| - | - | `quote_asset` | Set to `"CNY"` |
| `list_status` | str | `valid_until_ts` | If `"D"` (delisted), use `delist_date` |
| `list_date` | str | `valid_from_ts` | Parse `YYYYMMDD` → microseconds |
| `delist_date` | str | `valid_until_ts` | Parse `YYYYMMDD` → microseconds (if delisted) |
| - | - | `asset_type` | Set to `0` (spot stocks) |
| - | - | `tick_size` | Default: `0.01` CNY |
| - | - | `lot_size` | Default: `100` shares |
| - | - | `price_increment` | Set to `0.01` (tick-based encoding) |
| - | - | `amount_increment` | Set to `100` (lot-based encoding) |
| - | - | `contract_size` | Set to `1.0` |

**Notes:**
- Tushare doesn't provide tick_size/lot_size - use standard defaults
- SZSE and SSE both use: tick_size=0.01 CNY, lot_size=100 shares
- Special cases (ST stocks, etc.) can be handled with manual updates later

### 0.4 Implementation: Tushare Client Module

**File:** `pointline/io/tushare/client.py` (NEW)

```python
"""Tushare API client for Chinese stock data."""

from __future__ import annotations
import os
from datetime import datetime
from pathlib import Path

import polars as pl
import tushare as ts


class TushareClient:
    """Client for Tushare Pro API."""

    def __init__(self, token: str | None = None):
        """
        Initialize Tushare client.

        Args:
            token: Tushare API token (or use TUSHARE_TOKEN env var)
        """
        token = token or os.getenv("TUSHARE_TOKEN")
        if not token:
            raise ValueError(
                "Tushare token required. Set TUSHARE_TOKEN env var or pass token parameter."
            )
        ts.set_token(token)
        self.pro = ts.pro_api()

    def get_stock_basic(
        self,
        exchange: str | None = None,
        list_status: str = "L",
    ) -> pl.DataFrame:
        """
        Fetch basic stock information.

        Args:
            exchange: Filter by exchange ("SSE", "SZSE", "BSE", or None for all)
            list_status:
                "L" = Listed (default)
                "D" = Delisted
                "P" = Suspended
                None = All

        Returns:
            Polars DataFrame with columns:
                ts_code, symbol, name, area, industry, list_date,
                delist_date, exchange, list_status
        """
        df_pandas = self.pro.stock_basic(
            exchange=exchange or "",
            list_status=list_status or "",
            fields="ts_code,symbol,name,area,industry,fullname,enname,"
                   "market,exchange,list_status,list_date,delist_date,is_hs",
        )

        # Convert to Polars
        df = pl.from_pandas(df_pandas)

        return df

    def get_szse_stocks(self, include_delisted: bool = False) -> pl.DataFrame:
        """
        Get all SZSE stocks.

        Args:
            include_delisted: Include delisted stocks

        Returns:
            DataFrame with SZSE stocks only
        """
        if include_delisted:
            df = self.get_stock_basic(exchange="SZSE", list_status=None)
        else:
            df = self.get_stock_basic(exchange="SZSE", list_status="L")

        return df

    def get_sse_stocks(self, include_delisted: bool = False) -> pl.DataFrame:
        """Get all SSE stocks."""
        if include_delisted:
            df = self.get_stock_basic(exchange="SSE", list_status=None)
        else:
            df = self.get_stock_basic(exchange="SSE", list_status="L")

        return df
```

**File:** `pointline/io/tushare/__init__.py` (NEW)

```python
from pointline.io.tushare.client import TushareClient

__all__ = ["TushareClient"]
```

### 0.5 Implementation: Symbol Sync Command

**File:** `pointline/cli/commands/dim_symbol.py` (UPDATE existing file)

Add new function:

```python
def sync_tushare_symbols(
    exchange: str = "szse",
    include_delisted: bool = False,
    token: str | None = None,
) -> int:
    """
    Sync Chinese stock symbols from Tushare to dim_symbol.

    Args:
        exchange: "szse", "sse", or "all"
        include_delisted: Include delisted stocks
        token: Tushare API token (or use TUSHARE_TOKEN env var)

    Returns:
        Number of symbols inserted/updated
    """
    from pointline.config import get_exchange_id, EXCHANGE_MAP
    from pointline.dim_symbol import scd2_upsert, read_dim_symbol_table
    from pointline.io.base_repository import BaseDeltaRepository
    from pointline.io.tushare import TushareClient
    from pointline.config import get_table_path

    # Initialize Tushare client
    client = TushareClient(token=token)

    # Fetch stocks
    if exchange.lower() == "szse":
        df = client.get_szse_stocks(include_delisted=include_delisted)
    elif exchange.lower() == "sse":
        df = client.get_sse_stocks(include_delisted=include_delisted)
    elif exchange.lower() == "all":
        df_szse = client.get_szse_stocks(include_delisted=include_delisted)
        df_sse = client.get_sse_stocks(include_delisted=include_delisted)
        df = pl.concat([df_szse, df_sse])
    else:
        raise ValueError(f"Invalid exchange: {exchange}. Use 'szse', 'sse', or 'all'.")

    # Transform to dim_symbol schema
    def parse_tushare_date(date_str: str | None) -> int:
        """Parse YYYYMMDD string to microseconds timestamp."""
        if not date_str or date_str == "":
            return 0  # Use 0 for missing dates
        dt = datetime.strptime(date_str, "%Y%m%d")
        return int(dt.timestamp() * 1_000_000)

    updates = df.with_columns([
        # Remove exchange suffix from ts_code: "000001.SZ" -> "000001"
        pl.col("symbol").alias("exchange_symbol"),

        # Map exchange name
        pl.when(pl.col("exchange") == "SZSE")
            .then(pl.lit("szse"))
            .when(pl.col("exchange") == "SSE")
            .then(pl.lit("sse"))
            .otherwise(pl.lit("unknown"))
            .alias("exchange"),

        # Map exchange_id
        pl.when(pl.col("exchange") == "SZSE")
            .then(pl.lit(30))
            .when(pl.col("exchange") == "SSE")
            .then(pl.lit(31))
            .otherwise(pl.lit(0))
            .cast(pl.Int16)
            .alias("exchange_id"),

        # Use name as base_asset
        pl.col("name").alias("base_asset"),

        # Fixed fields
        pl.lit("CNY").alias("quote_asset"),
        pl.lit(0).cast(pl.UInt8).alias("asset_type"),
        pl.lit(0.01).alias("tick_size"),
        pl.lit(100.0).alias("lot_size"),
        pl.lit(0.01).alias("price_increment"),
        pl.lit(100.0).alias("amount_increment"),
        pl.lit(1.0).alias("contract_size"),

        # Parse dates
        pl.col("list_date").map_elements(
            parse_tushare_date, return_dtype=pl.Int64
        ).alias("valid_from_ts"),

        # For delisted stocks, use delist_date as valid_until_ts
        pl.when(pl.col("list_status") == "D")
            .then(
                pl.col("delist_date").map_elements(
                    parse_tushare_date, return_dtype=pl.Int64
                )
            )
            .otherwise(pl.lit(2**63 - 1))  # Default: max timestamp
            .alias("valid_until_ts"),
    ]).select([
        "exchange_id",
        "exchange",
        "exchange_symbol",
        "base_asset",
        "quote_asset",
        "asset_type",
        "tick_size",
        "lot_size",
        "price_increment",
        "amount_increment",
        "contract_size",
        "valid_from_ts",
    ])

    # Load existing dim_symbol
    repo = BaseDeltaRepository(get_table_path("dim_symbol"))
    try:
        current_dim = read_dim_symbol_table()
    except Exception:
        # dim_symbol doesn't exist yet, bootstrap
        from pointline.dim_symbol import scd2_bootstrap
        current_dim = scd2_bootstrap(updates)
        repo.write_full(current_dim)
        return len(updates)

    # Upsert
    updated_dim = scd2_upsert(current_dim, updates)

    # Write back
    repo.write_full(updated_dim)

    return len(updates)
```

**Update CLI parser:** `pointline/cli/parser.py`

Add to symbol subcommands:

```python
symbol_sync_tushare = symbol_sub.add_parser(
    "sync-tushare",
    help="Sync symbols from Tushare API (Chinese stocks)"
)
symbol_sync_tushare.add_argument(
    "--exchange",
    default="szse",
    choices=["szse", "sse", "all"],
    help="Exchange to sync (default: szse)"
)
symbol_sync_tushare.add_argument(
    "--include-delisted",
    action="store_true",
    help="Include delisted stocks"
)
symbol_sync_tushare.add_argument(
    "--token",
    help="Tushare API token (or use TUSHARE_TOKEN env var)"
)
symbol_sync_tushare.set_defaults(func=cmd_symbol_sync_tushare)
```

### 0.6 CLI Usage

**Setup Tushare token:**
```bash
export TUSHARE_TOKEN="your_token_here"
```

**Sync SZSE symbols:**
```bash
# Sync only listed stocks (default)
pointline symbol sync-tushare --exchange szse

# Include delisted stocks
pointline symbol sync-tushare --exchange szse --include-delisted

# Sync both SZSE and SSE
pointline symbol sync-tushare --exchange all
```

**Verify symbols:**
```bash
# Search for specific symbol
pointline symbol search 000001 --exchange szse

# List all SZSE symbols
pointline symbol search --exchange szse
```

### 0.7 Configuration Updates

**File:** `pointline/config.py`

Add SSE exchange (bonus - supports both Chinese exchanges):

```python
EXCHANGE_MAP = {
    # ... existing exchanges
    "szse": 30,  # Shenzhen Stock Exchange
    "sse": 31,   # Shanghai Stock Exchange
}
```

### 0.8 Testing Symbol Resolution

After syncing, verify:

```python
from pointline.registry import find_symbol

# Find SZSE symbol
results = find_symbol(query="000001", exchange="szse")
print(results)
# Expected: symbol_id, exchange="szse", exchange_symbol="000001",
#           base_asset="平安银行"

# Find SSE symbol (if synced)
results = find_symbol(query="600000", exchange="sse")
print(results)
# Expected: Pudong Development Bank (浦发银行)
```

### 0.9 Dependencies

**Python package:**
```bash
pip install tushare
```

**Tushare account:**
1. Register at https://tushare.pro/register
2. Get API token from: https://tushare.pro/user/token
3. Requires: **2000 points minimum** (free tier available)

### 0.10 Initial Symbol Population Plan

**For MVP (Minimum Viable Product):**
1. Register Tushare account, get token (Day 1)
2. Set `TUSHARE_TOKEN` environment variable (Day 1)
3. Implement Tushare client and sync function (Days 2-3)
4. Run `pointline symbol sync-tushare --exchange szse` (Day 4)
5. Verify with `pointline symbol search --exchange szse` (Day 4)
6. Test symbol resolution in Python (Day 5)

**For Production:**
1. Sync both SZSE and SSE: `pointline symbol sync-tushare --exchange all`
2. Include delisted stocks for historical backtests: `--include-delisted`
3. Set up periodic refresh (weekly/monthly) to catch new listings
4. Monitor for metadata changes (tick size updates for ST stocks, etc.)

### 0.11 Estimated Timeline

**Phase 0 tasks:**
- [ ] Register Tushare account and obtain token (Day 1)
- [ ] Implement TushareClient class (Day 2)
- [ ] Implement sync_tushare_symbols() function (Day 3)
- [ ] Add CLI command and parser (Day 3)
- [ ] Test sync for SZSE (Day 4)
- [ ] Verify symbol resolution (Day 4-5)
- [ ] Optional: Test SSE sync (Day 5)

**Total: 1 week**

### 0.12 Advantages of Tushare

✅ **Authoritative source** - Official Chinese financial data provider
✅ **Comprehensive coverage** - SZSE, SSE, BSE (Beijing)
✅ **Historical data** - Includes delisting dates, historical listings
✅ **Easy integration** - Simple Python API
✅ **Free tier available** - 2000 points sufficient for stock_basic
✅ **Regular updates** - New listings automatically available
✅ **Additional data** - Can extend to fetch fundamentals, daily indicators, etc.

### 0.13 Limitations & Mitigations

**Limitation 1:** Tushare doesn't provide tick_size/lot_size
- **Mitigation:** Use standard defaults (0.01 CNY, 100 shares) - accurate for 99% of stocks
- **Future:** Can implement manual override CSV for special cases (ST stocks)

**Limitation 2:** Requires API token and 2000 points
- **Mitigation:** Free tier registration available, one-time setup
- **Alternative:** Can cache data locally after first sync

**Limitation 3:** Rate limits may apply for high-frequency calls
- **Mitigation:** Call once and cache (recommended by Tushare), refresh periodically

---

**Exchange Symbol Format:**
- 6-digit numeric codes (e.g., `000001`, `000002`, `300750`)
- Prefix indicates board:
  - `000xxx` - Main Board
  - `001xxx` - Main Board (new)
  - `002xxx` - SME Board (Small and Medium Enterprise)
  - `300xxx` - ChiNext (Growth Enterprise Market)
  - `301xxx` - ChiNext (new)

**Example Symbols:**
- `000001` - Ping An Bank (平安银行)
- `000002` - Vanke A (万科A)
- `300750` - Contemporary Amperex Technology (宁德时代)---

## Phase 1: Schema Design

### 1.1 Silver Table: `szse_l3_orders` (Order Placements)

**Purpose:** New order placements entering the matching engine

**Partitioning:** `exchange` + `date` (like existing tables)

**Schema:**
```python
SZSE_L3_ORDERS_SCHEMA = {
    "date": pl.Date,                    # derived from transact_time
    "exchange": pl.Utf8,                # "szse"
    "exchange_id": pl.Int16,            # from EXCHANGE_MAP
    "symbol_id": pl.Int64,              # from dim_symbol
    "ts_local_us": pl.Int64,            # parsed from TransactTime
    "appl_seq_num": pl.Int64,           # Order ID (unique per day)
    "side": pl.UInt8,                   # 0=buy, 1=sell (remap from 1/2)
    "ord_type": pl.UInt8,               # 0=market, 1=limit (remap from 1/2)
    "price_int": pl.Int64,              # fixed-point encoded
    "order_qty_int": pl.Int64,          # fixed-point encoded
    "channel_no": pl.Int32,             # exchange channel ID
    "file_id": pl.Int32,                # lineage tracking
    "file_line_number": pl.Int32,       # lineage tracking
}
```

**Key Columns:**
- `appl_seq_num` - **Primary identifier** for this order
- `ts_local_us` - Arrival time timeline (use TransactTime as proxy for now)
- Fixed-point encoding using `dim_symbol` increments

### 1.2 Silver Table: `szse_l3_ticks` (Executions & Cancellations)

**Purpose:** Trade executions and order cancellations

**Partitioning:** `exchange` + `date`

**Schema:**
```python
SZSE_L3_TICKS_SCHEMA = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "exchange_id": pl.Int16,
    "symbol_id": pl.Int64,
    "ts_local_us": pl.Int64,            # parsed from TransactTime
    "appl_seq_num": pl.Int64,           # Event ID (unique per day)
    "bid_appl_seq_num": pl.Int64,       # Bid order ID (0 if N/A)
    "offer_appl_seq_num": pl.Int64,     # Ask order ID (0 if N/A)
    "price_int": pl.Int64,              # 0 for cancellations
    "qty_int": pl.Int64,                # filled or cancelled quantity
    "exec_type": pl.UInt8,              # 0=fill, 1=cancel (remap from 'F'/4)
    "channel_no": pl.Int32,
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}
```

**Key Columns:**
- `appl_seq_num` - Event sequence number
- `bid_appl_seq_num` / `offer_appl_seq_num` - Link to orders in `szse_l3_orders`
- `exec_type` - Fill (trade) or Cancel

**Semantic Notes:**
- Fill: Both bid_appl_seq_num and offer_appl_seq_num are non-zero
- Cancel: One of bid_appl_seq_num or offer_appl_seq_num is non-zero
- Market orders may execute immediately (appear only in ticks, not in book)

## Phase 2: Bronze Layer Integration

### 2.1 Bronze Storage Structure

```
/lake/bronze/quant360/
  exchange=szse/
    type=l3_orders/
      date=2024-05-01/
        symbol={symbol}/
          order_new_STK_SZ_20240501.7z  # compressed archive
    type=l3_ticks/
      date=2024-05-01/
        symbol={symbol}/
          tick_new_STK_SZ_20240501.7z   # compressed archive
```

### 2.2 Bronze Source Implementation

Create `pointline/io/quant360/` module:

**Files to create:**
- `__init__.py`
- `client.py` - Download client (if API available)
- `extractor.py` - 7zip extraction utilities
- `metadata.py` - Symbol list retrieval

**Key functionality:**
```python
def extract_7z_to_bronze(
    archive_path: Path,
    output_dir: Path,
    data_type: Literal["l3_orders", "l3_ticks"],
) -> list[Path]:
    """Extract 7z archive to per-symbol CSV files."""
    # Use py7zr library for extraction
    # Return list of extracted CSV paths
```

## Phase 3: Table Parsing & Validation

### 3.1 Orders Table Module (`pointline/tables/szse_l3_orders.py`)

**Functions to implement:**
```python
def parse_quant360_orders_csv(path: Path) -> pl.DataFrame:
    """
    Parse Quant360 order CSV.

    Input columns: ApplSeqNum, Side, OrdType, Price, OrderQty,
                   TransactTime, ChannelNo

    Returns: DataFrame with ts_local_us, appl_seq_num, side, ord_type,
             price, order_qty (floats, not yet encoded)
    """

def normalize_orders_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to canonical types, add computed columns (date)."""

def validate_orders(df: pl.DataFrame) -> pl.DataFrame:
    """
    Validation checks:
    - appl_seq_num > 0
    - side in [1, 2]
    - ord_type in [1, 2]
    - price >= 0 (0 allowed for market orders)
    - order_qty > 0
    - ts_local_us monotonic within file

    Filter invalid rows, log warnings.
    """

def encode_fixed_point_orders(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
) -> pl.DataFrame:
    """
    Encode price and order_qty to fixed-point integers.
    Join with dim_symbol to get price_increment and amount_increment.
    """

def remap_enums(df: pl.DataFrame) -> pl.DataFrame:
    """
    Remap vendor enums to internal:
    - side: 1 -> 0 (buy), 2 -> 1 (sell)
    - ord_type: 1 -> 0 (market), 2 -> 1 (limit)
    """
```

### 3.2 Ticks Table Module (`pointline/tables/szse_l3_ticks.py`)

**Functions to implement:**
```python
def parse_quant360_ticks_csv(path: Path) -> pl.DataFrame:
    """
    Parse Quant360 tick CSV.

    Input columns: ApplSeqNum, BidApplSeqNum, OfferApplSeqNum,
                   Price, Qty, ExecType, TransactTime, ChannelNo
    """

def validate_ticks(df: pl.DataFrame) -> pl.DataFrame:
    """
    Validation:
    - appl_seq_num > 0
    - For fill: both bid_appl_seq_num and offer_appl_seq_num > 0
    - For cancel: exactly one of bid/offer_appl_seq_num > 0
    - For fill: price_int > 0
    - For cancel: price_int == 0 is okay
    - qty_int > 0
    """

def remap_exec_type(df: pl.DataFrame) -> pl.DataFrame:
    """
    Remap ExecType:
    - 'F' -> 0 (fill)
    - '4' -> 1 (cancel)
    """
```

## Phase 4: Ingestion Services

### 4.1 Orders Ingestion Service

**File:** `pointline/services/szse_l3_orders_service.py`

Inherits from `BaseService`, implements:
- `validate()` - Call validation functions
- `compute_state()` - Parse, normalize, encode, resolve symbol_ids
- `write()` - Append to Delta table

**Key considerations:**
- Handle 7z extraction before CSV parsing
- Each CSV file is one symbol
- Must extract symbol from filename (e.g., `000001.csv` -> symbol `000001`)
- Look up symbol_id from dim_symbol based on exchange="szse" and exchange_symbol

### 4.2 Ticks Ingestion Service

Similar structure to orders service.

## Phase 5: Configuration Updates

### 5.1 Exchange Registry (`pointline/config.py`)

Add SZSE exchange:
```python
EXCHANGE_MAP = {
    # ... existing exchanges
    "szse": 30,  # Shenzhen Stock Exchange
}
```

### 5.2 Table Registry

```python
TABLE_PATHS = {
    # ... existing tables
    "szse_l3_orders": "silver/szse_l3_orders",
    "szse_l3_ticks": "silver/szse_l3_ticks",
}

TABLE_HAS_DATE = {
    # ... existing tables
    "szse_l3_orders": True,
    "szse_l3_ticks": True,
}
```

### 5.3 Ingestion Factory (`pointline/cli/ingestion_factory.py`)

Add to `TABLE_PARTITIONS`:
```python
TABLE_PARTITIONS = {
    # ... existing
    "szse_l3_orders": ["exchange", "date"],
    "szse_l3_ticks": ["exchange", "date"],
}
```

Add to `create_ingestion_service()`:
```python
if data_type == "l3_orders":
    repo = BaseDeltaRepository(
        get_table_path("szse_l3_orders"),
        partition_by=["exchange", "date"],
    )
    return SzseL3OrdersIngestionService(repo, dim_symbol_repo, manifest_repo)

if data_type == "l3_ticks":
    # similar for ticks
```

## Phase 6: CLI Commands

### 6.1 Bronze Download

```bash
pointline bronze download \
  --vendor quant360 \
  --exchange szse \
  --data-type l3_orders \
  --date 2024-05-01 \
  --symbols 000001,000002  # optional: specific symbols
```

### 6.2 Ingestion

```bash
pointline ingest run \
  --table szse_l3_orders \
  --exchange szse \
  --date 2024-05-01

pointline ingest run \
  --table szse_l3_ticks \
  --exchange szse \
  --date 2024-05-01
```

### 6.3 Validation

```bash
pointline validate szse_l3_orders \
  --exchange szse \
  --date 2024-05-01

pointline validate szse_l3_ticks \
  --exchange szse \
  --date 2024-05-01
```

## Phase 7: Research API

### 7.1 Load Functions

Add to `pointline/research.py`:

```python
def load_szse_l3_orders(
    symbol_id: int,
    start_ts_us: int,
    end_ts_us: int,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load SZSE L3 order placements."""

def load_szse_l3_ticks(
    symbol_id: int,
    start_ts_us: int,
    end_ts_us: int,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load SZSE L3 execution and cancellation events."""
```

### 7.2 L3 Book Reconstruction Utility

Create new module: `pointline/l3_book.py`

```python
def reconstruct_l3_book(
    orders: pl.DataFrame,
    ticks: pl.DataFrame,
    target_ts_us: int | None = None,
) -> dict[str, list[dict]]:
    """
    Reconstruct L3 order book at target_ts_us.

    Args:
        orders: szse_l3_orders DataFrame
        ticks: szse_l3_ticks DataFrame
        target_ts_us: Reconstruct book at this time (None = end of data)

    Returns:
        {
            "bids": [
                {"order_id": 123, "price_int": 50000, "qty_int": 100},
                ...
            ],
            "asks": [...],
        }

    Algorithm:
    1. Sort both streams by ts_local_us, then appl_seq_num
    2. Process events up to target_ts_us:
       - Orders: Add to book keyed by appl_seq_num
       - Ticks (Fill): Reduce qty of both bid and ask orders
       - Ticks (Cancel): Reduce qty of one order
    3. Remove orders with qty_int <= 0
    """

def l3_to_l2_aggregate(l3_book: dict) -> pl.DataFrame:
    """
    Aggregate L3 book to L2 (price-level view).

    Returns DataFrame with columns:
        side (0=bid, 1=ask), price_int, total_qty_int, order_count
    """

def stream_l3_snapshots(
    orders: pl.DataFrame,
    ticks: pl.DataFrame,
    snapshot_every_us: int = 1_000_000,  # 1 second
) -> Iterator[tuple[int, dict]]:
    """
    Yield (ts_local_us, l3_book) snapshots at regular intervals.

    Efficient streaming reconstruction for time-series analysis.
    """
```

## Phase 8: Testing

### 8.1 Unit Tests

**`tests/test_szse_l3_orders.py`:**
- `test_parse_quant360_orders_csv()` - CSV parsing
- `test_normalize_orders_schema()` - Schema normalization
- `test_validate_orders()` - Validation logic
- `test_encode_fixed_point_orders()` - Fixed-point encoding
- `test_remap_enums()` - Enum remapping
- `test_resolve_symbol_ids()` - Symbol resolution

**`tests/test_szse_l3_ticks.py`:**
- Similar tests for ticks table

**`tests/test_l3_book.py`:**
- `test_reconstruct_l3_book_simple()` - Basic reconstruction
- `test_reconstruct_l3_book_with_fills()` - Handle fills
- `test_reconstruct_l3_book_with_cancels()` - Handle cancels
- `test_l3_to_l2_aggregate()` - Aggregation logic
- `test_stream_l3_snapshots()` - Snapshot streaming

### 8.2 Integration Tests

**`tests/test_szse_l3_integration.py`:**
- End-to-end: Bronze extraction → Silver ingestion → L3 reconstruction
- Test with sample 7z archive

### 8.3 Sample Data

Create test fixtures:
- `tests/fixtures/quant360/order_new_STK_SZ_20240501_sample.csv`
- `tests/fixtures/quant360/tick_new_STK_SZ_20240501_sample.csv`

## Phase 9: Documentation

### 9.1 Schema Documentation

Update `docs/schemas.md`:

Add section **2.X `silver.szse_l3_orders`** and **2.Y `silver.szse_l3_ticks`**

Include:
- Full schema definitions
- Semantic notes about order tracking
- Linkage between orders and ticks via appl_seq_num
- Reconstruction algorithm overview

### 9.2 Researcher Guide

Update `docs/guides/researcher_guide.md`:

Add section on L3 order book:
- Difference between L2 and L3
- When to use L3 (order flow analysis, market making, queue position)
- How to reconstruct book
- Example code snippets

### 9.3 Data Source Documentation

Already exists: `docs/data_sources/quant360_szse_l2.md` ✅

Add note about implementation status once complete.

## Phase 10: Performance Considerations

### 10.1 Expected Data Volume

SZSE has ~2000 stocks. Typical L3 data:
- Orders: ~1M orders/day/symbol for liquid stocks
- Ticks: ~500K ticks/day/symbol

For 2000 symbols:
- ~2B orders/day
- ~1B ticks/day

**Partitioning strategy:** `exchange` + `date` should keep partition sizes manageable (~1GB/partition).

### 10.2 Reconstruction Performance

L3 reconstruction is CPU-intensive. Recommendations:
- Use streaming iterator for long time ranges
- Cache reconstructed books at checkpoints (optional Gold table)
- For backtesting, consider pre-computing L2 aggregation if L3 not needed

### 10.3 Optional: Gold Layer Checkpoint

**Future enhancement (not in initial scope):**

`gold.szse_l3_checkpoints` - Periodic full-book snapshots (e.g., every 10 minutes)
- Allows fast random access without replaying entire day
- Trade-off: Storage cost vs replay speed

## Implementation Order

### Stage 0: CRITICAL PRE-REQUISITE (Week 0 - MUST BE COMPLETED FIRST)
1. ⚠️ Obtain SZSE symbol list (CSV or API)
2. ⚠️ Create symbols_szse_seed.csv with metadata
3. ⚠️ Implement sync_szse_symbols_from_csv()
4. ⚠️ Populate dim_symbol with SZSE symbols
5. ⚠️ Verify symbol resolution works

**CHECKPOINT:** Cannot proceed to Stage 1 until dim_symbol contains SZSE symbols!

### Stage 1: Foundation (Week 1)
1. ✅ Create schema definitions (tables/szse_l3_orders.py, tables/szse_l3_ticks.py)
2. ✅ Update config.py (exchange, tables, partitions)
3. ✅ Write parsing functions
4. ✅ Write validation functions
5. ✅ Unit tests for parsing/validation

### Stage 2: Ingestion (Week 1-2)
6. ✅ Implement 7z extraction utility (io/quant360/)
7. ✅ Implement orders ingestion service
8. ✅ Implement ticks ingestion service
9. ✅ Update ingestion factory
10. ✅ Integration tests for ingestion

### Stage 3: CLI & Bronze (Week 2)
11. ✅ Add CLI commands (bronze download, ingest run)
12. ✅ Test end-to-end bronze→silver pipeline
13. ✅ Add validation commands

### Stage 4: Research API (Week 2-3)
14. ✅ Implement load_szse_l3_orders()
15. ✅ Implement load_szse_l3_ticks()
16. ✅ Implement L3 reconstruction utility (l3_book.py)
17. ✅ Unit tests for reconstruction
18. ✅ Example notebooks demonstrating usage

### Stage 5: Documentation (Week 3)
19. ✅ Update schemas.md
20. ✅ Update researcher_guide.md
21. ✅ Create example analysis notebook
22. ✅ Update CLAUDE.md

## Success Criteria

- [x] ⚠️ SZSE symbols populated in dim_symbol (CRITICAL PRE-REQUISITE)
- [ ] Can download SZSE L3 data to bronze
- [ ] Can ingest orders and ticks to silver tables
- [ ] All data passes validation checks
- [ ] Can reconstruct L3 book at any timestamp
- [ ] Can aggregate L3 to L2 view
- [ ] All tests pass (>80% coverage)
- [ ] Documentation complete
- [ ] Example notebook demonstrates full workflow

## Dependencies

**Python packages:**
- `py7zr` - 7zip extraction for Quant360 data
- `tushare` - Tushare Pro API client for symbol metadata
- Existing: `polars`, `deltalake`, `pyarrow`

**External accounts:**
- **Tushare Pro API token** - Register at https://tushare.pro/register
  - Requires: 2000 points minimum (free tier available)
  - Set environment variable: `TUSHARE_TOKEN`

**Data dependencies:**
- ⚠️ **CRITICAL:** `dim_symbol` must be populated with SZSE symbols BEFORE L3 ingestion
- Tushare provides: exchange_symbol, name, list_date, delist_date
- Default values used for: tick_size (0.01 CNY), lot_size (100 shares)
- See Phase 0 for complete symbol population plan

**Configuration dependencies:**
- Add SZSE (and optionally SSE) to EXCHANGE_MAP
  - SZSE: exchange_id = 30
  - SSE: exchange_id = 31
- Add szse_l3_orders and szse_l3_ticks to TABLE_PATHS and TABLE_HAS_DATE

## Risk Mitigation

**Risk 1: 7z extraction performance**
- Mitigation: Extract to temp directory, process incrementally, clean up

**Risk 2: Large data volumes**
- Mitigation: Use streaming processing, partition by date, compress with ZSTD

**Risk 3: Symbol resolution failures**
- Mitigation: Quarantine mechanism (existing pattern), log unmapped symbols

**Risk 4: Time zone handling**
- Mitigation: Document that TransactTime is likely China Standard Time (CST, UTC+8), convert to UTC if needed

## Open Questions

1. **Timestamp timezone:** Is TransactTime in CST or UTC? Need to verify and document.
2. **Bronze download API:** Does Quant360 provide download API or manual download only?
3. **✅ RESOLVED: Symbol metadata source** - Using Tushare Pro API (stock_basic endpoint)
4. **Market hours:** SZSE trading hours (09:30-15:00 CST)? Useful for validation.
5. **Data availability:** Historical data range from Quant360?
6. **Tick size exceptions:** Tushare doesn't provide tick_size. Are there stocks with tick_size ≠ 0.01 CNY?
   - Known exceptions: ST stocks, certain bond markets
   - Default 0.01 CNY should cover 99% of A-shares
   - Can add manual override mechanism if needed

## Notes

- This is **Level 3** data (order-by-order), fundamentally different from L2 aggregated updates
- L3 enables advanced analysis: order flow imbalance, queue position, market maker behavior
- Storage is ~2-3x larger than L2 but allows more granular analysis
- Consider this a foundation for future L3 order book replay framework (separate repo)
