# Schema Reference

Comprehensive reference for all Pointline table schemas.

## Table of Contents

- [Core Market Data Tables](#core-market-data-tables)
  - [trades](#trades)
  - [quotes](#quotes)
  - [book_snapshot_25](#book_snapshot_25)
  - [kline_1h](#kline_1h)
  - [derivative_ticker](#derivative_ticker)
- [Chinese Stocks (Level 3)](#chinese-stocks-level-3)
  - [szse_l3_orders](#szse_l3_orders)
  - [szse_l3_ticks](#szse_l3_ticks)
- [Dimension Tables](#dimension-tables)
  - [dim_symbol](#dim_symbol)
  - [dim_asset_stats](#dim_asset_stats)
- [Metadata Tables](#metadata-tables)
  - [ingest_manifest](#ingest_manifest)

---

## Core Market Data Tables

### trades

Trade executions with fixed-point integer encoding.

**Partitioning:** `exchange` + `date`

**Schema:**

| Column | Type | Description | Encoding |
|--------|------|-------------|----------|
| `date` | Date | Partition date (exchange-local timezone) | N/A |
| `exchange_id` | Int16 | Exchange identifier (from config.EXCHANGE_MAP) | Dictionary |
| `symbol_id` | Int64 | Symbol identifier (SCD Type 2) | Hash-based |
| `ts_local_us` | Int64 | Arrival timestamp (UTC, microseconds) | Integer |
| `ts_exch_us` | Int64 | Exchange timestamp (UTC, microseconds) | Integer |
| `side` | UInt8 | Trade side: 0=buy, 1=sell, 2=unknown | Enum |
| `px_int` | Int64 | Price in fixed-point integer | `round(price / price_increment)` |
| `qty_int` | Int64 | Quantity in fixed-point integer | `round(qty / qty_increment)` |
| `file_id` | Int32 | Bronze file identifier (SHA256-based) | Hash |
| `file_line_number` | Int32 | Line number within bronze file | Integer |

**Ordering:** `(ts_local_us, file_id, file_line_number)` ascending (deterministic)

**Decoding:**

```python
# Query API (automatic)
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
# Returns: price (float), qty (float)

# Core API (manual)
from pointline.tables.trades import decode_fixed_point
from pointline.dim_symbol import read_dim_symbol_table

trades = research.load_trades(symbol_id=12345, start_ts_us=..., end_ts_us=...)
dim_symbol = read_dim_symbol_table()
trades_decoded = decode_fixed_point(trades, dim_symbol)
```

**Side Constants:**

```python
SIDE_BUY = 0
SIDE_SELL = 1
SIDE_UNKNOWN = 2
```

---

### quotes

Best bid and ask quotes (Level 1 order book).

**Partitioning:** `exchange` + `date`

**Schema:**

| Column | Type | Description | Encoding |
|--------|------|-------------|----------|
| `date` | Date | Partition date (exchange-local timezone) | N/A |
| `exchange_id` | Int16 | Exchange identifier | Dictionary |
| `symbol_id` | Int64 | Symbol identifier (SCD Type 2) | Hash-based |
| `ts_local_us` | Int64 | Arrival timestamp (UTC, microseconds) | Integer |
| `ts_exch_us` | Int64 | Exchange timestamp (UTC, microseconds) | Integer |
| `bid_price_int` | Int64 | Best bid price (fixed-point) | `round(bid_price / price_increment)` |
| `bid_qty_int` | Int64 | Best bid quantity (fixed-point) | `round(bid_qty / qty_increment)` |
| `ask_price_int` | Int64 | Best ask price (fixed-point) | `round(ask_price / price_increment)` |
| `ask_qty_int` | Int64 | Best ask quantity (fixed-point) | `round(ask_qty / qty_increment)` |
| `file_id` | Int32 | Bronze file identifier | Hash |
| `file_line_number` | Int32 | Line number within bronze file | Integer |

**Ordering:** `(ts_local_us, file_id, file_line_number)` ascending

**Decoding:**

```python
# Query API
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
# Returns: bid_price, bid_qty, ask_price, ask_qty (floats)
```

**Validation:**
- `bid_price_int < ask_price_int` (no crossed book)
- `bid_qty_int > 0` and `ask_qty_int > 0`

---

### book_snapshot_25

Order book snapshots with top 25 depth levels.

**Partitioning:** `exchange` + `date`

**Schema:**

| Column | Type | Description | Encoding |
|--------|------|-------------|----------|
| `date` | Date | Partition date (exchange-local timezone) | N/A |
| `exchange_id` | Int16 | Exchange identifier | Dictionary |
| `symbol_id` | Int64 | Symbol identifier (SCD Type 2) | Hash-based |
| `ts_local_us` | Int64 | Arrival timestamp (UTC, microseconds) | Integer |
| `ts_exch_us` | Int64 | Exchange timestamp (UTC, microseconds) | Integer |
| `bid_price_0` to `bid_price_24` | Int64 | Bid prices at depth levels 0-24 (fixed-point) | `round(price / price_increment)` |
| `bid_qty_0` to `bid_qty_24` | Int64 | Bid quantities at depth levels 0-24 (fixed-point) | `round(qty / qty_increment)` |
| `ask_price_0` to `ask_price_24` | Int64 | Ask prices at depth levels 0-24 (fixed-point) | `round(price / price_increment)` |
| `ask_qty_0` to `ask_qty_24` | Int64 | Ask quantities at depth levels 0-24 (fixed-point) | `round(qty / qty_increment)` |
| `file_id` | Int32 | Bronze file identifier | Hash |
| `file_line_number` | Int32 | Line number within bronze file | Integer |

**Depth Levels:**
- Level 0: Best bid/ask (top of book)
- Level 1-24: Subsequent depth levels

**Ordering:** `(ts_local_us, file_id, file_line_number)` ascending

**Decoding:**

```python
# Query API
book = query.book_snapshot_25("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
# Returns: All price/qty columns as floats
```

**Example Usage:**

```python
# Calculate total bid liquidity in top 10 levels
book = query.book_snapshot_25("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
book = book.with_columns(
    pl.sum_horizontal([f"bid_qty_{i}" for i in range(10)]).alias("total_bid_liquidity_top10")
)
```

---

### kline_1h

OHLCV candlestick bars (1-hour intervals).

**Partitioning:** `exchange` + `date`

**Schema:**

| Column | Type | Description | Encoding |
|--------|------|-------------|----------|
| `date` | Date | Partition date (exchange-local timezone) | N/A |
| `exchange_id` | Int16 | Exchange identifier | Dictionary |
| `symbol_id` | Int64 | Symbol identifier (SCD Type 2) | Hash-based |
| `ts_open_us` | Int64 | Bar open timestamp (UTC, microseconds) | Integer |
| `ts_close_us` | Int64 | Bar close timestamp (UTC, microseconds) | Integer |
| `open_int` | Int64 | Open price (fixed-point) | `round(open / price_increment)` |
| `high_int` | Int64 | High price (fixed-point) | `round(high / price_increment)` |
| `low_int` | Int64 | Low price (fixed-point) | `round(low / price_increment)` |
| `close_int` | Int64 | Close price (fixed-point) | `round(close / price_increment)` |
| `volume_int` | Int64 | Volume (fixed-point) | `round(volume / qty_increment)` |
| `num_trades` | Int32 | Number of trades in bar | Integer |
| `file_id` | Int32 | Bronze file identifier | Hash |
| `file_line_number` | Int32 | Line number within bronze file | Integer |

**Ordering:** `(ts_open_us, file_id, file_line_number)` ascending

**Decoding:**

```python
# Query API
klines = query.kline_1h("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
# Returns: open, high, low, close, volume (floats)
```

**Validation:**
- `low_int <= open_int, close_int <= high_int`
- `ts_close_us > ts_open_us`

---

### derivative_ticker

Derivatives-specific data: funding rates, open interest, mark/index prices.

**Partitioning:** `exchange` + `date`

**Schema:**

| Column | Type | Description | Encoding |
|--------|------|-------------|----------|
| `date` | Date | Partition date (exchange-local timezone) | N/A |
| `exchange_id` | Int16 | Exchange identifier | Dictionary |
| `symbol_id` | Int64 | Symbol identifier (SCD Type 2) | Hash-based |
| `ts_local_us` | Int64 | Arrival timestamp (UTC, microseconds) | Integer |
| `ts_exch_us` | Int64 | Exchange timestamp (UTC, microseconds) | Integer |
| `mark_price_int` | Int64 | Mark price (fixed-point, nullable) | `round(mark_price / price_increment)` |
| `index_price_int` | Int64 | Index price (fixed-point, nullable) | `round(index_price / price_increment)` |
| `funding_rate_int` | Int64 | Funding rate (fixed-point, nullable) | `round(funding_rate / 1e-8)` |
| `next_funding_ts_us` | Int64 | Next funding timestamp (nullable) | Integer |
| `open_interest_int` | Int64 | Open interest (fixed-point, nullable) | `round(open_interest / qty_increment)` |
| `file_id` | Int32 | Bronze file identifier | Hash |
| `file_line_number` | Int32 | Line number within bronze file | Integer |

**Ordering:** `(ts_local_us, file_id, file_line_number)` ascending

**Funding Rate Encoding:**
- Stored as: `round(funding_rate / 1e-8)` (8 decimal places)
- Example: 0.0001 (0.01%) → 10000

**Decoding:**

```python
# Query API
ticker = query.derivative_ticker("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
# Returns: mark_price, index_price, funding_rate, open_interest (floats)
```

---

## Chinese Stocks (Level 3)

### szse_l3_orders

SZSE Level 3 order placements and cancellations.

**Partitioning:** `exchange` + `date`

**Schema:**

| Column | Type | Description | Encoding |
|--------|------|-------------|----------|
| `date` | Date | Partition date (CST timezone) | N/A |
| `exchange_id` | Int16 | Exchange identifier (szse) | Dictionary |
| `symbol_id` | Int64 | Symbol identifier (6-digit stock code) | Hash-based |
| `ts_local_us` | Int64 | Arrival timestamp (UTC, microseconds) | Integer |
| `ts_exch_us` | Int64 | Exchange timestamp (UTC, microseconds) | Integer |
| `order_id` | Int64 | Exchange order ID | Integer |
| `side` | UInt8 | Order side: 0=buy, 1=sell | Enum |
| `order_type` | UInt8 | Order type: 0=limit, 1=market, 2=cancel | Enum |
| `px_int` | Int64 | Limit price (lot-based, nullable) | `round(price / 0.01 / 100)` |
| `qty_int` | Int64 | Order quantity (lot-based) | `round(qty / 100)` |
| `file_id` | Int32 | Bronze file identifier | Hash |
| `file_line_number` | Int32 | Line number within bronze file | Integer |

**Lot-Based Encoding:**
- Chinese A-shares trade in lots (1 lot = 100 shares)
- Price: `px_int = round(price / 0.01 / 100)` (0.01 CNY per share, 100 shares/lot)
- Quantity: `qty_int = round(qty / 100)` (lots)

**Ordering:** `(ts_local_us, file_id, file_line_number)` ascending

**Decoding:**

```python
# Query API
orders = query.szse_l3_orders("szse", "000001", "2024-09-30", "2024-10-01", decoded=True)
# Returns: price (CNY/share), qty (shares)
```

**Order Types:**
```python
ORDER_TYPE_LIMIT = 0
ORDER_TYPE_MARKET = 1
ORDER_TYPE_CANCEL = 2
```

---

### szse_l3_ticks

SZSE Level 3 trade executions and cancellations.

**Partitioning:** `exchange` + `date`

**Schema:**

| Column | Type | Description | Encoding |
|--------|------|-------------|----------|
| `date` | Date | Partition date (CST timezone) | N/A |
| `exchange_id` | Int16 | Exchange identifier (szse) | Dictionary |
| `symbol_id` | Int64 | Symbol identifier (6-digit stock code) | Hash-based |
| `ts_local_us` | Int64 | Arrival timestamp (UTC, microseconds) | Integer |
| `ts_exch_us` | Int64 | Exchange timestamp (UTC, microseconds) | Integer |
| `buy_order_id` | Int64 | Buy order ID | Integer |
| `sell_order_id` | Int64 | Sell order ID | Integer |
| `px_int` | Int64 | Execution price (lot-based, 0 for cancellations) | `round(price / 0.01 / 100)` |
| `qty_int` | Int64 | Execution quantity (lot-based) | `round(qty / 100)` |
| `tick_type` | UInt8 | Tick type: 0=fill, 1=cancellation | Enum |
| `file_id` | Int32 | Bronze file identifier | Hash |
| `file_line_number` | Int32 | Line number within bronze file | Integer |

**Tick Semantics:**
- **Fill (tick_type=0):** Trade execution with price > 0
- **Cancellation (tick_type=1):** Order cancellation with price = 0

**Ordering:** `(ts_local_us, file_id, file_line_number)` ascending

**Decoding:**

```python
# Query API
ticks = query.szse_l3_ticks("szse", "000001", "2024-09-30", "2024-10-01", decoded=True)
# Returns: price (CNY/share, 0 for cancellations), qty (shares)
```

**Tick Types:**
```python
TICK_TYPE_FILL = 0
TICK_TYPE_CANCELLATION = 1
```

---

## Dimension Tables

### dim_symbol

Symbol master table with SCD Type 2 tracking.

**Partitioning:** None (unpartitioned)

**Schema:**

| Column | Type | Description | Encoding |
|--------|------|-------------|----------|
| `symbol_id` | Int64 | Unique symbol identifier (blake2b hash) | Hash-based |
| `exchange` | String | Exchange name (normalized) | String |
| `exchange_id` | Int16 | Exchange identifier (from config.EXCHANGE_MAP) | Dictionary |
| `exchange_symbol` | String | Symbol ticker on exchange (e.g., "BTCUSDT") | String |
| `base_asset` | String | Base asset (e.g., "BTC") | String |
| `quote_asset` | String | Quote asset (e.g., "USDT") | String |
| `asset_type` | UInt8 | Asset type: 0=spot, 1=perpetual, 2=future, 3=option | Enum |
| `price_increment` | Float64 | Minimum price movement (e.g., 0.01) | Float |
| `qty_increment` | Float64 | Minimum quantity movement (e.g., 0.001) | Float |
| `lot_size` | Float64 | Lot size (for stocks, e.g., 100 shares) | Float |
| `contract_size` | Float64 | Contract size (for derivatives) | Float |
| `valid_from_ts` | Int64 | SCD Type 2: Start of validity period (UTC, microseconds) | Integer |
| `valid_until_ts` | Int64 | SCD Type 2: End of validity period (9999-12-31 for current) | Integer |
| `is_current` | Boolean | SCD Type 2: True if currently active | Boolean |
| `metadata_json` | String | Additional metadata (JSON, nullable) | JSON string |

**SCD Type 2 Tracking:**

Pointline tracks symbol metadata changes over time:

```python
from pointline.dim_symbol import read_dim_symbol_table
import polars as pl

dim_symbol = read_dim_symbol_table()

# Get current BTCUSDT metadata
btc_current = dim_symbol.filter(
    (pl.col("exchange_symbol") == "BTCUSDT") &
    (pl.col("exchange") == "binance-futures") &
    (pl.col("is_current") == True)
)

# Get full history
btc_history = dim_symbol.filter(
    (pl.col("exchange_symbol") == "BTCUSDT") &
    (pl.col("exchange") == "binance-futures")
).sort("valid_from_ts")
```

**symbol_id Generation:**

```python
from pointline.dim_symbol import generate_symbol_id

# Deterministic hash-based ID
symbol_id = generate_symbol_id(
    exchange="binance-futures",
    exchange_symbol="BTCUSDT",
    valid_from_ts=1609459200000000,  # Microseconds
)
# Returns: Int64 (blake2b digest as integer)
```

**Asset Types:**
```python
ASSET_TYPE_SPOT = 0
ASSET_TYPE_PERPETUAL = 1
ASSET_TYPE_FUTURE = 2
ASSET_TYPE_OPTION = 3
```

---

### dim_asset_stats

Asset-level statistics (market cap, circulating supply, etc.).

**Partitioning:** None (unpartitioned)

**Schema:**

| Column | Type | Description | Encoding |
|--------|------|-------------|----------|
| `base_asset` | String | Base asset (e.g., "BTC") | String |
| `ts_us` | Int64 | Timestamp of statistics (UTC, microseconds) | Integer |
| `market_cap_usd` | Float64 | Market capitalization in USD (nullable) | Float |
| `circulating_supply` | Float64 | Circulating supply (nullable) | Float |
| `total_supply` | Float64 | Total supply (nullable) | Float |
| `max_supply` | Float64 | Maximum supply (nullable) | Float |
| `ath_usd` | Float64 | All-time high price in USD (nullable) | Float |
| `ath_date` | Date | Date of all-time high (nullable) | Date |
| `metadata_json` | String | Additional metadata (JSON, nullable) | JSON string |

**Data Source:** CoinGecko API

**Usage:**

```python
from pointline.dim_symbol import read_dim_asset_stats_table
import polars as pl

stats = read_dim_asset_stats_table()

# Get BTC stats
btc_stats = stats.filter(pl.col("base_asset") == "BTC")
print(btc_stats)
```

---

## Metadata Tables

### ingest_manifest

ETL tracking ledger for bronze → silver ingestion.

**Partitioning:** `date`

**Schema:**

| Column | Type | Description | Encoding |
|--------|------|-------------|----------|
| `date` | Date | Partition date (ingestion date) | N/A |
| `file_id` | Int32 | Unique file identifier (SHA256-based) | Hash |
| `table_name` | String | Target silver table (e.g., "trades") | String |
| `exchange` | String | Exchange name | String |
| `exchange_id` | Int16 | Exchange identifier | Dictionary |
| `bronze_path` | String | Bronze file path (relative) | String |
| `bronze_checksum` | String | SHA256 checksum of bronze file | Hex string |
| `bronze_size_bytes` | Int64 | Bronze file size | Integer |
| `status` | String | Ingestion status: "pending", "ingested", "failed" | Enum |
| `ingested_at_ts_us` | Int64 | Ingestion timestamp (UTC, microseconds, nullable) | Integer |
| `row_count` | Int64 | Number of rows ingested (nullable) | Integer |
| `error_message` | String | Error details if status="failed" (nullable) | String |

**File ID Generation:**

```python
import hashlib

def compute_file_id(bronze_path: str, checksum: str) -> int:
    """Deterministic file_id from path + checksum."""
    content = f"{bronze_path}:{checksum}".encode()
    digest = hashlib.sha256(content).digest()
    return int.from_bytes(digest[:4], byteorder="big", signed=True)
```

**Status Workflow:**
1. **pending** - File discovered in bronze layer
2. **ingested** - Successfully processed and written to silver
3. **failed** - Ingestion error (see error_message)

**Usage:**

```python
from pointline.io.delta_manifest_repo import DeltaManifestRepository

manifest_repo = DeltaManifestRepository()

# Check pending files
pending = manifest_repo.list_pending(
    table_name="trades",
    exchange="binance-futures",
    date="2024-05-01",
)

# Update status after ingestion
manifest_repo.update_status(
    file_id=12345,
    status="ingested",
    ingested_at_ts_us=1714521600000000,
    row_count=100000,
)
```

---

## Common Encoding Patterns

### Fixed-Point Integer Encoding

**Purpose:** Avoid floating-point precision errors in price/quantity calculations.

**Encoding:**
```python
px_int = round(price / price_increment)
qty_int = round(qty / qty_increment)
```

**Decoding:**
```python
price = px_int * price_increment
qty = qty_int * qty_increment
```

**Example:**
```python
# BTCUSDT: price_increment = 0.01
price = 50123.45
px_int = round(50123.45 / 0.01) = 5012345  # Stored as Int64

# Decode
price = 5012345 * 0.01 = 50123.45  # Exact!
```

### Lot-Based Encoding (Chinese Stocks)

**Purpose:** Chinese A-shares trade in lots (1 lot = 100 shares).

**Encoding:**
```python
px_int = round(price / 0.01 / 100)  # CNY/share → lots
qty_int = round(qty / 100)  # shares → lots
```

**Decoding:**
```python
price = px_int * 0.01 * 100  # lots → CNY/share
qty = qty_int * 100  # lots → shares
```

**Example:**
```python
# Stock price: 15.23 CNY/share, quantity: 500 shares
px_int = round(15.23 / 0.01 / 100) = 15  # Stored as Int64
qty_int = round(500 / 100) = 5  # Stored as Int64

# Decode
price = 15 * 0.01 * 100 = 15.00 CNY/share
qty = 5 * 100 = 500 shares
```

---

## Timezone Handling

### Partition Date Semantics

**Critical:** Partition `date` is in exchange-local timezone, not UTC.

| Exchange Type | Timezone | Example |
|---------------|----------|---------|
| Crypto (24/7) | UTC | binance-futures, coinbase, okx |
| SZSE/SSE | Asia/Shanghai (CST) | szse, sse |
| Future US | America/New_York (ET) | (not yet implemented) |

**Example:**
```python
# SZSE: 2024-09-30 00:30 CST → date=2024-09-30 (NOT 2024-09-29)
# binance-futures: 2024-05-01 23:00 UTC → date=2024-05-01

# Cross-exchange queries: Use ts_local_us for precise filtering
trades_binance = query.trades("binance-futures", "BTCUSDT", "2024-05-01 00:00", "2024-05-01 23:59")
trades_szse = query.trades("szse", "000001", "2024-09-30 00:00", "2024-09-30 23:59")
```

### Timestamp Columns

**All timestamps stored in UTC (microseconds):**
- `ts_local_us` - Arrival time (default timeline for replay)
- `ts_exch_us` - Exchange time (for exchange-specific analysis)

**Conversion:**
```python
import polars as pl

# Microseconds → datetime
trades = trades.with_columns([
    (pl.col("ts_local_us") / 1_000_000).cast(pl.Int64).cast(pl.Datetime("us")).alias("datetime_local"),
    (pl.col("ts_exch_us") / 1_000_000).cast(pl.Int64).cast(pl.Datetime("us")).alias("datetime_exch"),
])
```
