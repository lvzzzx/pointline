---
name: pointline-research
description: "Guide for using Pointline, a high-performance point-in-time (PIT) accurate data lake for quantitative trading research. Use when working with HFT/crypto/stock market data for: (1) Discovering available data (exchanges, symbols, coverage), (2) Loading trades/quotes/orderbook data, (3) Analyzing market microstructure (spreads, volume, order flow), (4) Running reproducible backtests, (5) Validating data quality. Supports crypto (spot/derivatives on 26+ exchanges) and Chinese stocks (SZSE/SSE Level 3 data). Emphasizes PIT correctness, deterministic ordering, and avoiding lookahead bias."
---

# Pointline Research Guide

Guide LLM agents through efficient, correct usage of the Pointline data lake for quantitative research.

## Quick Start Workflow

**ALWAYS follow this sequence:**

1. **Discover** → Check what data exists
2. **Query** → Load data with Query API (default)
3. **Analyze** → Apply analysis patterns with PIT correctness

```python
from pointline import research

# Step 1: Discover
exchanges = research.list_exchanges(asset_class="crypto-derivatives")
symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")
coverage = research.data_coverage("binance-futures", "BTCUSDT")

# Step 2: Query (decoded=True returns floats)
from pointline.research import query
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Step 3: Analyze (use ts_local_us for PIT correctness)
import polars as pl
trades = trades.sort("ts_local_us")  # Arrival order (PIT correct)
vwap = (trades["price"] * trades["qty"]).sum() / trades["qty"].sum()
```

---

## Discovery API (Start Here)

**CRITICAL:** Always check data availability before querying.

### List Exchanges

```python
from pointline import research

# All exchanges
exchanges = research.list_exchanges()

# Filter by asset class
crypto_spot = research.list_exchanges(asset_class="crypto-spot")
crypto_derivatives = research.list_exchanges(asset_class="crypto-derivatives")
chinese_stocks = research.list_exchanges(asset_class="stocks-cn")
```

**Asset Classes:**
- `"crypto"` - All crypto exchanges
- `"crypto-spot"` - Spot only (binance, coinbase, kraken, etc.)
- `"crypto-derivatives"` - Derivatives only (binance-futures, deribit, bybit, etc.)
- `"stocks-cn"` - Chinese stocks (szse, sse)

### List Symbols

```python
# All symbols on exchange
symbols = research.list_symbols(exchange="binance-futures")

# Filter by base asset
btc_symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")

# Fuzzy search
eth_symbols = research.list_symbols(search="ETH")
```

### Check Data Coverage

```python
# Check what data exists for a symbol
coverage = research.data_coverage("binance-futures", "BTCUSDT")
print(f"Trades: {coverage['trades']['available']}")
print(f"Quotes: {coverage['quotes']['available']}")
print(f"Book snapshots: {coverage['book_snapshot_25']['available']}")
```

### Summarize Symbol

```python
# Rich summary with metadata and coverage
research.summarize_symbol("BTCUSDT", exchange="binance-futures")
# Prints: exchange, symbol_id, price_increment, data availability, etc.
```

---

## API Selection Guide

### Default: Query API (90% of Use Cases)

**Use Query API for:**
- Exploratory analysis
- Quick data checks
- Ad-hoc investigations
- Most backtests

**Benefits:**
- Automatic symbol resolution
- Automatic decoding (decoded=True)
- ISO date strings or datetime objects
- Simple, concise code

```python
from pointline.research import query

# One-liner - automatic everything
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
book = query.book_snapshot_25("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

### Advanced: Core API (10% of Use Cases)

**Use Core API ONLY when user explicitly needs:**
- Reproducible research with symbol_id tracking
- Explicit control over symbol resolution
- Raw integer data (no decoding)
- Production-grade workflows

**Requires:**
- Manual symbol resolution (registry.find_symbol)
- Microsecond timestamp conversion
- Manual decoding (if needed)

```python
from pointline import registry, research
from datetime import datetime, timezone

# Step 1: Resolve symbol_id
symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]

# Step 2: Convert timestamps
start = datetime(2024, 5, 1, tzinfo=timezone.utc)
end = datetime(2024, 5, 2, tzinfo=timezone.utc)
start_ts_us = int(start.timestamp() * 1_000_000)
end_ts_us = int(end.timestamp() * 1_000_000)

# Step 3: Load with explicit symbol_id
trades = research.load_trades(symbol_id=symbol_id, start_ts_us=start_ts_us, end_ts_us=end_ts_us)

# Step 4: Decode if needed
from pointline.tables.trades import decode_fixed_point
from pointline.dim_symbol import read_dim_symbol_table
dim_symbol = read_dim_symbol_table()
trades = decode_fixed_point(trades, dim_symbol)
```

### Decision Tree

```
User asks to load data?
│
├─ Mentions "symbol_id" or "reproducibility"? → Core API
├─ Mentions "production research"? → Ask if they need explicit symbol_id control
│   ├─ Yes → Core API
│   └─ No → Query API
└─ Default → Query API (with decoded=True)
```

---

## Essential Schemas

### trades

**Common columns (decoded=True):**

| Column | Type | Description |
|--------|------|-------------|
| `ts_local_us` | Int64 | Arrival timestamp (UTC, µs) - USE THIS for replay |
| `ts_exch_us` | Int64 | Exchange timestamp (UTC, µs) |
| `symbol_id` | Int64 | Symbol identifier |
| `side` | UInt8 | 0=buy, 1=sell, 2=unknown |
| `price` | Float64 | Trade price (decoded) |
| `qty` | Float64 | Trade quantity (decoded) |

**PIT correctness:** Always use `ts_local_us` for sorting (arrival order).

```python
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
trades = trades.sort("ts_local_us")  # ✅ CORRECT - arrival order
```

### quotes

**Common columns (decoded=True):**

| Column | Type | Description |
|--------|------|-------------|
| `ts_local_us` | Int64 | Arrival timestamp (UTC, µs) |
| `bid_price` | Float64 | Best bid (decoded) |
| `bid_qty` | Float64 | Best bid quantity (decoded) |
| `ask_price` | Float64 | Best ask (decoded) |
| `ask_qty` | Float64 | Best ask quantity (decoded) |

**Example: Calculate mid price**

```python
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
quotes = quotes.with_columns(
    ((pl.col("ask_price") + pl.col("bid_price")) / 2).alias("mid_price")
)
```

### book_snapshot_25

**Common columns (decoded=True):**

| Column | Type | Description |
|--------|------|-------------|
| `ts_local_us` | Int64 | Arrival timestamp (UTC, µs) |
| `bid_price_0` to `bid_price_24` | Float64 | Bid prices at depth 0-24 (decoded) |
| `bid_qty_0` to `bid_qty_24` | Float64 | Bid quantities at depth 0-24 (decoded) |
| `ask_price_0` to `ask_price_24` | Float64 | Ask prices at depth 0-24 (decoded) |
| `ask_qty_0` to `ask_qty_24` | Float64 | Ask quantities at depth 0-24 (decoded) |

**Level 0 = top of book** (best bid/ask)

```python
book = query.book_snapshot_25("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Calculate total bid liquidity in top 10 levels
book = book.with_columns(
    pl.sum_horizontal([f"bid_qty_{i}" for i in range(10)]).alias("bid_liquidity_top10")
)
```

**Complete schemas:** See [references/schemas.md](references/schemas.md) for all tables (klines, derivative_ticker, szse_l3_orders, etc.)

---

## Critical Anti-Patterns

### ❌ DON'T: Use Core API for Simple Queries

```python
# ❌ BAD - Unnecessary complexity
from pointline import registry, research
symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]
start_ts_us = 1714521600000000
trades = research.load_trades(symbol_id=symbol_id, start_ts_us=start_ts_us, end_ts_us=...)
```

### ✅ DO: Use Query API

```python
# ✅ GOOD - Simple and correct
from pointline.research import query
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

### ❌ DON'T: Manually Convert Timestamps

```python
# ❌ BAD - Error-prone
from datetime import datetime, timezone
start = datetime(2024, 5, 1, tzinfo=timezone.utc)
start_ts_us = int(start.timestamp() * 1_000_000)  # Easy to mess up
```

### ✅ DO: Use ISO Strings

```python
# ✅ GOOD - Query API accepts ISO strings
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

### ❌ DON'T: Use Exchange Time for Replay

```python
# ❌ BAD - Lookahead bias!
trades = trades.sort("ts_exch_us")  # Exchange time
```

### ✅ DO: Use Arrival Time

```python
# ✅ GOOD - PIT correct
trades = trades.sort("ts_local_us")  # Arrival time
```

### ❌ DON'T: Skip Data Discovery

```python
# ❌ BAD - Assumes data exists
trades = query.trades("binance-futures", "UNKNOWN_SYMBOL", "2024-05-01", "2024-05-02")
# Error: Symbol not found!
```

### ✅ DO: Check Coverage First

```python
# ✅ GOOD - Verify before loading
coverage = research.data_coverage("binance-futures", "BTCUSDT")
if coverage["trades"]["available"]:
    trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

---

## Common Workflows

### Spread Analysis

```python
from pointline.research import query
import polars as pl

# Load quotes
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Calculate spread
quotes = quotes.with_columns([
    (pl.col("ask_price") - pl.col("bid_price")).alias("spread"),
    ((pl.col("ask_price") + pl.col("bid_price")) / 2).alias("mid_price"),
])

# Spread in basis points
quotes = quotes.with_columns(
    (pl.col("spread") / pl.col("mid_price") * 10000).alias("spread_bps")
)

print(quotes.select(pl.col("spread_bps").mean()))
```

### VWAP Calculation

```python
from pointline.research import query
import polars as pl

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Cumulative VWAP (PIT correct)
trades = trades.with_columns([
    ((pl.col("price") * pl.col("qty")).cum_sum() / pl.col("qty").cum_sum()).alias("vwap")
])
```

### As-of Join (Trades with Quotes)

```python
from pointline.research import query

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Match each trade with most recent quote (PIT correct)
trades_with_quotes = trades.join_asof(
    quotes.select(["ts_local_us", "bid_price", "ask_price"]),
    on="ts_local_us",
    strategy="backward",  # Use PAST quote only
)
```

**More patterns:** See [references/analysis_patterns.md](references/analysis_patterns.md) for:
- Order flow metrics (trade imbalance, order book imbalance)
- Market microstructure (price impact, effective spread)
- Execution quality (slippage, fill rates)

---

## Reproducibility Principles

### Point-in-Time (PIT) Correctness

**Default timeline: `ts_local_us` (arrival time), NOT `ts_exch_us` (exchange time)**

**Rationale:** Live trading reacts to arrival time. Using exchange time creates lookahead bias.

```python
# ✅ CORRECT - Arrival order
trades = trades.sort("ts_local_us")

# ❌ INCORRECT - Exchange order (lookahead bias)
trades = trades.sort("ts_exch_us")
```

### Deterministic Ordering

**Canonical ordering:** `(ts_local_us, file_id, file_line_number)` ascending

Pointline guarantees stable ordering for reproducible outputs.

```python
# ✅ CORRECT - Deterministic
trades = trades.sort("ts_local_us", "file_id", "file_line_number")

# ❌ INCORRECT - Arbitrary tie-breaking
trades = trades.sort("ts_local_us")
```

### Avoiding Lookahead Bias

**Use as-of joins (backward strategy):**

```python
# ✅ CORRECT - Only past data
trades_with_quotes = trades.join_asof(quotes, on="ts_local_us", strategy="backward")

# ❌ INCORRECT - May use future data
trades_with_quotes = trades.join(quotes, on="ts_local_us", how="left")
```

**Use cumulative calculations:**

```python
# ✅ CORRECT - Expanding window
vwap = (pl.col("price") * pl.col("qty")).cum_sum() / pl.col("qty").cum_sum()

# ❌ INCORRECT - Uses future data
vwap = pl.sum("price * qty") / pl.sum("qty")  # Whole period!
```

**Complete guide:** See [references/best_practices.md](references/best_practices.md) for:
- Symbol resolution workflow (SCD Type 2)
- Fixed-point encoding details
- Partition pruning optimization
- Experiment logging standards

---

## Supported Data

**26+ exchanges across:**
- **Crypto Spot:** binance, coinbase, kraken, okx, huobi, gate, bitfinex, bitstamp, gemini, crypto-com, kucoin
- **Crypto Derivatives:** binance-futures, deribit, bybit, okx-futures, bitmex, ftx, dydx
- **Chinese Stocks:** szse (SZSE), sse (SSE) with Level 3 order book data

**Tables:**
- `trades` - Trade executions
- `quotes` - Best bid/ask (Level 1)
- `book_snapshot_25` - Order book depth (25 levels)
- `kline_1h` - OHLCV candlesticks (1-hour)
- `derivative_ticker` - Funding rates, OI, mark/index prices
- `szse_l3_orders` - SZSE order placements/cancellations
- `szse_l3_ticks` - SZSE trade executions

**Discovery first:** Always use `research.list_exchanges()`, `research.list_symbols()`, and `research.data_coverage()` to check availability.

---

## Reference Files

Load these when needed for detailed guidance:

- **[references/analysis_patterns.md](references/analysis_patterns.md)** - Common quant analysis patterns (spreads, volume, order flow, market microstructure, execution quality)
- **[references/best_practices.md](references/best_practices.md)** - Reproducibility principles (PIT correctness, deterministic ordering, symbol resolution, avoiding lookahead bias)
- **[references/schemas.md](references/schemas.md)** - Comprehensive table schemas with all fields and encoding details
