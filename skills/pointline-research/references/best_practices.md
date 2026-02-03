# Reproducibility Best Practices

This guide covers critical principles for deterministic, reproducible quantitative research with Pointline.

## Point-in-Time (PIT) Correctness

### Default Timeline: ts_local_us (Arrival Time)

**Always use `ts_local_us` as the default replay timeline, NOT `ts_exch_us`.**

**Rationale:** Live trading systems react to arrival time (when data reaches your system), not exchange timestamps. Using exchange time creates lookahead bias because you're simulating knowledge of events before they could have been observed.

```python
# ✅ CORRECT - Arrival time (PIT correct)
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
trades = trades.sort("ts_local_us")  # Replay in arrival order

# ❌ INCORRECT - Exchange time (lookahead bias)
trades = trades.sort("ts_exch_us")  # Dangerous! Not how you would have seen events
```

**When to use ts_exch_us:**
- Exchange-specific analysis (e.g., "what happened on the exchange at exactly 12:00 UTC")
- Regulatory reporting requiring exchange timestamps
- Debugging exchange behavior

**Default assumption:** Unless explicitly analyzing exchange-time semantics, always use `ts_local_us`.

### Arrival vs Exchange Time Example

```python
# Scenario: Network latency causes events to arrive out of order

# Exchange timestamps (ts_exch_us):
# Event A: 12:00:00.100
# Event B: 12:00:00.200

# Arrival timestamps (ts_local_us):
# Event B: 12:00:00.150 (arrived first due to network)
# Event A: 12:00:00.180 (arrived second)

# Live trading would see: B, then A (arrival order)
# Using ts_exch_us would see: A, then B (lookahead bias!)
```

## Deterministic Ordering

**Canonical ordering:** `(ts_local_us, file_id, file_line_number)` ascending

Pointline guarantees stable ordering within each partition. All silver tables are sorted by:
1. `ts_local_us` - Primary timeline (microsecond precision)
2. `file_id` - Breaks ties between files with same timestamp
3. `file_line_number` - Breaks ties within same file

**Why this matters:**
- Reproducible backtests require identical event sequences
- Debugging requires stable line-by-line replay
- Regression testing depends on deterministic outputs

```python
# ✅ CORRECT - Deterministic ordering
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02")
trades = trades.sort("ts_local_us", "file_id", "file_line_number")

# ❌ INCORRECT - Unstable ordering
trades = trades.sort("ts_local_us")  # Ties broken arbitrarily!
```

**Note:** When using decoded=True, the Query API automatically handles sorting correctly.

## Symbol Resolution Workflow

### Always Resolve symbol_id Upfront

**Critical:** Never use raw exchange symbols (like "BTCUSDT") directly in partition filters. Always resolve to `symbol_id` first.

**Why:**
1. Symbol metadata changes over time (SCD Type 2)
2. Same ticker can represent different contracts (e.g., BTCUSDT perpetual vs quarterly futures)
3. Partition pruning requires symbol_id for optimal performance

### Query API (Automatic Resolution)

```python
# ✅ CORRECT - Query API handles resolution automatically
from pointline.research import query

trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",  # Automatically resolved to symbol_id
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,
)
```

The Query API:
1. Calls `registry.find_symbol()` to resolve "BTCUSDT" → symbol_id
2. Validates the symbol exists for the exchange
3. Applies partition pruning with symbol_id
4. Returns data with decoded prices (if decoded=True)

### Core API (Explicit Resolution)

```python
# ✅ CORRECT - Core API requires explicit symbol_id
from pointline import registry, research

# Step 1: Resolve symbol_id
symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]

# Step 2: Convert time range to microsecond timestamps
from datetime import datetime, timezone
start = datetime(2024, 5, 1, tzinfo=timezone.utc)
end = datetime(2024, 5, 2, tzinfo=timezone.utc)
start_ts_us = int(start.timestamp() * 1_000_000)
end_ts_us = int(end.timestamp() * 1_000_000)

# Step 3: Load data with explicit symbol_id
trades = research.load_trades(
    symbol_id=symbol_id,
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
)
```

**When to use Core API:**
- Production research requiring reproducibility guarantees
- Explicit tracking of symbol_id for audit trails
- Advanced workflows needing direct control

**Default:** Use Query API unless you have a specific reason to use Core API.

### Handling Symbol Changes (SCD Type 2)

Symbols can change metadata over time (price_increment, lot_size, etc.). Pointline tracks this with SCD Type 2:

```python
# Check symbol history
from pointline.dim_symbol import read_dim_symbol_table

dim_symbol = read_dim_symbol_table()
btc_history = dim_symbol.filter(
    (pl.col("exchange_symbol") == "BTCUSDT") &
    (pl.col("exchange") == "binance-futures")
).sort("valid_from_ts")

# Example output:
# symbol_id  | exchange_symbol | price_increment | valid_from_ts      | valid_until_ts     | is_current
# 12345      | BTCUSDT         | 0.01           | 2020-01-01 00:00   | 2023-06-15 12:00   | False
# 12346      | BTCUSDT         | 0.1            | 2023-06-15 12:00   | 9999-12-31 23:59   | True
```

**Implication:** A backtest spanning 2020-2024 may encounter multiple symbol_ids for "BTCUSDT" due to metadata changes.

**Query API handles this:** Automatically selects the current symbol_id unless you specify a time range requiring historical symbol_ids.

## Avoiding Lookahead Bias

### Common Lookahead Pitfalls

**1. Using future data in calculations**

```python
# ❌ INCORRECT - Using close price from next bar
klines["signal"] = pl.when(klines["close"] > klines["close"].shift(-1)).then(1).otherwise(0)

# ✅ CORRECT - Using only historical data
klines["signal"] = pl.when(klines["close"] > klines["close"].shift(1)).then(1).otherwise(0)
```

**2. Using end-of-period data for intra-period decisions**

```python
# ❌ INCORRECT - Using VWAP calculated over full period
trades_with_vwap = trades.with_columns(
    vwap=pl.sum("price * qty") / pl.sum("qty")  # Uses all trades!
)

# ✅ CORRECT - Using cumulative VWAP (expanding window)
trades_with_vwap = trades.with_columns(
    vwap=(pl.col("price") * pl.col("qty")).cum_sum() / pl.col("qty").cum_sum()
)
```

**3. Using exchange time instead of arrival time**

See "Point-in-Time Correctness" section above.

### As-of Joins

When joining tables, always use as-of joins to avoid lookahead:

```python
# ✅ CORRECT - As-of join (uses only past data)
trades_with_quotes = trades.join_asof(
    quotes,
    left_on="ts_local_us",
    right_on="ts_local_us",
    strategy="backward",  # Use most recent quote BEFORE trade
)

# ❌ INCORRECT - Regular join (may use future quotes)
trades_with_quotes = trades.join(quotes, on="ts_local_us", how="left")
```

## Fixed-Point Encoding

### Understanding Integer Encoding

Pointline stores all prices as **fixed-point integers** to avoid floating-point precision errors:

```
px_int = round(price / price_increment)
```

**Example:**
- BTCUSDT price: 50,123.45
- price_increment: 0.01
- px_int: 5,012,345 (stored as Int64)

### Decoding Workflow

**Query API (Automatic):**

```python
# ✅ CORRECT - Automatic decoding
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
# Returns DataFrame with float columns: price, qty, etc.
```

**Core API (Manual):**

```python
# Load integer data
trades = research.load_trades(symbol_id=12345, start_ts_us=..., end_ts_us=...)

# Decode to floats
from pointline.tables.trades import decode_fixed_point
from pointline.dim_symbol import read_dim_symbol_table

dim_symbol = read_dim_symbol_table()
trades_decoded = decode_fixed_point(trades, dim_symbol)
```

### Why Fixed-Point?

**Precision errors with floats:**

```python
# ❌ Float precision loss
0.1 + 0.2 == 0.3  # False! (0.30000000000000004)

# ✅ Integer math is exact
1 + 2 == 3  # True
```

**In trading context:**
- Price: 1234.56 vs 1234.5600000000001 (can cause spurious spread calculations)
- Cumulative PnL drift over millions of trades
- Hash-based checksums fail due to non-deterministic rounding

**Pointline approach:** Keep integers until final decoding at research edge.

## Partition Pruning for Performance

### Require symbol_id + Time Range

Pointline partitions tables by `exchange` and `date`. Within partitions, Delta Lake maintains statistics on `symbol_id` and `ts_local_us` columns (via Z-ordering).

**Critical:** Always filter by symbol_id + time range to leverage partition pruning.

```python
# ✅ CORRECT - Partition pruning enabled
trades = research.load_trades(
    symbol_id=12345,
    start_ts_us=1714521600000000,
    end_ts_us=1714608000000000,
)
# Delta Lake skips irrelevant files using statistics

# ❌ INCORRECT - Full table scan!
trades = research.scan_table("trades").filter(
    pl.col("symbol_id") == 12345
).collect()
```

**Performance difference:**
- With pruning: Reads ~1 file (1-10 MB)
- Without pruning: Scans ~1000 files (10-100 GB)

### Query API Automatic Pruning

The Query API automatically applies partition pruning:

```python
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02")
# Internally:
# 1. Resolves symbol_id
# 2. Converts dates to timestamps
# 3. Applies partition filters (exchange=binance-futures, date=2024-05-01)
# 4. Applies symbol_id + ts_local_us filters (Delta Lake statistics pruning)
```

## Idempotent ETL

**Principle:** Same inputs + metadata → same outputs

Pointline ETL is designed to be idempotent:
- Same bronze file → same file_id (SHA256 hash)
- Same timestamp ordering → same file_line_number
- Re-running ingestion produces identical outputs

**Validation:**

```python
# Run ingestion twice
pointline ingest run --table trades --exchange binance-futures --date 2024-05-01
pointline ingest run --table trades --exchange binance-futures --date 2024-05-01

# Verify identical outputs
trades_run1 = research.scan_table("trades", exchange="binance-futures", date="2024-05-01").collect()
trades_run2 = research.scan_table("trades", exchange="binance-futures", date="2024-05-01").collect()

assert trades_run1.frame_equal(trades_run2)  # Should be identical
```

**Why this matters:**
- Reproducible research across environments
- Debugging with confidence
- Regression testing

## Experiment Logging

**Template:** `research/03_experiments/exp_YYYY-MM-DD_name/`

```
exp_2024-05-15_spread_alpha/
├── README.md        # Hypothesis, method, results
├── config.yaml      # All parameters (symbols, dates, etc.)
├── queries/         # SQL or query notes
├── logs/            # JSONL run logs (one line per run)
├── results/         # Metrics, CSVs
└── plots/           # Figures
```

### Run Logging (logs/runs.jsonl)

**Required fields:**
- `run_id` - Unique identifier (UUID or timestamp)
- `git_commit` - Git SHA for code reproducibility
- `lake_root` - Data lake path
- `symbol_ids` - List of symbol_ids used (NOT exchange symbols!)
- `tables` - Tables accessed (trades, quotes, etc.)
- `start_ts_us` / `end_ts_us` - Time range
- `ts_col` - Timeline used (default: ts_local_us)
- `params` - All hyperparameters
- `metrics` - Results (Sharpe, PnL, etc.)

**Example:**

```json
{
  "run_id": "20240515-143022",
  "git_commit": "a1b2c3d4",
  "lake_root": "/data/lake",
  "symbol_ids": [12345, 12346],
  "tables": ["trades", "quotes"],
  "start_ts_us": 1714521600000000,
  "end_ts_us": 1714608000000000,
  "ts_col": "ts_local_us",
  "params": {"spread_threshold": 0.001, "window_size": 100},
  "metrics": {"sharpe": 1.85, "pnl": 12500.00, "num_trades": 450}
}
```

### Why Log symbol_ids (Not Symbols)?

```python
# ❌ INCORRECT - Symbol names can change meaning
{"symbols": ["BTCUSDT"]}  # Which symbol_id? Current or historical?

# ✅ CORRECT - symbol_id is deterministic
{"symbol_ids": [12345]}  # Immutable identifier
```

**Rationale:** Symbol metadata changes over time (SCD Type 2). Recording symbol_id ensures exact reproducibility.

## Summary Checklist

Before running production research, verify:

- [ ] Using `ts_local_us` as default timeline (not ts_exch_us)
- [ ] Deterministic ordering: `(ts_local_us, file_id, file_line_number)`
- [ ] Symbol resolution upfront (Query API automatic, Core API explicit)
- [ ] No lookahead bias (as-of joins, historical-only calculations)
- [ ] Decoded prices only at research edge (keep integers during processing)
- [ ] Partition pruning enabled (symbol_id + time range filters)
- [ ] Experiment logged with symbol_ids, git_commit, lake_root
