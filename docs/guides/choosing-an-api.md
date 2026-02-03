# Choosing the Right API

Pointline provides two API layers designed for different use cases. This guide helps you choose the right one.

---

## Quick Decision Matrix

| Use Case | API to Use | Example |
|----------|------------|---------|
| Exploring data in Jupyter | **Query API** | `query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)` |
| Quick analysis or prototyping | **Query API** | Same as above |
| Answering user questions (LLM agents) | **Query API** | Same as above |
| Teaching or documentation | **Query API** | Same as above |
| One-off research tasks | **Query API** | Same as above |
| Production research pipeline | **Core API** | `research.load_trades(symbol_id=[123], start_ts_us=..., end_ts_us=...)` |
| Explicit reproducibility requirements | **Core API** | Same as above |
| Handling SCD Type 2 symbol changes | **Core API** | Same as above |
| Performance-critical queries | **Core API** | Same as above |

**Rule of thumb:** Use query API by default. Switch to core API only when you need explicit control.

---

## Query API (Default for 90% of Use Cases)

### When to Use ✅

- ✅ Exploration and prototyping
- ✅ Jupyter notebooks
- ✅ Quick data checks
- ✅ Teaching and examples
- ✅ LLM agent workflows
- ✅ One-off analysis

### Benefits

- **Ergonomic:** One-liner data loading
- **Intuitive:** Human-friendly timestamps (ISO strings, datetime objects)
- **Automatic:** Symbol resolution happens behind the scenes
- **Decoded by default:** Returns readable float prices (with `decoded=True`)

### Example

```python
from pointline.research import query

trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,
)
```

### Timestamp Flexibility

```python
# ISO strings (simplest)
trades = query.trades(..., start="2024-05-01", end="2024-05-02")

# Datetime objects
from datetime import datetime, timezone
trades = query.trades(
    ...,
    start=datetime(2024, 5, 1, tzinfo=timezone.utc),
    end=datetime(2024, 5, 2, tzinfo=timezone.utc),
)

# Microsecond timestamps (if you prefer)
trades = query.trades(..., start=1714521600000000, end=1714608000000000)
```

---

## Core API (Advanced Use Cases)

### When to Use ✅

- ✅ Production research requiring reproducibility
- ✅ Explicit symbol_id control (e.g., logging exact symbol_id used)
- ✅ Performance-critical queries needing optimization
- ✅ Handling SCD Type 2 symbol changes explicitly
- ✅ Multi-symbol queries with complex filtering

### Benefits

- **Explicit:** Full control over symbol resolution
- **Reproducible:** Record exact symbol_id in research logs
- **Optimizable:** Custom partition pruning strategies
- **SCD-aware:** Handle symbol delisting/relisting explicitly

### Example

```python
from pointline import research, registry

# Step 1: Resolve symbol explicitly
symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]

# Log symbol_id for reproducibility
print(f"Using symbol_id={symbol_id}")

# Step 2: Load data with explicit control
trades = research.load_trades(
    symbol_id=symbol_id,
    start_ts_us=1714521600000000,
    end_ts_us=1714608000000000,
)

# Step 3: Decode manually if needed
from pointline.tables.trades import decode_fixed_point
from pointline.dim_symbol import read_dim_symbol_table
dim_symbol = read_dim_symbol_table()
trades = decode_fixed_point(trades, dim_symbol)
```

### Why Use Core API for Production?

**Reproducibility Example:**
```python
# In your research logs (logs/runs.jsonl), you record:
{
  "run_id": "exp_001",
  "symbol_ids": [12345],  # Explicit ID
  "start_ts_us": 1714521600000000,
  "end_ts_us": 1714608000000000,
  "lake_root": "/path/to/lake",
  "git_commit": "abc123",
  "metrics": {...}
}

# 6 months later, you can reproduce EXACTLY:
trades = research.load_trades(
    symbol_id=[12345],  # Same ID, even if symbol changed
    start_ts_us=1714521600000000,
    end_ts_us=1714608000000000,
)
```

**SCD Type 2 Handling Example:**
```python
# If BTCUSDT was delisted and relisted, there might be 2 symbol_ids:
symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
# Returns:
# symbol_id  valid_from           valid_to
# 12345      2024-01-01 00:00:00  2024-06-30 23:59:59
# 67890      2024-07-01 00:00:00  9999-12-31 23:59:59

# Core API lets you query both explicitly:
trades_before = research.load_trades(symbol_id=[12345], ...)
trades_after = research.load_trades(symbol_id=[67890], ...)

# Query API automatically handles this (queries both):
trades_all = query.trades("binance-futures", "BTCUSDT", "2024-01-01", "2024-12-31")
```

---

## Comparison Table

| Feature | Query API | Core API |
|---------|-----------|----------|
| **Symbol resolution** | Automatic | Manual (explicit) |
| **Timestamp input** | ISO strings, datetime, int | Microseconds only |
| **Decoding** | `decoded=True` parameter | Manual with `decode_fixed_point()` |
| **Code verbosity** | 1-3 lines | 5-10 lines |
| **Reproducibility** | Good (queries by exchange+symbol) | Excellent (queries by symbol_id) |
| **SCD Type 2 handling** | Automatic (warns on changes) | Explicit (you control) |
| **Performance** | Same (uses core API internally) | Same |
| **Best for** | Exploration, prototyping | Production, auditable research |

---

## Common Misconceptions

### ❌ "Core API is faster"

**Not true.** Both APIs use the same underlying Delta Lake queries. Performance differences are minimal unless you're doing custom optimizations (which requires core API).

### ❌ "Query API is for beginners only"

**False.** Query API is the right choice for most research, even for advanced users. Use core API when you need explicit control, not because of skill level.

### ❌ "I should always use core API in scripts"

**False.** Scripts benefit from query API's ergonomics too. Only use core API when you need reproducibility guarantees or explicit symbol handling.

### ❌ "Query API doesn't handle symbol changes"

**False.** Query API automatically handles SCD Type 2 changes and warns you when symbol metadata changes during the query period.

---

## Migration Path

Already using core API and want to simplify?

**Before (core API):**
```python
from pointline import registry, research

symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]

trades = research.load_trades(
    symbol_id=symbol_id,
    start_ts_us=1714521600000000,
    end_ts_us=1714608000000000,
)

from pointline.tables.trades import decode_fixed_point
from pointline.dim_symbol import read_dim_symbol_table
dim_symbol = read_dim_symbol_table()
trades = decode_fixed_point(trades, dim_symbol)
```

**After (query API):**
```python
from pointline.research import query

trades = query.trades(
    "binance-futures",
    "BTCUSDT",
    "2024-05-01",
    "2024-05-02",
    decoded=True,
)
```

**Result:** Same data, 90% less code.

---

## Real-World Decision Examples

### Example 1: Jupyter Notebook Exploration

**Scenario:** Exploring BTC price volatility in May 2024

**Decision:** Use query API ✅

**Rationale:**
- Quick exploration task
- No reproducibility requirements
- Query API is simpler and faster to write

```python
from pointline.research import query
import polars as pl

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-06-01", decoded=True)
volatility = trades.select(pl.col("price_px").std())
```

---

### Example 2: Production Backtest

**Scenario:** Multi-symbol momentum strategy with audit trail

**Decision:** Use core API ✅

**Rationale:**
- Production research requiring reproducibility
- Need to log exact symbol_ids used
- Will run multiple times with version control

```python
from pointline import research, registry
import json

# Explicitly resolve symbols and log
symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]

# Log for reproducibility
run_config = {
    "symbol_ids": [symbol_id],
    "start_ts_us": 1714521600000000,
    "end_ts_us": 1717200000000000,
}
with open("logs/run_config.json", "w") as f:
    json.dump(run_config, f)

# Load with explicit control
trades = research.load_trades(**run_config)
```

---

### Example 3: LLM Agent Answering Questions

**Scenario:** User asks "What was BTC's average price in May?"

**Decision:** Use query API ✅

**Rationale:**
- Quick query
- User-facing response
- Query API is concise for agents

```python
from pointline.research import query
import polars as pl

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-06-01", decoded=True)
avg_price = trades.select(pl.col("price_px").mean()).item()
print(f"Average BTC price in May: ${avg_price:,.2f}")
```

---

### Example 4: Symbol Metadata Change Analysis

**Scenario:** Analyzing impact of tick size change on Aug 15, 2024

**Decision:** Use core API ✅

**Rationale:**
- Need to analyze each symbol version separately
- Metadata changes are core to the research question
- Core API gives explicit control

```python
from pointline import research, registry
import polars as pl

# Find both symbol versions
symbols = registry.find_symbol("SOLUSDT", exchange="binance-futures")
print(symbols[["symbol_id", "valid_from_ts", "tick_size"]])

# Analyze each version separately
for symbol_id in symbols["symbol_id"].to_list():
    trades = research.load_trades(symbol_id=symbol_id, ...)
    # Analyze...
```

---

## Still Not Sure?

**Default to query API.** You can always switch to core API later if you discover you need explicit control.

**Ask yourself:**
1. Do I need to log exact symbol_ids for reproducibility? → Core API
2. Is this exploration or production research? → Exploration = Query API, Production = Core API
3. Will I run this code once or many times? → Once = Query API, Many times = Core API
4. Do symbol metadata changes matter to my analysis? → Yes = Core API, No = Query API

**When in doubt, start with query API.** It's easier to write, read, and maintain.

---

## API Compatibility

Both APIs work together seamlessly:

```python
from pointline.research import query
from pointline import research, registry

# Discover with query API
coverage = query.data_coverage("binance-futures", "BTCUSDT")

# Load sample with query API
sample = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# If you like what you see, switch to core API for production
symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]

# Production code with explicit symbol_id
trades = research.load_trades(symbol_id=symbol_id, start_ts_us=..., end_ts_us=...)
```

---

## Summary

| API | Best For | Code Complexity | Reproducibility | Use Case % |
|-----|----------|-----------------|-----------------|------------|
| **Query API** | Exploration, prototyping | Low (1-3 lines) | Good | **90%** |
| **Core API** | Production, auditable research | High (5-10 lines) | Excellent | **10%** |

**Key takeaway:** Query API for exploration, core API for production. When in doubt, use query API.
