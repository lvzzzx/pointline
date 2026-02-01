# User-Friendliness Review: Pointline Research Data Lake

**Date:** 2026-02-01
**Reviewers:** Human Researcher + LLM Agent Perspectives
**Status:** Comprehensive Analysis

---

## Executive Summary

The Pointline project has **solid technical foundations** with a well-designed two-layer API (Core + Query), rigorous point-in-time correctness, and sophisticated SCD Type 2 symbol management. However, it suffers from **discoverability, onboarding friction, and cognitive load issues** that affect both human and LLM researchers.

**Key Finding:** Most user-friendliness issues are **documentation and discoverability problems**, not technical design flaws. The query API already provides excellent ergonomics—it just needs better promotion.

---

## Critical Issues (High Impact)

### 1. Data Discovery Gap 🔴

**Severity:** HIGH
**Impact:** New users can't explore without prior knowledge
**Affected Users:** All researchers, especially LLM agents

#### Problem
Researchers cannot easily answer fundamental questions:
- "What data do I have?"
- "What symbols are available on Binance?"
- "What date ranges exist for BTCUSDT?"
- "Which tables have data for this symbol?"

#### Current State
- No quick way to browse available symbols, exchanges, or date ranges
- Must know exchange name + symbol upfront
- No visual data catalog or summary
- Trial-and-error workflow to find data

#### Impact Examples
- New users spend hours discovering what data exists
- LLM agents cannot recommend symbols without external context
- Researchers write custom scripts for data inventory
- Onboarding friction leads to abandonment

#### Recommended Solutions

**Add discovery API:**
```python
from pointline import research

# List all exchanges with data
exchanges = research.list_exchanges()
# → ["binance-futures", "deribit", "coinbase", ...]

# List all symbols on an exchange
symbols = research.list_symbols(exchange="binance-futures")
# → DataFrame with columns: [symbol_id, exchange_symbol, base_asset, quote_asset, asset_type]

# Check data coverage for a specific symbol
coverage = research.data_coverage(
    exchange="binance-futures",
    symbol="BTCUSDT"
)
# → {
#     "trades": {"start_date": "2024-01-01", "end_date": "2024-12-31", "row_count": 1.2e9},
#     "quotes": {"start_date": "2024-01-01", "end_date": "2024-12-31", "row_count": 5.3e8},
#     "book_snapshot_25": {"start_date": "2024-05-01", "end_date": "2024-12-31", "row_count": 2.1e8},
# }

# Get symbol summary with metadata + data availability
summary = research.summarize_symbol("BTCUSDT", exchange="binance-futures")
# → Rich output with metadata, data tables, date ranges
```

**Add CLI commands:**
```bash
# List exchanges
pointline data list-exchanges

# List symbols
pointline data list-symbols --exchange binance-futures

# Check coverage
pointline data coverage --symbol BTCUSDT --exchange binance-futures
```

**Priority:** Immediate (highest ROI)

---

### 2. Symbol Resolution Friction 🟡

**Severity:** MEDIUM
**Impact:** Verbose workflows for common tasks
**Affected Users:** All researchers

#### Problem
Two-step symbol resolution is architecturally correct but cumbersome for exploration:

```python
# Current workflow (core API):
from pointline import registry, research

# Step 1: Find symbol_id (verbose)
symbols = registry.find_symbol("SOLUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]  # Manual extraction

# Step 2: Load data
trades = research.load_trades(
    symbol_id=symbol_id,
    start_ts_us=1700000000000000,  # Also non-intuitive
    end_ts_us=1700003600000000,
)
```

#### Good News
The query API already solves this:

```python
# Query API (simpler):
from pointline.research import query

trades = query.trades(
    exchange="binance-futures",
    symbol="SOLUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,  # Human-readable floats
)
```

#### Issue
Documentation doesn't emphasize the query API enough. Users default to the verbose core API because:
1. README.md shows core API first
2. Researcher guide emphasizes core API
3. CLAUDE.md has equal emphasis on both layers

#### Impact
- Unnecessary boilerplate in notebooks
- Cognitive load from symbol_id management
- LLM agents generate verbose code

#### Recommended Solutions

**Documentation restructuring:**
1. **README.md:** Show query API as primary example
2. **docs/quickstart.md:** Start with query API, mention core API as "Advanced"
3. **CLAUDE.md:** Lead with query API, relegate core API to production use cases
4. **Researcher guide:** Flip order—query API first, core API in "Advanced Topics"

**When to use which API:**
```markdown
## Choosing the Right API

### Use Query API (Default)
✅ Exploration and prototyping
✅ Jupyter notebooks
✅ Quick data checks
✅ Teaching and examples
✅ LLM agent workflows

**Example:**
```python
from pointline.research import query
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

### Use Core API (Advanced)
✅ Production research requiring reproducibility
✅ Handling SCD Type 2 symbol changes explicitly
✅ Performance-critical queries needing optimization
✅ Multi-symbol queries with complex filtering

**Example:**
```python
from pointline import research, registry
symbol_ids = registry.find_symbol("BTCUSDT", exchange="binance-futures")["symbol_id"].to_list()
trades = research.load_trades(symbol_id=symbol_ids, start_ts_us=..., end_ts_us=...)
```
```

**Priority:** High (quick documentation fix)

---

### 3. Timestamp Confusion 🟡

**Severity:** MEDIUM
**Impact:** Non-intuitive API for human users
**Affected Users:** Human researchers, notebook users

#### Problem
Microsecond timestamps are technically correct but cognitively expensive:

```python
# Non-intuitive (what date is this?):
start_ts_us = 1700000000000000
end_ts_us = 1700003600000000
```

#### Good News
Datetime support already exists:

```python
from datetime import datetime, timezone

# Intuitive:
start = datetime(2024, 5, 1, tzinfo=timezone.utc)
end = datetime(2024, 5, 2, tzinfo=timezone.utc)
```

#### Issue
Documentation shows int timestamps first, datetime second. Order should be reversed for human readability.

#### Impact
- Mental math to convert timestamps
- Copy-paste errors (milliseconds vs microseconds)
- Timezone confusion (naive vs aware datetimes)

#### Recommended Solutions

**Show datetime examples first everywhere:**
```python
# GOOD (show this first):
from datetime import datetime, timezone

trades = query.trades(
    "binance-futures",
    "BTCUSDT",
    start=datetime(2024, 5, 1, tzinfo=timezone.utc),
    end=datetime(2024, 5, 2, tzinfo=timezone.utc),
)

# ALSO SUPPORTED (mention second):
trades = query.trades(
    "binance-futures",
    "BTCUSDT",
    start="2024-05-01",  # ISO string
    end="2024-05-02",
)

# ADVANCED (mention last):
trades = query.trades(
    "binance-futures",
    "BTCUSDT",
    start=1714521600000000,  # Microseconds since epoch
    end=1714608000000000,
)
```

**Add timezone helper:**
```python
# Create pointline/utils.py
from datetime import datetime, timezone

def utc(*args, **kwargs):
    """Convenience wrapper for UTC datetime.

    Examples:
        >>> from pointline.utils import utc
        >>> utc(2024, 5, 1)  # midnight UTC
        >>> utc(2024, 5, 1, 12, 30)  # 12:30 UTC
    """
    return datetime(*args, **kwargs, tzinfo=timezone.utc)

# Usage:
from pointline.utils import utc
trades = query.trades("binance-futures", "BTCUSDT", utc(2024, 5, 1), utc(2024, 5, 2))
```

**Priority:** Medium (documentation + small API addition)

---

### 4. Fixed-Point Decoding Cognitive Load 🟡

**Severity:** MEDIUM
**Impact:** Extra mental overhead for common tasks
**Affected Users:** Human researchers

#### Problem
`price_int` vs `price` creates unnecessary cognitive load:

```python
# Current (core API):
trades = research.load_trades(...)  # Returns price_int, qty_int
# User must remember to decode manually
from pointline.tables.trades import decode_fixed_point
trades = decode_fixed_point(trades, dim_symbol)
```

#### Good News
`decoded=True` parameter exists:

```python
# Already works:
trades = research.load_trades_decoded(...)
# Or:
trades = query.trades(..., decoded=True)
```

#### Issue
- Not prominent enough in documentation
- Naming is inconsistent (`load_trades()` vs `load_trades_decoded()`)
- Users don't know when to use fixed-point vs float

#### Impact
- Researchers forget to decode, get wrong calculations
- Extra imports and boilerplate
- Confusion about which columns are available

#### Recommended Solutions

**Promote decoded loaders:**
```markdown
## Working with Prices and Quantities

**For exploration (default):** Use decoded loaders
```python
# Returns human-readable float columns: price, qty
trades = query.trades(..., decoded=True)
```

**For production (advanced):** Use fixed-point integers
```python
# Returns fixed-point int columns: price_int, qty_int
# Use when you need exact precision or performance
trades = research.load_trades(...)
```
```

**Consider API renaming (breaking change):**
```python
# Current:
research.load_trades()           # Fixed-point
research.load_trades_decoded()   # Floats

# Proposed (more intuitive):
research.load_trades_raw()       # Explicit about fixed-point
research.load_trades()           # Default to human-readable
```

**Add warning if user forgets to decode:**
```python
# In __repr__ for DataFrame with *_int columns:
def __repr__(self):
    if any(col.endswith("_int") for col in self.columns):
        print("⚠️  Warning: This DataFrame contains fixed-point integer columns.")
        print("   Use decoded=True or load_trades_decoded() for human-readable prices.")
    return super().__repr__()
```

**Priority:** Medium (documentation + optional API enhancement)

---

## Moderate Issues (Medium Impact)

### 5. Experiment Setup Friction 🟡

**Severity:** MEDIUM
**Impact:** Slows down research iteration
**Affected Users:** Human researchers

#### Problem
Too much boilerplate to start a new experiment:

1. Copy `research/03_experiments/_template/` directory
2. Manually rename directory with date
3. Manually edit config.yaml
4. Manually resolve symbol_ids
5. Write boilerplate data loading code
6. Create directory structure (logs/, results/, plots/)

#### Impact
- 15-30 minutes to start each experiment
- Copy-paste errors
- Inconsistent experiment structure
- Discourages experimentation

#### Recommended Solution

**Add experiment scaffold CLI:**
```bash
# Create new experiment with automatic setup
pointline experiment create \
  --name "my-new-test" \
  --symbol "BTCUSDT" \
  --exchange "binance-futures" \
  --start "2024-05-01" \
  --end "2024-05-31" \
  --tables trades,quotes

# → Generates:
# research/03_experiments/exp_2026-02-01_my-new-test/
# ├── README.md (pre-filled with metadata)
# ├── config.yaml (symbol_ids auto-resolved)
# ├── notebook.ipynb (starter code with data loading)
# ├── logs/
# ├── results/
# └── plots/
```

**Generated config.yaml:**
```yaml
# Auto-generated by pointline experiment create
# Date: 2026-02-01

experiment:
  name: my-new-test
  created: 2026-02-01T10:30:00Z

data:
  lake_root: /path/to/lake
  tables:
    - trades
    - quotes

symbols:
  - exchange: binance-futures
    symbol: BTCUSDT
    symbol_id: 12345  # Auto-resolved

time_range:
  start: 2024-05-01T00:00:00Z
  end: 2024-05-31T23:59:59Z
  ts_col: ts_local_us
```

**Generated starter notebook:**
```python
# Auto-generated starter code
from pointline.research import query
from datetime import datetime, timezone

# Load data (adjust as needed)
trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start=datetime(2024, 5, 1, tzinfo=timezone.utc),
    end=datetime(2024, 5, 31, tzinfo=timezone.utc),
    decoded=True,
)

print(f"Loaded {trades.height:,} trades")
trades.head(5)
```

**Priority:** Medium (quality-of-life improvement)

---

### 6. Error Messages Could Be More Actionable 🟡

**Severity:** MEDIUM
**Impact:** Slows down debugging
**Affected Users:** All researchers

#### Problem
Current error messages are informative but lack next steps.

**Example 1: Symbol not found**
```python
# Current:
ValueError: No symbols found for exchange='binance-futures', symbol='BTCUSD'

# Improved:
ValueError: No symbols found for exchange='binance-futures', symbol='BTCUSD'

Did you mean?
  - BTCUSDT (available on binance-futures)
  - BTCUSD-PERP (available on deribit)

Search for symbols:
  from pointline import registry
  registry.find_symbol("BTC")  # Fuzzy search across all exchanges
```

**Example 2: No data for date range**
```python
# Current:
ValueError: No data found for symbol_id=12345 in range [1700000000000000, 1700003600000000)

# Improved:
ValueError: No data found for symbol_id=12345 in range [2023-11-14 12:00:00 UTC, 2023-11-14 13:00:00 UTC)

Available data for this symbol:
  - trades: 2024-01-01 to 2024-12-31 (365 days)
  - quotes: 2024-01-01 to 2024-12-31 (365 days)

Your query is outside this range. Try:
  research.data_coverage(exchange="binance-futures", symbol="BTCUSDT")
```

**Example 3: Exchange not recognized**
```python
# Current:
ValueError: Exchange 'binance' not found in EXCHANGE_MAP

# Improved:
ValueError: Exchange 'binance' not found

Available exchanges:
  - binance-futures (USDT-margined perpetuals)
  - binance-coin-futures (COIN-margined perpetuals)
  - binance-us (spot trading)

Did you mean 'binance-futures'?

List all exchanges:
  from pointline import research
  research.list_exchanges()
```

#### Recommended Solutions

**Enhance error messages with:**
1. Human-readable timestamp formatting
2. Fuzzy search suggestions ("Did you mean?")
3. Data availability context
4. Copy-paste code snippets to fix the issue

**Implementation:**
```python
# In pointline/_error_messages.py

def symbol_not_found_error_with_suggestions(exchange: str, symbol: str) -> str:
    # Fuzzy search for similar symbols
    similar = _fuzzy_search_symbols(symbol, limit=3)

    msg = f"No symbols found for exchange='{exchange}', symbol='{symbol}'\n\n"

    if similar:
        msg += "Did you mean?\n"
        for s in similar:
            msg += f"  - {s['symbol']} (available on {s['exchange']})\n"
        msg += "\n"

    msg += "Search for symbols:\n"
    msg += "  from pointline import registry\n"
    msg += f"  registry.find_symbol('{symbol[:3]}')  # Fuzzy search\n"

    return msg
```

**Priority:** Medium (incremental improvement)

---

### 7. Documentation Fragmentation 🟡

**Severity:** MEDIUM
**Impact:** Users don't know where to start
**Affected Users:** All new users

#### Problem
Information is scattered across multiple files with unclear hierarchy:

**Current structure:**
```
docs/
├── CLAUDE.md                    # LLM instructions (comprehensive)
├── README.md                    # High-level overview (sparse)
├── docs/research_api_guide.md   # API reference (detailed)
├── docs/guides/researcher_guide.md  # Comprehensive guide (overwhelming)
├── docs/schemas.md              # Schema reference
└── docs/architecture/design.md  # Architecture details
```

#### Issues
1. No clear entry point for new users
2. README is too sparse (doesn't show actual usage)
3. Researcher guide is comprehensive but overwhelming (409 lines)
4. No "5-minute quickstart" tutorial
5. CLAUDE.md is the best onboarding doc, but it's for LLMs!

#### Impact
- New users read wrong doc first
- Duplicate information across files
- Outdated content in some docs
- LLM agents get confused about canonical source

#### Recommended Solutions

**Restructure documentation:**
```
docs/
├── README.md                    # Project overview + quickstart link
├── quickstart.md               # 🌟 NEW: 5-minute tutorial (copy-paste examples)
├── user-guide.md               # 🌟 NEW: Consolidated researcher guide
├── api-reference.md            # API docs (auto-generated from docstrings)
├── recipes.md                  # 🌟 NEW: Common tasks (copy-paste snippets)
├── architecture/
│   └── design.md
├── guides/
│   ├── advanced-topics.md      # Core API, SCD Type 2, performance tuning
│   └── llm-agent-guide.md      # CLAUDE.md renamed
└── schemas.md
```

**Create quickstart.md (5-minute tutorial):**
```markdown
# Quickstart: 5 Minutes to Your First Plot

## 1. Load Data (1 minute)
```python
from pointline.research import query

trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,
)

print(f"Loaded {trades.height:,} trades")
```

## 2. Explore Data (2 minutes)
```python
# View sample
trades.head(5)

# Summary statistics
trades.select(["price", "qty"]).describe()

# Filter large trades
large_trades = trades.filter(pl.col("qty") > 1.0)
```

## 3. Visualize (2 minutes)
```python
import matplotlib.pyplot as plt

# Convert timestamps
trades_df = trades.with_columns(
    pl.from_epoch("ts_local_us", time_unit="us").alias("timestamp")
)

# Plot price over time
plt.figure(figsize=(12, 6))
plt.plot(trades_df["timestamp"], trades_df["price"])
plt.xlabel("Time")
plt.ylabel("Price (USD)")
plt.title("BTC-USDT Price on 2024-05-01")
plt.show()
```

## Next Steps
- [Common Recipes](recipes.md) - Copy-paste examples for common tasks
- [User Guide](user-guide.md) - Comprehensive reference
- [API Reference](api-reference.md) - Full API documentation
```

**Create recipes.md (copy-paste snippets):**
```markdown
# Common Recipes

## Table of Contents
1. [Load and Filter Data](#load-and-filter-data)
2. [Calculate VWAP](#calculate-vwap)
3. [Join Trades and Quotes](#join-trades-and-quotes)
4. [Compute Spread](#compute-spread)
5. [Aggregate to Bars](#aggregate-to-bars)
6. [Backtest Simple Signal](#backtest-simple-signal)

---

## Load and Filter Data

**Load 1 day of trades:**
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

**Load with lazy evaluation (large datasets):**
```python
trades_lf = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-09-30",
    lazy=True,  # LazyFrame
    decoded=True,
)

# Filter before collecting
large_trades = trades_lf.filter(pl.col("qty") > 1.0).collect()
```

---

## Calculate VWAP

```python
import polars as pl

vwap = trades.select([
    (pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()
]).item()

print(f"VWAP: ${vwap:.2f}")
```

[... more recipes ...]
```

**Priority:** High (critical for onboarding)

---

## LLM Agent-Specific Issues

### 8. LLM Agents Struggle with Multi-Step Workflows 🟡

**Severity:** MEDIUM
**Impact:** LLM agents generate verbose, error-prone code
**Affected Users:** LLM agents (Claude, GPT, etc.)

#### Problem
Symbol resolution requires multi-step workflows that agents struggle with:

**Example agent workflow:**
```
User: "Show me BTC trades on Binance"

Agent thinks:
  1. Need symbol_id → must call registry.find_symbol()
  2. Extract symbol_id from DataFrame result
  3. Convert date string to microseconds
  4. Call research.load_trades()
  5. Decode fixed-point integers
  6. Convert to human-readable format

→ 5-6 steps with error potential at each step
```

**Actual agent code generated:**
```python
# Verbose, error-prone:
from pointline import registry, research
from datetime import datetime, timezone
from pointline.tables.trades import decode_fixed_point
from pointline.dim_symbol import read_dim_symbol_table

# Step 1: Find symbol
symbols = registry.find_symbol("BTC", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]  # What if multiple results?

# Step 2: Convert timestamps
start = datetime(2024, 5, 1, tzinfo=timezone.utc)
start_ts_us = int(start.timestamp() * 1_000_000)  # Easy to mess up
end = datetime(2024, 5, 2, tzinfo=timezone.utc)
end_ts_us = int(end.timestamp() * 1_000_000)

# Step 3: Load data
trades = research.load_trades(
    symbol_id=symbol_id,
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
)

# Step 4: Decode
dim_symbol = read_dim_symbol_table()
trades = decode_fixed_point(trades, dim_symbol)
```

#### Good News
Query API already solves this:

```python
# One-liner for agents:
from pointline.research import query

trades = query.trades(
    "binance-futures",
    "BTCUSDT",
    "2024-05-01",
    "2024-05-02",
    decoded=True,
)
```

#### Issue
CLAUDE.md gives equal weight to both APIs. Agents don't know which to prefer.

#### Recommended Solutions

**Update CLAUDE.md with clear agent guidance:**
```markdown
## Agent Interface (Quick Reference)

**Default workflow (ALWAYS use this for exploration):**
```python
from pointline.research import query

# One-liner - automatic symbol resolution + decoding:
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
book = query.book_snapshot_25("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

**Only use core API when the user specifically requests:**
- Explicit symbol_id control
- Production research requiring reproducibility
- Performance-critical queries
- Handling SCD Type 2 symbol changes explicitly

**Never:**
- Use core API for exploration or quick checks
- Write multi-step symbol resolution code when query API exists
- Convert timestamps manually when ISO strings work
- Decode fixed-point manually when decoded=True exists
```

**Priority:** High (immediate CLAUDE.md update)

---

### 9. Missing Agent-Friendly Utilities 🟡

**Severity:** LOW
**Impact:** LLM agents lack convenience helpers
**Affected Users:** LLM agents

#### Problem
Agents need utilities for common assistant tasks:

**Missing utilities:**
1. Quick visualization (one-liner plots)
2. Table introspection (schema + sample)
3. Query validation (pre-flight checks)
4. Data summarization (describe + statistics)

#### Recommended Solutions

**Add agent-friendly helpers:**
```python
# pointline/research/helpers.py

def quick_plot(
    data: pl.DataFrame,
    x: str = "ts_local_us",
    y: str | list[str] = "price",
    title: str | None = None,
    **kwargs
):
    """One-liner plot for quick visualization.

    Examples:
        >>> trades = query.trades(...)
        >>> quick_plot(trades, y="price")
        >>> quick_plot(trades, y=["price", "qty"])  # Dual axis
    """
    import matplotlib.pyplot as plt
    # Implementation...

def describe_table(table_name: str) -> None:
    """Print table schema + sample rows.

    Examples:
        >>> describe_table("trades")
        Table: trades
        Path: /lake/silver/trades
        Schema:
          - symbol_id: Int64
          - ts_local_us: Int64
          - price_int: Int64
          - qty_int: Int64
          ...

        Sample (5 rows):
        [displays head(5)]
    """
    # Implementation...

def validate_query(
    exchange: str,
    symbol: str,
    start: str,
    end: str,
    table: str = "trades",
) -> dict:
    """Pre-flight check before expensive query.

    Returns:
        {
            "valid": bool,
            "warnings": list[str],
            "estimated_rows": int,
            "estimated_memory_mb": float,
        }

    Examples:
        >>> check = validate_query("binance-futures", "BTCUSDT", "2024-01-01", "2024-12-31")
        >>> if check["warnings"]:
        >>>     print("Warnings:", check["warnings"])
        >>> print(f"Estimated rows: {check['estimated_rows']:,}")
    """
    # Implementation...

def summarize(data: pl.DataFrame) -> None:
    """Rich summary of DataFrame with automatic type detection.

    Examples:
        >>> summarize(trades)
        Shape: (1,234,567 rows, 12 columns)

        Timestamp range:
          - ts_local_us: 2024-05-01 00:00:00 to 2024-05-01 23:59:59

        Numeric columns:
          - price: mean=$67,123.45, std=$234.56, min=$66,500, max=$67,800
          - qty: mean=0.123, std=0.456, min=0.001, max=10.5

        Categorical:
          - side: [BUY: 52.3%, SELL: 47.7%]
    """
    # Implementation...
```

**Usage in CLAUDE.md:**
```markdown
## Agent Helpers

When the user asks to visualize or explore data, use these helpers:

```python
from pointline.research import query, helpers

# Load data
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Quick visualization
helpers.quick_plot(trades, y="price", title="BTC Price")

# Rich summary
helpers.summarize(trades)

# Check query before running
check = helpers.validate_query("binance-futures", "BTCUSDT", "2024-01-01", "2024-12-31")
if check["estimated_memory_mb"] > 1000:
    print("Warning: Large query, consider using lazy=True")
```
```

**Priority:** Low (nice-to-have)

---

## Low-Priority Enhancements

### 10. Notebook-Friendly Defaults 🟢

**Severity:** LOW
**Impact:** Quality-of-life for notebook users
**Affected Users:** Jupyter notebook users

#### Recommendations

**Auto-limit large DataFrames:**
```python
# In research/__init__.py
import polars as pl

# Set notebook-friendly defaults
pl.Config.set_tbl_rows(100)  # Limit display to 100 rows
pl.Config.set_fmt_str_lengths(50)  # Truncate long strings
```

**Rich repr for LazyFrame:**
```python
def _repr_html_(self):
    """Show query plan in Jupyter."""
    return f"""
    <div style="border: 1px solid #ccc; padding: 10px;">
        <strong>LazyFrame (not yet executed)</strong><br>
        Estimated rows: {self.estimated_rows:,}<br>
        <code>{self.query_plan()}</code>
    </div>
    """
```

**Progress bars for long queries:**
```python
from tqdm.auto import tqdm

# Integrate with Polars progress callbacks
trades = query.trades(..., show_progress=True)
# → Shows: Loading: 45% |████████████     | 450k/1M rows
```

**Priority:** Low

---

### 11. Type Hints Coverage 🟢

**Severity:** LOW
**Impact:** IDE autocomplete and type checking
**Affected Users:** Python developers

#### Current State
Type hints exist and are generally good.

#### Recommendations
Add examples in docstrings with type annotations:

```python
def trades(
    exchange: str,
    symbol: str,
    start: TimestampInput,
    end: TimestampInput,
    *,
    ts_col: str = "ts_local_us",
    columns: list[str] | tuple[str, ...] | None = None,
    decoded: bool = False,
    keep_ints: bool = False,
    lazy: bool = True,
) -> pl.LazyFrame | pl.DataFrame:
    """Load trades with automatic symbol resolution.

    Args:
        exchange: Exchange name (e.g., "binance-futures")
        symbol: Exchange symbol (e.g., "BTCUSDT")
        start: Start time (accepts multiple formats)
            - datetime: timezone-aware datetime object
            - int: microseconds since Unix epoch
            - str: ISO 8601 date/datetime ("2024-05-01", "2024-05-01T12:00:00Z")
        end: End time (same formats as start)
        ts_col: Timestamp column to filter on
        columns: Columns to select
        decoded: Decode fixed-point integers to floats
        keep_ints: Keep integer columns when decoded=True
        lazy: Return LazyFrame (True) or DataFrame (False)

    Returns:
        Trades data (LazyFrame or DataFrame depending on lazy parameter)

    Examples:
        >>> from pointline.research import query
        >>> from datetime import datetime, timezone
        >>>
        >>> # With datetime objects:
        >>> trades: pl.DataFrame = query.trades(
        ...     exchange="binance-futures",
        ...     symbol="BTCUSDT",
        ...     start=datetime(2024, 5, 1, tzinfo=timezone.utc),
        ...     end=datetime(2024, 5, 2, tzinfo=timezone.utc),
        ...     lazy=False,
        ... )
        >>>
        >>> # With ISO strings:
        >>> trades_lf: pl.LazyFrame = query.trades(
        ...     "binance-futures",
        ...     "BTCUSDT",
        ...     "2024-05-01",
        ...     "2024-05-02",
        ...     lazy=True,
        ... )
    """
```

**Priority:** Low

---

## What's Actually Great ✅

**Don't change these aspects—they're working well:**

1. ✅ **Two-layer API design** (core + query) is architecturally sound
2. ✅ **SCD Type 2 handling** is sophisticated and correct
3. ✅ **PIT correctness philosophy** prevents lookahead bias
4. ✅ **Polars LazyFrame support** enables large dataset processing
5. ✅ **Decoded loaders exist** - just need better promotion
6. ✅ **Symbol registry design** is robust
7. ✅ **Delta Lake storage** with Z-ordering for performance
8. ✅ **Timezone handling** is rigorous
9. ✅ **Fixed-point encoding** preserves precision
10. ✅ **Comprehensive documentation** exists (just needs reorganization)

---

## Prioritized Action Plan

### Phase 1: Quick Wins (1-2 days) 🚀

**Immediate impact, low effort:**

1. **Rewrite README.md** with query API as primary example
   - Show one-liner data loading
   - Link to quickstart guide
   - Estimated effort: 2 hours

2. **Create docs/quickstart.md** (5-minute tutorial)
   - Load data → explore → visualize
   - Copy-paste examples
   - Estimated effort: 3 hours

3. **Update CLAUDE.md** with query API emphasis
   - Lead with query API for agents
   - Relegate core API to advanced use cases
   - Estimated effort: 2 hours

4. **Add data discovery functions** to research module
   - `list_exchanges()`
   - `list_symbols(exchange)`
   - `data_coverage(exchange, symbol)`
   - Estimated effort: 4 hours

**Total Phase 1: ~11 hours (1-2 days)**

---

### Phase 2: Quality of Life (1 week) 💎

**High value, moderate effort:**

5. **Improve error messages** with actionable suggestions
   - Fuzzy search for typos
   - Show available data ranges
   - Code snippets to fix issues
   - Estimated effort: 8 hours

6. **Add experiment scaffold CLI** (`pointline experiment create`)
   - Auto-generate directory structure
   - Pre-fill config with resolved symbol_ids
   - Generate starter notebook
   - Estimated effort: 12 hours

7. **Create docs/recipes.md** with common patterns
   - Load and filter
   - Calculate VWAP
   - Join trades + quotes
   - Compute spread
   - Aggregate to bars
   - Backtest simple signal
   - Estimated effort: 6 hours

8. **Consolidate documentation** (restructure)
   - Create user-guide.md
   - Create api-reference.md
   - Deprecate redundant docs
   - Estimated effort: 8 hours

**Total Phase 2: ~34 hours (1 week)**

---

### Phase 3: Advanced (2 weeks) 🔬

**Nice-to-have, larger effort:**

9. **Add agent-friendly helpers**
   - `quick_plot()`
   - `describe_table()`
   - `validate_query()`
   - `summarize()`
   - Estimated effort: 16 hours

10. **Add timezone utility** (`utc()` helper)
    - Convenience wrapper
    - Documentation updates
    - Estimated effort: 3 hours

11. **Notebook enhancements**
    - Progress bars
    - Rich repr for LazyFrame
    - Auto-limiting large outputs
    - Estimated effort: 8 hours

12. **Build data catalog viewer** (optional web UI)
    - Browse exchanges, symbols, tables
    - Interactive date range picker
    - Schema viewer
    - Estimated effort: 40 hours (optional)

**Total Phase 3: ~67 hours (2 weeks, excluding web UI)**

---

## Success Metrics

### Quantitative Metrics

1. **Onboarding time:** New user → first plot
   - Current: ~2 hours (estimate)
   - Target: <10 minutes

2. **Documentation bounce rate:** % users finding what they need
   - Current: Unknown
   - Target: >80% find answer in first doc

3. **Error resolution time:** Time to fix query errors
   - Current: ~15 minutes (estimate)
   - Target: <5 minutes

4. **Code verbosity:** Lines of code for common tasks
   - Current: ~15 lines (symbol resolution + loading)
   - Target: ~3 lines (query API)

### Qualitative Metrics

1. **User feedback:** "Easy to use" rating
2. **LLM agent success rate:** % of queries that work first try
3. **Community questions:** Frequency of common issues in support
4. **Documentation clarity:** User comprehension surveys

---

## Conclusion

**Key Findings:**

1. **Technical architecture is sound** - The two-layer API, SCD Type 2 handling, and PIT correctness are excellent design choices.

2. **Most issues are documentation and discoverability problems** - The query API already provides great ergonomics; it just needs better promotion.

3. **Biggest ROI improvements:**
   - Promote query API as the default (documentation fix)
   - Add data discovery helpers (4 hours of coding)
   - Create 5-minute quickstart tutorial (3 hours of writing)
   - Improve error messages (8 hours of enhancement)

4. **LLM agents struggle unnecessarily** - CLAUDE.md should lead with query API to reduce multi-step workflows.

**Recommendation:** Focus on Phase 1 (Quick Wins) first. These are high-impact, low-effort changes that will immediately improve the researcher experience. Phase 2 and 3 can follow based on user feedback and priorities.

---

## Appendix: User Personas

### Persona 1: Academic Researcher (Human)
- **Goal:** Test trading hypotheses with historical data
- **Pain points:** Steep learning curve, verbose code, unclear data availability
- **Needs:** Quick onboarding, copy-paste examples, clear documentation

### Persona 2: Quant Developer (Human)
- **Goal:** Build production research pipelines
- **Pain points:** Need explicit control, performance tuning, reproducibility
- **Needs:** Core API, advanced docs, optimization guides

### Persona 3: LLM Agent (AI)
- **Goal:** Answer user questions about market data
- **Pain points:** Multi-step workflows, ambiguous documentation, no shortcuts
- **Needs:** One-liner APIs, clear preferences, helper functions

### Persona 4: Data Scientist (Human)
- **Goal:** Explore data in Jupyter notebooks
- **Pain points:** Too much boilerplate, non-intuitive timestamps, forgetting to decode
- **Needs:** Notebook-friendly defaults, decoded by default, quick visualization
