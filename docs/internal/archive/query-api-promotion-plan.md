# Query API Promotion Plan

**Goal:** Make the query API the default path for 90% of users while preserving the core API for production use cases.

**Status:** Proposed
**Owner:** TBD
**Estimated Effort:** 12-16 hours
**Target Completion:** 1 week

---

## Problem Statement

The Pointline project has **two API layers** that serve different purposes:

1. **Core API** (`research.load_*()` + `registry.find_symbol()`)
   - Explicit symbol_id resolution
   - Production-grade reproducibility
   - SCD Type 2 handling
   - Performance optimization opportunities

2. **Query API** (`query.trades()`, `query.quotes()`, etc.)
   - Automatic symbol resolution
   - Human-friendly timestamps
   - Decoded prices by default
   - One-liner ergonomics

**Current Issue:** Documentation doesn't clearly guide users toward the right API for their use case, leading to:
- New users writing verbose, error-prone code
- LLM agents generating multi-step workflows unnecessarily
- Cognitive overhead for common exploration tasks
- Poor first impressions during onboarding

**Root Cause:** Documentation hierarchy problem, not technical architecture issue.

---

## Success Criteria

### Quantitative Metrics
- [ ] 90% of quickstart examples use query API
- [ ] README.md hero example uses query API
- [ ] Core API documentation has clear "when to use this" guidance
- [ ] All user-facing docs reviewed and updated

### Qualitative Metrics
- [ ] New users can load data in <5 minutes without reading full docs
- [ ] LLM agents default to query API for exploration tasks
- [ ] Core API usage increases for production research (right tool for right job)
- [ ] User feedback indicates clear understanding of API layering

---

## Phase 1: Documentation Audit (2 hours)

### Deliverables
- [ ] **Inventory current documentation**
  - List all files that show code examples
  - Tag each with "query API first" or "core API first"
  - Identify outdated or inconsistent examples

- [ ] **Create prioritization matrix**
  - High visibility: README.md, quickstart guide
  - Medium visibility: API reference, researcher guide
  - Low visibility: Architecture docs, advanced guides

### Files to Audit
```
High Priority (user-facing):
├── README.md
├── CLAUDE.md (already good, verify consistency)
├── docs/quickstart.md (create if missing)
├── docs/guides/researcher_guide.md

Medium Priority (reference):
├── docs/research_api_guide.md
├── Docstrings in research.py
├── Docstrings in research/query.py

Low Priority (advanced):
├── docs/architecture/design.md
└── research/03_experiments/_template/README.md
```

### Action
```bash
# Create audit checklist
rg "load_trades|query\.trades" --files-with-matches docs/ > /tmp/docs_to_audit.txt
```

---

## Phase 2: README.md Hero Example (1 hour)

### Current State
README.md likely shows core API or is too sparse.

### Target State
README.md should have a compelling "5-minute quickstart" section that shows the query API.

### Implementation

**New README.md structure:**

```markdown
# Pointline

High-performance, point-in-time accurate data lake for quantitative research.

## Quick Start (5 Minutes)

```python
from pointline.research import query

# 1. Discover what data is available
from pointline import research
exchanges = research.list_exchanges(asset_class="crypto-derivatives")
symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")

# 2. Check data coverage
coverage = research.data_coverage("binance-futures", "BTCUSDT")

# 3. Load and explore data
trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,  # Human-readable prices
)

print(f"Loaded {trades.height:,} trades")
trades.head(5)
```

**For production research:** See [Advanced API](docs/guides/advanced-topics.md) for explicit symbol_id control and performance optimization.

## Installation

[... existing content ...]

## Documentation

- **[5-Minute Tutorial](docs/quickstart.md)** ← Start here
- **[Common Recipes](docs/recipes.md)** - Copy-paste examples
- **[User Guide](docs/user-guide.md)** - Comprehensive reference
- **[API Reference](docs/research_api_guide.md)** - Full API documentation
- **[Advanced Topics](docs/guides/advanced-topics.md)** - Core API, SCD Type 2, performance tuning

[... rest of README ...]
```

### Deliverables
- [ ] Updated README.md with query API hero example
- [ ] Clear link to advanced docs for core API

---

## Phase 3: Create docs/quickstart.md (3 hours)

### Purpose
A standalone, copy-paste tutorial that gets users to their first plot in 5 minutes.

### Structure

```markdown
# Quickstart: 5 Minutes to Your First Plot

## Prerequisites

```bash
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
```

## Step 1: Discover Available Data (1 minute)

```python
from pointline import research

# What exchanges have data?
exchanges = research.list_exchanges()
print(exchanges)

# What symbols are available on Binance?
symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")
print(symbols)

# Check data coverage for BTCUSDT
coverage = research.data_coverage("binance-futures", "BTCUSDT")
print(f"Trades available: {coverage['trades']['available']}")
```

**Expected output:**
```
Trades available: True
Date range: 2024-01-01 to 2024-12-31
```

## Step 2: Load Data (1 minute)

```python
from pointline.research import query

trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,  # Returns price/qty as floats
)

print(f"Loaded {trades.height:,} trades")
```

**What just happened?**
- ✅ Automatically resolved symbol → symbol_id
- ✅ Converted ISO date strings → microsecond timestamps
- ✅ Decoded fixed-point integers → human-readable floats
- ✅ Used lazy evaluation for memory efficiency

## Step 3: Explore Data (2 minutes)

```python
import polars as pl

# View sample
trades.head(5)

# Summary statistics
trades.select(["price_px", "qty"]).describe()

# Filter large trades
large_trades = trades.filter(pl.col("qty") > 1.0)

# Calculate VWAP
vwap = trades.select([
    (pl.col("price_px") * pl.col("qty")).sum() / pl.col("qty").sum()
]).item()

print(f"VWAP: ${vwap:.2f}")
```

## Step 4: Visualize (1 minute)

```python
import matplotlib.pyplot as plt

# Convert to eager DataFrame for plotting
trades_df = trades.collect()

# Convert timestamps to datetime
trades_df = trades_df.with_columns(
    pl.from_epoch("ts_local_us", time_unit="us").alias("timestamp")
)

# Plot price over time
plt.figure(figsize=(12, 6))
plt.plot(trades_df["timestamp"], trades_df["price_px"])
plt.xlabel("Time")
plt.ylabel("Price (USD)")
plt.title("BTC-USDT Price on 2024-05-01")
plt.show()
```

## Next Steps

✅ **Exploration:** Continue using `query.trades()`, `query.quotes()`, etc.
📚 **Common patterns:** See [Recipes](recipes.md) for copy-paste examples
🔬 **Production research:** See [Advanced Topics](guides/advanced-topics.md) for core API

## When to Use the Core API

The query API is perfect for 90% of use cases. Use the **core API** when you need:

- Explicit symbol_id control for reproducibility
- Performance optimization with custom partition pruning
- SCD Type 2 symbol change handling
- Multi-symbol queries with complex filters

**Example (advanced):**
```python
from pointline import research, registry

# Explicit symbol resolution
symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]

# Core API with explicit control
trades = research.load_trades(
    symbol_id=symbol_id,
    start_ts_us=1714521600000000,
    end_ts_us=1714608000000000,
)

# Manual decoding
from pointline.tables.trades import decode_fixed_point
from pointline.dim_symbol import read_dim_symbol_table
dim_symbol = read_dim_symbol_table()
trades = decode_fixed_point(trades, dim_symbol)
```

See [Advanced Topics](guides/advanced-topics.md) for details.

---

## Troubleshooting

**"No symbols found for exchange='binance-futures', symbol='BTCUSD'"**

Use discovery API to search:
```python
symbols = research.list_symbols(search="BTC", exchange="binance-futures")
```

**"No data found for date range"**

Check coverage:
```python
coverage = research.data_coverage("binance-futures", "BTCUSDT")
print(coverage)
```

**"Memory error when loading large date ranges"**

Use lazy evaluation and filter before collecting:
```python
trades_lf = query.trades(..., lazy=True)  # Returns LazyFrame
large_trades = trades_lf.filter(pl.col("qty") > 1.0)
result = large_trades.collect()  # Only materializes filtered data
```
```

### Deliverables
- [ ] docs/quickstart.md created
- [ ] Tested examples work end-to-end
- [ ] Linked from README.md

---

## Phase 4: Update CLAUDE.md (1 hour)

### Current State
CLAUDE.md already emphasizes discovery API and query workflow (good!).

### Enhancements Needed
Add explicit anti-patterns section for LLM agents.

### Implementation

**Add to CLAUDE.md after "Data Discovery" section:**

```markdown
## API Selection Guide (CRITICAL FOR AGENTS)

### Default Workflow: Query API

**ALWAYS use this for exploration, analysis, and user questions:**

```python
from pointline.research import query

# One-liner - automatic symbol resolution + decoding
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
book = query.book_snapshot_25("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

**When user asks:** "Show me BTC trades on Binance"
**Correct response:** Use `query.trades()` directly ✅
**Incorrect response:** Multi-step workflow with `registry.find_symbol()` + manual extraction ❌

### Advanced Workflow: Core API

**ONLY use when user explicitly requests:**
- Production research requiring reproducibility
- Explicit symbol_id control
- Performance-critical queries with custom optimization
- Handling SCD Type 2 symbol changes explicitly

### Anti-Patterns (DO NOT DO THIS)

❌ **Don't use core API for simple queries:**
```python
# BAD - Unnecessary complexity
from pointline import registry, research
symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]  # Manual extraction
trades = research.load_trades(symbol_id=symbol_id, start_ts_us=..., end_ts_us=...)
```

✅ **Do use query API:**
```python
# GOOD - Simple and correct
from pointline.research import query
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

❌ **Don't manually convert timestamps:**
```python
# BAD
from datetime import datetime, timezone
start = datetime(2024, 5, 1, tzinfo=timezone.utc)
start_ts_us = int(start.timestamp() * 1_000_000)  # Error-prone
```

✅ **Do use ISO strings or datetime objects directly:**
```python
# GOOD - ISO string
trades = query.trades(..., start="2024-05-01", end="2024-05-02")

# GOOD - datetime object (query API accepts both)
from datetime import datetime, timezone
trades = query.trades(..., start=datetime(2024, 5, 1, tzinfo=timezone.utc), ...)
```

❌ **Don't manually decode fixed-point:**
```python
# BAD
trades = research.load_trades(...)
from pointline.tables.trades import decode_fixed_point
from pointline.dim_symbol import read_dim_symbol_table
dim_symbol = read_dim_symbol_table()
trades = decode_fixed_point(trades, dim_symbol)
```

✅ **Do use decoded=True:**
```python
# GOOD
trades = query.trades(..., decoded=True)
```

## Decision Tree for Agents

```
User asks to load data?
│
├─ Is this exploration/analysis? ──> Use query.trades(..., decoded=True)
│
├─ Is this production research? ──> Ask user if they need explicit symbol_id control
│   ├─ Yes ──> Use core API (research.load_trades + registry.find_symbol)
│   └─ No ──> Use query API
│
└─ User explicitly mentions "symbol_id" or "reproducibility"?
    └─ Yes ──> Use core API
```
```

### Deliverables
- [ ] CLAUDE.md updated with anti-patterns section
- [ ] Decision tree for LLM agent API selection
- [ ] Examples showing correct vs incorrect patterns

---

## Phase 5: Docstring Updates (3 hours)

### Goal
Add "See also" references from core API to query API.

### Implementation

**Update `pointline/research.py`:**

```python
def load_trades(
    symbol_id: int | list[int],
    start_ts_us: int,
    end_ts_us: int,
    *,
    ts_col: str = "ts_local_us",
    columns: list[str] | tuple[str, ...] | None = None,
    lazy: bool = True,
) -> pl.LazyFrame | pl.DataFrame:
    """Load trades with explicit symbol_id control.

    This is the **core API** for production research requiring reproducibility
    and explicit symbol_id handling.

    For exploration and prototyping, see `pointline.research.query.trades()`
    which provides automatic symbol resolution and human-friendly timestamps.

    Args:
        symbol_id: Symbol ID(s) from dim_symbol table
        start_ts_us: Start timestamp (microseconds since epoch, UTC)
        end_ts_us: End timestamp (microseconds since epoch, UTC)
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: Specific columns to select
        lazy: Return LazyFrame (True) or DataFrame (False)

    Returns:
        Trades with fixed-point integer columns (px_int, qty_int).
        Use `decode_fixed_point()` or `load_trades_decoded()` for floats.

    Examples:
        >>> # Core API (explicit control):
        >>> from pointline import registry, research
        >>> symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
        >>> symbol_id = symbols["symbol_id"][0]
        >>> trades = research.load_trades(
        ...     symbol_id=symbol_id,
        ...     start_ts_us=1714521600000000,
        ...     end_ts_us=1714608000000000,
        ... )

        >>> # Query API (simpler for exploration):
        >>> from pointline.research import query
        >>> trades = query.trades(
        ...     "binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True
        ... )

    See Also:
        - `pointline.research.query.trades()` - Simpler API for exploration
        - `load_trades_decoded()` - Returns float columns instead of integers
        - `registry.find_symbol()` - Resolve exchange symbol to symbol_id
    """
    # ... existing implementation ...
```

**Update `pointline/research/query.py`:**

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

    This is the **query API** designed for exploration, prototyping, and
    interactive analysis. It automatically resolves symbols, handles timestamps,
    and decodes prices to human-readable floats.

    For production research requiring explicit symbol_id control, see
    `pointline.research.load_trades()`.

    Args:
        exchange: Exchange name (e.g., "binance-futures")
        symbol: Exchange symbol (e.g., "BTCUSDT")
        start: Start time (accepts multiple formats):
            - datetime: timezone-aware datetime object
            - int: microseconds since Unix epoch
            - str: ISO 8601 date/datetime ("2024-05-01", "2024-05-01T12:00:00Z")
        end: End time (same formats as start)
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: Specific columns to select
        decoded: Decode fixed-point integers to floats (recommended)
        keep_ints: Keep integer columns when decoded=True
        lazy: Return LazyFrame (True) or DataFrame (False)

    Returns:
        Trades data (LazyFrame or DataFrame depending on lazy parameter).
        With decoded=True: price_px/qty as floats.
        With decoded=False: px_int/qty_int as integers.

    Examples:
        >>> from pointline.research import query
        >>>
        >>> # With ISO strings (simplest):
        >>> trades = query.trades(
        ...     "binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True
        ... )
        >>>
        >>> # With datetime objects:
        >>> from datetime import datetime, timezone
        >>> trades = query.trades(
        ...     exchange="binance-futures",
        ...     symbol="BTCUSDT",
        ...     start=datetime(2024, 5, 1, tzinfo=timezone.utc),
        ...     end=datetime(2024, 5, 2, tzinfo=timezone.utc),
        ...     decoded=True,
        ... )
        >>>
        >>> # Lazy evaluation for large datasets:
        >>> trades_lf = query.trades(..., lazy=True)  # LazyFrame
        >>> large_trades = trades_lf.filter(pl.col("qty") > 1.0)
        >>> result = large_trades.collect()  # Materialize filtered data

    See Also:
        - `pointline.research.load_trades()` - Core API with explicit symbol_id control
        - `pointline.research.data_coverage()` - Check data availability first
        - `pointline.research.list_symbols()` - Discover available symbols
    """
    # ... existing implementation ...
```

### Deliverables
- [ ] Updated docstrings in research.py
- [ ] Updated docstrings in research/query.py
- [ ] Cross-references between core and query APIs
- [ ] Clear examples showing both approaches

---

## Phase 6: Create "When to Use Which API" Guide (2 hours)

### Purpose
A definitive reference that users and LLM agents can consult.

### Location
`docs/guides/choosing-an-api.md`

### Content

```markdown
# Choosing the Right API

Pointline provides two API layers designed for different use cases:

## Quick Decision Matrix

| Use Case | API to Use | Example |
|----------|------------|---------|
| Exploring data in Jupyter | Query API | `query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)` |
| Quick analysis or prototyping | Query API | Same as above |
| Answering user questions (LLM agents) | Query API | Same as above |
| Teaching or documentation | Query API | Same as above |
| One-off research tasks | Query API | Same as above |
| Production research pipeline | Core API | `research.load_trades(symbol_id=[123], start_ts_us=..., end_ts_us=...)` |
| Explicit reproducibility requirements | Core API | Same as above |
| Handling SCD Type 2 symbol changes | Core API | Same as above |
| Performance-critical queries | Core API | Same as above |

## Query API (Default for 90% of Use Cases)

### When to Use
✅ Exploration and prototyping
✅ Jupyter notebooks
✅ Quick data checks
✅ Teaching and examples
✅ LLM agent workflows
✅ One-off analysis

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

## Core API (Advanced Use Cases)

### When to Use
✅ Production research requiring reproducibility
✅ Explicit symbol_id control (e.g., logging exact symbol_id used)
✅ Performance-critical queries needing optimization
✅ Handling SCD Type 2 symbol changes explicitly
✅ Multi-symbol queries with complex filtering

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

## Common Misconceptions

### "Core API is faster"
**Not always true.** Both APIs use the same underlying Delta Lake queries. Performance differences are minimal unless you're doing custom optimizations (which requires core API).

### "Query API is for beginners only"
**False.** Query API is the right choice for most research, even for advanced users. Use core API when you need explicit control, not because of skill level.

### "I should always use core API in scripts"
**False.** Scripts benefit from query API's ergonomics too. Only use core API when you need reproducibility guarantees or explicit symbol handling.

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

## Still Not Sure?

Default to query API. You can always switch to core API later if you discover you need explicit control.
```

### Deliverables
- [ ] docs/guides/choosing-an-api.md created
- [ ] Linked from main documentation index
- [ ] Reviewed by team for accuracy

---

## Phase 7: Validation & Testing (2 hours)

### Test Plan

1. **Fresh User Test**
   - [ ] Give README.md to someone unfamiliar with Pointline
   - [ ] Time how long it takes them to load their first dataset
   - [ ] Collect feedback on clarity

2. **LLM Agent Test**
   - [ ] Clear Claude Code conversation
   - [ ] Ask: "Show me BTC trades on Binance for May 1st, 2024"
   - [ ] Verify it uses query API, not multi-step workflow
   - [ ] Test with GPT-4 as well

3. **Documentation Consistency Check**
   - [ ] All examples use consistent parameter names
   - [ ] All timestamps use consistent formatting
   - [ ] No contradictory guidance across docs

4. **Code Examples Validation**
   - [ ] Run all code snippets in documentation
   - [ ] Verify they produce expected output
   - [ ] Check for typos or outdated API calls

### Deliverables
- [ ] Test results documented
- [ ] Issues found and fixed
- [ ] Sign-off from at least 2 reviewers

---

## Rollout

### Week 1
- ✅ Phase 1: Documentation audit (2 hours)
- ✅ Phase 2: README.md update (1 hour)
- ✅ Phase 3: Create quickstart.md (3 hours)
- ✅ Phase 4: Update CLAUDE.md (1 hour)

**Checkpoint:** Review draft docs with team

### Week 2
- ✅ Phase 5: Docstring updates (3 hours)
- ✅ Phase 6: "Choosing an API" guide (2 hours)
- ✅ Phase 7: Validation & testing (2 hours)

**Checkpoint:** User testing and feedback

### Week 3
- Iterate based on feedback
- Final review and approval
- Publish updated documentation

---

## Success Metrics (30-day post-launch)

### User Behavior
- [ ] 80%+ of new users start with query API
- [ ] Core API usage increases for production research (right tool for right job)
- [ ] Support questions about "how to load data" decrease by 50%

### Code Quality
- [ ] Internal research notebooks use query API consistently
- [ ] Experiment template updated to show query API first

### Agent Performance
- [ ] LLM agents default to query API for exploration
- [ ] Multi-step symbol resolution code reduced by 90%

---

## Risk Mitigation

### Risk: Users think core API is "deprecated"
**Mitigation:** Emphasize in all docs that core API is for production, not deprecated

### Risk: Breaking changes to existing research
**Mitigation:** This plan doesn't change any APIs, only documentation. Existing code continues to work.

### Risk: Documentation becomes outdated again
**Mitigation:**
- Add CI check for code examples in docs
- Quarterly documentation review process
- Update template to use query API

---

## Open Questions

1. Should we add a `--simple` flag to CLI that uses query API?
   ```bash
   pointline query trades --exchange binance-futures --symbol BTCUSDT --date 2024-05-01
   ```

2. Should we create video tutorials showing query API?

3. Should we add a "migration guide" for users already using core API?

---

## Appendix: File Checklist

### To Create
- [ ] docs/quickstart.md
- [ ] docs/guides/choosing-an-api.md

### To Update
- [ ] README.md
- [ ] CLAUDE.md
- [ ] pointline/research.py (docstrings)
- [ ] pointline/research/query.py (docstrings)
- [ ] docs/research_api_guide.md (add API selection section)
- [ ] research/03_experiments/_template/README.md (use query API in examples)

### To Review (ensure consistency)
- [ ] docs/guides/researcher_guide.md
- [ ] docs/architecture/design.md
- [ ] All existing notebooks in research/

---

**Next Steps:**
1. Review and approve this plan
2. Assign owner and timeline
3. Create tracking issues for each phase
4. Begin Phase 1 (documentation audit)
