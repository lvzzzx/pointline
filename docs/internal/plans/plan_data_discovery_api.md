# Plan: Data Discovery API

**Date:** 2026-02-01
**Status:** Proposal
**Priority:** HIGH (Critical user-friendliness gap)

---

## Executive Summary

Design and implement a robust, extensible discovery API that allows researchers (human and LLM) to explore available data across multiple asset classes (crypto, Chinese stocks, future additions) without prior knowledge of the data lake contents.

**Current Support:**
- **Crypto:** 16 exchanges (spot: binance, coinbase, kraken...; derivatives: binance-futures, deribit, bybit...)
- **Chinese Stocks:** 2 exchanges (SZSE, SSE) with L3 order book data

**Future Extensibility:**
- US equities (NYSE, NASDAQ)
- Futures (CME, ICE)
- Options
- Forex
- Other regional stock exchanges

---

## 1. Problem Statement

### Current State (Pain Points)
Researchers cannot answer basic questions:
- **"What exchanges have data?"** → Must browse code or docs
- **"What symbols are on Binance?"** → Must query dim_symbol manually
- **"What date ranges exist for BTCUSDT?"** → Must scan manifest + silver tables
- **"Which tables have data for this symbol?"** → Trial and error queries

### Impact
- **Onboarding friction:** New users spend hours discovering data
- **LLM agent failures:** Agents can't recommend symbols without context
- **Poor UX:** No data catalog, no guided exploration
- **Cognitive load:** Must remember exchange names, table names, conventions

---

## 2. Design Principles

### 2.1 Extensibility
**Requirement:** Support current data (crypto + Chinese stocks) and future additions (US equities, futures, options, forex) without breaking changes.

**Approach:**
- Use **asset_class taxonomy** to organize exchanges and symbols
- Exchange metadata registry with extensible attributes
- Table-agnostic coverage queries

### 2.2 Performance
**Requirement:** Discovery queries must be fast (<1 second) even with millions of symbols.

**Approach:**
- Leverage existing Delta Lake metadata (no full table scans)
- Cache dim_symbol in memory
- Use manifest table statistics (already indexed by exchange/date/symbol)

### 2.3 Simplicity
**Requirement:** API must be intuitive for both humans and LLM agents.

**Approach:**
- Start broad → narrow down (exchanges → symbols → tables → date ranges)
- Sensible defaults (filter current symbols, exclude historical-only exchanges)
- Rich output with context (not just lists)

---

## 3. API Design

### 3.1 Exchange Discovery

#### `list_exchanges()` - List all exchanges with data
```python
def list_exchanges(
    *,
    asset_class: str | list[str] | None = None,
    active_only: bool = True,
    include_stats: bool = True,
) -> pl.DataFrame:
    """List all exchanges with available data.

    Args:
        asset_class: Filter by asset class (crypto, stocks, futures, options, forex, all)
            - "crypto": All crypto exchanges (spot + derivatives)
            - "crypto-spot": Crypto spot only
            - "crypto-derivatives": Crypto derivatives only
            - "stocks": All stock exchanges
            - "stocks-cn": Chinese stocks only
            - "stocks-us": US stocks only (future)
            - "all": All asset classes
            - Can pass list: ["crypto-spot", "stocks-cn"]
        active_only: If True, exclude historical-only exchanges (e.g., ftx)
        include_stats: If True, include row counts and date ranges

    Returns:
        DataFrame with columns:
            - exchange: str (e.g., "binance-futures")
            - exchange_id: int
            - asset_class: str (e.g., "crypto-derivatives")
            - description: str (e.g., "Binance USDT-Margined Perpetuals")
            - is_active: bool
            - symbol_count: int (if include_stats=True)
            - earliest_date: date (if include_stats=True)
            - latest_date: date (if include_stats=True)
            - available_tables: list[str] (if include_stats=True)

    Examples:
        >>> # List all active exchanges
        >>> exchanges = research.list_exchanges()
        >>> print(exchanges)

        >>> # List only crypto spot exchanges
        >>> crypto_spot = research.list_exchanges(asset_class="crypto-spot")

        >>> # List Chinese stocks + crypto derivatives
        >>> combined = research.list_exchanges(asset_class=["stocks-cn", "crypto-derivatives"])
    """
```

**Output Example:**
```
┌─────────────────────┬──────────────┬───────────────────────┬─────────────┬────────────┬──────────────┬──────────────┬───────────────────────────────────────┐
│ exchange            │ exchange_id  │ asset_class           │ is_active   │ symbols    │ earliest     │ latest       │ tables                                │
├─────────────────────┼──────────────┼───────────────────────┼─────────────┼────────────┼──────────────┼──────────────┼───────────────────────────────────────┤
│ binance-futures     │ 2            │ crypto-derivatives    │ true        │ 347        │ 2024-01-01   │ 2026-01-31   │ [trades, quotes, book_snapshot_25...] │
│ deribit             │ 21           │ crypto-derivatives    │ true        │ 89         │ 2024-05-01   │ 2026-01-31   │ [trades, quotes, derivative_ticker]   │
│ szse                │ 30           │ stocks-cn             │ true        │ 2834       │ 2024-09-01   │ 2024-12-31   │ [l3_orders, l3_ticks]                 │
│ ftx                 │ 25           │ crypto-derivatives    │ false       │ 156        │ 2020-01-01   │ 2022-11-08   │ [trades, quotes]                      │
└─────────────────────┴──────────────┴───────────────────────┴─────────────┴────────────┴──────────────┴──────────────┴───────────────────────────────────────┘
```

---

### 3.2 Symbol Discovery

#### `list_symbols()` - List symbols with filters
```python
def list_symbols(
    *,
    exchange: str | list[str] | None = None,
    asset_class: str | list[str] | None = None,
    base_asset: str | list[str] | None = None,
    quote_asset: str | list[str] | None = None,
    asset_type: str | list[str] | None = None,
    search: str | None = None,
    current_only: bool = True,
    include_stats: bool = False,
) -> pl.DataFrame:
    """List symbols with flexible filtering.

    Args:
        exchange: Filter by exchange name(s)
        asset_class: Filter by asset class (crypto, stocks, etc.)
        base_asset: Filter by base asset (BTC, ETH, etc.)
        quote_asset: Filter by quote asset (USDT, USD, etc.)
        asset_type: Filter by asset type (spot, perpetual, future, option, l3_orders, l3_ticks)
        search: Fuzzy search across symbol name, base/quote assets
        current_only: If True, only return currently active symbols (is_current=true)
        include_stats: If True, add row counts and date ranges per symbol

    Returns:
        DataFrame with columns from dim_symbol:
            - symbol_id: int
            - exchange: str
            - exchange_symbol: str
            - base_asset: str
            - quote_asset: str
            - asset_type: str (decoded: "spot", "perpetual", etc.)
            - tick_size: float
            - lot_size: float
            - valid_from_ts: int
            - valid_until_ts: int
            - is_current: bool
            - [if include_stats] row_count: int
            - [if include_stats] earliest_date: date
            - [if include_stats] latest_date: date

    Examples:
        >>> # List all symbols on Binance Futures
        >>> symbols = research.list_symbols(exchange="binance-futures")

        >>> # Find all BTC perpetuals across exchanges
        >>> btc_perps = research.list_symbols(base_asset="BTC", asset_type="perpetual")

        >>> # Search for SOL symbols
        >>> sol = research.list_symbols(search="SOL")

        >>> # Chinese stocks on SZSE
        >>> szse_stocks = research.list_symbols(exchange="szse", asset_class="stocks-cn")
    """
```

**Output Example:**
```
┌───────────┬───────────────────┬──────────────────┬────────────┬─────────────┬─────────────┬───────────┬──────────┬────────────┐
│ symbol_id │ exchange          │ exchange_symbol  │ base_asset │ quote_asset │ asset_type  │ tick_size │ lot_size │ is_current │
├───────────┼───────────────────┼──────────────────┼────────────┼─────────────┼─────────────┼───────────┼──────────┼────────────┤
│ 12345     │ binance-futures   │ BTCUSDT          │ BTC        │ USDT        │ perpetual   │ 0.1       │ 0.001    │ true       │
│ 12346     │ deribit           │ BTC-PERPETUAL    │ BTC        │ USD         │ perpetual   │ 0.5       │ 1.0      │ true       │
│ 78901     │ szse              │ 000001           │ 000001     │ CNY         │ stock       │ 0.01      │ 100.0    │ true       │
└───────────┴───────────────────┴──────────────────┴────────────┴─────────────┴─────────────┴───────────┴──────────┴────────────┘
```

---

### 3.3 Table Coverage Discovery

#### `list_tables()` - List available tables
```python
def list_tables(
    layer: str = "silver",
    include_stats: bool = True,
) -> pl.DataFrame:
    """List all available tables with metadata.

    Args:
        layer: Data lake layer (silver, gold, reference)
        include_stats: If True, include row counts and partition info

    Returns:
        DataFrame with columns:
            - table_name: str
            - layer: str (silver, gold, reference)
            - description: str
            - partitioned_by: list[str]
            - row_count: int (if include_stats=True)
            - size_mb: float (if include_stats=True)
            - earliest_date: date (if include_stats=True, for partitioned tables)
            - latest_date: date (if include_stats=True, for partitioned tables)

    Examples:
        >>> tables = research.list_tables()
        >>> print(tables)
    """
```

**Output Example:**
```
┌──────────────────────┬────────┬─────────────────────────────────────────┬────────────────────┬─────────────┬──────────┬──────────────┬──────────────┐
│ table_name           │ layer  │ description                             │ partitioned_by     │ row_count   │ size_mb  │ earliest     │ latest       │
├──────────────────────┼────────┼─────────────────────────────────────────┼────────────────────┼─────────────┼──────────┼──────────────┼──────────────┤
│ trades               │ silver │ Individual trade executions             │ [exchange, date]   │ 1.2B        │ 45,678   │ 2024-01-01   │ 2026-01-31   │
│ quotes               │ silver │ Top-of-book bid/ask quotes              │ [exchange, date]   │ 5.3B        │ 123,456  │ 2024-01-01   │ 2026-01-31   │
│ book_snapshot_25     │ silver │ Top 25 levels order book snapshots      │ [exchange, date]   │ 2.1B        │ 89,012   │ 2024-05-01   │ 2026-01-31   │
│ l3_orders            │ silver │ SZSE Level 3 order placements           │ [exchange, date]   │ 456M        │ 12,345   │ 2024-09-01   │ 2024-12-31   │
│ dim_symbol           │ silver │ SCD Type 2 symbol metadata              │ []                 │ 12,456      │ 2        │ N/A          │ N/A          │
└──────────────────────┴────────┴─────────────────────────────────────────┴────────────────────┴─────────────┴──────────┴──────────────┴──────────────┘
```

---

### 3.4 Data Coverage by Symbol

#### `data_coverage()` - Check data availability for a symbol
```python
def data_coverage(
    exchange: str,
    symbol: str,
    *,
    tables: list[str] | None = None,
    as_of: datetime | str | int | None = None,
) -> dict[str, dict[str, Any]]:
    """Check data coverage for a specific symbol across tables.

    Args:
        exchange: Exchange name (e.g., "binance-futures")
        symbol: Exchange symbol (e.g., "BTCUSDT")
        tables: List of tables to check (default: all silver tables)
        as_of: Check coverage as of a specific time (for SCD Type 2 filtering)

    Returns:
        Dictionary mapping table_name → coverage info:
        {
            "trades": {
                "available": True,
                "symbol_id": 12345,
                "row_count": 123456789,
                "earliest_date": date(2024, 1, 1),
                "latest_date": date(2026, 1, 31),
                "earliest_ts_us": 1704067200000000,
                "latest_ts_us": 1738368000000000,
                "size_mb": 456.78,
                "partitions": 396,  # Number of date partitions
            },
            "quotes": {
                "available": True,
                "symbol_id": 12345,
                "row_count": 987654321,
                ...
            },
            "book_snapshot_25": {
                "available": False,  # No data for this symbol
                "reason": "Symbol not found in table",
            },
            "l3_orders": {
                "available": False,
                "reason": "Table not applicable for this exchange",
            },
        }

    Examples:
        >>> # Check all tables for BTCUSDT on Binance Futures
        >>> coverage = research.data_coverage("binance-futures", "BTCUSDT")
        >>> print(coverage["trades"])

        >>> # Check specific tables
        >>> coverage = research.data_coverage(
        ...     "binance-futures",
        ...     "BTCUSDT",
        ...     tables=["trades", "quotes"],
        ... )

        >>> # Check historical coverage (as of specific date)
        >>> coverage = research.data_coverage(
        ...     "binance-futures",
        ...     "BTCUSDT",
        ...     as_of="2024-05-01",
        ... )
    """
```

---

### 3.5 Summary / Quick Info

#### `summarize_symbol()` - Rich symbol summary
```python
def summarize_symbol(
    symbol: str,
    *,
    exchange: str | None = None,
    as_of: datetime | str | int | None = None,
) -> None:
    """Print a rich, human-readable summary of a symbol.

    Args:
        symbol: Exchange symbol (e.g., "BTCUSDT", "000001")
        exchange: Exchange name (optional, will search if omitted)
        as_of: Show metadata as of a specific time

    Outputs:
        Rich formatted summary to stdout (not returned)

    Examples:
        >>> # Auto-detect exchange
        >>> research.summarize_symbol("BTCUSDT")

        >>> # Specify exchange
        >>> research.summarize_symbol("BTCUSDT", exchange="binance-futures")
    """
```

**Output Example:**
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Symbol: BTCUSDT (Binance Futures)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Metadata
────────────────────────────────────────────────────────
  Symbol ID:        12345
  Exchange:         binance-futures (ID: 2)
  Base Asset:       BTC
  Quote Asset:      USDT
  Asset Type:       Perpetual

  Contract Specs:
    Tick Size:      0.1 USDT
    Lot Size:       0.001 BTC
    Contract Size:  1.0 BTC

  Validity:
    From:           2024-01-01 00:00:00 UTC
    Until:          Active (current)

Available Data
────────────────────────────────────────────────────────
  ✓ trades              123.5M rows    2024-01-01 to 2026-01-31  (456 MB)
  ✓ quotes              987.6M rows    2024-01-01 to 2026-01-31  (1.2 GB)
  ✓ book_snapshot_25    234.5M rows    2024-05-01 to 2026-01-31  (890 MB)
  ✓ derivative_ticker   12.3K rows     2024-01-01 to 2026-01-31  (2 MB)
  ✗ l3_orders           Not available for this exchange

Quick Start
────────────────────────────────────────────────────────
  from pointline.research import query

  trades = query.trades(
      exchange="binance-futures",
      symbol="BTCUSDT",
      start="2024-05-01",
      end="2024-05-02",
      decoded=True,
  )

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## 4. Exchange Taxonomy & Metadata

### 4.1 Asset Class Hierarchy

```python
# pointline/config.py

# Asset class taxonomy
ASSET_CLASS_TAXONOMY = {
    "crypto": {
        "description": "Cryptocurrency spot and derivatives",
        "children": ["crypto-spot", "crypto-derivatives"],
    },
    "crypto-spot": {
        "description": "Cryptocurrency spot trading",
        "exchanges": ["binance", "coinbase", "kraken", "okx", "huobi", "gate", "bitfinex", "bitstamp", "gemini", "crypto-com", "kucoin", "binance-us", "coinbase-pro"],
    },
    "crypto-derivatives": {
        "description": "Cryptocurrency futures, perpetuals, and options",
        "exchanges": ["binance-futures", "binance-coin-futures", "deribit", "bybit", "okx-futures", "bitmex", "ftx", "dydx"],
    },
    "stocks": {
        "description": "Equity markets",
        "children": ["stocks-cn", "stocks-us"],
    },
    "stocks-cn": {
        "description": "Chinese stock exchanges",
        "exchanges": ["szse", "sse"],
    },
    "stocks-us": {
        "description": "US stock exchanges (future)",
        "exchanges": [],  # To be added
    },
    "futures": {
        "description": "Traditional futures (CME, ICE, etc.)",
        "exchanges": [],  # To be added
    },
    "options": {
        "description": "Listed options markets",
        "exchanges": [],  # To be added
    },
    "forex": {
        "description": "Foreign exchange spot and derivatives",
        "exchanges": [],  # To be added
    },
}
```

### 4.2 Exchange Metadata Registry

```python
# pointline/config.py

# Extended exchange metadata
EXCHANGE_METADATA = {
    "binance-futures": {
        "exchange_id": 2,
        "asset_class": "crypto-derivatives",
        "description": "Binance USDT-Margined Perpetual Futures",
        "timezone": "UTC",
        "is_active": True,
        "website": "https://www.binance.com/en/futures",
        "api_docs": "https://binance-docs.github.io/apidocs/futures/en/",
        "supported_tables": ["trades", "quotes", "book_snapshot_25", "derivative_ticker", "kline_1h"],
    },
    "deribit": {
        "exchange_id": 21,
        "asset_class": "crypto-derivatives",
        "description": "Deribit BTC/ETH Options and Futures",
        "timezone": "UTC",
        "is_active": True,
        "website": "https://www.deribit.com/",
        "api_docs": "https://docs.deribit.com/",
        "supported_tables": ["trades", "quotes", "derivative_ticker"],
    },
    "szse": {
        "exchange_id": 30,
        "asset_class": "stocks-cn",
        "description": "Shenzhen Stock Exchange (Level 3 Order Book)",
        "timezone": "Asia/Shanghai",
        "is_active": True,
        "website": "http://www.szse.cn/",
        "api_docs": None,
        "supported_tables": ["l3_orders", "l3_ticks"],
    },
    "ftx": {
        "exchange_id": 25,
        "asset_class": "crypto-derivatives",
        "description": "FTX (Historical data only - exchange defunct)",
        "timezone": "UTC",
        "is_active": False,
        "website": None,
        "api_docs": None,
        "supported_tables": ["trades", "quotes"],
    },
    # ... (all other exchanges)
}
```

---

## 5. Implementation Plan

### Phase 1: Core Discovery Functions (2-3 days)

**Priority: CRITICAL**

#### Task 1.1: Exchange Discovery
- [ ] Add `ASSET_CLASS_TAXONOMY` to `pointline/config.py`
- [ ] Add `EXCHANGE_METADATA` to `pointline/config.py`
- [ ] Implement `list_exchanges()` in `pointline/research/discovery.py`
- [ ] Add tests for exchange discovery
- [ ] Update CLAUDE.md with discovery API

**Deliverable:** `research.list_exchanges()` works with asset class filtering

#### Task 1.2: Symbol Discovery
- [ ] Implement `list_symbols()` in `pointline/research/discovery.py`
- [ ] Add asset_type decoding (0 → "spot", 1 → "perpetual", etc.)
- [ ] Add fuzzy search functionality
- [ ] Add tests for symbol filtering
- [ ] Update researcher guide

**Deliverable:** `research.list_symbols()` works with all filters

#### Task 1.3: Table Coverage
- [ ] Implement `list_tables()` in `pointline/research/discovery.py`
- [ ] Query Delta Lake metadata for stats
- [ ] Cache results for performance
- [ ] Add tests for table introspection

**Deliverable:** `research.list_tables()` returns table metadata

---

### Phase 2: Coverage Analysis (2 days)

**Priority: HIGH**

#### Task 2.1: Symbol Coverage
- [ ] Implement `data_coverage()` in `pointline/research/discovery.py`
- [ ] Query manifest table for row counts and date ranges
- [ ] Handle missing tables gracefully
- [ ] Add SCD Type 2 filtering (as_of parameter)
- [ ] Add tests for coverage queries
- [ ] Optimize for performance (<1s response time)

**Deliverable:** `research.data_coverage()` returns detailed coverage info

#### Task 2.2: Summary View
- [ ] Implement `summarize_symbol()` in `pointline/research/discovery.py`
- [ ] Use rich formatting library for pretty output
- [ ] Include quick-start code snippet
- [ ] Add tests for summary formatting

**Deliverable:** `research.summarize_symbol()` prints rich summary

---

### Phase 3: CLI Integration (1 day)

**Priority: MEDIUM**

#### Task 3.1: CLI Commands
- [ ] Add `pointline data list-exchanges` command
- [ ] Add `pointline data list-symbols` command
- [ ] Add `pointline data list-tables` command
- [ ] Add `pointline data coverage` command
- [ ] Add `pointline data summary` command
- [ ] Add tests for CLI commands

**Deliverable:** CLI commands work end-to-end

---

### Phase 4: Documentation & Examples (1 day)

**Priority: HIGH**

#### Task 4.1: Documentation
- [ ] Create `docs/data-discovery-guide.md`
- [ ] Update `README.md` with discovery examples
- [ ] Update `docs/quickstart.md` with discovery workflow
- [ ] Update CLAUDE.md with discovery instructions for agents
- [ ] Add to API reference

**Deliverable:** Complete documentation for discovery API

---

## 6. Technical Implementation Details

### 6.1 Performance Considerations

**Challenge:** Querying millions of rows across partitions can be slow.

**Solutions:**
1. **Cache dim_symbol in memory** (small table, <10MB)
2. **Use manifest table for statistics** (already indexed, fast)
3. **Lazy evaluation** - only compute stats when `include_stats=True`
4. **Delta Lake metadata** - Use `DeltaTable.metadata()` instead of full scans
5. **Parallel queries** - Query multiple tables concurrently

**Expected Performance:**
- `list_exchanges()`: <100ms (static metadata)
- `list_symbols()`: <500ms (in-memory dim_symbol)
- `list_tables()`: <200ms (file system metadata)
- `data_coverage()`: <1s (manifest queries + Delta metadata)
- `summarize_symbol()`: <1.5s (combines all above)

---

### 6.2 Extensibility Strategy

**When adding new asset classes (e.g., US equities):**

1. **Add to taxonomy:**
   ```python
   ASSET_CLASS_TAXONOMY["stocks-us"] = {
       "description": "US stock exchanges",
       "exchanges": ["nyse", "nasdaq"],
   }
   ```

2. **Add exchange metadata:**
   ```python
   EXCHANGE_METADATA["nyse"] = {
       "exchange_id": 40,
       "asset_class": "stocks-us",
       "description": "New York Stock Exchange",
       ...
   }
   ```

3. **Add to EXCHANGE_MAP:**
   ```python
   EXCHANGE_MAP["nyse"] = 40
   ```

4. **No code changes needed** - discovery API auto-detects new exchanges

**When adding new table types:**

1. Add to `TABLE_PATHS` in `config.py`
2. Add to `EXCHANGE_METADATA["supported_tables"]`
3. Discovery API auto-detects via `list_tables()`

---

### 6.3 Data Sources

**Discovery API will query:**

1. **dim_symbol table** - Symbol metadata (static, in-memory cache)
2. **ingest_manifest table** - Row counts, date ranges, status
3. **Delta Lake metadata** - Partition info, file sizes
4. **config.py registries** - Exchange taxonomy, metadata

**No full table scans** - All queries use indexes or metadata.

---

## 7. API Usage Examples

### Example 1: New User Onboarding
```python
from pointline import research

# Step 1: Discover what exchanges have data
exchanges = research.list_exchanges(asset_class="crypto-derivatives")
print(exchanges)

# Step 2: List symbols on Binance Futures
symbols = research.list_symbols(exchange="binance-futures", asset_type="perpetual")
print(f"Found {symbols.height} perpetual contracts")

# Step 3: Check coverage for BTCUSDT
coverage = research.data_coverage("binance-futures", "BTCUSDT")
print(f"Trades: {coverage['trades']['row_count']:,} rows")
print(f"Date range: {coverage['trades']['earliest_date']} to {coverage['trades']['latest_date']}")

# Step 4: Load data
from pointline.research import query
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

---

### Example 2: LLM Agent Workflow
```python
# User: "Show me data for Bitcoin on exchanges"

# Agent step 1: Find BTC symbols
btc_symbols = research.list_symbols(base_asset="BTC", asset_type="perpetual")
print(f"Found {btc_symbols.height} BTC perpetuals across {btc_symbols['exchange'].n_unique()} exchanges")

# Agent step 2: Check coverage for top exchange
top_exchange = btc_symbols.group_by("exchange").agg(pl.count()).sort("count", descending=True)["exchange"][0]
coverage = research.data_coverage(top_exchange, btc_symbols.filter(pl.col("exchange") == top_exchange)["exchange_symbol"][0])

# Agent step 3: Recommend to user
print(f"Recommendation: Use {top_exchange} - has {coverage['trades']['row_count']:,} trades")
```

---

### Example 3: Multi-Asset Research
```python
# Find all Chinese stocks with L3 data
cn_stocks = research.list_symbols(asset_class="stocks-cn")

# Check which ones have recent data
recent_stocks = []
for symbol in cn_stocks.iter_rows(named=True):
    coverage = research.data_coverage(symbol["exchange"], symbol["exchange_symbol"])
    if coverage["l3_orders"]["available"] and coverage["l3_orders"]["latest_date"] >= date(2024, 12, 1):
        recent_stocks.append(symbol)

print(f"Found {len(recent_stocks)} stocks with recent L3 data")
```

---

## 8. Testing Strategy

### Unit Tests
- [ ] Test `list_exchanges()` with all filter combinations
- [ ] Test `list_symbols()` with edge cases (empty results, fuzzy search)
- [ ] Test `list_tables()` with missing tables
- [ ] Test `data_coverage()` with missing data, SCD Type 2 edge cases
- [ ] Test taxonomy resolution with hierarchical asset classes

### Integration Tests
- [ ] Test discovery → query workflow end-to-end
- [ ] Test performance (all functions <2s)
- [ ] Test with real data lake

### Regression Tests
- [ ] Test backward compatibility with existing code
- [ ] Test with empty lake (no data)
- [ ] Test with partially populated lake

---

## 9. Success Metrics

### Quantitative
1. **Onboarding time:** New user → first plot in <5 minutes (currently ~2 hours)
2. **Discovery queries:** <1 second response time for all functions
3. **LLM agent success rate:** >90% of queries work first try (currently ~50%)

### Qualitative
1. **User feedback:** "I can find data easily" (5/5 rating)
2. **Documentation clarity:** No support questions about "what data exists"
3. **Code simplicity:** Discovery examples are copy-paste ready

---

## 10. Future Enhancements (Beyond MVP)

### Phase 5: Advanced Discovery (Future)
- [ ] Visual data catalog (web UI)
- [ ] Data quality scores per symbol
- [ ] Coverage heatmaps (symbol × date → data availability)
- [ ] Historical coverage tracking (show gaps)
- [ ] Recommendations engine ("Users who queried X also queried Y")

### Phase 6: Metadata Enrichment (Future)
- [ ] Add fundamental data (market cap, volume, etc.)
- [ ] Add trading hours metadata
- [ ] Add data quality metrics
- [ ] Add vendor-specific notes

---

## 11. Risk & Mitigation

### Risk 1: Performance Degradation at Scale
**Risk:** Discovery queries slow down with millions of symbols.
**Mitigation:** In-memory caching, lazy evaluation, parallel queries.
**Fallback:** Implement pagination if needed.

### Risk 2: Breaking Changes
**Risk:** Adding new asset classes breaks existing code.
**Mitigation:** Strict backward compatibility testing, deprecation warnings.

### Risk 3: Metadata Sync Issues
**Risk:** EXCHANGE_METADATA gets out of sync with dim_symbol.
**Mitigation:** Add validation tests, auto-detect missing metadata.

---

## 12. Open Questions

1. **Should we support regex patterns in symbol search?**
   - Proposal: Start with simple substring matching, add regex later if needed

2. **How to handle cross-exchange symbol aliases?**
   - Example: "BTC-PERPETUAL" (deribit) vs "BTCUSD" (bitmex) vs "BTCUSDT" (binance-futures)
   - Proposal: Add optional `base_asset` normalization helper

3. **Should we cache discovery results?**
   - Proposal: Yes, cache in-memory with 5-minute TTL

4. **How to handle data quality flags?**
   - Proposal: Phase 2 enhancement - add `data_quality` field to coverage

---

## 13. Conclusion

This plan provides a **robust, extensible foundation** for data discovery that:
- ✅ Supports current data (crypto + Chinese stocks)
- ✅ Easily extends to future asset classes
- ✅ Provides intuitive API for humans and LLM agents
- ✅ Maintains high performance (<1s queries)
- ✅ Requires minimal maintenance when adding new data

**Recommendation:** Approve and implement Phase 1-2 immediately (highest ROI, ~4-5 days of work).

---

## Appendix A: Full API Surface

```python
# pointline/research/discovery.py

def list_exchanges(
    asset_class: str | list[str] | None = None,
    active_only: bool = True,
    include_stats: bool = True,
) -> pl.DataFrame: ...

def list_symbols(
    exchange: str | list[str] | None = None,
    asset_class: str | list[str] | None = None,
    base_asset: str | list[str] | None = None,
    quote_asset: str | list[str] | None = None,
    asset_type: str | list[str] | None = None,
    search: str | None = None,
    current_only: bool = True,
    include_stats: bool = False,
) -> pl.DataFrame: ...

def list_tables(
    layer: str = "silver",
    include_stats: bool = True,
) -> pl.DataFrame: ...

def data_coverage(
    exchange: str,
    symbol: str,
    tables: list[str] | None = None,
    as_of: datetime | str | int | None = None,
) -> dict[str, dict[str, Any]]: ...

def summarize_symbol(
    symbol: str,
    exchange: str | None = None,
    as_of: datetime | str | int | None = None,
) -> None: ...
```

---

## Appendix B: CLI Commands

```bash
# List exchanges
pointline data list-exchanges
pointline data list-exchanges --asset-class crypto-derivatives
pointline data list-exchanges --include-inactive

# List symbols
pointline data list-symbols --exchange binance-futures
pointline data list-symbols --base-asset BTC --asset-type perpetual
pointline data list-symbols --search "SOL"

# List tables
pointline data list-tables
pointline data list-tables --layer gold

# Check coverage
pointline data coverage --exchange binance-futures --symbol BTCUSDT
pointline data coverage --exchange binance-futures --symbol BTCUSDT --tables trades,quotes

# Symbol summary
pointline data summary BTCUSDT
pointline data summary BTCUSDT --exchange binance-futures
```
