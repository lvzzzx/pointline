# Reference Documentation

Complete API, CLI, and schema reference for Pointline.

---

## 📚 Available References

### [Research API Guide](../reference/api-reference.md) ✅

Complete Python API reference:
- **Query API:** Automatic symbol resolution for exploration
- **Core API:** Explicit symbol resolution for production
- Symbol metadata handling (SCD Type 2)
- Timestamp formats and conversion
- LazyFrame usage for large datasets
- Real-world examples

**Use this when:** You need to look up specific API functions or parameters.

---

### [Schemas](../reference/schemas.md) ✅

Complete table schema reference:
- Silver layer tables (trades, quotes, book_snapshot_25, etc.)
- Column names, types, and descriptions
- Partition schemes
- Fixed-point encoding details
- Dimension tables (dim_symbol, dim_asset_stats)

**Use this when:** You need to know exact column names, types, or table structure.

---

### CLI Reference 📝

**Status:** Planned
**Path:** `cli-reference.md`

Complete command-line interface reference:
- Discovery commands (`list-exchanges`, `list-symbols`, `coverage`)
- Data management (`ingest`, `manifest`)
- Symbol management (`dim-symbol upsert`)
- Validation and maintenance
- Examples for every command

**Use this when:** You want to use Pointline from the command line.

---

## 🗂️ Data Sources

Vendor-specific documentation:

### [Quant360 SZSE L2](../data_sources/quant360_szse_l2.md) ✅

Chinese stock exchange Level 2 data:
- Data format and schema
- Ingestion workflow
- Vendor-specific quirks

---

## 🎯 Quick Lookup

### Python API

**Discovery:**
```python
from pointline import research

# What's available?
exchanges = research.list_exchanges()
symbols = research.list_symbols(exchange="binance-futures")
coverage = research.data_coverage("binance-futures", "BTCUSDT")
```

**Query API (recommended for exploration):**
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

**Core API (production research):**
```python
from pointline import research, registry

symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]

trades = research.load_trades(
    symbol_id=symbol_id,
    start_ts_us=1714521600000000,
    end_ts_us=1714608000000000,
)
```

See [Research API Guide](../reference/api-reference.md) for complete reference.

---

### CLI Commands

**Discovery:**
```bash
pointline data list-exchanges
pointline data list-symbols --exchange binance-futures --base-asset BTC
pointline data coverage --exchange binance-futures --symbol BTCUSDT
```

**Data Management:**
```bash
pointline ingest discover --pending-only
pointline ingest run --table trades --exchange binance-futures --date 2024-05-01
pointline manifest show
```

**Symbol Management:**
```bash
pointline dim-symbol upsert --file ./symbols.csv
```

See CLI Reference (coming soon) for complete command list.

---

### Table Schemas

**Silver layer tables:**

| Table | Columns | Partitions |
|-------|---------|------------|
| `trades` | ts_local_us, symbol_id, price_px_int, qty_int, side | exchange, date |
| `quotes` | ts_local_us, symbol_id, bid_px_int, ask_px_int, bid_sz_int, ask_sz_int | exchange, date |
| `book_snapshot_25` | ts_local_us, symbol_id, bids_px_int[], asks_px_int[], bids_sz_int[], asks_sz_int[] | exchange, date |
| `dim_symbol` | symbol_id, exchange_id, exchange_symbol, tick_size, valid_from_ts, valid_until_ts | (none) |

See [Schemas](../reference/schemas.md) for complete details.

---

## 🔍 Finding Specific Information

| I want to... | Look in... |
|--------------|------------|
| Look up a Python function | [Research API Guide](../reference/api-reference.md) |
| Look up a CLI command | CLI Reference (coming soon) |
| Find column names | [Schemas](../reference/schemas.md) |
| Understand SCD Type 2 | [Research API Guide §Symbol Metadata](../reference/api-reference.md#handling-symbol-metadata-changes) |
| See vendor data formats | [Data Sources](../data_sources/) |

---

## 📖 Related Documentation

- **Getting Started:** [Quickstart](../quickstart.md)
- **User Guides:** [guides/](../guides/)
- **Architecture:** [architecture/](../architecture/)

---

## 💡 Contributing

Found a mistake or missing information?

1. Check if it's already documented elsewhere
2. Submit a pull request with corrections
3. For new reference material, ensure it's factual and complete

**Reference documentation best practices:**
- Be precise and exhaustive (this is where users look up details)
- Include type signatures and parameter descriptions
- Provide minimal examples for each function
- Link to guides for how-to explanations
