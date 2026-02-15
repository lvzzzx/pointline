# Extension Patterns — Detailed Guide

## Table of Contents
- [Adding a New Event Table](#adding-a-new-event-table)
- [Adding a New Vendor](#adding-a-new-vendor)
- [Adding a New Dimension Table](#adding-a-new-dimension-table)
- [Adding a New Research API](#adding-a-new-research-api)
- [Modifying an Existing Schema](#modifying-an-existing-schema)
- [Adding Exchange Support](#adding-exchange-support)

## Adding a New Event Table

**Risk level:** L2 (schema contract change)

### Decision Checklist

Before creating a new table, ask:

1. **Can this be a column on an existing table?** E.g., `liquidations` could theoretically be a special trade type. Separate table only if schema differs meaningfully.
2. **What's the partition strategy?** Default: `(exchange, trading_date)`. Only deviate with strong justification.
3. **What are the tie-break keys?** Must provide total ordering. Include `file_id, file_seq` as final tiebreakers.
4. **Which columns are scaled?** Prices → `PRICE_SCALE`, quantities → `QTY_SCALE`. Mark in `ColumnSpec`.
5. **What validation rules apply?** Define what makes a row invalid.
6. **Does it need PIT symbol resolution?** Most event tables need `symbol_id` from dim_symbol.

### Implementation Steps

```
1. pointline/schemas/events.py (or events_cn.py, events_crypto.py)
   → Define TableSpec with ColumnSpecs
   → Add to *_SPECS dict

2. pointline/ingestion/pipeline.py
   → Add entry to _TABLE_ALIASES
   → Add validation dispatch if table-specific rules exist

3. pointline/ingestion/event_validation.py
   → Add validation function for new table

4. pointline/vendors/<vendor>/parsers.py
   → Implement parser returning DataFrame matching new spec

5. tests/
   → Unit test for parser
   → Unit test for validation
   → Integration test for full pipeline

6. pointline/research/query.py
   → Verify load_events() handles new table (usually automatic via registry)
```

### ColumnSpec Design Decisions

```python
# Scaled column (price or quantity)
ColumnSpec("price", pl.Int64, nullable=False, scale=PRICE_SCALE)

# Optional column (may be absent in some vendor feeds)
ColumnSpec("trade_id", pl.Utf8, nullable=True)

# Partition column (must be non-nullable)
ColumnSpec("exchange", pl.Utf8, nullable=False)
ColumnSpec("trading_date", pl.Date, nullable=False)

# Lineage column (added by pipeline, not parser)
ColumnSpec("file_id", pl.Int64, nullable=False)
ColumnSpec("file_seq", pl.Int64, nullable=False)
```

## Adding a New Vendor

**Risk level:** L1 (new code, no contract changes) — unless new tables needed (then L2)

### Decision Checklist

1. **Which existing tables does this vendor map to?** Map vendor data types to canonical tables.
2. **Are new tables needed?** If yes, this becomes L2.
3. **What's the Bronze format?** CSV, JSON, Parquet, archive (7z, zip)?
4. **How are timestamps encoded?** UTC already? Local time? What precision?
5. **How are prices/quantities encoded?** Float? String? Already integer?
6. **Are there exchange-specific quirks?** Column naming, missing fields, encoding differences?
7. **What's the archive/delivery structure?** One file per symbol? Per date? Per type?

### Module Structure

```
pointline/vendors/<vendor_name>/
├── __init__.py
├── parsers.py          # Parser functions
├── dispatch.py         # If multiple data types need routing
└── constants.py        # Vendor-specific constants (optional)
```

### Parser Development Order

1. **Start with the simplest data type** (usually trades). Get end-to-end working.
2. **Add data types incrementally**. Each parser follows the same pattern.
3. **Test with real vendor files**. Sample files in `tests/fixtures/<vendor>/`.
4. **Handle edge cases last**. Missing columns, empty files, malformed rows.

### Exchange Name Convention

- Lowercase, hyphenated: `"binance-futures"`, `"okex-swap"`, `"sse"`, `"szse"`
- Spot vs derivatives: `"binance"` (spot) vs `"binance-futures"` (perps/futures)
- Must match `EXCHANGE_TIMEZONE_MAP` keys

## Adding a New Dimension Table

**Risk level:** L2 (new contract)

### SCD Type Selection

| Type | When to Use | Example |
|---|---|---|
| **Type 1** (overwrite) | History doesn't matter for research | Exchange fee schedules |
| **Type 2** (versioned) | PIT-correct historical lookup needed | Symbol metadata, contract specs |
| **Type 3** (limited) | Only previous + current needed | Rarely used in quant context |

### Type 2 Design Template

Follow `dim_symbol` pattern:

1. **Natural key**: Columns that identify the entity (e.g., `exchange, exchange_symbol`)
2. **Tracked columns**: Changes trigger new version
3. **Validity window**: `valid_from_ts_us` (inclusive), `valid_until_ts_us` (exclusive)
4. **Current flag**: `is_current` boolean, redundant with `valid_until_ts_us == VALID_UNTIL_MAX`
5. **Surrogate key**: Deterministic hash of natural key + valid_from (for stable joins)

### Implementation Checklist

- [ ] Define `TableSpec` in `pointline/schemas/dimensions.py`
- [ ] Implement pure functions: `bootstrap()`, `upsert()`, `validate()`
- [ ] Design `assign_*_ids()` using blake2b for deterministic surrogate keys
- [ ] Add storage protocol (extend `DimensionStore` or create new protocol)
- [ ] Implement Delta Lake storage
- [ ] Add PIT coverage check integration if events reference this dimension
- [ ] Test: bootstrap from snapshot, incremental upsert, validate invariants

## Adding a New Research API

**Risk level:** L1 (usually)

### Design Principles

1. **PIT-correct by default.** No function should make it easy to introduce lookahead.
2. **Decode at the boundary.** Internal functions return scaled Int64. Only final user-facing output decodes.
3. **Deterministic ordering.** Results sorted by tie-break keys.
4. **Minimal surface area.** One function per concern. Composable, not monolithic.

### Placement

```
pointline/research/
├── query.py           # load_events() — data loading
├── discovery.py       # discover_symbols() — symbol search
├── metadata.py        # load_symbol_meta() — dimension lookup
├── primitives.py      # decode_scaled_columns(), join_symbol_meta()
├── spine.py           # build_spine(), align_to_spine()
└── cn_trading_phases.py  # CN-specific phase logic
```

New research functions go in the most appropriate existing module, or a new module if the concern is distinct.

### Research API Conventions

- Accept `silver_root: Path` for storage location
- Accept `TimestampInput` for time parameters (supports int, str, date, datetime)
- Return `pl.DataFrame` (never LazyFrame — research API is eager)
- Sort output by tie-break keys
- Document PIT semantics in docstring

## Modifying an Existing Schema

**Risk level:** Always L2.

### Decision Framework

```
Is the change additive (new nullable column)?
  ├── Yes → Lower risk, but still L2. Existing data gets null for new column.
  └── No (modify/remove column, change type, change scale)
       → Full rebuild required. Assess:
           1. How much data needs rebuilding?
           2. Is Bronze available for all affected data?
           3. What downstream code breaks?
           4. Can the change be deferred and batched with other schema changes?
```

### Change Categories

| Change | Impact | Rebuild? |
|---|---|---|
| Add nullable column | Low — existing data gets null | Yes (Delta schema evolution may help, but spec must match) |
| Add non-nullable column | High — all existing rows need values | Yes |
| Remove column | Medium — downstream code may reference it | Yes |
| Change column type | High — all values recast | Yes |
| Change scale factor | Critical — all scaled values change | Yes, full re-ingestion |
| Change tie-break keys | High — all sorting changes | Yes |
| Change partition_by | High — storage layout changes | Yes, full rebuild |

### Before Proceeding

1. Read the current spec in `pointline/schemas/`
2. Search for all references to the affected columns: `grep -r "column_name" pointline/ tests/`
3. Identify all downstream consumers (research API, feature pipelines, notebooks)
4. Write the change as an ExecPlan with explicit rebuild steps
5. Get L2 approval

## Adding Exchange Support

**Risk level:** L1 (if using existing tables)

### Steps

1. **Add timezone mapping** in `pointline/ingestion/exchange.py`:
   ```python
   EXCHANGE_TIMEZONE_MAP["new-exchange"] = "UTC"  # or appropriate tz
   ```

2. **Add parser** (or extend existing vendor parser) if data format differs

3. **Add dim_symbol entries** — bootstrap from vendor's symbol list:
   - Set `exchange = "new-exchange"` (lowercase, hyphenated)
   - Map vendor symbol names to `exchange_symbol`
   - Set `market_type` (spot, perpetual, futures, main_board, etc.)

4. **Test** with sample data from the new exchange

5. **Verify** trading_date derivation works correctly for the exchange's timezone
