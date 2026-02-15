# dim_symbol SCD Type 2 Reference

## Table of Contents
- [Schema](#schema)
- [Core Functions](#core-functions)
- [SCD2 Semantics](#scd2-semantics)
- [Symbol ID Generation](#symbol-id-generation)
- [Validation Invariants](#validation-invariants)

## Schema

| Column | Type | Notes |
|---|---|---|
| symbol_id | Int64 | Stable surrogate key (blake2b hash) |
| exchange | Utf8 | Lowercased exchange name |
| exchange_symbol | Utf8 | Native exchange symbol |
| canonical_symbol | Utf8 | Normalized symbol |
| market_type | Utf8 | e.g., "perpetual", "spot", "main_board" |
| base_asset | Utf8 | |
| quote_asset | Utf8 | |
| valid_from_ts_us | Int64 | Validity window start (inclusive) |
| valid_until_ts_us | Int64 | Validity window end (exclusive) |
| is_current | Boolean | True for latest version |
| tick_size | Int64 | PRICE_SCALE scaled, nullable |
| lot_size | Int64 | QTY_SCALE scaled, nullable |
| contract_size | Int64 | QTY_SCALE scaled, nullable |
| updated_at_ts_us | Int64 | |

**Natural key:** `(exchange, exchange_symbol)`
**Business key:** `(exchange, exchange_symbol, valid_from_ts_us)`

## Core Functions

All in `pointline/dim_symbol.py`. Pure functions — no side effects, no storage I/O.

### bootstrap(snapshot, effective_ts_us)

Create initial dim_symbol from a full snapshot DataFrame.

```python
dim = bootstrap(snapshot_df, effective_ts_us=1700000000_000000)
```

- Sets `valid_from_ts_us = effective_ts_us` for all rows
- Sets `valid_until_ts_us = VALID_UNTIL_MAX` (2^63 - 1)
- Sets `is_current = True`
- Calls `assign_symbol_ids(dim)` to generate `symbol_id`

Input snapshot must contain: `exchange`, `exchange_symbol`, `canonical_symbol`, `market_type`, `base_asset`, `quote_asset`. Optional: `tick_size`, `lot_size`, `contract_size`.

### upsert(dim, snapshot, effective_ts_us, delistings=None)

Incremental SCD2 merge. Handles three cases:

1. **New symbols** — Not in dim → insert with `valid_from_ts_us = effective_ts_us`
2. **Changed symbols** — Tracked column differs → close old row (`valid_until_ts_us = effective_ts_us`, `is_current = False`), open new row
3. **Unchanged symbols** — No tracked column changes → no-op (update `updated_at_ts_us` only)

**Delisting handling:** If `delistings` DataFrame provided, close matching current rows (`valid_until_ts_us = effective_ts_us`, `is_current = False`).

```python
dim = upsert(dim, new_snapshot, effective_ts_us=1700100000_000000, delistings=delisted_df)
```

**Tracked columns** (changes trigger new version):
`canonical_symbol`, `market_type`, `base_asset`, `quote_asset`, `tick_size`, `lot_size`, `contract_size`

### validate(dim)

Check SCD2 invariants. Raises `ValueError` on violation.

```python
validate(dim)  # raises if invalid
```

### assign_symbol_ids(df)

Generate deterministic `symbol_id` from `blake2b(exchange|exchange_symbol|valid_from_ts_us)`, truncated to `Int64`.

```python
dim = assign_symbol_ids(dim)
```

Each unique business key combination gets a stable, reproducible ID.

## SCD2 Semantics

**Validity window:** `valid_from_ts_us <= event_ts < valid_until_ts_us`

- Current rows: `valid_until_ts_us = VALID_UNTIL_MAX` (2^63 - 1), `is_current = True`
- Historical rows: `valid_until_ts_us = effective_ts_us` of the superseding snapshot, `is_current = False`

**As-of join pattern** (used by `check_pit_coverage` and `join_symbol_meta`):
```python
# Find dim_symbol row valid at each event's timestamp
df.join_asof(
    dim_symbol,
    left_on="ts_event_us", right_on="valid_from_ts_us",
    by=["exchange", ...],
).filter(pl.col("ts_event_us") < pl.col("valid_until_ts_us"))
```

## Symbol ID Generation

`assign_symbol_ids` uses blake2b:
```
input  = f"{exchange}|{exchange_symbol}|{valid_from_ts_us}"
digest = blake2b(input.encode(), digest_size=8)
symbol_id = int.from_bytes(digest, "big", signed=True)
```

Properties:
- Deterministic: same inputs always produce the same ID
- Stable: rebuilding from scratch produces identical IDs
- Collision-resistant: 64-bit hash space

## Validation Invariants

`validate(dim)` checks:

1. **Window ordering**: `valid_from_ts_us < valid_until_ts_us` for all rows
2. **Single current per key**: At most one `is_current=True` row per `(exchange, exchange_symbol)`
3. **No overlapping windows**: For each natural key, validity windows do not overlap
4. **Unique symbol_ids**: No duplicate `symbol_id` values across the entire table
5. **Current consistency**: `is_current=True` iff `valid_until_ts_us == VALID_UNTIL_MAX`
