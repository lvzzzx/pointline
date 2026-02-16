---
name: pointline-researcher
description: >-
  Pointline data lake research API reference for quant researchers. Use when:
  (1) querying event data from the pointline Silver layer (trades, quotes, orderbook,
  CN L2/L3 tables), (2) discovering or resolving symbols via dim_symbol,
  (3) building or aligning to time-series spines (clock, trades, volume, dollar bars),
  (4) decoding fixed-point scaled integers to floats, (5) attaching PIT-correct symbol
  metadata via as-of joins, (6) filtering CN A-share data by trading phase,
  (7) ingesting Bronze files into Silver, (8) writing or reviewing code that reads
  from or writes to the pointline data lake, (9) understanding pointline schemas,
  table specs, or storage layout.
---

# Pointline Data Lake — Research API

PIT-accurate offline data lake for quant research. Polars + Delta Lake, single-machine.

**Core invariants:** PIT correctness, deterministic replay, idempotent ingestion, lineage traceability via `file_id` + `file_seq`.

## Quick Start

```python
from pathlib import Path
from pointline.research import (
    load_events, discover_symbols, load_symbol_meta,
    decode_scaled_columns, join_symbol_meta,
    build_spine, align_to_spine, ClockSpineConfig,
    TradingPhase, filter_by_phase, add_phase_column,
)

silver = Path("./data/silver")

# 1. Discover symbols
syms = discover_symbols(silver_root=silver, exchange="binance-futures", q="BTC")

# 2. Load events — time window is [start, end) on ts_event_us
trades = load_events(
    silver_root=silver, table="trades",
    exchange="binance-futures", symbol="BTCUSDT",
    start="2024-05-01", end="2024-05-02",
)

# 3. Decode fixed-point → float (only at research output, never mid-pipeline)
trades = decode_scaled_columns(trades, "trades")  # adds price_decoded, qty_decoded

# 4. Attach PIT symbol metadata
trades = join_symbol_meta(trades, silver_root=silver, columns=["base_asset", "tick_size"])

# 5. Build spine and align
spine = build_spine(
    silver_root=silver, exchange="binance-futures", symbol="BTCUSDT",
    start="2024-05-01", end="2024-05-02",
    builder="clock", config=ClockSpineConfig(step_us=60_000_000),  # 1-min bars
)
aligned = align_to_spine(events=trades, spine=spine)
```

## Timestamp & Fixed-Point Rules

- All timestamps: **Int64 UTC microseconds** (`*_ts_us`). Event time = `ts_event_us`.
- `TimestampInput` accepts: `int` (us), `str` (ISO), `date`, `datetime`. Naive datetimes treated as UTC.
- `trading_date`: derived from `ts_event_us` in exchange-local tz (crypto=UTC, CN=Asia/Shanghai).
- Prices/quantities: **Int64 scaled** by `PRICE_SCALE = QTY_SCALE = 1_000_000_000`.
- Decode only at final research output via `decode_scaled_columns()`. Never mid-pipeline.

## Research API

### `load_events()`

```python
load_events(
    silver_root: Path, table: str, exchange: str, symbol: str,
    start: TimestampInput, end: TimestampInput,
    columns: list[str] | None = None, include_lineage: bool = False,
) -> pl.DataFrame
```

- Window: `[start, end)` on `ts_event_us`. No implicit dim_symbol join.
- `table`: `"trades"`, `"quotes"`, `"orderbook_updates"`, `"cn_order_events"`, `"cn_tick_events"`, `"cn_l2_snapshots"`.
- `include_lineage=True` to include `file_id`, `file_seq`.
- Returns sorted by table's tie-break keys.

### `discover_symbols()` / `load_symbol_meta()`

```python
discover_symbols(silver_root, exchange, q=None, as_of=None, include_meta=False, limit=50)
load_symbol_meta(silver_root, exchange, symbols=None, as_of=None, columns=None)
```

- `as_of=None`: returns current rows only. With `as_of`: rows valid at that timestamp.
- `q`: text search across `exchange_symbol`, `canonical_symbol`, `base_asset`.

### `decode_scaled_columns()`

```python
decode_scaled_columns(df, table, columns=None, in_place=False, suffix="_decoded")
```

- Default: adds `<col>_decoded` columns, preserves originals.
- `in_place=True`: replaces original columns.

### `join_symbol_meta()`

```python
join_symbol_meta(df, silver_root, columns, ts_col="ts_event_us")
```

- PIT as-of join: `valid_from_ts_us <= ts_col < valid_until_ts_us`.
- Requires `exchange`, `symbol_id`, `ts_col` in df.
- Available metadata columns: `exchange_symbol`, `canonical_symbol`, `market_type`, `base_asset`, `quote_asset`, `tick_size`, `lot_size`, `contract_size`, `is_current`, `updated_at_ts_us`.

### Spine API

```python
build_spine(silver_root, exchange, symbol, start, end, builder, config)
align_to_spine(events, spine, ts_col="ts_event_us", by=("exchange", "symbol"))
```

**Builders:**

| Builder | Config | Description |
|---|---|---|
| `"clock"` | `ClockSpineConfig(step_us=N)` | Regular time intervals |
| `"trades"` | `TradesSpineConfig()` | One point per unique trade timestamp |
| `"volume"` | `VolumeSpineConfig(volume_threshold_scaled=N)` | Cumulative volume bucket crossings |
| `"dollar"` | `DollarSpineConfig(dollar_threshold_scaled=N)` | Cumulative notional bucket crossings |

Volume/dollar thresholds use **scaled integers** (multiply by `QTY_SCALE`).

`align_to_spine()` uses forward as-of join (PIT-safe): events at spine boundary map to next bar.

### CN Trading Phases

```python
add_phase_column(df, exchange="szse", ts_col="ts_event_us", market_type=None)
filter_by_phase(df, exchange="szse", phases=[TradingPhase.MORNING, TradingPhase.AFTERNOON])
```

Phases: `CLOSED`, `PRE_OPEN` (09:15-09:25), `MORNING` (09:30-11:30), `NOON_BREAK` (11:30-13:00), `AFTERNOON` (13:00-14:57), `CLOSING` (14:57-15:00, SZSE only), `AFTER_HOURS` (15:05-15:30, STAR/ChiNext only).

## Ingestion

```python
from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata

result = ingest_file(
    meta=BronzeFileMetadata(vendor="tardis", data_type="trades", ...),
    silver_root=silver, bronze_root=bronze,
)
```

Idempotent via manifest `(vendor, data_type, bronze_path, file_hash)`.

## Schema Reference

For complete table schemas (all columns, types, scales, partition/tie-break keys), see [references/schemas.md](references/schemas.md).

## Key Import Paths

```python
from pointline import TRADES, QUOTES, ORDERBOOK_UPDATES, DIM_SYMBOL
from pointline.schemas import get_table_spec, list_table_specs
from pointline.schemas.types import PRICE_SCALE, QTY_SCALE, TableSpec, ColumnSpec
from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata
from pointline.storage.delta import DeltaEventStore, DeltaDimensionStore
from pointline.storage.delta.layout import table_path
```

## Critical Gotchas

1. **Never decode mid-pipeline.** Fixed-point integers preserve precision; decode only at output.
2. **As-of joins, not exact.** Symbol metadata is SCD2 with validity windows.
3. **Lunch break discontinuity.** Never compute features across 11:30-13:00 CST for CN markets.
4. **No lookahead.** `align_to_spine` maps forward. PIT coverage check quarantines orphans.
5. **Deterministic ordering.** Rely on tie-break keys, not insertion order.
6. **Schema-as-code.** Always read canonical spec from `pointline/schemas/` before touching tables.
