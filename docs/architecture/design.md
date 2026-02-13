# Pointline Architecture v2

**Status:** Current
**Scope:** Single-node offline market data lake for deterministic ingestion and research

---

## 1) Philosophy

Keep it clean, clear, and simple:

- **Schemas are code.** One canonical schema per table, versioned only by git history.
- **Ingestion is function-first.** No heavy framework; just parsers, pipelines, and manifests.
- **Keep only mechanisms that protect PIT correctness and replay determinism.**
- **No backward compatibility.** Rebuild/re-ingest is the migration path.

---

## 2) Canonical Stack

| Layer | Technology |
|-------|------------|
| Storage | Delta Lake (`delta-rs`) |
| Compute | Polars |
| Runtime | Local filesystem, single machine only |

No cloud/distributed assumptions. No legacy adapter paths.

---

## 3) Data Lake Model

### Bronze
Immutable raw vendor files and API captures.

### Silver
Typed, normalized event/dimension/control tables with deterministic lineage.

### Gold
Optional derived tables (user-defined, out of scope for core).

### Lineage Fields (Silver Event Tables)
Every event row carries:
- `file_id` — stable identifier for the source Bronze file
- `file_seq` — deterministic sequence within that file

---

## 4) Partition Contract (Timezone Aware)

Event tables are partitioned by:

```
(exchange, trading_date)
```

`trading_date` is derived from `ts_event_us` converted to **exchange-local time**:

1. `ts_event_us` is UTC microseconds.
2. Convert using canonical `exchange -> timezone` mapping.
3. `trading_date = local_datetime.date()`.

**Timezone Mappings:**
- Crypto exchanges: UTC (unless explicitly overridden)
- China A-share (`sse`, `szse`): `Asia/Shanghai`

---

## 5) Ingestion Contract

Single-file flow (`ingest_file(meta) -> result`):

1. **Discover** candidate Bronze files.
2. **Idempotency gate** via manifest success state.
3. **Parse** and canonicalize rows.
4. **Derive and validate** `(exchange, trading_date)` partition alignment.
5. **Resolve/check** PIT symbol coverage via `dim_symbol`.
6. **Apply validation** and quarantine rules.
7. **Write Silver** with deterministic lineage (`file_id`, `file_seq`).
8. **Record status** and diagnostics in control tables.

### Manifest Identity
Key: `(vendor, data_type, bronze_path, file_hash)`

### Status Model
```
pending | success | failed | quarantined
```

### Failure Paths
- Empty parse → `failed`
- Exchange-timezone partition mismatch → `failed`
- No PIT symbol coverage → `quarantined`
- Validation removes all rows → `quarantined`

---

## 6) Schema Contract

- Event timestamps: `Int64` microseconds (`*_ts_us`)
- Monetary/quantity fields: scaled `Int64` (no float in canonical storage)
- `symbol_id`: deterministic and stable across reruns
- `dim_symbol`: SCD2 validity windows (`valid_from_ts_us <= ts < valid_until_ts_us`)

Canonical schema definitions live in `pointline/schemas/`.

---

## 7) Determinism Contract

Replay ordering must be explicit and stable.

**Default Tie-Break Keys:**

| Table Type | Tie-Break Order |
|------------|-----------------|
| trades, quotes | `(exchange, symbol_id, ts_event_us, file_id, file_seq)` |
| orderbook updates | `(exchange, symbol_id, ts_event_us, book_seq, file_id, file_seq)` |

---

## 8) Module Layout

```
pointline/
├── __init__.py       # Public exports (TRADES, QUOTES, get_table_spec, etc.)
├── protocols.py      # Core protocols (BronzeFileMetadata, etc.)
├── dim_symbol.py     # SCD2 dimension utilities
├── schemas/          # Canonical schema registry (types, events, dimensions, control)
├── ingestion/        # Function-first ingestion pipeline
│   ├── pipeline.py   # ingest_file()
│   ├── manifest.py   # Manifest operations
│   ├── timezone.py   # Exchange-timezone derivation
│   ├── pit.py        # PIT coverage checks
│   └── lineage.py    # file_id, file_seq assignment
├── storage/          # Storage adapters
│   ├── contracts.py  # Store protocols
│   └── delta/        # Delta Lake implementations
│       ├── event_store.py
│       ├── dimension_store.py
│       ├── manifest_store.py
│       └── quarantine_store.py
├── vendors/          # Vendor integrations
│   ├── quant360/     # Quant360 CN L2/L3 data
│   └── tushare/      # Tushare symbol data
└── research/         # Research API
    ├── query.py      # Event loading
    ├── spine.py      # Spine builders
    └── discovery.py  # Symbol discovery
```

### Key Import Paths

```python
# Core schemas
from pointline import TRADES, QUOTES, ORDERBOOK_UPDATES, DIM_SYMBOL
from pointline.schemas import get_table_spec

# Ingestion
from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata

# Storage
from pointline.storage.delta import DeltaEventStore, DeltaDimensionStore

# Research
from pointline.research import load_events, build_spine, discover_symbols
```

---

## 9) Operational Defaults

- Correctness over throughput.
- Explicit maintenance (`optimize`, `vacuum`) by operator choice.
- No backward compatibility guarantee for legacy contracts.
- No mandatory migration path for old schema versions; rebuild/re-ingest is acceptable.

---

## 10) Out of Scope

- Live execution/routing
- Cloud object storage
- Distributed compute/storage backends
- CLI orchestration (separate concern)

---

## 11) References

- Ingestion design: `docs/architecture/simplified-ingestion-design-v2.md`
- V2 cleanup plan: `docs/internal/execplan-v2-final-cleanup.md`
