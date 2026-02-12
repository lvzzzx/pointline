# Schema Design v3

**Status:** Proposal
**Scope:** Clean, Polars-native schema definitions for event tables, dimensions, and control tables

---

## Philosophy

> **"Schemas are code, not configuration."**

- Single source of truth in Python (not YAML/JSON/proto)
- Polars-native data types (not SQL DDL)
- Self-documenting with column metadata
- Importable and introspectable at runtime

---

## File Layout

```
pointline/
├── schemas/                    # Schema definitions
│   ├── __init__.py            # Public API exports
│   ├── types.py               # Core types, Column dataclass, constants
│   ├── events.py              # Event tables: trades, quotes, orderbook
│   ├── dimensions.py          # Dimension tables: dim_symbol
│   └── control.py             # Control tables: manifest, validation_log
```

---

## Core Abstraction

### `Column` Dataclass

```python
@dataclass(frozen=True)
class Column:
    """Schema column definition with metadata."""
    name: str
    dtype: pl.DataType
    nullable: bool = False
    description: str = ""
```

**Why frozen dataclass:**
- Immutable = hashable, safe for lookups
- Self-documenting: `TradesSchema.PRICE.description`
- Type-safe: `Column.dtype` is `pl.DataType`, not arbitrary string

---

## Schema Definitions

### 1. Event Schemas (`events.py`)

#### `TradesSchema`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ts_event` | `Int64` | No | Exchange timestamp (microseconds since epoch) |
| `ts_ingest` | `Int64` | No | Ingestion timestamp (microseconds) |
| `symbol` | `Utf8` | No | Normalized symbol (e.g., "BTCUSDT-PERP") |
| `exchange` | `Utf8` | No | Exchange code (e.g., "binance") |
| `is_buy` | `UInt8` | No | 1 = taker bought, 0 = taker sold |
| `price` | `Int64` | No | Fixed-point price (8 decimal places) |
| `qty` | `Int64` | No | Fixed-point quantity (8 decimal places) |
| `amount` | `Int64` | No | Pre-computed: price * qty |
| `is_buyer_maker` | `UInt8` | No | Buyer was maker (0 = taker was buyer) |
| `trade_id` | `Utf8` | Yes | Exchange trade ID (for idempotency) |
| `file_id` | `Int64` | No | Source file FK → manifest.file_id |
| `file_seq` | `Int32` | No | Line number within source file |
| `date` | `Date` | No | Event date (partition column) |

**Partitions:** `(exchange, date)`
**Sort order:** `(symbol, ts_event)` within files

---

#### `QuotesSchema`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ts_event` | `Int64` | No | Exchange timestamp (microseconds) |
| `ts_ingest` | `Int64` | No | Ingestion timestamp (microseconds) |
| `symbol` | `Utf8` | No | Normalized symbol |
| `exchange` | `Utf8` | No | Exchange code |
| `bid_price` | `Int64` | No | Best bid price (fixed-point) |
| `bid_qty` | `Int64` | No | Best bid quantity (fixed-point) |
| `ask_price` | `Int64` | No | Best ask price (fixed-point) |
| `ask_qty` | `Int64` | No | Best ask quantity (fixed-point) |
| `seq_num` | `Int64` | Yes | Exchange sequence number |
| `level` | `Int16` | No | Book level (1 = L1, 2+ = L2) |
| `file_id` | `Int64` | No | Source file FK |
| `file_seq` | `Int32` | No | Line number in source |
| `date` | `Date` | No | Event date (partition) |

**Partitions:** `(exchange, date, level)`

---

#### `OrderbookSchema`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ts_event` | `Int64` | No | Exchange timestamp (microseconds) |
| `ts_ingest` | `Int64` | No | Ingestion timestamp (microseconds) |
| `symbol` | `Utf8` | No | Normalized symbol |
| `exchange` | `Utf8` | No | Exchange code |
| `is_bid` | `UInt8` | No | 1 = bid, 0 = ask |
| `price` | `Int64` | No | Price level (fixed-point) |
| `qty` | `Int64` | No | Quantity at level (fixed-point) |
| `order_count` | `Int32` | Yes | Number of orders at level |
| `book_seq` | `Int64` | No | Book sequence for ordering |
| `is_snapshot` | `UInt8` | No | 1 = full snapshot, 0 = delta |
| `file_id` | `Int64` | No | Source file FK |
| `file_seq` | `Int32` | No | Line number in source |
| `date` | `Date` | No | Event date (partition) |

**Partitions:** `(exchange, date, symbol)`

---

### 2. Dimension Schemas (`dimensions.py`)

#### `DimSymbolSchema` (SCD Type 2)

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `symbol_sk` | `Int64` | No | Surrogate key (auto-increment) |
| `symbol` | `Utf8` | No | Exchange-native symbol |
| `exchange` | `Utf8` | No | Exchange code |
| `market_type` | `Utf8` | No | spot/perp/future/option |
| `base_asset` | `Utf8` | No | Base currency (e.g., "BTC") |
| `quote_asset` | `Utf8` | No | Quote currency (e.g., "USDT") |
| `expiry_date` | `Date` | Yes | Contract expiry (NULL for spot/perp) |
| `valid_from` | `Date` | No | Validity start (SCD2) |
| `valid_to` | `Date` | Yes | Validity end (NULL = current) |
| `is_current` | `UInt8` | No | 1 = current record, 0 = historical |
| `contract_size` | `Float64` | Yes | For futures/options |
| `tick_size` | `Float64` | Yes | Minimum price increment |
| `lot_size` | `Float64` | Yes | Minimum quantity increment |
| `updated_at` | `Datetime` | No | Record update timestamp |

**Query pattern:** PIT lookup by `(symbol, exchange)` with date range

---

### 3. Control Schemas (`control.py`)

#### `ManifestSchema`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `file_id` | `Int64` | No | Surrogate PK (auto-increment) |
| `vendor` | `Utf8` | No | Data vendor (tardis, binance_vision, etc.) |
| `data_type` | `Utf8` | No | trades/quotes/orderbook |
| `bronze_path` | `Utf8` | No | Source file path |
| `file_hash` | `Utf8` | No | SHA256 of source file |
| `exchange` | `Utf8` | No | Target exchange |
| `symbol` | `Utf8` | No | Target symbol |
| `date` | `Date` | No | Target date |
| `status` | `Utf8` | No | pending/success/failed/quarantined |
| `rows_total` | `Int64` | Yes | Rows in source file |
| `rows_written` | `Int64` | Yes | Rows written to silver |
| `rows_quarantined` | `Int64` | Yes | Rows quarantined |
| `created_at` | `Datetime` | No | Record creation time |
| `processed_at` | `Datetime` | Yes | Processing completion time |
| `status_reason` | `Utf8` | Yes | Failure/quarantine reason |

**Unique constraint:** `(vendor, data_type, bronze_path, file_hash)`

---

#### `ValidationLogSchema`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `log_id` | `Int64` | No | Surrogate PK |
| `file_id` | `Int64` | No | FK → manifest.file_id |
| `file_seq` | `Int32` | No | Line number in source file |
| `row_hash` | `Utf8` | Yes | Hash of row content |
| `rule_name` | `Utf8` | No | Failed validation rule |
| `field_name` | `Utf8` | Yes | Field that failed |
| `field_value` | `Utf8` | Yes | String representation of bad value |
| `ts_event` | `Int64` | Yes | Parseable timestamp from row |
| `symbol` | `Utf8` | Yes | Parseable symbol from row |
| `logged_at` | `Datetime` | No | Log entry time |

---

## Type Constants

```python
# pointline/schemas/types.py

# Fixed-point precision
PRICE_PRECISION = 8      # 8 decimal places for prices
QTY_PRECISION = 8        # 8 decimal places for quantities

# Status values
STATUS_PENDING = "pending"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_QUARANTINED = "quarantined"

# Market types
MARKET_SPOT = "spot"
MARKET_PERP = "perp"
MARKET_FUTURE = "future"
MARKET_OPTION = "option"
```

---

## Schema Class Interface

Each schema class provides:

```python
class TradesSchema:
    # Column definitions
    TS_EVENT = Column("ts_event", pl.Int64, description="...")
    ...

    @classmethod
    def to_polars(cls) -> dict[str, pl.DataType]:
        """Return as polars schema dict for DataFrame creation."""
        ...

    @classmethod
    def columns(cls) -> list[str]:
        """Return ordered column names."""
        ...

    @classmethod
    def required_columns(cls) -> list[str]:
        """Return non-nullable column names."""
        ...
```

---

## Usage Patterns

### Creating a DataFrame with Schema

```python
from pointline.schemas import TRADES_SCHEMA

df = pl.DataFrame(data, schema=TRADES_SCHEMA)
```

### Validating a DataFrame

```python
def validate_schema(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> None:
    """Assert DataFrame matches expected schema."""
    for col, dtype in schema.items():
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
        if df[col].dtype != dtype:
            raise TypeError(f"Column {col}: expected {dtype}, got {df[col].dtype}")
```

### Introspecting Columns

```python
from pointline.schemas import TradesSchema

# Get column description
print(TradesSchema.PRICE.description)

# Check nullability
if not TradesSchema.SYMBOL.nullable:
    df = df.filter(pl.col("symbol").is_not_null())
```

---

## Design Decisions

### Why Polars types over SQL DDL?
- Ingestion pipeline is Polars-native
- Avoid ORM/SQL translation layer
- Schema travels with code, not database

### Why class attributes over dict?
- IDE autocomplete: `TradesSchema.PRICE`
- Documentation: docstrings on class
- Extensibility: methods on class

### Why frozen dataclass for Column?
- Immutable = safe for global constants
- Hashable = can use in sets/dicts if needed
- Clear __repr__ for debugging

### Why not Pydantic?
- Pydantic validates Python objects, not DataFrames
- Polars has native schema enforcement
- One less dependency

---

## Migration Path

1. Create `schemas/` module alongside existing schema definitions
2. Migrate parsers to use new schemas incrementally
3. Update ingestion pipeline to validate against schemas
4. Deprecate old schema definitions

---

## Acceptance Criteria

- [ ] All event tables have Polars-native schema definitions
- [ ] Dimension tables support SCD2 PIT queries
- [ ] Control tables support idempotency and audit
- [ ] Schemas are importable and introspectable
- [ ] IDE autocomplete works for column names
