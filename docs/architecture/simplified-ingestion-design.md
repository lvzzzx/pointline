# Simplified Ingestion Design

**Status:** Proposal
**Scope:** Replace layered service/repository architecture with direct file-to-Delta transforms

---

## 1. Problem Statement

Current architecture has too many layers for a file-based operation:

```
CLI → GenericIngestionService → BaseService (ABC)
  → TableRepository → BaseDeltaRepository → delta-rs
  → VendorPlugin → Parser
  + ManifestRepo + ValidationLog + DomainContract
```

**Complexity cost:** ~2000 lines across 15+ files for what is essentially:
```python
df = pl.read_csv(file)
df = transform(df)
df.write_delta(table_path)
```

---

## 2. Guiding Principle

**"File-based operations should be functions, not frameworks."**

- Polars already provides `read_csv`, `read_parquet`, `write_delta`
- Vendor differences are just parsing functions
- Validation is just filter expressions
- No need for abstract base classes, repositories, or plugin systems

---

## 3. Proposed Architecture

### 3.1 Layer Structure (3 layers)

```
┌─────────────────────────────────────────┐
│  CLI / Scripts                          │  Entry points
├─────────────────────────────────────────┤
│  Ingest Functions (pure transforms)     │  Business logic
├─────────────────────────────────────────┤
│  Parsers (vendor-specific readers)      │  I/O adapters
└─────────────────────────────────────────┘
```

### 3.2 Module Layout

```
pointline/
├── ingest/
│   ├── __init__.py              # Public API: ingest_csv, ingest_parquet
│   ├── core.py                  # Core transform functions
│   ├── validate.py              # Validation helpers
│   └── parsers/
│       ├── __init__.py
│       ├── tardis.py            # parse_trades(), parse_quotes(), etc.
│       ├── binance_vision.py
│       └── quant360.py
├── tables/
│   └── schemas.py               # Schema definitions (just dicts)
└── delta_utils.py               # Thin wrappers for delta operations
```

---

## 4. Core Design

### 4.1 Ingestion Function

```python
# pointline/ingest/core.py
import polars as pl
from pathlib import Path
from typing import Callable
from datetime import date


def ingest_file(
    source_path: Path,
    table_path: Path,
    parser: Callable[[Path], pl.DataFrame],
    exchange: str,
    data_date: date,
    schema: dict[str, pl.DataType] | None = None,
) -> int:
    """Read file, transform, write to Delta table.

    Args:
        source_path: Path to source file (csv, parquet, gz, etc.)
        table_path: Path to Delta table directory
        parser: Function that reads source file into DataFrame
        exchange: Exchange identifier (e.g., "binance", "okx")
        data_date: Trading date for partition
        schema: Optional schema to enforce before write

    Returns:
        Number of rows written
    """
    # 1. Parse
    df = parser(source_path)
    if df.is_empty():
        return 0

    # 2. Add metadata columns
    df = df.with_columns([
        pl.lit(exchange).alias("exchange"),
        pl.lit(data_date).alias("date"),
        pl.int_range(1, pl.len() + 1, dtype=pl.UInt32).alias("file_line_number"),
    ])

    # 3. Validate
    df = _validate_data(df)

    # 4. Encode (fixed-point for prices/quantities)
    df = _encode_fixed_point(df)

    # 5. Enforce schema (if table exists, match its schema)
    df = _align_to_table_schema(df, table_path, schema)

    # 6. Write
    mode = "append" if _table_exists(table_path) else "overwrite"
    df.write_delta(table_path, mode=mode)

    return df.height
```

### 4.2 Parser Functions

```python
# pointline/ingest/parsers/tardis.py
import polars as pl
from pathlib import Path


def parse_trades(path: Path) -> pl.DataFrame:
    """Parse Tardis trades CSV to DataFrame."""
    df = pl.read_csv(
        path,
        dtypes={
            "timestamp": pl.Int64,
            "local_timestamp": pl.Int64,
            "price": pl.Float64,
            "amount": pl.Float64,
        }
    )

    return df.rename({
        "local_timestamp": "ts_local_us",
        "price": "price_raw",
        "amount": "qty_raw",
        "side": "side",
    }).with_columns([
        pl.col("ts_local_us").cast(pl.Int64),
        pl.when(pl.col("side") == "b")
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("is_buyer_maker")
    ])
```

### 4.3 Schema Definitions

```python
# pointline/tables/schemas.py
import polars as pl

TRADES_SCHEMA = {
    "exchange": pl.Utf8,
    "date": pl.Date,
    "symbol": pl.Utf8,
    "ts_local_us": pl.Int64,
    "price": pl.Int64,           # fixed-point encoded
    "qty": pl.Int64,             # fixed-point encoded
    "is_buyer_maker": pl.UInt8,
    "file_line_number": pl.UInt32,
}

QUOTES_SCHEMA = {
    "exchange": pl.Utf8,
    "date": pl.Date,
    "symbol": pl.Utf8,
    "ts_local_us": pl.Int64,
    "bid_price": pl.Int64,
    "bid_qty": pl.Int64,
    "ask_price": pl.Int64,
    "ask_qty": pl.Int64,
    "file_line_number": pl.UInt32,
}
```

---

## 5. Key Simplifications

| Current | Proposed | Rationale |
|---------|----------|-----------|
| `BaseService` ABC | Plain functions | No inheritance overhead |
| `TableRepository` | `df.write_delta()` | Polars already handles this |
| Vendor plugin registry | Module with functions | Import what you need |
| Manifest for idempotency | File existence check | Simpler, good enough |
| SCD2 quarantine check | Optional pre-filter | Move to data quality layer |
| Validation logging | Return result + log optional | Keep ingestion pure |
| Domain contract classes | Schema dicts + functions | Data, not objects |

---

## 6. Idempotency Pattern

Instead of manifest tracking, use simple file-to-partition mapping:

```python
def should_ingest(
    source_path: Path,
    table_path: Path,
    exchange: str,
    data_date: date,
) -> bool:
    """Check if source file has been ingested."""
    # Option 1: Check if partition exists (coarse)
    partition_path = table_path / f"exchange={exchange}" / f"date={data_date}"

    # Option 2: Check file hash in sidecar (fine-grained)
    hash_file = table_path / ".source_hashes" / f"{source_path.stem}.hash"
    if hash_file.exists():
        current_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
        return hash_file.read_text().strip() != current_hash

    return True
```

---

## 7. Usage Examples

### 7.1 Simple Ingestion

```python
from pathlib import Path
from pointline.ingest import ingest_file
from pointline.ingest.parsers import tardis

rows = ingest_file(
    source_path=Path("bronze/tardis/trades/binance/2024-01-15.csv.gz"),
    table_path=Path("silver/trades"),
    parser=tardis.parse_trades,
    exchange="binance",
    data_date=date(2024, 1, 15),
)
print(f"Ingested {rows} rows")
```

### 7.2 Batch Processing

```python
from pointline.ingest import ingest_file
from pointline.ingest.parsers import tardis

def ingest_batch(files: list[Path], exchange: str) -> int:
    total = 0
    for file in files:
        date = extract_date_from_path(file)
        total += ingest_file(
            source_path=file,
            table_path=Path("silver/trades"),
            parser=tardis.parse_trades,
            exchange=exchange,
            data_date=date,
        )
    return total
```

### 7.3 Custom Parser

```python
import polars as pl
from pointline.ingest import ingest_file

def my_custom_parser(path: Path) -> pl.DataFrame:
    """Parse proprietary format."""
    df = pl.read_csv(path, separator="|")
    # ... custom transforms
    return df

rows = ingest_file(
    source_path=Path("data/custom/trades.csv"),
    table_path=Path("silver/trades"),
    parser=my_custom_parser,
    exchange="custom",
    data_date=date(2024, 1, 15),
)
```

---

## 8. Migration Path

1. **Phase 1:** Create `pointline/ingest/` with new pattern alongside existing code
2. **Phase 2:** Migrate CLI commands to use new pattern
3. **Phase 3:** Deprecate old layers (`services/`, `io/base_repository.py`)
4. **Phase 4:** Remove deprecated code after validation

---

## 9. Out of Scope (for this design)

- Live/streaming ingestion (still file-based)
- Distributed compute (still single-node)
- Cloud storage (still local filesystem)
- Complex SCD2 symbol resolution (separate concern)

---

## 10. Comparison Summary

| Metric | Current | Proposed |
|--------|---------|----------|
| Files | 15+ | 5-7 |
| Lines of code | ~2000 | ~400 |
| Classes | 10+ | 0 (just functions) |
| Abstract base classes | 3 | 0 |
| Test complexity | High (many mocks) | Low (pure functions) |
| Time to write new parser | ~2 hours | ~15 minutes |
