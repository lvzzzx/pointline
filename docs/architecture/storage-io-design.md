# Storage-IO Architecture (Delta Lake + Polars)

This document defines a storage-agnostic architecture for table logic and a Delta‑RS IO layer. It keeps `pointline/dim_symbol.py` pure and establishes a repeatable pattern for adding new tables.

## Goals
- Keep domain logic testable without storage dependencies.
- Isolate Delta‑RS specifics in a single IO layer.
- Make new tables easy to add with minimal boilerplate.

## Layered Design

### 1) Domain (Pure Table Logic)
**Location:** `pointline/<table>.py`
- Owns schema, validation, and transformations.
- Operates on `polars.DataFrame` only.
- No file paths or Delta imports.

Example: `pointline/dim_symbol.py` provides SCD2 rules and schema validation.

### 2) Service (Orchestration)
**Location:** `pointline/services/<table>_service.py`
- Calls domain functions in the correct order.
- Chooses read/write strategy (overwrite vs merge).
- Contains no storage details beyond the repo interface.

### 3) IO Adapter (Storage)
**Location:** `pointline/io/delta_<table>_repo.py`
- Reads/writes using Delta‑RS.
- Owns table path and physical layout.
- Exposes a small interface to services.

## Interfaces

### Repository Protocol (Storage‑Agnostic)
**Location:** `pointline/io/protocols.py`
- Define what a table repository must implement.
- Reused by all tables.

```python
from typing import Protocol
import polars as pl

class TableRepository(Protocol):
    def read_all(self) -> pl.DataFrame: ...
    def write_full(self, df: pl.DataFrame) -> None: ...
    def merge(self, df: pl.DataFrame, keys: list[str]) -> None: ...
```

## Data Flow Example (dim_symbol)

1. `DimSymbolService.update(updates)`
2. `repo.read_all()`
3. `scd2_upsert(current, updates)` in `pointline/dim_symbol.py`
4. `repo.write_full(updated)` (deterministic rebuild) or `repo.merge(...)`

## Delta‑RS IO Behavior
- **Overwrite** is preferred for deterministic rebuilds.
- **Merge** is optional for incremental updates.
- IO layer converts between Polars and Arrow and handles Delta‑RS calls.

## Adding a New Table
For a new table (e.g., `trades`):
1. Create `pointline/trades.py` with schema + validation.
2. Create `pointline/services/trades_service.py` with read → transform → write.
3. Create `pointline/io/delta_trades_repo.py` with Delta‑RS read/write.
4. Register table paths in `pointline/config.py`.

This keeps the pattern consistent and the domain layer reusable.

## Configuration
**Location:** `pointline/config.py`
- Central place for table paths and storage settings.
- Example: `TABLE_PATHS = {"dim_symbol": "/lake/silver/dim_symbol"}`

## Ingestion & Manifest Interfaces
For the ingestion pipeline (Bronze → Silver), we separate **scanning** files from **tracking** their state.

### 1. BronzeSource (Scanner)
Scans physical storage (Local, S3, etc.) to find candidate files. Does not know about ingestion state.

```python
@dataclass
class BronzeFileMetadata:
    exchange: str
    data_type: str
    symbol: str
    date: date
    bronze_file_path: str
    file_size_bytes: int
    last_modified_ts: int

class BronzeSource(Protocol):
    def list_files(self, glob_pattern: str) -> Iterator[BronzeFileMetadata]:
        """Scans storage for files matching the pattern."""
        ...
```

### 2. IngestionManifestRepository (State Ledger)
Manages the `silver.ingest_manifest` table. Handles skip logic and `file_id` assignment.

```python
class IngestionManifestRepository(Protocol):
    def resolve_file_id(self, meta: BronzeFileMetadata) -> int:
        """Gets existing ID or mints a new one for a file."""
        ...

    def filter_pending(self, candidates: list[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
        """Returns only files that need processing (efficient batch anti-join)."""
        ...
        
    def update_status(self, file_id: int, status: str, meta: BronzeFileMetadata, result=None) -> None:
        """Records success/failure."""
        ...
```

### Workflow
1. **Discover**: `source.list_files(...)` → `all_files`
2. **Filter**: `manifest.filter_pending(all_files)` → `todo_files`
3. **Loop**:
   - `file_id = manifest.resolve_file_id(file)`
   - `transform_and_write(file, file_id)`
   - `manifest.update_status(...)`

## Non‑Goals
- No storage logic in domain modules.
- No cross‑table orchestration (each service handles one table).