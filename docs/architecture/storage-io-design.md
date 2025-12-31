# Storage-IO Architecture (Delta Lake + Polars)

This document defines a storage-agnostic architecture for table logic and a Delta‑RS IO layer. It keeps `src/dim_symbol.py` pure and establishes a repeatable pattern for adding new tables.

## Goals
- Keep domain logic testable without storage dependencies.
- Isolate Delta‑RS specifics in a single IO layer.
- Make new tables easy to add with minimal boilerplate.

## Layered Design

### 1) Domain (Pure Table Logic)
**Location:** `src/<table>.py`
- Owns schema, validation, and transformations.
- Operates on `polars.DataFrame` only.
- No file paths or Delta imports.

Example: `src/dim_symbol.py` provides SCD2 rules and schema validation.

### 2) Service (Orchestration)
**Location:** `src/services/<table>_service.py`
- Calls domain functions in the correct order.
- Chooses read/write strategy (overwrite vs merge).
- Contains no storage details beyond the repo interface.

### 3) IO Adapter (Storage)
**Location:** `src/io/delta_<table>_repo.py`
- Reads/writes using Delta‑RS.
- Owns table path and physical layout.
- Exposes a small interface to services.

## Interfaces

### Repository Protocol (Storage‑Agnostic)
**Location:** `src/io/protocols.py`
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
3. `scd2_upsert(current, updates)` in `src/dim_symbol.py`
4. `repo.write_full(updated)` (deterministic rebuild) or `repo.merge(...)`

## Delta‑RS IO Behavior
- **Overwrite** is preferred for deterministic rebuilds.
- **Merge** is optional for incremental updates.
- IO layer converts between Polars and Arrow and handles Delta‑RS calls.

## Adding a New Table
For a new table (e.g., `trades`):
1. Create `src/trades.py` with schema + validation.
2. Create `src/services/trades_service.py` with read → transform → write.
3. Create `src/io/delta_trades_repo.py` with Delta‑RS read/write.
4. Register table paths in `src/config.py`.

This keeps the pattern consistent and the domain layer reusable.

## Configuration
**Location:** `src/config.py`
- Central place for table paths and storage settings.
- Example: `TABLE_PATHS = {"dim_symbol": "/lake/silver/dim_symbol"}`

## Non‑Goals
- No storage logic in domain modules.
- No cross‑table orchestration (each service handles one table).
