# V2 Final Cleanup: Remove Legacy, Flatten to Clean Structure

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan must be maintained in accordance with `/Users/zjx/Documents/pointline/PLANS.md`.

## Purpose / Big Picture

After this change, Pointline will have a clean, minimal v2-only codebase with no legacy compatibility paths. The structure becomes:

```
pointline/
├── __init__.py          # Minimal, clean exports
├── schemas/             # Canonical schema specs (unchanged)
├── ingestion/           # Function-first v2 ingestion pipeline
├── storage/             # Delta storage adapters
├── vendors/             # Vendor-specific parsers and adapters
├── research/            # V2 research API (discovery, query, spines)
├── protocols.py         # Core protocols (BronzeFileMetadata, etc.)
└── dim_symbol.py        # Dimension table utilities
```

All legacy modules (cli/, tables/, services/, dq/, io/, legacy research/, config.py, etc.) are removed. Tests are consolidated under `tests/` with v2 tests as the primary test suite.

## Progress

- [ ] (YYYY-MM-DD HH:MMZ) Create this ExecPlan with concrete steps and validation criteria.
- [ ] Move pointline/io/protocols.py to pointline/protocols.py and update imports.
- [ ] Move pointline/v2/ingestion/ to pointline/ingestion/ and update imports.
- [ ] Move pointline/v2/storage/ to pointline/storage/ and update imports.
- [ ] Move pointline/v2/vendors/ to pointline/vendors/ and update imports.
- [ ] Replace pointline/research/ with pointline/v2/research/ content.
- [ ] Move pointline/v2/dim_symbol.py to pointline/dim_symbol.py.
- [ ] Clean pointline/__init__.py with minimal v2-only exports.
- [ ] Remove all legacy modules (cli/, dq/, io/, services/, tables/, config.py, encoding.py, introspection.py, _error_messages.py).
- [ ] Clean tests/: remove 49 legacy test files, remove legacy test dirs (research/, parsers/), move v2 tests to root.
- [ ] Update pyproject.toml to remove legacy entry points.
- [ ] Run full quality gates and record evidence.

## Surprises & Discoveries

(None yet - to be recorded during implementation)

## Decision Log

- Decision: Keep `pointline/schemas/` exactly as-is; it is already clean v2.
  Rationale: The schemas module is already well-designed and has no legacy coupling.
  Date/Author: 2026-02-13 / Codex

- Decision: Move `pointline/io/protocols.py` to `pointline/protocols.py` rather than keeping the io/ directory.
  Rationale: The protocols module is lightweight and used by both ingestion and storage; keeping it at root avoids a single-file directory.
  Date/Author: 2026-02-13 / Codex

- Decision: Replace `pointline/research/` entirely with `pointline/v2/research/` content.
  Rationale: The v2 research API is designed for the clean architecture; legacy research has different semantics.
  Date/Author: 2026-02-13 / Codex

- Decision: Remove CLI entirely in this cleanup.
  Rationale: CLI was designed for legacy ingestion flow; a new CLI should be built on clean v2 core if needed.
  Date/Author: 2026-02-13 / Codex

- Decision: Remove legacy tests entirely, keep only v2 tests.
  Rationale: Legacy tests test code that no longer exists; v2 tests validate the new architecture.
  Date/Author: 2026-02-13 / Codex

## Outcomes & Retrospective

(To be completed at end of implementation)

## Context and Orientation

### Current State

**Source Code:**
- `pointline/schemas/` - Clean v2 schema specs (keep)
- `pointline/v2/` - New v2 core (move to root)
- `pointline/cli/`, `pointline/dq/`, `pointline/io/`, `pointline/services/`, `pointline/tables/` - Legacy (remove)
- `pointline/research/` - Legacy research API (replace with v2/research/)
- `pointline/config.py`, `pointline/encoding.py`, etc. - Legacy utilities (remove)

**Tests:**
- `tests/v2/` - 35 clean v2 test files (move to root)
- `tests/*.py` - 49 legacy test files (remove)
- `tests/research/`, `tests/parsers/` - Legacy test directories (remove)

### Target State

After cleanup, only v2 code remains with imports like:

```python
from pointline.schemas import TRADES, get_table_spec
from pointline.ingestion.pipeline import ingest_file
from pointline.storage.delta import EventStore
from pointline.protocols import BronzeFileMetadata
from pointline.research import load_events, build_spine
```

### Import Mapping

| Old Import | New Import |
|------------|------------|
| `pointline.v2.ingestion.pipeline` | `pointline.ingestion.pipeline` |
| `pointline.v2.storage.delta` | `pointline.storage.delta` |
| `pointline.v2.vendors.quant360` | `pointline.vendors.quant360` |
| `pointline.v2.research.query` | `pointline.research.query` |
| `pointline.io.protocols` | `pointline.protocols` |
| `pointline.v2.dim_symbol` | `pointline.dim_symbol` |

## Plan of Work

### Phase 1: Move Core Protocols

Move `pointline/io/protocols.py` to `pointline/protocols.py`. Update any imports within the file if needed (should be self-contained).

### Phase 2: Move V2 Modules to Root

For each module in `pointline/v2/`, move to root level:

1. `pointline/v2/ingestion/` → `pointline/ingestion/`
2. `pointline/v2/storage/` → `pointline/storage/`
3. `pointline/v2/vendors/` → `pointline/vendors/`
4. `pointline/v2/research/` → `pointline/research/` (replace existing)
5. `pointline/v2/dim_symbol.py` → `pointline/dim_symbol.py`

After moving, delete empty `pointline/v2/` directory.

### Phase 3: Update Internal Imports

Update all internal imports in moved modules:

- `from pointline.v2.ingestion.X` → `from pointline.ingestion.X`
- `from pointline.v2.storage.X` → `from pointline.storage.X`
- `from pointline.io.protocols` → `from pointline.protocols`
- `from pointline.v2.research.X` → `from pointline.research.X`

### Phase 4: Clean Root Exports

Rewrite `pointline/__init__.py` to export only clean v2 APIs:

```python
"""Pointline v2 - Clean market data lake."""

from pointline.schemas import (
    TRADES,
    QUOTES,
    ORDERBOOK_UPDATES,
    DIM_SYMBOL,
    get_table_spec,
    list_table_specs,
)
from pointline.protocols import BronzeFileMetadata

__all__ = [
    "TRADES",
    "QUOTES",
    "ORDERBOOK_UPDATES",
    "DIM_SYMBOL",
    "get_table_spec",
    "list_table_specs",
    "BronzeFileMetadata",
]
```

### Phase 5: Remove Legacy Modules

Delete entire directories:
- `pointline/cli/`
- `pointline/dq/`
- `pointline/io/` (after protocols.py moved)
- `pointline/services/`
- `pointline/tables/`

Delete files:
- `pointline/config.py`
- `pointline/encoding.py`
- `pointline/introspection.py`
- `pointline/_error_messages.py`

### Phase 6: Clean Tests

Current state:
- `tests/v2/` - Clean v2 tests (preserve)
- `tests/*.py` - 49 legacy test files (remove)
- `tests/research/`, `tests/parsers/`, etc. - Legacy test directories (remove)

Steps:
1. Remove all legacy test files and directories
2. Move `tests/v2/` contents to `tests/` root
3. Update imports in moved tests
4. Ensure pytest can discover and run tests

#### Legacy test files to remove (49 files):

All files in `tests/*.py` except any that might test schemas (verify first).

#### Legacy test directories to remove:
- `tests/research/` - Old research API tests
- `tests/parsers/` - Old parser tests
- `tests/v2/` - Will be moved to root then deleted

#### V2 tests to preserve (move to root):
- `tests/v2/test_*.py` → `tests/test_*.py`
- `tests/v2/quant360/` → `tests/quant360/`
- `tests/v2/research/` → `tests/research/`
- `tests/v2/storage/` → `tests/storage/`
- `tests/v2/tushare/` → `tests/tushare/`

### Phase 7: Update pyproject.toml

Remove legacy entry points and scripts:
- Remove `[project.scripts]` console script entries for legacy CLI
- Update `[tool.setuptools.packages.find]` if needed

## Concrete Steps

Run all commands from `/Users/zjx/Documents/pointline`.

### Phase 1: Move Protocols

    # Move protocols to root
    mv pointline/io/protocols.py pointline/protocols.py

    # Remove empty io directory after move
    rmdir pointline/io 2>/dev/null || rm -rf pointline/io

### Phase 2: Move V2 Modules

    # Move ingestion
    mv pointline/v2/ingestion pointline/ingestion

    # Move storage
    mv pointline/v2/storage pointline/storage

    # Move vendors
    mv pointline/v2/vendors pointline/vendors

    # Replace research (backup first if needed)
    rm -rf pointline/research
    mv pointline/v2/research pointline/research

    # Move dim_symbol
    mv pointline/v2/dim_symbol.py pointline/dim_symbol.py

    # Remove empty v2 directory
    rmdir pointline/v2 2>/dev/null || rm -rf pointline/v2

### Phase 3: Update Imports

Use sed/rg to find and update import patterns:

    # Update v2.ingestion imports
    find pointline -name "*.py" -exec sed -i 's/from pointline\.v2\.ingestion/from pointline.ingestion/g' {} \;
    find pointline -name "*.py" -exec sed -i 's/import pointline\.v2\.ingestion/import pointline.ingestion/g' {} \;

    # Update v2.storage imports
    find pointline -name "*.py" -exec sed -i 's/from pointline\.v2\.storage/from pointline.storage/g' {} \;
    find pointline -name "*.py" -exec sed -i 's/import pointline\.v2\.storage/import pointline.storage/g' {} \;

    # Update v2.vendors imports
    find pointline -name "*.py" -exec sed -i 's/from pointline\.v2\.vendors/from pointline.vendors/g' {} \;
    find pointline -name "*.py" -exec sed -i 's/import pointline\.v2\.vendors/import pointline.vendors/g' {} \;

    # Update v2.research imports
    find pointline -name "*.py" -exec sed -i 's/from pointline\.v2\.research/from pointline.research/g' {} \;
    find pointline -name "*.py" -exec sed -i 's/import pointline\.v2\.research/import pointline.research/g' {} \;

    # Update io.protocols imports
    find pointline -name "*.py" -exec sed -i 's/from pointline\.io\.protocols/from pointline.protocols/g' {} \;
    find pointline -name "*.py" -exec sed -i 's/import pointline\.io\.protocols/import pointline.protocols/g' {} \;

    # Update v2.dim_symbol imports
    find pointline -name "*.py" -exec sed -i 's/from pointline\.v2\.dim_symbol/from pointline.dim_symbol/g' {} \;
    find pointline -name "*.py" -exec sed -i 's/import pointline\.v2\.dim_symbol/import pointline.dim_symbol/g' {} \;

### Phase 4: Clean Tests

    # First backup v2 tests temporarily
    mv tests/v2 /tmp/v2_tests_backup

    # Remove all legacy test files
    rm -f tests/*.py

    # Remove legacy test directories
    rm -rf tests/research tests/parsers
    rm -rf tests/__pycache__ tests/**/__pycache__

    # Move v2 tests back to root
    mv /tmp/v2_tests_backup/* tests/
    rmdir /tmp/v2_tests_backup 2>/dev/null || true

    # Update imports in tests to match new structure
    find tests -name "*.py" -exec sed -i 's/from pointline\.v2/from pointline/g' {} \;
    find tests -name "*.py" -exec sed -i 's/from pointline\.io\.protocols/from pointline.protocols/g' {} \;

    # Verify pytest collection
    uv run pytest tests/ --collect-only -q 2>&1 | head -30

### Phase 5: Clean Root __init__.py and Remove Legacy Modules

    # Create minimal __init__.py
    cat > pointline/__init__.py << 'EOF'
    """Pointline v2 - Clean market data lake."""

    from pointline.schemas import (
        TRADES,
        QUOTES,
        ORDERBOOK_UPDATES,
        DIM_SYMBOL,
        get_table_spec,
        list_table_specs,
    )
    from pointline.protocols import BronzeFileMetadata

    __all__ = [
        "TRADES",
        "QUOTES",
        "ORDERBOOK_UPDATES",
        "DIM_SYMBOL",
        "get_table_spec",
        "list_table_specs",
        "BronzeFileMetadata",
    ]
    EOF

    # Remove legacy modules
    rm -rf pointline/cli
    rm -rf pointline/dq
    rm -rf pointline/io
    rm -rf pointline/services
    rm -rf pointline/tables
    rm -f pointline/config.py
    rm -f pointline/encoding.py
    rm -f pointline/introspection.py
    rm -f pointline/_error_messages.py

### Phase 6: Update pyproject.toml

    # Remove CLI entry points - edit pyproject.toml manually or use sed
    # The [project.scripts] section should be removed or updated

### Phase 7: Quality Gates

Run validation after all changes:

    uv run ruff check pointline tests
    uv run ruff format pointline tests --check
    uv run python -c "import pointline; print(pointline.__all__)"
    uv run pytest tests/ -q --collect-only 2>&1 | head -50

## Validation and Acceptance

Acceptance is verified through:

1. **Import Test**: Can import pointline with clean v2 exports
   ```python
   from pointline import TRADES, get_table_spec, BronzeFileMetadata
   from pointline.ingestion.pipeline import ingest_file
   from pointline.storage.delta import EventStore
   from pointline.research import load_events
   ```

2. **No Legacy Imports**: Repository search shows no imports from:
   - `pointline.v2.*` (should be flattened)
   - `pointline.io.*` (should use `pointline.protocols`)
   - `pointline.cli.*`, `pointline.dq.*`, `pointline.services.*`, `pointline.tables.*` (removed)

3. **Quality Gates Pass**:
   - `ruff check pointline tests` passes
   - `ruff format --check pointline tests` passes
   - `pytest tests/` collects and runs v2 tests (expect ~30+ tests from v2 suite)

4. **Test Structure Verification**:
   ```
   $ ls tests/
   test_*.py              # Core v2 tests (schema, ingestion, lineage, etc.)
   quant360/              # Vendor-specific tests
   research/              # Research API tests
   storage/               # Storage adapter tests
   tushare/               # Tushare vendor tests
   ```

   No `tests/v2/` directory should remain (flattened to root).

4. **Structure Verification**:
   ```
   $ find pointline -type d -maxdepth 1
   pointline/
   pointline/schemas/
   pointline/ingestion/
   pointline/storage/
   pointline/vendors/
   pointline/research/
   ```

## Idempotence and Recovery

This cleanup is destructive to legacy code. Before starting:

1. Ensure all work is committed to git
2. The cleanup can be replayed by resetting and re-running the steps
3. If issues arise, recovery is via `git reset --hard` or `git checkout`

Data safety: This only removes code, no data files are touched.

## Artifacts and Notes

(To be populated with command outputs during implementation)

## Interfaces and Dependencies

### Final Module Structure

```
pointline/
├── __init__.py              # Public exports
├── protocols.py             # BronzeFileMetadata, TableRepository protocols
├── dim_symbol.py            # SCD2 dimension utilities
├── schemas/                 # Table specs, column specs, types
│   ├── __init__.py
│   ├── types.py
│   ├── events.py
│   ├── events_cn.py
│   ├── dimensions.py
│   ├── control.py
│   └── registry.py
├── ingestion/               # Ingestion pipeline
│   ├── __init__.py
│   ├── pipeline.py          # ingest_file()
│   ├── models.py            # IngestionResult
│   ├── manifest.py          # Manifest operations
│   ├── timezone.py          # derive_trading_date
│   ├── pit.py               # PIT coverage checks
│   ├── lineage.py           # file_id, file_seq assignment
│   ├── normalize.py         # Schema normalization
│   ├── exchange.py          # Exchange timezone mapping
│   └── cn_validation.py     # CN-specific validations
├── storage/                 # Storage adapters
│   ├── __init__.py
│   ├── contracts.py         # Store protocols
│   ├── models.py
│   └── delta/               # Delta Lake implementations
│       ├── __init__.py
│       ├── event_store.py
│       ├── dimension_store.py
│       ├── manifest_store.py
│       ├── quarantine_store.py
│       ├── optimizer_store.py
│       ├── layout.py
│       └── _utils.py
├── vendors/                 # Vendor integrations
│   ├── __init__.py
│   ├── quant360/
│   └── tushare/
└── research/                # Research API
    ├── __init__.py
    ├── query.py
    ├── discovery.py
    ├── metadata.py
    ├── primitives.py
    ├── spine.py
    ├── cn_trading_phases.py
    └── ...
```

### Final Test Structure

```
tests/
├── test_schema_contracts.py              # Schema registry and contracts
├── test_ingestion_pipeline_contract.py   # Ingestion pipeline behavior
├── test_manifest_semantics.py            # Manifest idempotency
├── test_timezone_partitioning.py         # Exchange-timezone partition logic
├── test_lineage.py                       # file_id, file_seq assignment
├── test_dim_symbol_scd2.py              # SCD2 dimension tests
├── test_core_integration_no_legacy_imports.py  # Import isolation
├── quant360/                             # Quant360 vendor tests
│   ├── test_dispatch.py
│   ├── test_parsers_cn_streams.py
│   ├── test_pipeline_cn_quant360.py
│   ├── test_schema_cn_quant360.py
│   ├── test_timestamp_parser.py
│   ├── test_filename_and_symbol_extraction.py
│   └── upstream/                         # Upstream adapter tests
│       ├── test_upstream_contracts.py
│       ├── test_upstream_discover.py
│       ├── test_upstream_ledger.py
│       ├── test_upstream_publish.py
│       └── test_upstream_runner.py
├── storage/                              # Storage adapter tests
│   ├── test_contracts.py
│   ├── test_event_store_delta.py
│   ├── test_dimension_store_delta.py
│   ├── test_manifest_store_delta.py
│   ├── test_quarantine_store_delta.py
│   ├── test_compaction_contracts.py
│   ├── test_compaction_delta.py
│   ├── test_vacuum_delta.py
│   └── test_pipeline_delta_adapters.py
├── research/                             # Research API tests
│   ├── test_discovery_query.py
│   ├── test_primitives_decode.py
│   ├── test_spine_contract.py
│   ├── test_spine_builders.py
│   ├── test_spine_alignment.py
│   ├── test_cn_trading_phases.py
│   └── test_spine_no_legacy_imports.py
└── tushare/                              # Tushare vendor tests
    └── test_symbol_snapshot.py
```

---

Revision Note (2026-02-13 19:45Z): Initial ExecPlan created for final v2 cleanup and legacy removal.
Revision Note (2026-02-13 20:00Z): Updated with comprehensive test cleanup strategy (49 legacy files removed, 35 v2 tests preserved).
