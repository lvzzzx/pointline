# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Pointline is a point-in-time (PIT) accurate offline data lake for quantitative trading research. Single-machine, function-first architecture built on Polars + Delta Lake. Supports crypto (26+ exchanges, spot and derivatives) and Chinese stocks (SZSE/SSE Level 3).

**Core invariants:** PIT correctness (no lookahead bias), deterministic replay ordering, idempotent ingestion, lineage traceability from Silver back to Bronze via `file_id` + `file_seq`.

## Commands

```bash
# Setup
uv sync --all-extras && source .venv/bin/activate && pre-commit install

# Testing
pytest                                            # all tests
pytest tests/test_dim_symbol_scd2.py              # single file
pytest tests/test_dim_symbol_scd2.py::test_name   # single test
pytest -m "not slow"                              # skip slow tests
pytest --cov=pointline --cov-report=term -v       # with coverage

# Code quality
ruff check .                        # lint
ruff format .                       # format
mypy pointline --ignore-missing-imports   # type check (continue-on-error in CI)
bandit -r pointline -ll             # security scan
pre-commit run --all-files          # all hooks (run before pushing)
```

## Architecture

### Data Flow: Bronze → Silver → Research

- **Bronze:** Immutable raw vendor files (CSV.gz, ZIP), never modified.
- **Silver:** Typed, normalized Delta Lake tables with deterministic lineage. Partitioned by `(exchange, trading_date)`.
- **Research:** PIT-correct query/discovery/spine APIs returning decoded Polars DataFrames.

### Module Layout

```
pointline/
├── __init__.py        # Public exports: TRADES, QUOTES, ORDERBOOK_UPDATES, DIM_SYMBOL
├── protocols.py       # Core protocols: BronzeFileMetadata, TableRepository
├── dim_symbol.py      # SCD Type 2 symbol management (pure functions)
├── schemas/           # Canonical schema registry (one spec per table, versioned by git only)
│   ├── types.py       # ColumnSpec, TableSpec, PRICE_SCALE, QTY_SCALE
│   ├── events.py      # TRADES, QUOTES, ORDERBOOK_UPDATES
│   ├── events_cn.py   # CN_L2_SNAPSHOTS, CN_ORDER_EVENTS, CN_TICK_EVENTS
│   ├── dimensions.py  # DIM_SYMBOL
│   ├── control.py     # INGEST_MANIFEST, VALIDATION_LOG
│   └── registry.py    # get_table_spec(), list_table_specs()
├── ingestion/         # Function-first ETL pipeline
│   ├── pipeline.py    # ingest_file() — main entry point
│   ├── manifest.py    # Idempotency via (vendor, data_type, bronze_path, file_hash)
│   ├── timezone.py    # trading_date derivation (crypto=UTC, SZSE/SSE=Asia/Shanghai)
│   ├── pit.py         # PIT coverage checks against dim_symbol
│   └── lineage.py     # file_id + file_seq assignment
├── storage/           # Storage layer
│   ├── contracts.py   # EventStore, ManifestStore, QuarantineStore protocols
│   └── delta/         # Delta Lake implementations
├── research/          # Research API
│   ├── query.py       # load_events()
│   ├── discovery.py   # discover_symbols()
│   ├── metadata.py    # load_symbol_meta()
│   ├── primitives.py  # decode_scaled_columns(), join_symbol_meta()
│   ├── spine.py       # build_spine(), align_to_spine()
│   └── cn_trading_phases.py  # TradingPhase, add_phase_column()
└── vendors/           # Vendor integrations
    ├── quant360/      # CN L2/L3 parsers and canonicalization
    └── tushare/       # Symbol metadata
```

### Key Import Paths

```python
from pointline import TRADES, QUOTES, ORDERBOOK_UPDATES, DIM_SYMBOL
from pointline.schemas import get_table_spec
from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata
from pointline.storage.delta import DeltaEventStore, DeltaDimensionStore
from pointline.research import load_events, build_spine, discover_symbols
```

## Critical Design Rules

1. **Timestamps:** All stored as `Int64` UTC microseconds (`*_ts_us`). Use `ts_event_us` for event time. `trading_date` derived from `ts_event_us` converted to exchange-local time.
2. **Fixed-point integers:** Prices and quantities are scaled `Int64` (via `PRICE_SCALE`/`QTY_SCALE`). Decode only at final research output, never mid-pipeline.
3. **Deterministic ordering:** Tie-break keys are `(exchange, symbol_id, ts_event_us, file_id, file_seq)` for trades/quotes; add `book_seq` before `file_id` for orderbook.
4. **SCD Type 2:** `dim_symbol` tracks metadata changes with validity windows (`valid_from_ts_us <= ts < valid_until_ts_us`). Always resolve symbols through `dim_symbol`, not raw exchange symbols.
5. **No backward compatibility.** Schema migration = rebuild/re-ingest. No compatibility shims.
6. **Schemas are code.** One canonical spec per table in `pointline/schemas/`, versioned only by git history.

## Workflow Protocol

**Autonomy levels (assess before starting):**
- **L0 (Auto):** Formatting, typos, non-semantic refactors. Just do it.
- **L1 (Guarded, default):** Code/test changes with clear requirements. Tests must pass.
- **L2 (Approval Required):** Schema contracts, PIT semantics, storage/replay behavior. State the contract change and get explicit approval before implementing.

**Before implementing:** Read `docs/architecture/design.md` before changing schema or ETL semantics. For larger features, create an ExecPlan per `PLANS.md`.

**Self-review:** Deterministic? Idempotent? No lookahead bias? `ts_local_us` for replay? As-of joins (not exact)? Fixed-point decoded only at output?

**LLM failure modes to watch:** Verify every API call exists in the codebase. Double-check timestamp conversions and fixed-point math. Don't use patterns from old v1 code. Always read the canonical schema in `pointline/schemas/` before touching table definitions. Verify Polars method signatures exist.

## Coding Conventions

- Python 3.10+, type hints on public functions, `snake_case`/`PascalCase`, line-length 100 (Ruff).
- Test markers: `@pytest.mark.slow`, `@pytest.mark.integration`. TDD preferred. Target 80% coverage for features.
- Commit prefixes: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `style:`.
- PR risk levels: L0 (formatting), L1 (default), L2 (schema/PIT/storage). See `.github/PULL_REQUEST_TEMPLATE.md`.
