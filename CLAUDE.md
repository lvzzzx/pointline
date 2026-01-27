# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Pointline is a high-performance, point-in-time (PIT) accurate offline data lake for high-frequency trading (HFT) research. The codebase provides ETL utilities and a structured data lake optimized for deterministic, reproducible quantitative research.

**Core Philosophy:**
- **PIT correctness:** Backtests reproduce what could have been known at the time (no lookahead bias)
- **Determinism:** Stable ordering guarantees reproducible ETL outputs
- **Performance:** Polars-based processing for high-performance data transformations
- **Storage efficiency:** Compressed Parquet with integer encoding

## Commands

### Setup
```bash
# Install Python dependencies
pip install -e .
```

### Testing
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_trades.py

# Run with verbose output
pytest -v

# Run specific test function
pytest tests/test_trades.py::test_parse_tardis_trades_csv
```

### CLI Usage
```bash
# Symbol management
pointline symbol search BTCUSDT --exchange binance-futures
pointline symbol sync --file ./symbols.csv

# Bronze layer (raw vendor data)
pointline bronze download --vendor tardis --exchange binance-futures --date 2024-05-01
pointline bronze discover --pending-only

# Silver layer (ETL ingestion)
pointline ingest discover --pending-only
pointline ingest run --table trades --exchange binance-futures --date 2024-05-01

# Manifest and maintenance
pointline manifest show
pointline delta optimize --table trades --partition exchange=binance-futures/date=2024-05-01
pointline delta vacuum --table trades --retention-hours 168

# Validation
pointline validate trades --exchange binance-futures --date 2024-05-01
```

### Linting
```bash
# Ruff is configured in pyproject.toml (line-length = 100)
ruff check .
ruff format .
```

## Architecture

### Data Pipeline: Bronze → Silver → Gold

**Bronze Layer** (Immutable Vendor Truth)
- Path: `/lake/bronze/{vendor}/exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/`
- Format: Raw vendor files (CSV.gz, ZIP) exactly as downloaded
- Purpose: Preserve original data with checksums for reproducibility

**Silver Layer** (Canonical Research Foundation)
- Path: `/lake/silver/{table_name}/exchange={exchange}/date={date}/`
- Format: Delta Lake (Parquet + ACID transactions)
- Compression: ZSTD
- Schema: Integer timestamps, dictionary-encoded IDs, fixed-point prices
- Key tables:
  - `dim_symbol` - SCD Type 2 symbol master (unpartitioned)
  - `trades`, `quotes`, `book_snapshot_25` - Market data events
  - `derivative_ticker` - Funding, OI, mark/index prices
  - `kline_1h` - OHLCV candles
  - `ingest_manifest` - ETL tracking ledger

**Gold Layer** (Derived Research Tables)
- Path: `/lake/gold/{table_name}/`
- Purpose: Pre-computed fast paths for specific workflows
- Status: Deferred until concrete needs identified

### Key Python Modules

**`config.py`** - Global configuration registry
- `LAKE_ROOT`, `BRONZE_ROOT`, `TABLE_PATHS`
- Exchange registry: `EXCHANGE_MAP` (exchange name → exchange_id)
- Type registry: `TYPE_MAP` (instrument type → asset_type)

**`research.py`** - Primary researcher API
- `scan_table()` - LazyFrame with partition pruning
- `read_table()` - Eager DataFrame loading
- `load_trades()`, `load_quotes()`, `load_book_snapshot_25()` - Type-safe loaders
- **Safety:** Requires explicit symbol_id + time range to prevent accidental full scans

**`registry.py` + `dim_symbol.py`** - SCD Type 2 symbol management
- `resolve_symbol()` - symbol_id → (exchange, exchange_id, exchange_symbol)
- `resolve_symbols()` - Batch resolution for partition pruning
- `find_symbol()` - Fuzzy search with filters
- **Critical:** Always resolve symbol_id upfront; don't use raw exchange symbols

**`tables/*.py`** - Domain logic per table type
- Schema definition (canonical Polars schema)
- Vendor-specific parsing (e.g., `parse_tardis_trades_csv`)
- Validation and normalization
- Fixed-point encoding: `price_int = round(price / price_increment)`
- Example: `tables/trades.py` defines `TRADES_SCHEMA`, side constants, encoding/decoding

**`services/*.py`** - ETL orchestration
- Base class: `BaseService` (validate → compute_state → write)
- Per-table services: `TradesIngestionService`, `QuotesIngestionService`, etc.
- Responsibilities: Parse bronze, quarantine checks, fixed-point encoding, Delta Lake append, manifest tracking

**`io/*.py`** - Data access layer
- Protocols: `TableRepository`, `AppendableTableRepository`, `BronzeSource`
- Implementations: `DeltaManifestRepo`, `LocalSource`
- Vendor clients: `tardis/`, `binance/`, `coingecko.py`

**`cli/*.py`** - Command-line interface
- Entry point: `pointline.cli:main()` (installed as `pointline` command)
- Nested argparse subcommands for all ETL operations

## Critical Design Principles

1. **Point-in-Time Correctness:** Always use `ts_local_us` for replay (arrival time, not exchange time); joins are as-of joins
2. **Deterministic Ordering:** `(ts_local_us, file_id, file_line_number)` ascending
3. **Immutability:** Bronze never modified; Silver is append-only for events
4. **Lineage:** Every silver row traces back to bronze file via `file_id` + `file_line_number`
5. **Symbol Resolution:** Always resolve symbol_id upfront via `dim_symbol`
6. **Fixed-Point Integers:** Keep integers until final decode to avoid floating-point errors
7. **Partition Pruning:** Require symbol_id + time range to leverage Delta Lake statistics
8. **Idempotent ETL:** Same inputs + metadata → same outputs

## Timeline Semantics

**Default replay timeline:** `ts_local_us` (arrival time), **not** `ts_exch_us` (exchange time)

**Rationale:** Live trading reacts to arrival time; using exchange time creates lookahead bias. This is a fundamental correctness requirement.

## Schema Constraints

**Delta Lake Type Limitations:**
- No `UInt16`, `UInt32` support
- Use `Int16` for `exchange_id`
- Use `Int32` for `file_id`, `file_line_number`, `flags`
- Use `Int64` for `symbol_id` (matches dim_symbol)
- `UInt8` is supported (used for `side`, `asset_type`)

**Partitioning Strategy:**
- Most tables: `exchange` + `date`
- Within partitions: Z-order by `(symbol_id, ts_local_us)` for pruning
- No partitioning: `dim_symbol`, `dim_asset_stats` (small dimension tables)

## Research Experiment Structure

**Template:** `research/03_experiments/_template/`
```
exp_YYYY-MM-DD_name/
├── README.md        # Hypothesis, method, results
├── config.yaml      # All parameters
├── queries/         # SQL or query notes
├── logs/            # JSONL run logs (one line per run)
├── results/         # Metrics, CSVs
└── plots/           # Figures
```

**Run Logging** (`logs/runs.jsonl`):
Each run appends a single JSON object with: run_id, git_commit, lake_root, symbol_ids, tables, start_ts_us, end_ts_us, ts_col, params, metrics.

**Reproducibility Requirements:**
- Symbol IDs resolved and recorded
- Timestamp column recorded (default: `ts_local_us`)
- Lake root recorded
- Git commit hash recorded
- All parameters recorded
- Metrics recorded

## Code Standards

**Type Safety:**
- Use strict Python type hints throughout
- Leverage Polars schema validation in table modules

**Error Handling:**
- Fail fast on data anomalies (e.g., crossed book, missing symbols)
- Flag issues immediately rather than silently propagating bad data

**Documentation Style:**
- Technical and precise (use industry terminology: "SCD Type 2", "Z-Ordering")
- Concise: Explain "why" and "how" without unnecessary fluff
- Example-driven for complex logic (bitwise flags, fixed-point math)

**Testing Requirements:**
- Test-driven development (TDD): Write failing tests first (Red phase)
- Minimum 80% code coverage target
- Test both success and failure cases
- Use fixtures and mocks for external dependencies
- See `tests/test_trades.py` for example test structure

## Key Entry Points

**For Data Engineering:**
- CLI parser: `pointline/cli/parser.py`
- Service templates: `pointline/services/base_service.py`
- Table schemas: `docs/schemas.md`

**For Research:**
- Research API: `pointline/research.py`
- Experiment template: `research/03_experiments/_template/`
- Symbol resolution: `pointline/registry.py`

**For Architecture Understanding:**
- Design document: `docs/architecture/design.md`
- Schema reference: `docs/schemas.md`
- Product vision: `conductor/product.md`

## Technology Stack

- **Python 3.10+** - Primary language
- **Polars** - Vectorized data processing
- **Delta Lake (delta-rs)** - Storage layer with ACID
- **Apache Parquet** - Columnar format
- **ZSTD** - Compression
- **Pytest** - Testing framework
- **Ruff** - Linting/formatting (100 char line length)
