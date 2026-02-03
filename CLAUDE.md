# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Pointline is a high-performance, point-in-time (PIT) accurate offline data lake for high-frequency trading (HFT) research. The codebase provides ETL utilities and a structured data lake optimized for deterministic, reproducible quantitative research.

**Core Philosophy:**
- **PIT correctness:** Backtests reproduce what could have been known at the time (no lookahead bias)
- **Determinism:** Stable ordering guarantees reproducible ETL outputs
- **Performance:** Polars-based processing for high-performance data transformations
- **Storage efficiency:** Compressed Parquet with integer encoding

**Supported Asset Classes:**
- **Crypto:** Spot and derivatives (binance-futures, deribit, bybit, etc.)
- **Chinese Stocks:** SZSE, SSE with Level 3 order book data
- **Future:** US equities, futures, options, forex (easily extensible)

## Data Discovery (Start Here!)

**IMPORTANT:** Before querying data, ALWAYS use the discovery API to check what's available.

### Quick Discovery Workflow

```python
from pointline import research

# 1. What exchanges have data?
exchanges = research.list_exchanges(asset_class="crypto-derivatives")

# 2. What symbols are available?
symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")

# 3. Check data coverage for a symbol
coverage = research.data_coverage("binance-futures", "BTCUSDT")
print(f"Trades available: {coverage['trades']['available']}")

# 4. Load data
from pointline.research import query
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

### Discovery API Reference

**`research.list_exchanges(asset_class=None, active_only=True)`**
- Lists all exchanges with data
- Filter by asset_class: "crypto", "crypto-spot", "crypto-derivatives", "stocks-cn", or list
- Returns DataFrame with: exchange, exchange_id, asset_class, description, is_active

**`research.list_symbols(exchange=None, asset_class=None, base_asset=None, search=None)`**
- Lists symbols with flexible filters
- Use `search="BTC"` for fuzzy matching
- Returns DataFrame with symbol metadata

**`research.list_tables(layer="silver")`**
- Lists available tables
- Returns DataFrame with: table_name, layer, has_date_partition, description

**`research.data_coverage(exchange, symbol)`**
- Checks what data exists for a symbol
- Returns dict: `{"trades": {"available": True, "symbol_id": 12345}, ...}`

**`research.summarize_symbol(symbol, exchange=None)`**
- Prints rich summary with metadata and coverage
- Use when user asks "tell me about BTCUSDT"

## API Selection Guide (CRITICAL FOR LLM AGENTS)

### Default Workflow: Query API

**ALWAYS use the query API for exploration, analysis, and user questions:**

```python
from pointline.research import query

# One-liner - automatic symbol resolution + decoding
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
book = query.book_snapshot_25("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

**When user asks:** "Show me BTC trades on Binance"
- ✅ **Correct:** Use `query.trades()` directly
- ❌ **Incorrect:** Multi-step workflow with `registry.find_symbol()` + manual extraction

### Advanced Workflow: Core API

**ONLY use when user explicitly requests:**
- Production research requiring reproducibility
- Explicit symbol_id control
- Performance-critical queries with custom optimization
- Handling SCD Type 2 symbol changes explicitly

### Anti-Patterns for LLM Agents

#### ❌ DON'T: Use core API for simple queries

```python
# ❌ BAD - Unnecessary complexity
from pointline import registry, research

symbols = registry.find_symbol("BTCUSDT", exchange="binance-futures")
symbol_id = symbols["symbol_id"][0]  # Manual extraction
trades = research.load_trades(
    symbol_id=symbol_id,
    start_ts_us=1714521600000000,
    end_ts_us=1714608000000000,
)
```

#### ✅ DO: Use query API

```python
# ✅ GOOD - Simple and correct
from pointline.research import query

trades = query.trades(
    "binance-futures",
    "BTCUSDT",
    "2024-05-01",
    "2024-05-02",
    decoded=True,
)
```

#### ❌ DON'T: Manually convert timestamps

```python
# ❌ BAD - Error-prone
from datetime import datetime, timezone

start = datetime(2024, 5, 1, tzinfo=timezone.utc)
start_ts_us = int(start.timestamp() * 1_000_000)  # Easy to mess up
```

#### ✅ DO: Use ISO strings or datetime objects directly

```python
# ✅ GOOD - ISO string (simplest)
trades = query.trades(..., start="2024-05-01", end="2024-05-02")

# ✅ GOOD - datetime object (query API accepts both)
from datetime import datetime, timezone
trades = query.trades(
    ...,
    start=datetime(2024, 5, 1, tzinfo=timezone.utc),
    end=datetime(2024, 5, 2, tzinfo=timezone.utc),
)
```

#### ❌ DON'T: Manually decode fixed-point

```python
# ❌ BAD - Verbose and unnecessary
trades = research.load_trades(...)
from pointline.tables.trades import decode_fixed_point
from pointline.dim_symbol import read_dim_symbol_table

dim_symbol = read_dim_symbol_table()
trades = decode_fixed_point(trades, dim_symbol)
```

#### ✅ DO: Use decoded=True parameter

```python
# ✅ GOOD - Automatic decoding
trades = query.trades(..., decoded=True)
```

### Decision Tree for LLM Agents

```
User asks to load data?
│
├─ Is this exploration/analysis/quick check? ──> Use query API
│   └─ query.trades(..., decoded=True)
│
├─ Is this production research? ──> Ask if they need explicit symbol_id control
│   ├─ Yes ──> Use core API
│   │   └─ research.load_trades(symbol_id=..., start_ts_us=..., end_ts_us=...)
│   └─ No ──> Use query API
│
└─ User explicitly mentions "symbol_id" or "reproducibility"?
    └─ Yes ──> Use core API
```

**Summary for agents:**
- **Default:** Query API (90% of use cases)
- **Advanced:** Core API (10% of use cases, when explicitly needed)
- **Never:** Multi-step workflows when query API exists

## Commands

### Setup
```bash
# Install Python dependencies with uv (required)
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e ".[dev]"

# IMPORTANT: Install pre-commit hooks for your worktree
# (Required when creating/switching worktrees)
pre-commit install

# Verify pre-commit is working
pre-commit run --all-files
```

**Note for Git Worktrees:** You MUST run `pre-commit install` in each worktree. See [Worktree Setup Guide](docs/development/worktree-setup.md) for details.

**Why uv?** This project uses [uv](https://github.com/astral-sh/uv) for fast, deterministic dependency management. The `uv.lock` file ensures reproducible builds across all environments.

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
# Simple: just specify vendor
pointline bronze discover --vendor quant360 --pending-only
pointline bronze discover --vendor tardis --pending-only

# Or explicitly specify bronze-root (for non-standard layouts)
pointline bronze reorganize --source-dir ~/archives --bronze-root ~/data/lake/bronze

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

# Pre-commit hooks (recommended for development)
# Install pre-commit hooks (one-time setup)
pip install pre-commit
pre-commit install

# Hooks will now run automatically on git commit
# To run manually on all files:
pre-commit run --all-files

# To skip hooks for a specific commit (use sparingly):
git commit --no-verify
```

## Architecture

### Data Pipeline: Bronze → Silver → Gold

**Bronze Layer** (Immutable Vendor Truth)
- Path: `/lake/bronze/{vendor}/exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/`
- Format: Raw vendor files (CSV.gz, ZIP) exactly as downloaded
- Purpose: Preserve original data with checksums for reproducibility
- **Vendor-specific preprocessing:**
  - **Tardis**: Direct downloads already in Hive-partitioned format
  - **Quant360**: Requires reorganization from `.7z` archives → use `pointline bronze reorganize`
  - Archives (`.7z`, `.zip`) must be reorganized before ingestion

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
  - `szse_l3_orders`, `szse_l3_ticks` - SZSE Level 3 order book events
  - `ingest_manifest` - ETL tracking ledger

**Gold Layer** (Derived Research Tables)
- Path: `/lake/gold/{table_name}/`
- Purpose: Pre-computed fast paths for specific workflows
- Status: Deferred until concrete needs identified

### Key Python Modules

**`config.py`** - Global configuration registry
- `LAKE_ROOT`, `BRONZE_ROOT`, `TABLE_PATHS`
- Exchange registry: `EXCHANGE_MAP` (exchange name → exchange_id)
- Exchange timezone registry: `EXCHANGE_TIMEZONES` (exchange → IANA timezone)
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
- Fixed-point encoding: `px_int = round(price / price_increment)`
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

## Timezone Handling

**Timestamp Storage:**
- All timestamps stored in UTC (`ts_local_us`, `ts_exch_us`)
- Microsecond precision (Int64)

**Partition Date Semantics:** Exchange-local trading date
- **Crypto (24/7):** `date` = UTC date from `ts_local_us`
  - Example: binance-futures, coinbase, okx
- **SZSE/SSE:** `date` = CST (Asia/Shanghai) date from `ts_local_us`
  - Example: 2024-09-30 00:30 CST → date=2024-09-30 (not 2024-09-29)
- **Future US exchanges:** `date` = ET (America/New_York) date

**Rationale:**
- Ensures one trading day = one partition for efficient queries
- Researchers query by trading day, not UTC day
- Aligns with bronze layer structure (bronze already uses exchange-local dates)

**Cross-Exchange Queries:**
- Use `ts_local_us` for precise timestamp filtering across exchanges
- Do not filter by `date` across exchanges with different timezones
- Each exchange partition has its own timezone semantics

**Configuration:**
- Timezone registry: `pointline.config.EXCHANGE_TIMEZONES`
- Lookup function: `get_exchange_timezone(exchange)` → IANA timezone string
- Default: "UTC" for unlisted exchanges
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

## Vendor-Specific Workflows

### Quant360 (SZSE Level 3 Data)

**Data Source:** Chinese stock exchanges (SZSE, SSE) Level 3 market data
- Individual order placements (`l3_orders`)
- Trade executions and cancellations (`l3_ticks`)

**Bronze Reorganization (Required Pre-ingestion Step):**
```bash
# Quant360 delivers data as monolithic .7z archives (one archive per date/type)
# Archives contain ~3000 per-symbol CSV files that must be reorganized

# Automatic (default): prehook auto-detects and reorganizes during discovery
pointline bronze discover --vendor quant360 --pending-only

# Manual (for batch operations or debugging):
pointline bronze reorganize \
  --source-dir ~/data/archives/quant360 \
  --bronze-root ~/data/lake/bronze \
  --vendor quant360

# Transforms: order_new_STK_SZ_20240930.7z
# Into: bronze/quant360/exchange=szse/type=l3_orders/date=2024-09-30/symbol={XXXXXX}/{XXXXXX}.csv.gz
```

**Ingestion Workflow:**
```bash
# Step 1: Discover reorganized files (prehook auto-reorganizes if needed)
pointline bronze discover --vendor quant360 --pending-only

# Step 2: Ingest to silver tables
pointline ingest run --table l3_orders --exchange szse --date 2024-09-30
pointline ingest run --table l3_ticks --exchange szse --date 2024-09-30

# Step 3: Validate (optional)
pointline validate l3_orders --exchange szse --date 2024-09-30
```

**Key Implementation Files:**
- `pointline/io/vendor/quant360/reorganize.py` - Python reorganization utilities
- `scripts/reorganize_quant360.sh` - Fast bash reorganization (preferred)
- `pointline/tables/szse_l3_orders.py` - Order schema and parsing
- `pointline/tables/szse_l3_ticks.py` - Tick schema and parsing
- `pointline/services/szse_l3_orders_service.py` - Order ingestion service
- `pointline/services/szse_l3_ticks_service.py` - Tick ingestion service

**Schema Specifics:**
- Timestamps: Asia/Shanghai (CST) → UTC conversion via `parse_quant360_timestamp()`
- Fixed-point encoding: Lot-based (100 shares/lot) for Chinese A-shares
- Side/Type remapping: Quant360 uses 1/2 codes → remapped to 0/1 for consistency
- Tick semantics: Fills (price>0) vs Cancellations (price=0)

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
- **uv** - Fast Python package installer and resolver (required)
- **Polars** - Vectorized data processing
- **Delta Lake (delta-rs)** - Storage layer with ACID
- **Apache Parquet** - Columnar format
- **ZSTD** - Compression
- **Pytest** - Testing framework
- **Ruff** - Linting/formatting (100 char line length)
