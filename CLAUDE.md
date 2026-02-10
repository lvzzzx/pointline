# CLAUDE.md

PIT-correct, deterministic, reproducible data for quantitative trading research — on a single machine, as fast as possible.

## Workflow Protocol

**Reference:** `docs/development/collaboration-playbook.md`

**Autonomy levels — always assess before starting:**
- **L0 (Auto):** Formatting, typos, non-semantic refactors. Just do it.
- **L1 (Guarded, default):** Code/test changes. Requires clear goal + constraints. Tests must pass.
- **L2 (Approval Required):** Schema contracts, PIT semantics, storage/replay behavior. Stop and confirm the contract with the user before writing implementation code.

**Before implementing L2 changes:**
1. State what contract is changing (schema, timestamps, ordering, partitioning)
2. Get explicit user approval on the contract
3. Only then write implementation code

**Self-review before presenting changes:**
- Ingestion: deterministic? idempotent? failure recovery?
- Schema: contract documented? breaking changes?
- Research: no lookahead bias? `ts_local_us` for replay? as-of joins?
- General: no hallucinated APIs, no over-engineering, no "just in case" code

**LLM-specific failure modes to watch for:**
- Verify every function/method call exists in the codebase
- Double-check fixed-point encoding, timestamp conversions, bitwise operations
- Don't use patterns from older code that has been refactored
- Don't add abstractions, feature flags, or error handling beyond what's needed

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

```python
from pointline import research

# 1. What exchanges have data?
exchanges = research.list_exchanges(asset_class="crypto-derivatives")

# 2. What symbols are available?
symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")

# 3. Check data coverage for a symbol
coverage = research.data_coverage("binance-futures", "BTCUSDT")

# 4. Load data
from pointline.research import query
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

### Discovery API Reference

**`research.list_exchanges(asset_class=None, active_only=True)`** — Lists exchanges. Filter by: "crypto", "crypto-spot", "crypto-derivatives", "stocks-cn".

**`research.list_symbols(exchange=None, asset_class=None, base_asset=None, search=None)`** — Lists symbols. Use `search="BTC"` for fuzzy matching.

**`research.list_tables(layer="silver")`** — Lists available tables.

**`research.data_coverage(exchange, symbol)`** — Checks what data exists. Returns dict with availability per table.

**`research.summarize_symbol(symbol, exchange=None)`** — Rich summary with metadata and coverage.

## API Selection Guide (CRITICAL FOR LLM AGENTS)

### Default: Query API (use this 90% of the time)

```python
from pointline.research import query

trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
quotes = query.quotes("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
book = query.book_snapshot_25("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
```

### Advanced: Core API (only when explicitly requested)

Use only for: explicit symbol_id control, production reproducibility, SCD Type 2 handling.

### Anti-Patterns

| Don't | Do |
|-------|-----|
| `registry.find_symbol()` + manual symbol_id extraction | `query.trades("binance-futures", "BTCUSDT", ...)` |
| `int(dt.timestamp() * 1_000_000)` manual timestamp conversion | ISO strings: `start="2024-05-01"` |
| `decode_fixed_point(trades, dim_symbol)` manual decoding | `decoded=True` parameter |
| Multi-step core API for simple queries | One-liner query API |

### Decision Tree

```
User asks to load data?
├─ Exploration/analysis/quick check? ──> query API
├─ User says "symbol_id" or "reproducibility"? ──> core API
└─ Otherwise? ──> query API
```

## Commands

### Setup
```bash
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
pre-commit install
```

### Testing
```bash
pytest                                              # all tests
pytest tests/test_trades.py                         # specific file
pytest tests/test_trades.py::test_parse_tardis_trades_csv  # specific test
```

### Linting
```bash
ruff check .        # lint
ruff format .       # format
pre-commit run --all-files  # all hooks
```

### CLI
```bash
# Symbol management
pointline symbol search BTCUSDT --exchange binance-futures

# Bronze layer
pointline bronze discover --vendor tardis --pending-only
pointline bronze discover --vendor quant360 --pending-only

# Silver layer (ETL)
pointline ingest discover --pending-only
pointline ingest run --table trades --exchange binance-futures --date 2024-05-01

# Maintenance
pointline delta optimize --table trades --partition exchange=binance-futures/date=2024-05-01
pointline delta vacuum --table trades --retention-hours 168

# Validation
pointline validate trades --exchange binance-futures --date 2024-05-01
```

## Architecture

### Data Pipeline: Bronze → Silver → Gold

**Bronze** (Immutable Vendor Truth)
- Path: `bronze/{vendor}/exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/`
- Raw vendor files (CSV.gz, ZIP) exactly as downloaded

**Silver** (Canonical Research Foundation)
- Path: `silver/{table_name}/exchange={exchange}/date={date}/`
- Delta Lake (Parquet + ACID), ZSTD compression
- Integer timestamps, dictionary-encoded IDs, fixed-point prices
- Tables: `dim_symbol`, `trades`, `quotes`, `book_snapshot_25`, `derivative_ticker`, `kline_1h`, `szse_l3_orders`, `szse_l3_ticks`, `ingest_manifest`

**Gold** (Derived Research Tables) — deferred until concrete needs identified.

### Vendor Plugin System

All vendors are self-contained plugins at `pointline/io/vendors/`.

- **Plugin Protocol:** Each vendor implements `VendorPlugin` interface
- **Capability Flags:** `supports_download`, `supports_parsers`, `supports_prehooks`
- **Runtime Dispatch:** `get_parser(vendor, data_type)` for parser lookup

Vendor types: self-download (tardis, binance_vision), external bronze (quant360), API-only (coingecko, tushare).

```python
from pointline.io.vendors import get_vendor, get_parser, list_vendors
```

Docs: [Vendor Plugin System](docs/vendor-plugin-system.md) | [Quick Reference](docs/vendor-plugin-quick-reference.md)

### Key Modules

**`config.py`** — `LAKE_ROOT`, `BRONZE_ROOT`, `TABLE_PATHS`, `EXCHANGE_MAP`, `EXCHANGE_TIMEZONES`, `TYPE_MAP`

**`research.py`** — `scan_table()`, `read_table()`, `load_trades()`, `load_quotes()`, `load_book_snapshot_25()`. Requires explicit symbol_id + time range.

**`registry.py` + `dim_symbol.py`** — SCD Type 2 symbol management. `resolve_symbol()`, `find_symbol()`. Always resolve symbol_id upfront.

**`tables/*.py`** — Schema definitions, validation, fixed-point encoding. Vendor-specific parsing lives in `io/vendors/<vendor>/parsers/`.

**`services/generic_ingestion_service.py`** — Unified vendor-agnostic ETL. Runtime parser dispatch, 12-step pipeline. Base class: `services/base_service.py`.

**`io/vendors/`** — Vendor plugins: `base.py` (protocol), `registry.py` (dispatch), `<vendor>/` (plugin + parsers + client).

**`io/*.py`** — Data access: `TableRepository`, `AppendableTableRepository`, `BronzeSource` protocols. `BaseDeltaRepository`, `DeltaManifestRepo`, `LocalSource` implementations.

**`cli/*.py`** — Entry point: `pointline.cli:main()`. Nested argparse subcommands.

## Critical Design Principles

1. **Point-in-Time Correctness:** Always use `ts_local_us` (arrival time) for replay, not `ts_exch_us`. Joins are as-of joins.
2. **Deterministic Ordering:** `(ts_local_us, file_id, file_line_number)` ascending.
3. **Immutability:** Bronze never modified; Silver is append-only for events.
4. **Lineage:** Every silver row traces to bronze via `file_id` + `file_line_number`.
5. **Symbol Resolution:** Always resolve symbol_id upfront via `dim_symbol`.
6. **Fixed-Point Integers:** Keep integers until final decode to avoid floating-point errors.
7. **Partition Pruning:** Require symbol_id + time range to leverage Delta Lake statistics.
8. **Idempotent ETL:** Same inputs + metadata → same outputs.

## Timezone Handling

- All timestamps stored in UTC microseconds (Int64): `ts_local_us`, `ts_exch_us`
- Partition `date` = exchange-local trading date:
  - **Crypto:** UTC date
  - **SZSE/SSE:** CST (Asia/Shanghai) date
  - **Future US exchanges:** ET (America/New_York) date
- Cross-exchange queries: filter by `ts_local_us`, not `date`
- Config: `EXCHANGE_TIMEZONES`, `get_exchange_timezone(exchange)` → IANA string, default "UTC"

## Schema Constraints

- Delta Lake type limitations: no `UInt16`/`UInt32`. Use `Int16` (exchange_id), `Int32` (file_id, file_line_number, flags), `Int64` (symbol_id). `UInt8` is supported (side, asset_type).
- Partitioning: `exchange` + `date`. Z-order by `(symbol_id, ts_local_us)`. Dimension tables (`dim_symbol`, `dim_asset_stats`) are unpartitioned.

## Vendor-Specific Notes

**Quant360 (SZSE L3):** Archives must be reorganized before ingestion. Use `pointline bronze discover --vendor quant360` (auto-reorganizes) or `pointline bronze reorganize` (manual). See `pointline/io/vendors/quant360/` for parsers and reorganization logic.

## Research Experiment Structure

Template: `research/03_experiments/_template/`

```
exp_YYYY-MM-DD_name/
├── README.md        # Hypothesis, method, results
├── config.yaml      # All parameters
├── queries/         # SQL or query notes
├── logs/            # JSONL run logs (one line per run)
├── results/         # Metrics, CSVs
└── plots/           # Figures
```

Reproducibility: record symbol_ids, timestamp column (`ts_local_us`), lake_root, git commit, all params, metrics.

## Code Standards

- **Type hints:** Strict Python type hints throughout. Polars schema validation in table modules.
- **Error handling:** Fail fast on data anomalies. Flag issues immediately.
- **Docs style:** Technical, concise, example-driven. Industry terminology ("SCD Type 2", "Z-Ordering").
- **Testing:** TDD (write failing tests first). 80% coverage target. Test success and failure cases. Fixtures/mocks for external dependencies.

## Key Entry Points

- **Data Engineering:** `pointline/cli/parser.py`, `pointline/services/base_service.py`, `docs/reference/schemas.md`
- **Research:** `pointline/research.py`, `research/03_experiments/_template/`, `pointline/registry.py`
- **Architecture:** `docs/architecture/design.md`, `docs/reference/schemas.md`

## Technology Stack

- **Python 3.10+**, **uv** (package management), **Polars** (data processing), **Delta Lake** (storage), **Parquet** + **ZSTD** (format/compression), **Pytest** (testing), **Ruff** (lint/format, 100 char line length)
