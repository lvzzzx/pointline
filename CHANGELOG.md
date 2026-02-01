# Changelog

All notable changes to Pointline will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-02-01

### Added

#### Core Features
- High-performance data lake with point-in-time (PIT) accuracy for quantitative research
- Support for 26+ crypto exchanges (spot and derivatives)
- Support for Chinese stocks (SZSE/SSE Level 3 order book data)
- Delta Lake storage with ACID transactions and Z-ordering
- SCD Type 2 symbol management for tracking metadata changes over time
- Fixed-point integer encoding to avoid floating-point precision errors

#### Research APIs
- **Discovery API** for data exploration:
  - `research.list_exchanges()` - Browse available exchanges
  - `research.list_symbols()` - Search symbols with filters
  - `research.data_coverage()` - Check data availability
  - `research.summarize_symbol()` - Rich symbol summaries

- **Query API** for simple, correct data loading:
  - `query.trades()` - Load trade executions
  - `query.quotes()` - Load best bid/ask
  - `query.book_snapshot_25()` - Load order book depth
  - `query.kline_1h()` - Load OHLCV candlesticks
  - `query.derivative_ticker()` - Load funding rates, OI, mark prices
  - Automatic symbol resolution and decoding
  - ISO date strings support

- **Core API** for production research:
  - `research.load_trades()` - Explicit symbol_id control
  - `research.load_quotes()` - Deterministic, reproducible loading
  - `research.scan_table()` - LazyFrame with partition pruning
  - Manual timestamp conversion and decoding

#### Data Tables
- `trades` - Trade executions with side classification
- `quotes` - Best bid/ask (Level 1)
- `book_snapshot_25` - Order book depth (25 levels)
- `kline_1h` - OHLCV candlesticks (1-hour intervals)
- `derivative_ticker` - Funding rates, open interest, mark/index prices
- `szse_l3_orders` - SZSE Level 3 order placements/cancellations
- `szse_l3_ticks` - SZSE Level 3 trade executions
- `dim_symbol` - Symbol master with SCD Type 2 tracking
- `dim_asset_stats` - Asset statistics (market cap, supply, etc.)
- `ingest_manifest` - ETL tracking ledger

#### CLI Commands
- `pointline data list-exchanges` - List exchanges with filters
- `pointline data list-symbols` - Search symbols
- `pointline data coverage` - Check data availability
- `pointline ingest discover` - Scan bronze layer
- `pointline ingest run` - Execute ETL ingestion
- `pointline manifest show` - View ETL status
- `pointline dim-symbol upsert` - Update symbol metadata
- `pointline validate` - Data quality checks
- `pointline delta optimize` - Z-ordering and compaction
- `pointline delta vacuum` - Cleanup old files

#### Claude Code Skill
- **pointline-research** skill for AI-assisted quantitative research
- Comprehensive guidance on:
  - Data discovery workflow (always check coverage first)
  - API selection (Query vs Core API decision tree)
  - Point-in-time correctness (ts_local_us vs ts_exch_us)
  - Deterministic ordering guarantees
  - Avoiding lookahead bias (as-of joins, cumulative calculations)
  - Symbol resolution workflow (SCD Type 2)
  - Fixed-point encoding/decoding

- Reference documentation:
  - **analysis_patterns.md** - Common quant patterns:
    - Spread analysis (quoted, effective, realized)
    - Volume profiling (VWAP, TWAP, distribution)
    - Order flow metrics (trade imbalance, tick rule)
    - Market microstructure (price impact, quote stability)
    - Execution quality (slippage, fill rates)

  - **best_practices.md** - Reproducibility principles:
    - Point-in-time correctness guidelines
    - Deterministic ordering
    - Symbol resolution workflow
    - Avoiding lookahead bias patterns
    - Fixed-point encoding details
    - Partition pruning optimization
    - Experiment logging standards

  - **schemas.md** - Complete table schemas:
    - All 13 table schemas with field descriptions
    - Fixed-point encoding specifications
    - Lot-based encoding (Chinese stocks)
    - Timezone handling rules
    - Common encoding patterns

#### Documentation
- [5-Minute Quickstart](docs/quickstart.md) - Quick introduction
- [Researcher's Guide](docs/guides/researcher-guide.md) - Comprehensive guide
- [Choosing an API](docs/guides/choosing-an-api.md) - Query vs Core API
- [API Reference](docs/reference/api-reference.md) - Complete API docs
- [Architecture](docs/architecture/design.md) - System design
- [Skills Integration](docs/skills-integration.md) - Claude Code skill setup
- [Shipping Guide](docs/shipping-guide.md) - Distribution strategies

#### Developer Tools
- uv-based dependency management for fast, deterministic builds
- Pre-commit hooks with ruff (formatting, linting)
- Comprehensive test suite (~400+ tests, 30+ test files)
- Git worktree support with setup guide
- GitHub Actions workflow for releases

### Design Principles
- **Point-in-Time Correctness**: Default to `ts_local_us` (arrival time) to avoid lookahead bias
- **Determinism**: Stable ordering `(ts_local_us, file_id, file_line_number)` for reproducible outputs
- **Immutability**: Bronze never modified, Silver append-only for events
- **Fixed-Point Math**: Integer encoding until final decode to avoid floating-point errors
- **Timezone Awareness**: Exchange-local partitioning (UTC for crypto, CST for SZSE/SSE)
- **Lineage Tracking**: Every row traces to bronze via `file_id` + `file_line_number`
- **Idempotent ETL**: Same inputs + metadata → same outputs

### Supported Exchanges
- **Crypto Spot**: binance, coinbase, kraken, okx, huobi, gate, bitfinex, bitstamp, gemini, crypto-com, kucoin
- **Crypto Derivatives**: binance-futures, deribit, bybit, okx-futures, bitmex, ftx, dydx
- **Chinese Stocks**: szse (SZSE), sse (SSE) with Level 3 order book data

[0.1.0]: https://github.com/your-org/pointline/releases/tag/v0.1.0
