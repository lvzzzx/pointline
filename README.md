# Pointline (HFT Research)

A high-performance, point-in-time (PIT) accurate offline data lake for Tardis market data.

## Overview
This project provides ETL utilities and a structured data lake design optimized for quantitative research. It focuses on:
- **Correctness:** Deterministic SCD Type 2 symbol management.
- **Performance:** Polars-based processing and Delta Lake storage with Z-Ordering.
- **Efficiency:** Compressed Parquet storage with integer encoding.

## Documentation
- **Architecture:** [Design Document](./docs/architecture/design.md) - Detailed data lake schema and design principles.
- **Product Guide:** [Product Vision](./conductor/product.md) - Goals and target audience.
- **Tech Stack:** [Technology Stack](./conductor/tech-stack.md) - Python, Polars, Delta Lake, DuckDB.

## Getting Started

### Prerequisites
- Python >= 3.10
- [uv](https://github.com/astral-sh/uv) (required for dependency management)

### Installation

Install uv if you haven't already:
```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or with pip
pip install uv
```

### Setup
```bash
# 1. Create virtualenv
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 2. Install dependencies
uv pip install -e ".[dev]"

# 3. Install pre-commit hooks (REQUIRED)
pre-commit install

# 4. Run tests to verify
pytest
```

**Important for Git Worktrees:** If using worktrees, run `pre-commit install` in each worktree. See [Worktree Setup Guide](./docs/development/worktree-setup.md).

## CLI
```bash
# Discover available data
pointline data list-exchanges --asset-class crypto-derivatives
pointline data list-symbols --exchange binance-futures --base-asset BTC
pointline data coverage --exchange binance-futures --symbol BTCUSDT

# Show pending bronze files
pointline ingest discover --pending-only

# Show manifest status counts
pointline manifest show

# Apply dim_symbol updates from a CSV or Parquet file
pointline dim-symbol upsert --file ./symbols.csv
```

## Quick Example: Data Discovery

```python
from pointline import research

# Step 1: What exchanges have data?
exchanges = research.list_exchanges(asset_class="crypto-derivatives")
print(exchanges)

# Step 2: What symbols are available?
symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")
print(f"Found {symbols.height} BTC symbols")

# Step 3: Check data coverage
coverage = research.data_coverage("binance-futures", "BTCUSDT")
print(f"Trades: {coverage['trades']['available']}")

# Step 4: Load data
from pointline.research import query

trades = query.trades(
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
    decoded=True,
)
print(f"Loaded {trades.height:,} trades")
```

See [examples/discovery_example.py](examples/discovery_example.py) for a complete walkthrough.

## Contributing
Please review our [Product Guidelines](./conductor/product-guidelines.md) and [Workflow](./conductor/workflow.md) before contributing.
