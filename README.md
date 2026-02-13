# Pointline (HFT Research)

A high-performance, point-in-time (PIT) accurate offline data lake for Tardis market data.

## Overview
This project provides ETL utilities and a structured data lake design optimized for quantitative research. It focuses on:
- **Correctness:** Deterministic SCD Type 2 symbol management.
- **Performance:** Polars-based processing and Delta Lake storage with Z-Ordering.
- **Efficiency:** Compressed Parquet storage with integer encoding.

## Documentation

📖 **[Full Documentation Hub](docs/README.md)** ← Complete navigation guide

**Quick links:**
- **[5-Minute Quickstart](docs/quickstart.md)** ← Start here for new users
- **[Researcher's Guide](docs/guides/researcher-guide.md)** - Comprehensive guide
- **[Choosing an API](docs/guides/choosing-an-api.md)** - Query API vs Core API
- **[API Reference](docs/reference/api-reference.md)** - Complete API documentation
- **[Architecture](docs/architecture/design.md)** - System design

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

### Claude Code Skill (Optional)

Pointline includes a Claude Code skill that enables AI-assisted quantitative research with:
- 📊 **Data Discovery** - Automatic exploration of exchanges, symbols, and coverage
- 🔍 **Query API Guidance** - Correct data loading with automatic symbol resolution
- ✅ **PIT Correctness** - Point-in-time accuracy and reproducibility guarantees
- 📈 **Analysis Patterns** - Common quant patterns (spreads, VWAP, order flow, market microstructure)
- 📚 **Best Practices** - Deterministic ordering, avoiding lookahead bias

**Install the skill:**

```bash
# Option 1: Build and install (recommended)
cd skills
./build.sh
cp pointline-research.skill ~/.claude/skills/

# Option 2: Symlink for live editing (development)
ln -s $(pwd)/pointline-research ~/.claude/skills/pointline-research
```

**Usage example with Claude Code:**
```
You: "Load BTC trades on Binance Futures for May 1, 2024 and calculate VWAP"

Claude: [Uses the skill to automatically]
1. Discover available symbols (discover_symbols)
2. Load trades with load_events(TRADES, exchange, symbol, start, end)
3. Apply PIT-correct VWAP calculation (cumulative, ts_event_us ordering)
4. Ensure reproducibility (deterministic ordering)
```

**Documentation:**
- [Skills Integration Guide](docs/skills-integration.md) - Setup and usage
- [Shipping Guide](docs/shipping-guide.md) - Distribution strategies

## Quick Example: Data Discovery

```python
from pathlib import Path
from pointline.research import discover_symbols, load_events
from pointline import TRADES

silver_root = Path("./data/silver")

# Step 1: Discover available symbols
symbols = discover_symbols(
    silver_root=silver_root,
    exchange="binance-futures",
    q="BTC",  # search filter
    limit=10,
)
print(f"Found symbols: {symbols['exchange_symbol'].to_list()}")

# Step 2: Load trades for a symbol
trades = load_events(
    silver_root=silver_root,
    table_spec=TRADES,
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-02",
)
print(f"Loaded {len(trades):,} trades")
```

See [examples/](examples/) for more complete walkthroughs.

**API Structure:**
```python
# Core schemas
from pointline import TRADES, QUOTES, ORDERBOOK_UPDATES, get_table_spec

# Research API
from pointline.research import (
    discover_symbols,      # Symbol discovery
    load_events,           # Load trades/quotes/orderbook
    build_spine,           # Build research spines
    load_symbol_meta,      # Symbol metadata
)

# Ingestion (for ETL pipelines)
from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata
```

## Contributing
Please review our [Collaboration Playbook](./docs/development/collaboration-playbook.md) before contributing.
