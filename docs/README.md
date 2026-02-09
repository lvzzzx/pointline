# Pointline Documentation

Welcome to Pointline, a high-performance data lake for HFT research. This guide will help you find what you need.

---

## 🚀 Getting Started (New Users)

**Never used Pointline before?** Follow this path:

1. **[5-Minute Quickstart](quickstart.md)** - Install, discover data, run your first analysis
2. **[30-Minute Tutorial](tutorial.md)** - Complete end-to-end workflow ✨ **NEW**
3. **[Researcher's Guide](guides/researcher-guide.md)** - Complete guide with common workflows
4. **[Troubleshooting](troubleshooting.md)** - Common errors and solutions ✨ **NEW**

**Quick example:**
```python
from pointline import research
from pointline.research import query

# Discover what's available
coverage = research.data_coverage("binance-futures", "BTCUSDT")

# Load and analyze
trades = query.trades(
    "binance-futures", "BTCUSDT",
    "2024-05-01", "2024-05-02",
    decoded=True
)
```

---

## ✅ uv Dev Workflow (Recommended)

Pointline uses `uv` for fast, deterministic dependency management. For a full dev setup:

```bash
cd /path/to/pointline
uv sync --all-extras
source .venv/bin/activate
pre-commit install
```

Run tests with:

```bash
pytest
```

---

## 📖 User Guides

**Want to accomplish something specific?** See [guides/](guides/):

| Guide | Description |
|-------|-------------|
| [Researcher's Guide](guides/researcher-guide.md) | Comprehensive guide for quant researchers |
| [Choosing an API](guides/choosing-an-api.md) | Query API vs Core API decision guide |
| [Dim Asset Stats Usage](guides/dim-asset-stats-usage.md) | Working with asset statistics |
| [Feature Engineering (MFT)](guides/feature-engineering.md) | PIT-correct feature framework for MFT workflows |
| [Research Pipeline v2](guides/research-pipeline-v2.md) | Contract-first `research.pipeline(request)` for production feature/resample/aggregate runs |
| [Hybrid Workflow v2](guides/research-workflow-hybrid.md) | Multi-stage composition across `tick_then_bar`, `bar_then_feature`, and `event_joined` |
| [Feature Pipeline Modes](guides/feature-pipeline-modes.md) | Decision matrix and PIT-safe templates for event-joined, tick-then-bar, and bar-then-feature workflows |
| [Roles & Responsibilities](roles-and-responsibilities.md) | Ownership boundaries, review routing, and handoff contracts |
| [Persona Prompt Templates](persona-prompts.md) | Copy-paste prompts for role-based LLM execution, including HFT/MFT variants |

Production note: `research.pipeline(...)` and `research.workflow(...)` are the canonical governed execution paths. Legacy helpers under `pointline.research.features` remain importable but are non-canonical for production runs.

**Common tasks:**
- Load market data → [Quickstart](quickstart.md)
- Join trades with quotes → [Researcher's Guide §6.1](guides/researcher-guide.md#61-join-trades-with-quotes-as-of-join)
- Calculate VWAP → [Researcher's Guide §6.2](guides/researcher-guide.md#62-calculate-vwap)
- Production research → [Researcher's Guide §7](guides/researcher-guide.md#7-advanced-topics-core-api)

---

## 📚 Reference Documentation

**Looking up specific details?** See [reference/](reference/):

| Reference | Description |
|-----------|-------------|
| [Research API Guide](reference/api-reference.md) | Complete API reference with examples |
| [CLI Reference](reference/cli-reference.md) | All CLI commands with examples ✨ **NEW** |
| [Schemas](reference/schemas.md) | Table structures, data types, and field definitions |
| [Data Sources](data_sources/quant360_szse_l2.md) | Vendor-specific documentation |

**CLI Reference:**
```bash
# Discovery
pointline data list-exchanges
pointline data list-symbols --exchange binance-futures
pointline data coverage --exchange binance-futures --symbol BTCUSDT

# Data management
pointline ingest discover --pending-only
pointline manifest show

# Symbol management
pointline dim-symbol upsert --file ./symbols.csv
```

---

## 🏗️ Advanced Topics

**Experienced users and system designers:**

| Topic | Description |
|-------|-------------|
| [Architecture](architecture/design.md) | Data lake design principles and schema |
| [Performance](architecture/performance-considerations.md) | Query optimization and best practices |
| [Storage & I/O](architecture/storage-io-design.md) | Bronze/Silver/Gold layer design |
| [North-Star Research Architecture](architecture/north-star-research-architecture.md) | One-page target architecture for v2 contract-first pipeline/workflow, governance gates, and milestones |
| [Bronze Prehooks](architecture/bronze-prehooks-design.md) | ETL preprocessing pipeline |
| [Resample & Aggregate Design](architecture/resample-aggregate-design.md) | Contract-first architecture for PIT-safe resampling, typed/custom aggregation, and mode-aware pipelines |
| [Quant Researcher Agent Spec](agents/quant-researcher-agent-spec.md) | Contract-first plan and I/O schemas for automated quant research runs |

---

## 👥 Contributing & Development

**Want to contribute or develop locally?** See [development/](development/):

| Guide | Description |
|-------|-------------|
| [Development Setup](development/README.md) | Getting started with development *(coming soon)* |
| [Worktree Setup](development/worktree-setup.md) | Git worktree workflow for parallel development |
| [CI/CD](development/ci-cd.md) | Continuous integration and deployment |

---

## 🔍 Finding What You Need

**Not sure where to look?**

| I want to... | Go to... |
|--------------|----------|
| Get started quickly | [Quickstart](quickstart.md) |
| Learn the concepts | [Researcher's Guide](guides/researcher-guide.md) |
| Look up an API function | [Research API Guide](reference/api-reference.md) |
| Fix an error | [Troubleshooting](troubleshooting.md) *(coming soon)* |
| Understand the design | [Architecture](architecture/design.md) |
| Contribute code | [Development](development/README.md) *(coming soon)* |

---

## 📦 Examples

**Want to see code?**

- [examples/discovery_example.py](../examples/discovery_example.py) - Data discovery workflow
- [examples/query_api_example.py](../examples/query_api_example.py) - Complete query API usage

---

## 🆘 Help & Support

**Still stuck?**

1. Check [Troubleshooting](troubleshooting.md) *(coming soon)*
2. Review [Common Workflows](guides/researcher-guide.md#6-common-workflows)
3. Open an issue on GitHub

---

## 📋 Documentation Status

| Status | Meaning |
|--------|---------|
| ✅ | Complete and up-to-date |
| 🚧 | In progress |
| 📝 | Planned |

Current status:
- ✅ Quickstart
- ✅ Researcher's Guide
- ✅ API Reference
- ✅ Architecture docs
- 📝 Tutorial (30-minute end-to-end)
- 📝 Troubleshooting guide
- 📝 CLI Reference
- 📝 Data Ingestion guide
