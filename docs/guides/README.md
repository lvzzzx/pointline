# User Guides

This directory contains task-oriented guides for using Pointline effectively.

---

## 📖 Available Guides

### [Researcher's Guide](researcher-guide.md) ✅ **Comprehensive**

The complete guide for quantitative researchers. Covers:
- Quick start (5 minutes)
- Data lake layout and table catalog
- Query API (recommended for exploration)
- Core API (for production research)
- Common workflows (VWAP, joins, aggregation)
- Advanced topics (symbol resolution, fixed-point math)
- LLM agent interface

**Read this if:** You're new to Pointline or want a complete reference.

---

### [Choosing an API](choosing-an-api.md) ✅ **Decision Guide**

Helps you decide between Query API and Core API:
- Quick decision matrix
- When to use each API
- Code examples comparing both
- Migration path from Core to Query API
- Real-world decision examples

**Read this if:** You're unsure whether to use `query.trades()` or `research.load_trades()`.

---

### [Dim Asset Stats Usage](dim-asset-stats-usage.md) ✅ **Specialized**

Working with asset statistics and metadata:
- Loading and using asset statistics
- Symbol metadata queries
- Asset class filtering

**Read this if:** You need symbol metadata or asset statistics.

---

## 🚧 Guides in Development

### Tutorial: Your First Analysis (30 Minutes) 📝

**Status:** Planned
**Path:** `../tutorial.md`

End-to-end workflow from setup to results:
- Environment setup
- Getting sample data
- Data ingestion
- Discovery and exploration
- Analysis and visualization

---

### Data Ingestion Guide 📝

**Status:** Planned
**Path:** `data-ingestion.md`

How to get data into the lake:
- Bronze layer organization
- Vendor-specific workflows (Tardis, Quant360)
- Running ETL ingestion
- Validation and troubleshooting

---

### Production Workflows 📝

**Status:** Planned
**Path:** `production-workflows.md`

Reproducible research patterns:
- Experiment structure and logging
- Symbol ID tracking for reproducibility
- Run configuration management
- Git integration best practices

---

## 🎯 Quick Navigation

| I want to... | Read... |
|--------------|---------|
| Learn Pointline basics | [Researcher's Guide §2-4](researcher-guide.md#2-quick-start-5-minutes) |
| Choose between APIs | [Choosing an API](choosing-an-api.md) |
| Load trades data | [Researcher's Guide §4.2](researcher-guide.md#42-loading-data) |
| Join trades and quotes | [Researcher's Guide §6.1](researcher-guide.md#61-join-trades-with-quotes-as-of-join) |
| Production research | [Researcher's Guide §7](researcher-guide.md#7-advanced-topics-core-api) |
| Get asset metadata | [Dim Asset Stats](dim-asset-stats-usage.md) |

---

## 📚 Other Documentation

- **Getting Started:** [Quickstart](../quickstart.md) (5 minutes)
- **API Reference:** [Research API Guide](../reference/api-reference.md)
- **Architecture:** [Design Document](../architecture/design.md)
- **Examples:** [examples/](../../examples/)

---

## 💡 Contributing

Missing a guide? Want to improve existing docs?

1. Check [planned guides](#-guides-in-development) to avoid duplication
2. Follow the guide template (coming soon)
3. Submit a pull request

**Guide writing tips:**
- Start with a clear goal ("After reading this, you'll be able to...")
- Use concrete examples with real code
- Link to reference documentation for details
- Include troubleshooting section
