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

### Setup
```bash
# Install dependencies
pip install -e .

# Run tests
pytest
```

## CLI
```bash
# Show pending bronze files
pointline ingest discover --pending-only

# Show manifest status counts
pointline manifest show

# Apply dim_symbol updates from a CSV or Parquet file
pointline dim-symbol upsert --file ./symbols.csv
```

## Contributing
Please review our [Product Guidelines](./conductor/product-guidelines.md) and [Workflow](./conductor/workflow.md) before contributing.
