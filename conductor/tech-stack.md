# Tech Stack

## Core Technologies
- **Python (>=3.10):** The primary programming language for ETL pipelines and researcher utilities.
- **Polars:** Used for high-performance, vectorized data processing and manipulation.
- **Delta Lake (via `delta-rs`):** The canonical storage layer, providing ACID transactions, scalable metadata handling, and time-travel capabilities.
- **Apache Parquet:** The underlying columnar storage format for efficient data storage and retrieval.

## Storage & Compression
- **ZSTD:** The default compression algorithm for Parquet files, balancing compression ratio and speed.
- **Deterministic Partitioning:** Data is partitioned by `exchange` and `date` to optimize directory structures.
- **Z-Ordering:** Applied within Delta Lake partitions to cluster data by `symbol_id`.

## Analysis & Research
- **DuckDB:** Recommended for ad-hoc SQL analysis over the Parquet/Delta Lake files.

## Infrastructure & Tooling
- **Maturin:** Used as the build system and for packaging the Rust replay engine extension in the wheel.
- **Pytest:** The primary framework for unit and integration testing.
- **Ruff:** Used for lightning-fast linting and code formatting to maintain code quality.

## Performance Extensions
- **Rust:** High-performance order book replay engine for full-depth L2 reconstruction.
- **PyO3:** Python bindings for the Rust replay engine to expose it to researchers.

Rationale: Full-depth L2 replay is CPU-intensive and benefits from a native core while preserving
the existing Python researcher interface, and the build uses a single wheel to ship the extension.
