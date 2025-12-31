# Initial Concept

Data lake design and ETL utilities

# Product Guide

## Target Audience
- **Data Engineers:** The primary users of this system, responsible for building, maintaining, and scaling the data infrastructure. Their focus is on the reliability, correctness, and efficiency of the data pipeline.

## Core Goals
- **Automated Data Integrity:** The system must guarantee deterministic ETL processes. It should automatically detect and alert on data gaps, vendor inconsistencies, or schema violations before they impact downstream research.
- **Storage Efficiency:** Given the high volume of HFT data, optimizing for storage footprint through efficient file formats (Parquet/Delta Lake) and compression techniques (ZSTD, integer encoding) is paramount, without sacrificing read performance.
- **Pipeline Scalability:** The architecture must be extensible, allowing for the seamless addition of new exchanges, complex data types (like options chains), and increasing data volumes without requiring significant refactoring.

## Key Features
- **Deterministic ETL Pipeline:** A robust ingestion engine that processes raw Tardis files into a normalized Silver layer with guaranteed reproducibility.
- **Automated Quality Assurance:** Built-in checks for snapshot consistency, cross-book validations, and timestamp alignment.
- **Optimized Storage Layout:** Implementation of Delta Lake with Z-Ordering and efficient partitioning strategies to support high-speed queries.
- **Extensible Schema Management:** A flexible system for managing symbol metadata (SCD Type 2) and evolving data schemas.