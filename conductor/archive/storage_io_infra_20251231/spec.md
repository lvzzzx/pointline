# Specification: Storage-IO Infrastructure Setup

## Overview
Implement the foundational infrastructure for the storage-agnostic I/O architecture as defined in `docs/architecture/storage-io-design.md`. This track focuses on creating the base protocols, configuration management, and the core Delta Lake repository logic to enable a layered design (Domain -> Service -> IO Adapter).

## Functional Requirements
- **Protocol Definition:** Create `pointline/io/protocols.py` to define the `TableRepository` protocol, ensuring all future repositories adhere to a standard interface (`read_all`, `write_full`, `append`, `merge`).
- **Base Repository Implementation:** Implement `BaseDeltaRepository` in `pointline/io/base_repository.py` (or `delta_repository.py`) to encapsulate common `delta-rs` and Polars I/O operations, including `overwrite`, `append`, and `merge` behaviors.
- **Centralized Configuration:** Create `pointline/config.py` to manage:
    - Root and layer paths (Lake Root, Silver, etc.).
    - A `TABLE_PATHS` registry for table-to-path mapping.
    - Global storage settings (e.g., ZSTD compression, file formats).
- **Directory Scaffolding:** Establish the `pointline/services/` and `pointline/io/` directories to support the new architectural layers.

## Non-Functional Requirements
- **Storage Agnosticism:** The domain layer must remain pure, with all storage details isolated in the I/O layer via protocols.
- **Type Safety:** Use Python type hints and Protocols to ensure strict interface adherence.
- **Performance:** Ensure configuration defaults (like ZSTD) align with the project's HFT data requirements.

## Acceptance Criteria
- [ ] `TableRepository` protocol is defined and correctly typed.
- [ ] `BaseDeltaRepository` provides functional methods for reading and writing Delta tables using Polars.
- [ ] `pointline/config.py` is the single source of truth for paths and storage parameters.
- [ ] Infrastructure tests verify that the base repository can handle basic Polars-to-Delta roundtrips.

## Out of Scope
- Migrating `dim_symbol` or any other existing domain logic.
- Implementing table-specific services.
- Actual data ingestion or ETL pipeline runs.
