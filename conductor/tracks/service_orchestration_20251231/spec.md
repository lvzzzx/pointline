# Specification: Service Layer Orchestration (Base & DimSymbol)

## Overview
Establish the Service Layer to orchestrate data flow between the Domain logic and the I/O Adapter (Repositories). This track implements an Abstract Base Class (`BaseService`) and a concrete `DimSymbolService` to handle the lifecycle of the symbol metadata table.

## Functional Requirements
- **Base Service ABC:** Implement `src/services/base_service.py` defining the standard lifecycle:
    - `validate()`: Pre-processing checks.
    - `compute_state()`: Transformation logic.
    - `write()`: Persistence orchestration.
    - `update()`: The template method or orchestration loop.
- **DimSymbolService Implementation:**
    - **SCD2 Orchestration:** Read current state from repo, call `scd2_upsert` from domain, and write back.
    - **Validation:** Enforce schema, check for nulls in natural keys, and validate time ranges.
    - **Transaction Management:** Implement a retry mechanism (e.g., 3-5 attempts) for Delta Lake write conflicts (concurrent append/overwrite).
    - **Idempotency:** Deduplicate incoming updates by `(exchange_id, exchange_symbol, valid_from_ts)`.
    - **Update Window Checks:** Prevent application of updates older than the latest record in the table unless forced.
    - **Audit Metadata:** Log row counts, number of changed symbols, and processed time ranges.

## Non-Functional Requirements
- **Storage Agnosticism:** The service interacts with repositories only via the `TableRepository` protocol.
- **Resilience:** Handle transient I/O failures and concurrency conflicts gracefully.
- **Type Safety:** Strict use of Python type hints and Polars schemas.

## Acceptance Criteria
- [ ] `BaseService` ABC enforces the required interface for all sub-services.
- [ ] `DimSymbolService` successfully performs an end-to-end SCD2 update using a repository.
- [ ] Conflicting writes trigger the retry logic and eventually succeed or log a failure.
- [ ] Duplicate input data is correctly ignored (deduplication).
- [ ] Tests verify that the service layer correctly isolates domain logic from storage.

## Out of Scope
- Implementation of services for other tables (e.g., trades, quotes).
- Frontend or API exposure of these services.
