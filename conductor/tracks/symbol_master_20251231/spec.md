# Spec: Complete and Verify Symbol Master (dim_symbol)

## Goal
Finalize the Symbol Master implementation (`pointline/dim_symbol.py`) to ensure it strictly follows the design requirements for SCD Type 2 market data metadata. Ensure the implementation is robust, high-performance, and has excellent test coverage.

## Requirements
1.  **SCD Type 2 Correctness**:
    -   `scd2_upsert` must correctly close historical records and insert new ones.
    -   `valid_until_ts` must be treated as an exclusive upper bound.
    -   `is_current` flag must be accurately maintained.
2.  **Deterministic Hashing**:
    -   `symbol_id` must be a deterministic hash of `(exchange_id, exchange_symbol, valid_from_ts)`.
3.  **High-Performance Resolution**:
    -   Implement a helper function `resolve_symbol_ids(data_df, dim_df)` using Polars' `join_asof` to resolve the correct `symbol_id` for a given timestamp.
4.  **Schema Enforcement**:
    -   Strictly enforce the schema defined in `design.md`.
5.  **Quality Gates**:
    -   >80% test coverage (aiming for >90% for this core module).
    -   Zero linting errors using Ruff.
    -   Type safety with Python type hints.

## Implementation Details
-   `pointline/dim_symbol.py` is the primary module.
-   `tests/test_dim_symbol.py` is the primary test suite.
-   Utilize Polars for all data manipulations.
