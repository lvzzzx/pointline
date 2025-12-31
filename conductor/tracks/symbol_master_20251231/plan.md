# Plan: Complete and Verify Symbol Master (dim_symbol)

## Phase 1: Robustness & Feature Completeness
Goal: Implement missing features and improve performance.

- [ ] Task: Implement `resolve_symbol_ids` helper using `join_asof`
    - [ ] Write failing tests for as-of join resolution
    - [ ] Implement `resolve_symbol_ids` in `src/dim_symbol.py`
    - [ ] Verify tests pass
- [ ] Task: Optimize `assign_symbol_id_hash` (avoid `map_elements`)
    - [ ] Write performance/functional tests
    - [ ] Implement vectorized hashing if possible, or use a more efficient approach
    - [ ] Verify tests pass
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Robustness & Feature Completeness' (Protocol in workflow.md) [checkpoint: ]

## Phase 2: Coverage & Quality
Goal: Reach >90% test coverage and ensure code quality.

- [ ] Task: Increase test coverage for `scd2_upsert` edge cases
    - [ ] Identify coverage gaps (e.g., empty updates, multiple symbols, simultaneous changes)
    - [ ] Write tests to cover gaps
    - [ ] Verify >90% coverage for `src/dim_symbol.py`
- [ ] Task: Run final quality checks (Ruff, type hints)
    - [ ] Fix any remaining linting or typing issues
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Coverage & Quality' (Protocol in workflow.md) [checkpoint: ]
