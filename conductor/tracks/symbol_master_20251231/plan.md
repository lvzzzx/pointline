# Plan: Complete and Verify Symbol Master (dim_symbol)

## Phase 1: Robustness & Feature Completeness [checkpoint: 20c2159]
Goal: Implement missing features and improve performance.

- [x] Task: Implement `resolve_symbol_ids` helper using `join_asof` 6fe18a4
    - [x] Write failing tests for as-of join resolution
    - [x] Implement `resolve_symbol_ids` in `src/dim_symbol.py`
    - [x] Verify tests pass
- [x] Task: Optimize `assign_symbol_id_hash` (avoid `map_elements`) f938b6a
    - [x] Write performance/functional tests
    - [x] Implement vectorized hashing if possible, or use a more efficient approach
    - [x] Verify tests pass
- [x] Task: Conductor - User Manual Verification 'Phase 1: Robustness & Feature Completeness' (Protocol in workflow.md) [checkpoint: 20c2159]

## Phase 2: Coverage & Quality
Goal: Reach >90% test coverage and ensure code quality.

- [x] Task: Increase test coverage for `scd2_upsert` edge cases 881cd10
    - [x] Identify coverage gaps (e.g., empty updates, multiple symbols, simultaneous changes)
    - [x] Write tests to cover gaps
    - [x] Verify >90% coverage for `src/dim_symbol.py`
- [ ] Task: Run final quality checks (Ruff, type hints)
    - [ ] Fix any remaining linting or typing issues
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Coverage & Quality' (Protocol in workflow.md) [checkpoint: ]
