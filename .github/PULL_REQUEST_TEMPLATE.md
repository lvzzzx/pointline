## Summary
<!-- Brief description of what this PR does -->

## Risk Level
<!-- Pick one and delete the others -->
- [ ] **L0** — formatting, typo, non-semantic refactor
- [ ] **L1** — code/test change with clear requirements (default)
- [ ] **L2** — schema contract, PIT semantics, storage/replay behavior

## Changes
<!-- List the key changes -->
-
-

## Self-Review Checklist
<!-- Check applicable items. Delete sections that don't apply. -->

### Always
- [ ] `pytest` passes
- [ ] `ruff check` and `ruff format` clean
- [ ] Pre-commit hooks pass

### Ingestion & Storage
<!-- Delete if not applicable -->
- [ ] Deterministic: same inputs + metadata produce same outputs
- [ ] Idempotent: safe to re-run without duplicates or corruption
- [ ] Failure recovery: partial failure mid-write handled
- [ ] Data layout: partition strategy and Z-ordering still optimal

### Data Quality & Schema
<!-- Delete if not applicable -->
- [ ] Schema contract documented and matches implementation
- [ ] Validation rules cover known bad-data paths
- [ ] No downstream consumers broken by this change

### Research & PIT Correctness
<!-- Delete if not applicable -->
- [ ] No lookahead bias: uses `ts_local_us` for replay
- [ ] Joins are as-of joins, not exact joins
- [ ] Symbol resolution via `dim_symbol`, not raw exchange symbols
- [ ] Fixed-point decoded only at final output

### Signal & Feature
<!-- Delete if not applicable -->
- [ ] Signal definition is leakage-free
- [ ] Tested across multiple regimes

### LLM-Assisted
<!-- Check if any part of this PR was written by an LLM agent -->
- [ ] All API/function calls verified to exist in codebase
- [ ] Fixed-point math, timestamp conversions double-checked
- [ ] No unnecessary abstractions or "just in case" code

## Testing
- [ ] Unit tests added/updated
- [ ] All tests pass locally
- [ ] Manual testing performed (if applicable)

## Related Issues
<!-- Link to related issues/tickets, or delete -->
Closes #
