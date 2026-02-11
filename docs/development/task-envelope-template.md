# Task Envelope (L2)

Use this template for L2 changes: schema contracts, PIT semantics, storage/replay behavior.

Copy this file, fill it in, and resolve all sections before implementation starts.

---

## Context
<!-- What modules/tables are affected? What's the current behavior? Link to relevant docs. -->

- Affected code:
- Affected tables:
- Current behavior:
- Related docs:

## Goal
<!-- Exact expected outcome. Be specific. -->

## Constraints
<!-- Check all that apply and fill in details -->

- [ ] **PIT correctness:** <!-- How does this change affect replay semantics? -->
- [ ] **Determinism:** <!-- Same inputs must produce same outputs? -->
- [ ] **Idempotency:** <!-- Safe to re-run? -->
- [ ] **Performance:** <!-- Acceptable for full historical backfill? Bounds? -->
- [ ] **Backwards compatibility:** <!-- Any consumers affected? -->

## Acceptance
<!-- What tests and quality gates must pass before this is done? -->

- [ ]
- [ ]
- [ ]

## Rollback
<!-- What do you do if this breaks something after merge? -->

- Revert plan:
- Data recovery:
- Affected downstream:
