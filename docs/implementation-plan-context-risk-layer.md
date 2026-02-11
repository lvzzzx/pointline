# Implementation Plan: Context/Risk Layer (`oi_capacity` first)

**Status:** ✅ Implemented | **Scope:** 7-commit series (completed)

Implementation plan for high-level context/risk layer in research framework. Pipeline: `aggregate → context/risk → labels → evaluation/gates`. First plugin: `oi_capacity` for Open Interest capacity controls with PIT-safe rolling semantics. 7-commit series: (1) Contracts+Schema, (2) Context Package+Registry, (3) `oi_capacity` Plugin, (4) Pipeline/Workflow Integration, (5) Gates+Decision Integration, (6) Docs+Templates, (7) CI Governance Lock. Outputs: OI notional, level ratio, capacity OK flag, capacity multiplier, max trade notional. Registry-governed with mode validation and deterministic guarantees.
