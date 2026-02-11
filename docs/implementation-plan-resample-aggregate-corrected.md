# Implementation Plan: Resample-Aggregate with Spine Integration (CORRECTED)

**Status:** Proposed | **Date:** 2026-02-07 | **Scope:** 7-week implementation

Corrected 7-phase plan for PIT-safe resample-aggregate framework. Critical fixes from initial draft: (1) Explicit half-open window semantics `[T_prev, T)` for bucket assignment, (2) Separate typed callables per stage (`AggregateRawCallable` vs `ComputeFeaturesCallable`), (3) Extends existing spine system at `pointline/research/spines/`. Phases: Spine Protocol → Registry System → Bucket Assignment → Custom Aggregations → Pipeline Orchestration → Validation → Integration. Targets three production modes: `event_joined`, `tick_then_bar`, `bar_then_feature`. Full technical specification in `docs/architecture/resample-aggregate-design.md`.
