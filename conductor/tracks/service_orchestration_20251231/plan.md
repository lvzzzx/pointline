# Plan: Service Layer Orchestration (Base & DimSymbol)

## Phase 1: Base Service Infrastructure [checkpoint: 7c6c24a]
- [x] Task: Write failing tests for `BaseService` ABC to ensure it enforces the lifecycle interface (Red Phase) [1fe84c3]
- [x] Task: Implement `BaseService` in `src/services/base_service.py` (Green Phase) [1fe84c3]
- [x] Task: Conductor - User Manual Verification 'Base Service Infrastructure' (Protocol in workflow.md)

## Phase 2: DimSymbolService Core Logic [checkpoint: 76185f8]
- [x] Task: Write failing tests for `DimSymbolService` covering SCD2 orchestration, validation, and deduplication (Red Phase) [20145b6]
- [x] Task: Implement `DimSymbolService` in `src/services/dim_symbol_service.py` (Green Phase) [20145b6]
- [x] Task: Conductor - User Manual Verification 'DimSymbolService Core Logic' (Protocol in workflow.md)

## Phase 3: Resilience & Audit
- [ ] Task: Write failing tests for retry logic (simulating write conflicts) and audit logging (Red Phase)
- [ ] Task: Implement optimistic concurrency retries and audit logging in `DimSymbolService` (Green Phase)
- [ ] Task: Conductor - User Manual Verification 'Resilience & Audit' (Protocol in workflow.md)
