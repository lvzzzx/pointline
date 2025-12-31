# Plan: Storage-IO Infrastructure Setup

## Phase 1: Environment & Configuration [checkpoint: a003667]
- [x] Task: Create base directory structure (`src/io/`, `src/services/`) [253a9d2]
- [x] Task: Write failing tests for `src/config.py` to verify path resolution and storage defaults (Red Phase) [6104640]
- [x] Task: Implement `src/config.py` with `LAKE_ROOT`, `TABLE_PATHS`, and storage settings (Green Phase) [6104640]
- [x] Task: Conductor - User Manual Verification 'Environment & Configuration' (Protocol in workflow.md)

## Phase 2: Repository Protocols
- [x] Task: Write failing tests/checks for `src/io/protocols.py` to ensure `TableRepository` enforces the correct interface (Red Phase) [50f421f]
- [x] Task: Implement `TableRepository` protocol in `src/io/protocols.py` (Green Phase) [50f421f]
- [ ] Task: Conductor - User Manual Verification 'Repository Protocols' (Protocol in workflow.md)

## Phase 3: Base Delta Repository Implementation
- [ ] Task: Write failing tests for `BaseDeltaRepository` covering `read_all` and `write_full` with ZSTD (Red Phase)
- [ ] Task: Implement `BaseDeltaRepository` in `src/io/base_repository.py` (Green Phase)
- [ ] Task: Conductor - User Manual Verification 'Base Delta Repository Implementation' (Protocol in workflow.md)
