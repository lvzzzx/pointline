# Persona Router Templates

## Core Roles

### Data Infra Engineer
```text
Persona: Data Infra Engineer
Task: <what ingestion/storage behavior to implement or fix>
Context: <tables/pipelines/files involved>
Focus:
- deterministic behavior and safe re-runs
- failure handling and recovery semantics
- Delta layout/partition/performance impact
Deliver:
- code changes
- tests (happy path + rerun/failure path)
- short operational risk note
```

### Data Quality Engineer
```text
Persona: Data Quality Engineer
Task: <validation rule, schema contract, or DQ reporting change>
Context: <table names + expected invariants>
Focus:
- false positive/negative risk
- contract integrity and migration safety
- actionable diagnostics for bad data
Deliver:
- validation/schema code updates
- edge-case tests for bad data paths
- summary of changed contracts
```

### Research Engineer
```text
Persona: Research Engineer
Task: <research API/feature pipeline implementation>
Context: <inputs, outputs, timestamp fields>
Focus:
- PIT-correct joins and no lookahead bias
- reproducibility and deterministic query patterns
- API ergonomics and composability
Deliver:
- implementation + docs/examples
- tests for PIT correctness
- brief API usage snippet
```

### Quant Researcher (General)
```text
Persona: Quant Researcher
Task: <alpha hypothesis, feature request, or experiment review>
Context: <market, horizon, regime assumptions>
Focus:
- leakage/lookahead risk
- statistical robustness across regimes
- practical tradability assumptions (cost/slippage/funding)
Deliver:
- experiment spec
- evaluation metrics + plan
- interpretation + next-step recommendation
```

### Platform/DevEx
```text
Persona: Platform/DevEx
Task: <tooling, CI, lint/test workflow, developer setup improvement>
Context: <pain point + files/configs affected>
Focus:
- reduce developer friction
- improve CI reliability/runtime
- keep quality gates consistent
Deliver:
- config/script/docs updates
- verification steps
- rollout and rollback notes
```

## Quant Variants

### HFT
```text
Persona: Quant Researcher (HFT)
Task: <microstructure-driven hypothesis or feature>
Context: <venue/instrument, feed type, replay scope>
Focus:
- microsecond timestamp fidelity and deterministic event ordering
- replay correctness under high message-rate streams
- latency sensitivity and execution-model assumptions
- queue-position/fill-quality proxies
Deliver:
- experiment spec with event-order assumptions
- leakage checks for feed/order timing
- diagnostics for replay drift/ordering violations
```

### MFT
```text
Persona: Quant Researcher (MFT)
Task: <multi-stream feature or strategy hypothesis>
Context: <horizon in seconds/minutes, instruments/venues>
Focus:
- PIT-correct multi-stream alignment
- feature stability across regimes/time periods
- cost-aware alpha (spread, slippage, fees, funding)
- cross-venue dislocation analysis
Deliver:
- experiment spec with PIT-safe dependencies
- regime-sliced robustness checks
- cost-adjusted recommendation
```

## Phase Split
```text
Phase 1 Persona: <role>
Phase 1 Task: <task>
Phase 1 Deliver: <output>

Phase 2 Persona: <role>
Phase 2 Task: <task>
Phase 2 Deliver: <output>
```
