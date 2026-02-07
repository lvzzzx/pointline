# Quant Researcher Agent Spec (v1)

This document defines a contract-first design for an automatic `Quant Researcher` agent.

## Purpose

The agent turns a research request into a reproducible, PIT-safe experiment and returns a decision-ready report.

## Scope

In scope:
- Hypothesis-to-experiment translation
- Pointline data discovery and feasibility checks
- PIT-safe feature/label joins
- Metric computation, robustness checks, and interpretation

Out of scope:
- Live order execution
- Portfolio deployment automation
- Risk engine replacement

## Input Contract

Schema file: `schemas/quant_research_input.v1.json`

Required top-level fields:
- `schema_version`
- `request_id`
- `objective`
- `universe`
- `time_range`
- `data_requirements`
- `constraints`
- `evaluation`

## Output Contract

Schema file: `schemas/quant_research_output.v1.json`

Required top-level fields:
- `schema_version`
- `request_id`
- `run`
- `assumptions`
- `data_feasibility`
- `experiment_spec`
- `quality_gates`
- `results`
- `decision`

## Execution Lifecycle

1. Intake and normalize request.
2. Validate input schema and fail fast on contract errors.
3. Run feasibility checks:
- symbol existence
- table support/availability
- probe query row counts in target windows
4. Build experiment spec:
- hypothesis
- feature set
- PIT-safe joins (`strategy="backward"` where applicable)
- evaluation splits/metrics
5. Execute analysis and capture artifacts.
6. Run quality gates:
- lookahead checks
- deterministic ordering checks
- reproducibility hash
7. Produce output schema payload and decision (`go`/`revise`/`reject`).

## Hard Guardrails

- Use `ts_local_us` as default timeline for replay/backtest semantics.
- Treat `data_coverage()` as availability check, then confirm with probe queries.
- Separate `facts` from `interpretation` in the final report.
- Block `go` if any critical gate fails.

## Modes

- `AUTO`: classify and select HFT vs MFT behavior.
- `HFT`: prioritize microstructure, ordering fidelity, latency assumptions.
- `MFT`: prioritize PIT-safe multi-stream alignment and regime robustness.

## Suggested Storage Layout

- Input payloads: `artifacts/quant_agent/inputs/<request_id>.json`
- Output payloads: `artifacts/quant_agent/outputs/<request_id>.json`
- Optional run logs: `artifacts/quant_agent/runs/<run_id>/`

## Review Checklist

Before accepting output:
- All required schema fields are present.
- Probe query evidence exists for requested windows.
- PIT/anti-leakage checks are explicit and pass.
- Metrics include split and regime context.
- Decision rationale is actionable.
