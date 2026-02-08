# Quant Agent Design Documentation

This directory contains the complete design specification for Pointline's automated Quant Researcher agent.

## Document Index

### 1. [Quant Researcher Agent Spec](quant-researcher-agent-spec.md)
**Purpose**: Contract-first design specification
**Key content**:
- Input/output contract definitions
- Execution lifecycle (7 stages)
- Hard guardrails and safety policies
- Quality gates requirements

### 2. [Quant Agent Architecture](../architecture/quant-agent-architecture.md)
**Purpose**: System architecture and component design
**Key content**:
- Hybrid LLM + Deterministic architecture
- Stage-by-stage responsibilities
- LLM integration strategy
- Deployment and operating model
- Risk mitigations

### 3. [Feature DSL Design](feature-dsl-design.md)
**Purpose**: Declarative Feature DSL specification
**Key content**:
- Why declarative DSL is optimal for LLM agents
- Technical specification of DSL primitives
- PIT-safety enforcement mechanisms
- Integration with agent architecture
- LLM prompt templates and error handling

## Quick Start

### Understanding the Design

**Start here** if you're new to the agent design:

1. Read [Quant Researcher Agent Spec](quant-researcher-agent-spec.md) for the contract-first design philosophy
2. Review [Quant Agent Architecture](../architecture/quant-agent-architecture.md) for the system architecture
3. Study [Feature DSL Design](feature-dsl-design.md) to understand how LLM generates features safely

### Key Design Principles

**Contract-First**:
- Explicit input/output schemas (versioned JSON)
- No ambiguity in request or response format
- Enables automation and testing

**Hybrid LLM + Deterministic**:
- LLM for: Planning, hypothesis formation, interpretation
- Deterministic for: Validation, data access, metrics, quality gates
- Clear boundary prevents hallucination bugs

**Declarative Feature DSL**:
- LLM generates JSON specs (not code)
- Compiler validates PIT-safety
- Compiles to safe Polars expressions
- **Sweet spot for LLM agents**: High success rate + safety guarantees

**PIT-Safety First**:
- All features use past data only (`direction="backward"`)
- Structural validation (not runtime checks)
- Quality gates block unsafe decisions

## Schema Files

Input and output contracts are defined in JSON Schema:

- **Input schema**: `/schemas/quant_research_input.v2.json` (current), `/schemas/quant_research_input.v1.json` (legacy)
- **Output schema**: `/schemas/quant_research_output.v2.json` (current), `/schemas/quant_research_output.v1.json` (legacy)
- **Feature DSL schema**: (Embedded in [feature-dsl-design.md](feature-dsl-design.md))

## Implementation Status

### ✅ Completed
- Contract specification (input/output schemas)
- Architecture design
- Feature DSL design

### 🚧 In Progress
- None (design phase complete)

### 📋 Next Steps
1. Implement FeatureCompiler (core primitives)
2. Build agent orchestrator (7-stage pipeline)
3. Create LLM prompt templates
4. Add quality gates validation
5. Build integration tests

## Design Decisions

### Why Declarative DSL Over Code Generation?

**Considered options**:
1. **Predefined feature library** - Too limited, doesn't scale
2. **LLM generates Python code** - Unsafe, hard to validate PIT-safety
3. **Declarative DSL** ← **CHOSEN** - Best balance

**Rationale**: See [Feature DSL Design - Why This Is The Sweet Spot](feature-dsl-design.md#why-declarative-dsl-is-the-sweet-spot)

**Key insight**: LLMs excel at structured JSON generation (95%+ success rate) but struggle with precise code generation. Declarative DSL plays to LLM strengths while maintaining safety.

### Why Hybrid Architecture?

**Pure LLM approach**: Would introduce non-determinism in metrics/joins (catastrophic for quant research)

**Pure deterministic approach**: Can't adapt to natural language requests (defeats purpose of agent)

**Hybrid approach**: LLM for reasoning + deterministic for execution = best of both worlds

### Why Contract-First?

**Alternative**: Flexible, ad-hoc agent responses

**Problem**: Hard to automate, test, and integrate

**Contract-first**: Enables reliable automation, regression testing, and downstream integration (e.g., CI/CD)

## Safety Guarantees

### PIT-Correctness
- All features use `direction="backward"` only
- Structural validation at compile time
- Quality gates verify no lookahead

### Reproducibility
- Input hash recorded in output
- Symbol IDs resolved explicitly
- Deterministic execution (same input → same output)

### Decision Safety
- Quality gates block `go` decision if critical checks fail
- Facts separated from interpretation
- Every decision includes rationale and risks

## Mode-Specific Behavior

### AUTO Mode
Automatically classifies as HFT or MFT based on:
- Time horizon (< 4 hours → HFT, > 7 days → MFT)
- Data tables (L3 orderbook → HFT, multi-symbol → MFT)
- Objective keywords (latency → HFT, regime → MFT)

### HFT Mode
- Max feature window: 30 minutes
- Requires microsecond ordering
- Only backward joins allowed
- Rejects multi-hour windows

### MFT Mode
- Max feature window: 30 days
- Allows regime analysis
- Requires cross-regime robustness checks
- Supports multi-symbol cross-sectional features

## Testing Strategy

### Unit Tests
- Feature compiler (each primitive type)
- PIT-safety validation rules
- Schema validation

### Integration Tests
- End-to-end agent runs
- LLM prompt → feature spec → compilation → execution
- Error recovery and refinement loops

### Regression Tests
- Benchmark tasks (spread prediction, momentum, mean reversion)
- Metric stability across runs
- Reproducibility verification

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| LLM feature spec success rate | >90% | First attempt |
| PIT violation detection | 100% | Zero false negatives |
| Feature compilation time | <100ms | Per experiment |
| Feature coverage | >90% | Common quant features |
| End-to-end latency | <5 min | Simple experiments |

## Contributing

When extending the agent design:

1. **Maintain contract versioning**: Increment schema version for breaking changes
2. **Add new DSL primitives carefully**: Ensure PIT-safety is structurally enforceable
3. **Document mode-specific behavior**: Clarify how HFT vs MFT modes differ
4. **Update this README**: Keep the document index current

## Questions?

For design questions or feedback:
- Review existing design documents first
- Check if your question is addressed in [Feature DSL Design rationale](feature-dsl-design.md#why-declarative-dsl-is-the-sweet-spot)
- Consider opening an issue with specific architectural concerns
