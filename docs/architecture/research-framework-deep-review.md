# Research Framework Deep Review

**Date**: 2026-02-09
**Reviewer**: Architecture Analysis
**Scope**: Four-layer architecture of `pointline/research`

---

## Executive Summary

The Pointline research framework implements a **well-architected, contract-first system** for quantitative trading research with strong fundamentals around point-in-time (PIT) correctness, determinism, and reproducibility. The framework successfully balances accessibility (query API) with rigor (pipeline API) while maintaining clean separation of concerns across four architectural layers.

### Overall Assessment: **7.8/10** - Production-Ready

**Strengths**:
- ✅ Clean four-layer architecture with clear responsibilities
- ✅ PIT correctness enforced at multiple levels
- ✅ Registry-based extensibility (operators, spines, rollups)
- ✅ Automatic quality gates with reproducibility verification
- ✅ Comprehensive documentation and examples

**Weaknesses**:
- ⚠️ Limited observability (no execution tracing)
- ⚠️ No typed contracts between layers
- ⚠️ Single-threaded execution only
- ⚠️ No incremental execution support

---

## Table of Contents

1. [Layer 1: Contract Layer](#layer-1-contract-layer)
2. [Layer 2: Compile Layer](#layer-2-compile-layer)
3. [Layer 3: Execute Layer](#layer-3-execute-layer)
4. [Layer 4: Governance Layer](#layer-4-governance-layer)
5. [Cross-Layer Analysis](#cross-layer-analysis)
6. [Final Recommendations](#final-recommendations)

---

# Layer 1: Contract Layer

## Overview

**Location**: `pointline/research/contracts.py`, `schemas/*.json`
**Responsibility**: Schema validation and normalization
**Lines of Code**: ~160 (Python) + JSON schemas
**Score**: **8.0/10**

## Architecture

### Core Components

```python
# contracts.py structure
├── load_schema(schema_filename)                      # JSON Schema loader
├── validate_against_schema(payload, schema_filename) # Validation engine
├── validate_quant_research_input_v2()                # Input validator
├── validate_quant_research_output_v2()               # Output validator
├── validate_quant_research_workflow_input_v2()       # Workflow input
└── validate_quant_research_workflow_output_v2()      # Workflow output
```

### Design Patterns

#### 1. Dual Validation Strategy ✅ **Excellent**

```python
if Draft202012Validator is not None:
    # Use jsonschema library (preferred)
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(payload), key=lambda e: list(e.path))
else:
    # Fallback validator (no dependencies)
    _validate_node(payload, schema, "<root>")
```

**Strength**: Graceful degradation when `jsonschema` unavailable
**Benefit**: Zero runtime dependencies for validation in restricted environments

#### 2. Schema Versioning ✅ **Production-Ready**

```
schemas/
├── quant_research_input.v1.json
├── quant_research_input.v2.json      # Active
├── quant_research_output.v1.json
└── quant_research_output.v2.json     # Active
```

**Pattern**: Can run v1 and v2 pipelines side-by-side during migration

#### 3. Error Message Quality ✅ **Developer-Friendly**

```python
rendered = []
for err in errors:
    loc = ".".join(str(part) for part in err.path) if err.path else "<root>"
    rendered.append(f"{loc}: {err.message}")
raise SchemaValidationError("; ".join(rendered))
```

**Example error**:
```
sources.0.inline_rows: missing required key 'ts_local_us'
operators.1.agg: expected one of ['trade_vwap', 'ohlcv', 'spread_stats']
```

## Schema Contract Structure

### Input Schema v2

```json
{
  "schema_version": "2.0",
  "request_id": "uuid",
  "mode": "bar_then_feature | tick_then_bar | event_joined",
  "timeline": {
    "start": "2024-05-01T00:00:00Z",
    "end": "2024-05-02T00:00:00Z",
    "ts_col": "ts_local_us"
  },
  "sources": [...],
  "spine": {...},
  "operators": [...],
  "labels": [...],
  "evaluation": {
    "metrics": [...]
  },
  "constraints": {
    "forbid_lookahead": true,
    "require_pit_ordering": true
  },
  "artifacts": {
    "emit_lineage": true,
    "output_dir": "/path/to/outputs"
  }
}
```

### Output Schema v2

```json
{
  "schema_version": "2.0",
  "request_id": "uuid",
  "run": {
    "run_id": "uuid",
    "started_at": "ISO8601",
    "completed_at": "ISO8601",
    "status": "success | failed"
  },
  "resolved_plan": {...},
  "data_feasibility": {
    "coverage_checks": [...],
    "probe_checks": [...]
  },
  "quality_gates": {
    "passed_gates": [...],
    "failed_gates": [...],
    "gate_results": {...}
  },
  "results": {
    "row_count": 1000,
    "columns": [...],
    "metrics": {...},
    "preview": [...]
  },
  "decision": {
    "status": "accept | reject | warn",
    "reasons": [...]
  },
  "artifacts": {
    "config_hash": "sha256",
    "paths": [...],
    "gate_metrics": {...}
  }
}
```

## Strengths 💪

### 1. Strict Type Safety ✅

- JSON Schema enforces types at API boundary
- No implicit coercion
- Prevents runtime type errors deep in execution

### 2. Reproducibility by Design ✅

```python
"artifacts": {
    "config_hash": "sha256(sorted(request))",  # Deterministic hash
    "paths": [...]
}
```

**Benefit**: Same `config_hash` → same research outputs

### 3. Mode Isolation ✅

```json
"mode": {
  "enum": ["bar_then_feature", "tick_then_bar", "event_joined"]
}
```

Each mode has distinct operator allowlists → compile-time validation

### 4. Constraints as First-Class Citizens ✅

```json
"constraints": {
  "forbid_lookahead": true,        // Hard gate
  "require_pit_ordering": true,    // Hard gate
  "max_unassigned_ratio": 0.01     // Soft gate (warn)
}
```

## Weaknesses ⚠️

### 1. Schema Verbosity ⚠️ **Usability Issue**

**Problem**: JSON contracts are verbose (200+ lines for simple queries)

**Impact**: High barrier to entry for new users

**Solution**: Add Python builder API:

```python
from pointline.research import PipelineRequestBuilder

request = (
    PipelineRequestBuilder()
    .mode("bar_then_feature")
    .timeline("2024-05-01", "2024-05-02", "ts_local_us")
    .source("trades", exchange="binance-futures", symbol="BTCUSDT")
    .spine(type="clock", step_ms=1000)
    .operator("vwap", stage="aggregate", agg="trade_vwap")
    .label("forward_return", window="5s")
    .gate("forbid_lookahead", True)
    .build()
)

output = research.pipeline(request)
```

### 2. No Schema Evolution Strategy ⚠️ **Technical Debt**

**Current approach**: Versioned schemas (v1, v2, v3...)

**Missing**:
- Migration path documentation
- Deprecation timeline
- Schema compatibility matrix

**Recommendation**:

```python
# Add migration helper
from pointline.research.contracts import migrate_request

v1_request = {...}
v2_request = migrate_request(v1_request, from_version="1.0", to_version="2.0")
```

### 3. Inline Data Limitations ❌ **Design Flaw**

**Current**:
```json
"sources": [{
  "name": "trades_src",
  "inline_rows": [
    {"ts_local_us": 100, "px_int": 50000, ...},
    {"ts_local_us": 101, "px_int": 50001, ...}
  ]
}]
```

**Problems**:
- JSON size explodes for >1000 rows
- No compression
- No streaming support

**Better design**:

```json
"sources": [{
  "name": "trades_src",
  "type": "file",
  "path": "/tmp/trades.parquet",
  "format": "parquet"
}]
```

**Recommendation**: Deprecate `inline_rows` for production; keep for tests only

### 4. Missing: Partial Validation ⚠️ **Developer Experience**

**Problem**: Validation is all-or-nothing

**Desired**: Validate incrementally during construction

```python
builder = PipelineRequestBuilder()
builder.mode("bar_then_feature")  # ✓ Valid mode
builder.timeline(start="invalid")  # ✗ Validation error immediately
```

**Benefit**: Fail-fast during request construction, not at pipeline execution

## Integration Points 🔗

### Input: User/Agent → Contract Layer

```python
# From JSON file
with open("request.json") as f:
    request = json.load(f)

# Validate
validate_quant_research_input_v2(request)
```

### Output: Contract Layer → Compile Layer

```python
# Contracts pass through unchanged
compiled = compile_request(request)
```

**Observation**: No object model transformation
**Pro**: Simple, no serialization overhead
**Con**: No IDE autocomplete, type checking happens at runtime

## Test Coverage 🧪

### Strengths ✅
- Parametrized fixtures: `input_v2_bar_then_feature.json`
- Roundtrip tests: Validate → execute → validate output
- Error path tests: Invalid schemas raise `SchemaValidationError`

### Gaps ⚠️
- **Missing**: Schema evolution tests (v1 → v2 compatibility)
- **Missing**: Performance tests (large payloads, 10k+ operators)
- **Missing**: Fuzzing tests (random invalid schemas)

## Recommendations 📋

### High Priority
1. **Add Python Builder API** for programmatic request construction
2. **Document migration path** (v1 → v2 → v3)
3. **Add partial validation** for incremental construction

### Medium Priority
4. **Deprecate `inline_rows`** for production use
5. **Add schema compatibility checker**
6. **Add JSON Schema $ref support** to reduce duplication

### Low Priority
7. **Generate TypeScript types** from schemas for web UIs

---

# Layer 2: Compile Layer

## Overview

**Location**: `pointline/research/pipeline.py`, `pointline/research/workflow.py`
**Responsibility**: Transform validated requests into executable plans
**Lines of Code**: ~1,500
**Score**: **7.5/10**

## Architecture

### Core Functions

```python
def compile_request(request: dict[str, Any]) -> dict[str, Any]:
    """Transform request into executable plan.

    Returns:
        compiled: {
            "request_id": str,
            "run_id": str,  # Generated UUID
            "mode": str,
            "timeline": {...},
            "spine": {...},
            "operators": [...],  # Enriched with registry metadata
            "config_hash": str,  # sha256(sorted(request))
            "constraints": {...},
            ...
        }
    """
```

### Compilation Pipeline

```python
def compile_request(request):
    # 1. Generate run_id
    run_id = str(uuid.uuid4())

    # 2. Resolve operators from registry
    operators = []
    for op_spec in request["operators"]:
        op_meta = get_operator_metadata(op_spec["agg"])

        # Validate mode compatibility
        if request["mode"] not in op_meta["mode_allowlist"]:
            raise PipelineError(
                f"Operator {op_spec['agg']} not allowed in mode {request['mode']}"
            )

        operators.append({**op_spec, "metadata": op_meta})

    # 3. Compute config hash (reproducibility)
    config_hash = _compute_config_hash(request)

    # 4. Resolve spine builder
    spine_builder = get_spine_builder(request["spine"]["type"])

    # 5. Return enriched plan
    return {
        "request_id": request["request_id"],
        "run_id": run_id,
        "mode": request["mode"],
        "operators": operators,
        "config_hash": config_hash,
        ...
    }
```

## Design Patterns

### 1. Registry-Based Resolution ✅ **Excellent**

**Pattern**: Operators/rollups resolved at compile-time, not runtime

```python
from pointline.research.resample import AggregationRegistry

registry = AggregationRegistry()

# Compile-time
op_meta = registry.get("trade_vwap")
# → {
#     "name": "trade_vwap",
#     "required_columns": ["px_int", "qty_int"],
#     "mode_allowlist": ["bar_then_feature", "tick_then_bar"],
#     "pit_policy": "backward_only",
#     "impl": <function trade_vwap>
# }

# Runtime (execute layer)
result = op_meta["impl"](df, config)
```

**Strength**:
- Invalid operators caught at compile-time
- No runtime reflection/imports
- IDE autocomplete for registered operators

### 2. Mode-Specific Compilation ✅ **Clean Separation**

```python
def _build_stage_request(workflow_compiled, stage):
    """Build single-mode request from workflow stage."""
    if stage["mode"] == "bar_then_feature":
        return _compile_bar_then_feature_stage(workflow_compiled, stage)
    elif stage["mode"] == "tick_then_bar":
        return _compile_tick_then_bar_stage(workflow_compiled, stage)
    elif stage["mode"] == "event_joined":
        return _compile_event_joined_stage(workflow_compiled, stage)
```

**Strength**: Each mode has isolated compilation logic

### 3. Config Hash for Reproducibility ✅ **Critical Feature**

```python
def _compute_config_hash(request: dict) -> str:
    """Deterministic hash of request configuration.

    Excludes: request_id, timestamps, output_dir
    Includes: mode, timeline ranges, operators, constraints
    """
    normalized = _normalize_for_hash(request)
    return hashlib.sha256(
        json.dumps(normalized, sort_keys=True).encode()
    ).hexdigest()
```

**Use case**:
```python
# Two runs with same config → same hash → cache hit
output1 = pipeline(request1)  # config_hash: "abc123"
output2 = pipeline(request2)  # config_hash: "abc123"

# Can skip re-execution if output1 exists
if cache.has(config_hash):
    return cache.get(config_hash)
```

## Strengths 💪

### 1. Fail-Fast Validation ✅

All validation happens before execution:
```python
def compile_request(request):
    # ✓ Operator exists?
    # ✓ Operator allowed in this mode?
    # ✓ Required columns available in sources?
    # ✓ Feature rollups registered?
    # ✓ Spine builder exists?

    # If any fail → PipelineError (before data loading)
```

**Benefit**: Save compute time, fail with actionable errors

### 2. Workflow DAG Resolution ✅ **Advanced**

```python
def compile_workflow_request(request):
    """Compile multi-stage workflow with dependency resolution."""
    stages = request["stages"]

    # Topological sort
    stage_plan = _resolve_stage_order(stages)

    # Detect cycles
    if _has_cycle(stage_plan):
        raise WorkflowError("Circular dependency detected")

    return {"stage_plan": stage_plan, ...}
```

**Example DAG**:
```
Stage 1 (event_joined): trades + quotes → joined_events
    ↓
Stage 2 (tick_then_bar): joined_events → features + 1s bars
    ↓
Stage 3 (bar_then_feature): 1s bars → 1m OHLCV + labels
```

**Strength**: Supports complex multi-stage research workflows

### 3. Metadata Enrichment ✅

Compiled plan includes everything needed for execution:
```python
compiled["operators"] = [
    {
        "name": "vwap",
        "agg": "trade_vwap",
        "source": "trades_src",

        # Enriched by compiler
        "metadata": {
            "required_columns": ["px_int", "qty_int", "ts_local_us"],
            "output_columns": ["vwap_px"],
            "stateful": False,
            "partition_key": None
        }
    }
]
```

**Benefit**: Execute layer is stateless, no registry lookups at runtime

## Weaknesses ⚠️

### 1. No Intermediate Representation ⚠️ **Missed Opportunity**

**Current**: Compiled plan is still a dict
```python
compiled = compile_request(request)  # dict[str, Any]
```

**Problem**:
- No type safety in compiled plan
- No validation that enrichment happened correctly
- IDE can't autocomplete `compiled["operators"][0]["metadata"]`

**Better design**:

```python
from dataclasses import dataclass

@dataclass
class CompiledOperator:
    name: str
    agg: str
    source: str
    metadata: OperatorMetadata

@dataclass
class CompiledPlan:
    request_id: str
    run_id: str
    mode: ExecutionMode
    operators: list[CompiledOperator]
    config_hash: str

# Type-safe compilation
compiled: CompiledPlan = compile_request(request)
```

**Benefits**:
- IDE autocomplete
- Type checker catches bugs
- Clear contract between compile and execute layers

### 2. Limited Optimization Passes ⚠️ **Performance**

**Current**: Compilation is single-pass

**Missing optimizations**:
1. **Operator fusion**: `vwap + ohlcv` on same data → single pass
2. **Column pruning**: Only load columns required by operators
3. **Predicate pushdown**: Push filters into source loading
4. **Common subexpression elimination**: Deduplicate identical computations

**Example**:
```python
# User specifies
operators = [
    {"name": "op1", "agg": "trade_vwap"},
    {"name": "op2", "agg": "trade_count"},
]

# Current: Two separate aggregations
# Optimized: Single pass computing both
```

**Impact**: 2-5x performance improvement for multi-operator queries

### 3. No Explain Plan ❌ **Critical Gap**

**Problem**: Users can't see execution plan before running

**Desired**:

```python
compiled = compile_request(request)
explain_plan(compiled)

# Output:
# Stage 1: Load sources
#   - trades: /lake/silver/trades (est. 1M rows)
#   - quotes: /lake/silver/quotes (est. 500K rows)
# Stage 2: Build spine
#   - Type: clock (step=1000ms)
#   - Est. spine points: 3600
# Stage 3: Execute operators
#   - trade_vwap: Requires [px_int, qty_int] → vwap_px
#   - spread: Requires [bid_px_int, ask_px_int] → spread
# Stage 4: Aggregate (tick_then_bar mode)
#   - Bucket assignment: 3600 buckets
#   - Feature rollups: [mean, last, std]
#   - Est. output rows: 3600
```

**Benefit**: Catch expensive queries before execution

### 4. Error Recovery ⚠️ **User Experience**

**Problem**: Compilation fails on first error

**Current**: Only reports first error
**Better**: Report all errors at once

```python
class CompilationErrors(Exception):
    def __init__(self, errors: list[str]):
        self.errors = errors

    def __str__(self):
        return f"{len(self.errors)} compilation errors:\n" + "\n".join(self.errors)
```

## Integration Points 🔗

### Input: Contract Layer → Compile Layer
```python
validate_quant_research_input_v2(request)
compiled = compile_request(request)
```

### Output: Compile Layer → Execute Layer
```python
frame, runtime = execute_compiled(compiled)
```

### Registry Dependencies
- `AggregationRegistry` (operators)
- `FeatureRollupRegistry` (rollups)
- `SpineBuilderRegistry` (spine builders)
- `ContextRegistry` (context plugins)

## Test Coverage 🧪

### Strengths ✅
- Mode-specific tests: Each mode has dedicated test
- Registry validation: Unknown operators caught
- DAG resolution: Workflow cycle detection tested

### Gaps ⚠️
- **Missing**: Optimization tests (fusion, pruning)
- **Missing**: Large-scale tests (100+ operators)
- **Missing**: Error recovery tests (multiple errors)

## Recommendations 📋

### High Priority
1. **Add typed intermediate representation** (dataclasses/Pydantic)
2. **Implement explain plan** for query preview
3. **Add batch error reporting** (collect all compilation errors)

### Medium Priority
4. **Add operator fusion optimization** (2-5x speedup)
5. **Add column pruning** (reduce I/O)
6. **Add estimated cost to explain plan** (row counts, memory)

### Low Priority
7. **Add compilation cache** for repeated requests
8. **Add visual DAG renderer** for workflow debugging
9. **Add compilation profiling** (identify slow registry lookups)

---

# Layer 3: Execute Layer

## Overview

**Location**: Multiple modules
- `pointline/research/core.py` - Data loading primitives
- `pointline/research/spines/` - Spine builders (~1,200 LOC)
- `pointline/research/resample/` - Bucketing & aggregation (~2,000 LOC)
- `pointline/research/features/` - Feature computation (~600 LOC)

**Responsibility**: Deterministic execution of compiled plans
**Lines of Code**: ~6,000
**Score**: **8.0/10**

## Architecture

### Execution Flow

```python
def execute_compiled(compiled: dict) -> tuple[pl.DataFrame, ExecutionRuntime]:
    """Execute compiled plan deterministically."""

    # 1. Load sources
    sources = load_sources(compiled["sources"])

    # 2. Build spine (resampling strategy)
    spine = build_spine(compiled["spine"], sources)

    # 3. Mode-specific execution
    if compiled["mode"] == "bar_then_feature":
        frame = execute_bar_then_feature(compiled, sources, spine)
    elif compiled["mode"] == "tick_then_bar":
        frame = execute_tick_then_bar(compiled, sources, spine)
    elif compiled["mode"] == "event_joined":
        frame = execute_event_joined(compiled, sources, spine)

    # 4. Compute labels (forward-looking, isolated from features)
    frame = compute_labels(frame, compiled["labels"])

    # 5. Return frame + runtime metadata
    runtime = ExecutionRuntime(
        coverage_checks=[...],
        probe_checks=[...],
        pit_violations=0,
        unassigned_rows=0,
        ...
    )
    return frame, runtime
```

## Component Deep Dive

### 3.1 Data Loading (`core.py`)

```python
def load_trades(
    symbol_id: int | Iterable[int],
    start_ts_us: TimestampInput,
    end_ts_us: TimestampInput,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = True,
) -> pl.LazyFrame | pl.DataFrame:
    """Load trades with explicit symbol_id."""

    # Validate inputs
    if symbol_id is None:
        raise ValueError(symbol_id_required_error())

    # Normalize timestamps
    start = _normalize_timestamp(start_ts_us, "start_ts_us")
    end = _normalize_timestamp(end_ts_us, "end_ts_us")

    # Partition pruning via symbol resolution
    exchanges = resolve_symbols(symbol_id)

    # Load with Delta Lake partition pruning
    lf = scan_table(
        "trades",
        symbol_id=symbol_id,
        start_ts_us=start,
        end_ts_us=end,
        ts_col=ts_col,
        columns=columns,
    )

    return lf if lazy else lf.collect()
```

**Strengths** ✅:
- **Partition pruning**: Automatically filters by `exchange` + `date`
- **Lazy by default**: Memory-efficient for large datasets
- **Timestamp normalization**: Accepts int, datetime, ISO strings

### 3.2 Spine Builders (`spines/`)

#### Registry Architecture

```python
# Auto-registration on import
from . import clock, dollar, trades, volume

# Get builder
builder = get_builder("clock")  # Returns ClockSpineBuilder instance
```

#### Clock Spine (Time-Based)

```python
@dataclass
class ClockSpineConfig:
    step_ms: int  # Interval in milliseconds

class ClockSpineBuilder(SpineBuilder):
    def build_spine(
        self,
        symbol_id: int,
        start_ts_us: int,
        end_ts_us: int,
        config: ClockSpineConfig,
    ) -> pl.LazyFrame:
        """Generate fixed time intervals."""

        step_us = config.step_ms * 1000
        spine_points = range(start_ts_us, end_ts_us, step_us)

        return pl.LazyFrame({
            "ts_local_us": list(spine_points),
            "symbol_id": [symbol_id] * len(spine_points),
        })
```

#### Volume Bars (Activity-Based)

```python
@dataclass
class VolumeBarConfig:
    volume_threshold: float
    use_absolute_volume: bool = True

class VolumeBarBuilder(SpineBuilder):
    def build_spine(self, ...) -> pl.LazyFrame:
        """Sample every N contracts traded."""

        # Load trades
        trades = load_trades(symbol_id, start_ts_us, end_ts_us)

        # Cumulative volume
        trades = trades.with_columns(
            pl.col("qty").cum_sum().alias("cum_volume")
        )

        # Sample points where cum_volume crosses threshold
        spine = trades.filter(
            (pl.col("cum_volume") % config.volume_threshold) < pl.col("qty")
        )

        return spine.select(["ts_local_us", "symbol_id"])
```

**Strengths** ✅:
- **Pluggable**: Add new strategies without modifying core
- **Typed configs**: Each builder has specific config dataclass
- **Activity normalization**: Volume/dollar bars handle varying liquidity

### 3.3 Resample & Aggregate (`resample/`)

#### Bucket Assignment

**Critical invariant**: Half-open windows `[T_prev, T)`

```python
def assign_to_buckets(
    events: pl.LazyFrame,
    spine: pl.LazyFrame,
    ts_col: str = "ts_local_us",
) -> pl.LazyFrame:
    """Assign events to time buckets.

    Invariant: event.ts < bucket_end (strict PIT)
    """

    # Add bucket boundaries
    spine = spine.with_columns([
        pl.col(ts_col).alias("bucket_start"),
        pl.col(ts_col).shift(-1).fill_null(pl.lit(2**63-1)).alias("bucket_end"),
    ])

    # Join events to buckets (as-of join)
    bucketed = events.join_asof(
        spine,
        left_on=ts_col,
        right_on="bucket_start",
        by="symbol_id",
        strategy="backward",
    )

    # PIT check: event.ts < bucket_end
    bucketed = bucketed.with_columns(
        (pl.col(ts_col) < pl.col("bucket_end")).alias("_pit_check")
    )

    pit_violations = bucketed.filter(~pl.col("_pit_check")).count()
    if pit_violations > 0:
        raise PITViolationError(f"{pit_violations} events violate PIT constraint")

    return bucketed
```

**Strengths** ✅:
- **PIT enforcement**: Automatically validates timeline correctness
- **Efficient**: Uses Polars `join_asof` (optimized for time-series)

#### Aggregation Engine

```python
def aggregate(
    bucketed: pl.LazyFrame,
    config: AggregateConfig,
) -> pl.DataFrame:
    """Aggregate events within buckets."""

    # Get aggregation spec from registry
    agg_spec = AggregationRegistry.get(config.agg_name)

    # Group by bucket
    grouped = bucketed.group_by(["bucket_start", "symbol_id"])

    # Apply aggregation
    result = grouped.agg(agg_spec.impl(config))

    # Apply feature rollups (for tick_then_bar mode)
    if config.feature_rollups:
        result = apply_feature_rollups(result, config.feature_rollups)

    return result.collect()
```

**Registry Example**:
```python
@register_aggregation
class TradeVWAP(AggregationSpec):
    name = "trade_vwap"
    required_columns = ["px_int", "qty_int"]
    mode_allowlist = ["bar_then_feature", "tick_then_bar"]

    def impl(self, config) -> pl.Expr:
        return [
            (pl.col("px_int") * pl.col("qty_int")).sum() / pl.col("qty_int").sum()
            .alias("vwap_px_int")
        ]
```

**Strengths** ✅:
- **Declarative**: Aggregations defined as Polars expressions
- **Composable**: Multiple aggregations in single pass
- **Type-safe**: Registry enforces required columns

### 3.4 Feature Computation (`features/`)

#### PIT-Safe Joins

```python
def pit_align(
    left: pl.LazyFrame,
    right: pl.LazyFrame,
    on: str = "ts_local_us",
    by: str | list[str] | None = "symbol_id",
    strategy: str = "backward",
) -> pl.LazyFrame:
    """Point-in-time safe as-of join.

    Ensures right table data is known BEFORE or AT left table timestamp.
    """

    aligned = left.join_asof(
        right,
        left_on=on,
        right_on=on,
        by=by,
        strategy=strategy,  # "backward" = past data only
    )

    # Validation: no future leakage
    if strategy == "backward":
        # Check: right.ts <= left.ts
        violations = aligned.filter(
            pl.col(f"{on}_right") > pl.col(on)
        ).count()
        if violations > 0:
            raise PITViolationError(f"{violations} future joins detected")

    return aligned
```

**Strengths** ✅:
- **Automatic PIT validation**: Catches lookahead bugs
- **Flexible**: Supports forward joins for labels (explicitly marked)

## Design Patterns

### 1. Lazy Evaluation Chain ✅ **Performance Critical**

```python
# Entire pipeline stays lazy until .collect()
trades = load_trades(..., lazy=True)          # LazyFrame
spine = build_spine(...)                      # LazyFrame
bucketed = assign_to_buckets(trades, spine)   # LazyFrame
aggregated = aggregate(bucketed, config)      # .collect() here
```

**Benefit**: Polars optimizer sees full query plan → better execution

### 2. Deterministic Ordering ✅ **Reproducibility**

```python
# Canonical sort key for trades
SORT_KEY = [
    "exchange_id",
    "symbol_id",
    "ts_local_us",
    "file_id",
    "file_line_number"
]

# All stateful operations must sort first
def compute_pct_change(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.sort(SORT_KEY)
        .group_by(["exchange_id", "symbol_id"])
        .agg([pl.col("px_int").pct_change().alias("pct_change")])
    )
```

**Strength**: Same inputs → same outputs (no race conditions)

### 3. Partitioning for Stateful Ops ✅ **Correctness**

```python
# BAD: pct_change across symbols mixes data
df.with_columns(pl.col("px_int").pct_change())

# GOOD: Partition by symbol first
df.sort(["symbol_id", "ts_local_us"]).with_columns(
    pl.col("px_int").pct_change().over("symbol_id")
)
```

**Enforcement**: Registry metadata includes `partition_key`

## Strengths 💪

### 1. Timeline Correctness ✅ **Mission-Critical**
- Half-open windows `[T_prev, T)` enforced
- PIT violations caught automatically
- No manual timeline management

### 2. Polars-Native ✅ **Performance**
- Zero Python loops for hot paths
- Vectorized operations throughout
- Memory-efficient lazy evaluation

### 3. Extensible Registries ✅ **Production-Grade**
- Spine builders: 4+ strategies, easy to add more
- Aggregations: Declarative, composable
- Feature rollups: Custom with typed params

### 4. Mode Isolation ✅ **Clean Architecture**
Each mode has separate execution path:
```python
execute_bar_then_feature(...)     # Bars → features
execute_tick_then_bar(...)        # Features → bars
execute_event_joined(...)         # Multi-table PIT joins
```

No mode cross-contamination

## Weaknesses ⚠️

### 1. No Execution Plan Visualization ❌ **Critical for Debugging**

**Problem**: Can't see what's actually executing

**Desired**:
```python
frame, runtime = execute_compiled(compiled)

# Show execution tree
runtime.execution_plan
# →
# LoadTrades(symbol_id=12345, rows=100K)
#   └─ BuildSpine(type=clock, points=3600)
#       └─ AssignBuckets(unassigned=0)
#           └─ Aggregate(agg=trade_vwap)
#               └─ Output(rows=3600)
```

### 2. Limited Error Context ⚠️ **Developer Experience**

**Problem**: Errors deep in execution lose context

**Example**:
```
PolarsError: could not find column 'bid_px_int'
```

**Missing context**:
- Which operator failed?
- Which bucket/symbol?
- What data was present?

**Better**:
```
ExecutionError: Operator 'spread' failed at bucket 123 (2024-05-01T00:02:03Z)
  Symbol: BTCUSDT (symbol_id=12345)
  Required column: bid_px_int
  Available columns: [ts_local_us, px_int, qty_int]
  Hint: Did you forget to join quotes table?
```

### 3. No Incremental Execution ⚠️ **Performance**

**Problem**: Re-execute everything on parameter change

**Opportunity**: Cache loaded sources, recompute only aggregation

```python
# Cacheable layers
cache["sources"] = load_sources(...)      # Expensive I/O
cache["spine"] = build_spine(...)         # Cheap
cache["bucketed"] = assign_buckets(...)   # Moderate

# Recompute only aggregation
aggregate(cache["bucketed"], new_config)  # Fast
```

### 4. No Parallelism ⚠️ **Scalability**

**Current**: Single-threaded execution

**Opportunities**:
1. **Multi-symbol parallelism**: Process symbols in parallel
2. **Operator parallelism**: Independent operators run concurrently
3. **Distributed**: Spark/Dask backend for TB-scale data

### 5. Stateful Transform Safety ⚠️ **Correctness Risk**

**Problem**: Easy to write incorrect stateful ops

**Current safeguard**: Documentation only

**Better**: Enforce at registry level
```python
@register_aggregation
class RollingMean(AggregationSpec):
    name = "rolling_mean"
    stateful = True  # Flag
    partition_key = ["symbol_id"]  # Required

    def impl(self, config):
        # Framework auto-partitions before calling
        return pl.col("px_int").rolling_mean(config.window)
```

## Integration Points 🔗

### Input: Compile Layer → Execute Layer
```python
compiled = compile_request(request)
frame, runtime = execute_compiled(compiled)
```

### Output: Execute Layer → Governance Layer
```python
gates = evaluate_quality_gates(compiled, runtime)
```

### Registry Dependencies
- **SpineBuilderRegistry**: Spine construction
- **AggregationRegistry**: Aggregation operators
- **FeatureRollupRegistry**: Bar-level rollups

## Test Coverage 🧪

### Strengths ✅
- PIT validation tests for lookahead detection
- Bucket assignment edge cases
- Mode-specific golden tests

### Gaps ⚠️
- **Missing**: Multi-symbol execution tests
- **Missing**: Large-scale performance tests (100M+ rows)
- **Missing**: Error recovery tests (partial failures)
- **Missing**: Stateful operator partition tests

## Recommendations 📋

### High Priority
1. **Add execution plan tracing** (see what's running, where time is spent)
2. **Improve error context** (operator name, bucket info in exceptions)
3. **Add stateful operator validation** (enforce partitioning at registry level)

### Medium Priority
4. **Add incremental execution** (cache loaded sources)
5. **Add multi-symbol parallelism** (ThreadPoolExecutor for independent symbols)
6. **Add execution profiling** (per-operator timing)

### Low Priority
7. **Add distributed backend** (Spark/Dask for TB-scale)
8. **Add execution checkpointing** (resume from failures)
9. **Add adaptive optimization** (auto-tune based on data stats)

---

# Layer 4: Governance Layer

## Overview

**Location**: `pointline/research/pipeline.py` (governance functions)
**Responsibility**: Quality gates, artifacts, decision logic
**Lines of Code**: ~500
**Score**: **7.5/10**

## Architecture

### Core Functions

```python
def evaluate_quality_gates(
    compiled: dict,
    runtime: ExecutionRuntime,
) -> dict[str, Any]:
    """Evaluate all quality gates on execution results."""

    gates = {
        "passed_gates": [],
        "failed_gates": [],
        "gate_results": {}
    }

    # Gate 1: Lookahead check
    if compiled["constraints"]["forbid_lookahead"]:
        if runtime.pit_violations > 0:
            gates["failed_gates"].append("pit_ordering_check")

    # Gate 2: Unassigned rows check
    if runtime.unassigned_rows > 0:
        ratio = runtime.unassigned_rows / runtime.total_rows
        if ratio > compiled["constraints"].get("max_unassigned_ratio", 0.01):
            gates["failed_gates"].append("assignment_coverage_check")

    # Gate 3: Reproducibility check
    reproducibility = check_reproducibility(compiled, runtime)
    if not reproducibility["passed"]:
        gates["failed_gates"].append("reproducibility_check")

    # Gate 4: Partition safety check
    partition_safety = check_partition_safety(runtime)
    if not partition_safety["passed"]:
        gates["failed_gates"].append("partition_safety_check")

    return gates
```

### Decision Logic

```python
def build_decision(
    gates: dict,
    runtime: ExecutionRuntime,
) -> dict[str, Any]:
    """Build final decision payload.

    Decision matrix:
    - accept: All gates passed
    - warn: Soft gates failed, hard gates passed
    - reject: Any hard gate failed
    """

    hard_gates = ["pit_ordering_check", "reproducibility_check"]

    failed_hard = any(g in gates["failed_gates"] for g in hard_gates)

    if failed_hard:
        status = "reject"
        reasons = [f"Critical gate '{g}' failed" for g in gates["failed_gates"]]
    elif gates["failed_gates"]:
        status = "warn"
        reasons = [f"Soft gate '{g}' failed" for g in gates["failed_gates"]]
    else:
        status = "accept"
        reasons = []

    return {
        "status": status,
        "reasons": reasons,
        "failed_gates": gates["failed_gates"],
        "passed_gates": gates["passed_gates"]
    }
```

### Artifact Emission

```python
def emit_artifacts(
    compiled: dict,
    gates: dict,
    frame: pl.DataFrame,
) -> list[str]:
    """Persist run artifacts to disk.

    Emits:
    - config.json: Request configuration
    - gates.json: Gate results
    - output.parquet: Result DataFrame
    - lineage.json: Data provenance
    """

    output_dir = Path(compiled["artifacts"]["output_dir"])
    run_id = compiled["run_id"]

    paths = []

    # Config
    config_path = output_dir / f"{run_id}_config.json"
    with config_path.open("w") as f:
        json.dump(compiled, f, indent=2)
    paths.append(str(config_path))

    # Gates
    gates_path = output_dir / f"{run_id}_gates.json"
    with gates_path.open("w") as f:
        json.dump(gates, f, indent=2)
    paths.append(str(gates_path))

    # Output
    output_path = output_dir / f"{run_id}_output.parquet"
    frame.write_parquet(output_path)
    paths.append(str(output_path))

    # Lineage (optional)
    if compiled["artifacts"]["emit_lineage"]:
        lineage = build_lineage(compiled, frame)
        lineage_path = output_dir / f"{run_id}_lineage.json"
        with lineage_path.open("w") as f:
            json.dump(lineage, f, indent=2)
        paths.append(str(lineage_path))

    return paths
```

## Quality Gates Deep Dive

### Gate 1: PIT Ordering Check ✅ **Hard Gate**

**Purpose**: Prevent lookahead bias

**Test**:
```python
def test_pipeline_critical_pit_failure_forces_reject():
    request = _fixture("input_v2_bar_then_feature.json")

    # Inject future data (ts > bucket_end)
    request["sources"][0]["inline_rows"].append({
        "ts_local_us": 130_000_000,  # Outside bucket range
        ...
    })

    output = pipeline(request)
    assert output["decision"]["status"] == "reject"
    assert "pit_ordering_check" in output["quality_gates"]["failed_gates"]
```

**Strength**: Automatically catches most common research bug

### Gate 2: Reproducibility Check ✅ **Hard Gate**

**Purpose**: Ensure deterministic outputs

**Implementation**:
```python
def check_reproducibility(compiled: dict, runtime: ExecutionRuntime) -> dict:
    """Re-execute and verify outputs match."""

    # Hash original output
    original_hash = _hash_output_frame(runtime.output_frame)

    # Re-execute with same config
    rerun_frame, _ = execute_compiled(compiled)
    rerun_hash = _hash_output_frame(rerun_frame)

    if original_hash != rerun_hash:
        return {
            "passed": False,
            "original_hash": original_hash,
            "rerun_hash": rerun_hash,
            "severity": "critical"
        }

    return {"passed": True}
```

**Test**:
```python
def test_pipeline_reproducibility_gate_is_mandatory(monkeypatch):
    # Mock hash function to return different values
    state = {"n": 0}
    def _fake_hash(_frame):
        state["n"] += 1
        return f"hash_{state['n']}"

    monkeypatch.setattr(pipeline_module, "_hash_output_frame", _fake_hash)

    output = pipeline(request)
    assert output["decision"]["status"] == "reject"
    assert "reproducibility_check" in output["quality_gates"]["failed_gates"]
```

**Strength**: Catches non-deterministic operations

### Gate 3: Partition Safety Check ⚠️ **Soft Gate**

**Purpose**: Warn if stateful ops might cross partition boundaries

### Gate 4: Assignment Coverage Check ⚠️ **Soft Gate**

**Purpose**: Warn if many events unassigned to buckets

## Artifact System

### Lineage Tracking

**Example lineage.json**:
```json
{
  "run_id": "abc-123",
  "config_hash": "sha256:...",
  "sources": [
    {
      "name": "trades_src",
      "table": "trades",
      "symbol_ids": [12345],
      "date_range": ["2024-05-01", "2024-05-02"],
      "row_count": 1000000,
      "files": [
        "/lake/silver/trades/exchange=binance-futures/date=2024-05-01/part-0.parquet"
      ]
    }
  ],
  "operators": [
    {
      "name": "vwap",
      "agg": "trade_vwap",
      "input_rows": 1000000,
      "output_rows": 3600,
      "execution_time_ms": 1234
    }
  ],
  "git_commit": "abc123",
  "python_version": "3.11.5",
  "polars_version": "0.19.0"
}
```

**Strength**: Can trace any output back to exact source files + code version

## Strengths 💪

### 1. Automatic Gate Enforcement ✅ **Critical**
- No way to bypass gates
- Clear pass/fail criteria
- Actionable error messages

### 2. Reproducibility as First-Class ✅ **Production-Grade**
- Hash-based verification
- Re-execution in same process
- Determinism bugs caught immediately

### 3. Comprehensive Artifacts ✅ **Audit Trail**
- Config + gates + output + lineage
- Versioned schemas
- Git commit tracking

### 4. Decision Payload ✅ **Workflow Integration**
```python
output = pipeline(request)

if output["decision"]["status"] == "accept":
    promote_to_production(output)
elif output["decision"]["status"] == "warn":
    notify_researcher(output["decision"]["reasons"])
else:  # reject
    log_failure(output)
```

## Weaknesses ⚠️

### 1. Limited Gate Extensibility ⚠️ **Customization**

**Problem**: Gates are hardcoded

**Desired**: Pluggable gate system
```python
@register_gate
class MaxMemoryGate(QualityGate):
    name = "max_memory"
    severity = "warning"

    def check(self, runtime: ExecutionRuntime) -> dict:
        if runtime.peak_memory_mb > 16000:
            return {"passed": False, "peak_memory_mb": runtime.peak_memory_mb}
        return {"passed": True}
```

### 2. No Gate Debugging Tools ❌ **Critical Gap**

**Problem**: When gates fail, hard to diagnose

**Desired**:
```python
if "reproducibility_check" in output["quality_gates"]["failed_gates"]:
    diff = analyze_reproducibility_failure(output)
    # → Shows which rows/columns differ
```

### 3. Artifact Storage Not Managed ⚠️ **Operations**

**Problems**:
- No retention policy (fills disk)
- No deduplication
- No cloud storage integration (S3, GCS)

**Better design**:
```python
store = ArtifactStore.from_config("~/.config/pointline/artifacts.toml")
paths = store.save_artifacts(
    run_id=compiled["run_id"],
    artifacts={...},
    ttl_days=30  # Auto-cleanup
)
```

### 4. No Gate History ⚠️ **Trend Analysis**

**Desired**:
```python
history = get_gate_history(
    config_hash="abc123",
    gate="pit_ordering_check",
    since="2024-01-01"
)
# → Shows gate pass/fail over time
```

### 5. No Cost Estimation Gate ⚠️ **Resource Management**

**Missing**: Reject queries that would be too expensive

### 6. Gate Results Not Typed ⚠️ **Developer Experience**

**Current**: `dict[str, Any]`
**Better**: Typed dataclasses

## Integration Points 🔗

### Input: Execute Layer → Governance Layer
```python
frame, runtime = execute_compiled(compiled)
gates = evaluate_quality_gates(compiled, runtime)
```

### Output: Governance Layer → User/System
```python
output = {
    "decision": build_decision(gates, runtime),
    "artifacts": emit_artifacts(compiled, gates, frame),
    "quality_gates": gates
}
```

## Test Coverage 🧪

### Strengths ✅
- Hard gate enforcement tests
- Reproducibility tests
- Unknown operator tests

### Gaps ⚠️
- **Missing**: Soft gate behavior tests
- **Missing**: Artifact retention tests
- **Missing**: Gate history tests
- **Missing**: Custom gate registration tests

## Recommendations 📋

### High Priority
1. **Add gate debugging tools** (diff analyzer)
2. **Add artifact store abstraction** (S3, GCS, TTL)
3. **Add cost estimation gate**

### Medium Priority
4. **Add pluggable gate system**
5. **Add gate history tracking**
6. **Type gate results** (dataclasses)

### Low Priority
7. **Add gate visualization** (HTML reports)
8. **Add artifact deduplication**
9. **Add gate performance profiling**

---

# Cross-Layer Analysis

## Integration Quality

### Layer Boundaries ✅ **Well-Defined**

```
Contract → Compile → Execute → Governance
  (dict)     (dict)    (DataFrame, Runtime)   (dict)
```

**Observation**: All boundaries use dicts (no typed contracts)

**Pro**: Simple, no serialization overhead
**Con**: No type safety, runtime errors only

### Data Flow ✅ **Unidirectional**

```
User Request
    ↓
Contract Validation (fail-fast)
    ↓
Compilation (fail-fast)
    ↓
Execution (deterministic)
    ↓
Quality Gates (fail-fast)
    ↓
Decision Output
```

**Strength**: Clear failure points, no backtracking

## Cross-Cutting Concerns

### 1. Observability ⚠️ **Needs Improvement**

**Current**: No execution tracing, no per-layer timing

**Desired**:
```python
output = pipeline(request, trace=True)

output["trace"]
# →
# contract_layer: 2ms
# compile_layer: 15ms
# execute_layer: 1234ms
#   - load_sources: 800ms
#   - build_spine: 50ms
#   - assign_buckets: 200ms
#   - aggregate: 184ms
# governance_layer: 500ms (reproducibility check)
```

### 2. Error Propagation ⚠️ **Context Loss**

**Problem**: Errors lose layer context

**Better**: Layer-aware exception wrapper

### 3. Testing Strategy ✅ **Good Coverage**

- Layer-specific tests: ✓
- Integration tests: ✓
- End-to-end tests: ✓ (north-star acceptance)

**Gap**: No chaos testing

## Architectural Strengths 💪

### 1. Clear Separation of Concerns ✅
Each layer has single responsibility

### 2. Fail-Fast at Each Layer ✅
Errors caught as early as possible

### 3. Registry-Based Extensibility ✅
All extension points use registries

### 4. Reproducibility by Design ✅
Every layer contributes

## Architectural Weaknesses ⚠️

### 1. No Typed Contracts Between Layers
All boundaries use `dict[str, Any]`

### 2. Limited Observability
Can't see inside layer execution

### 3. No Incremental Execution
Each layer re-runs completely

### 4. Single-Threaded Only
No parallelism

---

# Final Scores

| Layer | Score | Strengths | Primary Weaknesses |
|-------|-------|-----------|-------------------|
| **Contract** | 8.0/10 | Validation, versioning, error messages | Verbosity, no builder API |
| **Compile** | 7.5/10 | Registry-based, fail-fast, DAG | No typed IR, no optimizations |
| **Execute** | 8.0/10 | PIT correctness, Polars-native | No parallelism, observability |
| **Governance** | 7.5/10 | Auto enforcement, reproducibility | Limited extensibility, debugging |
| **Integration** | 7.5/10 | Clean boundaries, unidirectional | No typed contracts, observability |

## **Overall Architecture Score: 7.8/10**

---

# Final Recommendations

## Critical Path (Next 3 Months)

### 1. Observability Infrastructure ⭐⭐⭐
**Impact**: High | **Effort**: Medium

- Add execution tracing with per-layer timing
- Add execution plan visualization
- Add gate failure debugging tools

**Benefit**: 10x faster debugging, better performance optimization

### 2. Python Builder API ⭐⭐⭐
**Impact**: High | **Effort**: Medium

- Fluent API for request construction
- IDE autocomplete support
- Gradual migration from JSON

**Benefit**: Lower barrier to entry, faster iteration

### 3. Typed Intermediate Representations ⭐⭐
**Impact**: Medium | **Effort**: High

- Add dataclasses for compiled plans
- Type all layer boundaries
- Enable static type checking

**Benefit**: Catch bugs at compile time, better IDE support

## Performance Optimizations (Next 6 Months)

### 4. Incremental Execution ⭐⭐
**Impact**: High | **Effort**: High

- Cache loaded sources
- Recompute only changed layers
- Smart invalidation

**Benefit**: 5-10x faster iteration on parameter tuning

### 5. Multi-Symbol Parallelism ⭐⭐
**Impact**: Medium | **Effort**: Medium

- ThreadPoolExecutor for independent symbols
- Parallel operator execution
- Shared memory optimization

**Benefit**: Near-linear scaling with CPU cores

### 6. Query Optimization ⭐
**Impact**: Medium | **Effort**: High

- Operator fusion
- Column pruning
- Predicate pushdown

**Benefit**: 2-5x performance improvement

## Governance Enhancements (Ongoing)

### 7. Pluggable Gates ⭐⭐
**Impact**: Medium | **Effort**: Low

- Registry-based gate system
- Custom gate registration
- Team-specific policies

**Benefit**: Enables domain-specific quality checks

### 8. Artifact Management ⭐
**Impact**: Medium | **Effort**: Medium

- Cloud storage integration (S3, GCS)
- Retention policies
- Deduplication

**Benefit**: Operational sustainability

### 9. Gate History & Trends ⭐
**Impact**: Low | **Effort**: Medium

- Time-series gate results
- Regression detection
- Quality dashboards

**Benefit**: Continuous quality monitoring

---

## Conclusion

The Pointline research framework demonstrates **excellent architectural discipline** with clear layer separation, strong PIT correctness guarantees, and comprehensive quality gates. The foundation is **production-ready** and maintainable.

**Primary opportunity**: Enhance **observability** and **developer experience** through execution tracing, Python builders, and typed contracts. These improvements will accelerate research iteration cycles while maintaining the framework's rigorous correctness guarantees.

**Recommended next action**: Implement execution tracing (Recommendation #1) as it provides immediate value across all layers and enables better decision-making for subsequent optimizations.

---

**Document Version**: 1.0
**Last Updated**: 2026-02-09
**Next Review**: Q2 2026
