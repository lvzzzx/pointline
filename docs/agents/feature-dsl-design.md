# Feature DSL Design for Quant Agent

This document defines the Declarative Feature DSL (Domain-Specific Language) for the Quant Researcher agent and explains why this architecture is optimal for LLM-based automation.

## Executive Summary

**Design Choice**: Declarative JSON-based Feature DSL with deterministic compiler.

**Why This Is The Sweet Spot for LLM Agents**:
1. LLMs excel at generating structured JSON (proven capability)
2. Declarative specs are easy to validate for PIT-safety (no code execution risk)
3. Compiler translates to safe Polars code (eliminates hallucination bugs)
4. Compositional primitives cover 90% of quant features (sufficient expressiveness)
5. Clear error messages enable LLM self-correction (iterative refinement)

## Design Philosophy

### The LLM Agent Challenge

LLM agents face a fundamental tension in feature engineering:

**What LLMs are good at:**
- Understanding natural language research questions
- Mapping quant concepts to operations (e.g., "momentum" → "price return over window")
- Generating structured outputs (JSON, YAML, SQL)
- Iterative refinement based on feedback

**What LLMs are bad at:**
- Precise arithmetic and floating-point operations
- Remembering complex API signatures (Polars has 500+ methods)
- Guaranteeing PIT-safety in imperative code
- Avoiding off-by-one errors in time-based operations
- Type safety and schema consistency

### The Three Options

#### Option A: Predefined Feature Library

```python
# LLM selects from pre-built features
"features": ["spread", "volume_imbalance_5m", "mid_return_10s"]
```

**Pros:**
- ✅ Maximum safety (features are pre-validated)
- ✅ Fast to implement
- ✅ Zero risk of PIT violations

**Cons:**
- ❌ Limited expressiveness (only pre-defined features)
- ❌ Requires constant maintenance (add new features for each use case)
- ❌ Doesn't scale to novel research questions

#### Option B: LLM Generates Python/Polars Code

```python
# LLM generates executable Python
"features": [
  {
    "name": "volume_imbalance_5m",
    "code": """
df.with_columns([
  pl.col("qty").mul(pl.col("side").map_dict({0: -1, 1: 1}))
    .rolling_sum_by("ts_local_us", window_size="5m")
    .alias("volume_imbalance_5m")
])
"""
  }
]
```

**Pros:**
- ✅ Maximum expressiveness (can do anything Polars can do)
- ✅ No compiler needed

**Cons:**
- ❌ **DANGEROUS**: LLM can generate buggy/unsafe code
- ❌ Hard to validate PIT-safety automatically
- ❌ Code execution risk (security vulnerability)
- ❌ LLMs make subtle API errors (wrong parameter order, type mismatches)
- ❌ Non-reproducible (same request → different code each time)

#### Option C: Declarative DSL (RECOMMENDED)

```json
{
  "name": "volume_imbalance_5m",
  "type": "rolling_window",
  "expression": {
    "op": "multiply",
    "args": [
      {"column": "qty"},
      {"op": "map", "column": "side", "mapping": {"0": -1, "1": 1}}
    ]
  },
  "aggregation": "sum",
  "window": {"size": "5m", "direction": "backward"}
}
```

**Pros:**
- ✅ LLMs excel at generating JSON (high success rate)
- ✅ Declarative = easy to validate PIT-safety
- ✅ Compiler ensures safe Polars code (no execution risk)
- ✅ Compositional primitives = high expressiveness
- ✅ Reproducible (same spec → same code)
- ✅ Versionable and auditable
- ✅ Clear error messages for LLM refinement

**Cons:**
- ⚠️ Requires implementing compiler (one-time cost)
- ⚠️ Limited to supported primitives (but covers 90% of cases)

## Why Declarative DSL Is The Sweet Spot

### 1. LLMs Are Excellent at Structured Output

**Evidence from production systems:**
- Function calling (OpenAI, Anthropic): 95%+ success rate on JSON generation
- SQL generation: LLMs reliably produce valid SQL
- Configuration files: YAML/JSON generation is a solved problem

**Our DSL leverages this strength:**
```json
// LLM can reliably generate this structure
{
  "type": "rolling_window",
  "base_column": "qty",
  "aggregation": "sum",
  "window": {"size": "5m", "direction": "backward"}
}
```

**Contrast with code generation:**
```python
# LLM often makes subtle mistakes:
df.rolling_sum("qty", by="ts_local_us", window_size="5m")  # ❌ Wrong parameter name
df.rolling_sum_by("qty", "ts_local_us", "5m")              # ❌ Wrong order
df.with_columns(pl.col("qty").rolling_sum_by(...))         # ✅ Correct (but complex)
```

### 2. Declarative = Analyzable for PIT-Safety

**Key insight**: PIT-safety violations have structural signatures.

```json
// Compiler can detect this violation syntactically
{
  "type": "rolling_window",
  "window": {"direction": "forward"}  // ❌ DETECTED: future peeking
}
```

**Contrast with code analysis:**
```python
# Hard to detect PIT violation in arbitrary code
df = df.with_columns([
  pl.col("px").shift(-5).alias("future_px")  # ❌ Requires semantic analysis
])
```

**Our validation strategy:**
```python
def _check_pit_safety(self, feature: Dict) -> bool:
    """Syntactic PIT-safety check."""

    # Simple rule: "forward" direction is forbidden in features
    if feature.get("window", {}).get("direction") == "forward":
        raise PITViolationError(f"Feature uses forward-looking window")

    if feature.get("lag", {}).get("direction") == "forward":
        raise PITViolationError(f"Feature uses forward-looking lag (lead)")

    # Recursively check nested expressions
    if "expression" in feature:
        self._check_expression_pit_safety(feature["expression"])
```

### 3. Compiler Eliminates Hallucination Bugs

**LLM hallucination examples in code generation:**
```python
# LLM invents non-existent methods:
df.rolling_weighted_sum(...)        # ❌ Doesn't exist
df.time_based_groupby(...)          # ❌ Doesn't exist

# LLM mixes up parameter types:
df.rolling_sum_by("5m", "ts_local_us")  # ❌ Reversed parameters

# LLM uses wrong column names:
df.select(pl.col("timestamp"))      # ❌ Should be "ts_local_us"
```

**With DSL + Compiler:**
```json
// LLM generates declarative spec
{
  "type": "rolling_window",
  "aggregation": "weighted_sum"  // ❌ Invalid aggregation type
}
```

```python
# Compiler catches and reports error
AGG_MAP = {
    "sum": ...,
    "mean": ...,
    "std": ...,
    # "weighted_sum" not in map
}

if agg not in AGG_MAP:
    raise ValueError(
        f"Unsupported aggregation: {agg}. "
        f"Allowed: {list(AGG_MAP.keys())}"
    )
```

**Result**: LLM gets actionable feedback and can fix the spec in next iteration.

### 4. Compositional Primitives = High Expressiveness

**Coverage analysis** of common quant features:

| Feature Category | DSL Support | Example |
|-----------------|-------------|---------|
| Microstructure | ✅ | Spread, mid-price, quote imbalance |
| Volume | ✅ | Rolling volume, VWAP, volume imbalance |
| Returns | ✅ | Log returns, pct change, volatility |
| Order flow | ✅ | Signed volume, order imbalance |
| Cross-sectional | ✅ | Rank, percentile, z-score |
| Time-of-day | ✅ | Hour, minute, session indicator |
| Joins | ✅ | As-of joins (backward) |
| Complex ML | ⚠️ | Requires Python fallback |

**Estimation**: Declarative DSL covers **90%** of HFT/MFT features.

**Fallback strategy** for the remaining 10%:
```json
{
  "type": "custom",
  "function_name": "compute_order_book_imbalance_v2",  // Pre-registered function
  "params": {"levels": 5}
}
```

### 5. Iterative Refinement Works Well

**Typical LLM workflow with DSL:**

```
User: "I want volume imbalance over 5 minutes"
  ↓
LLM generates feature spec v1
  ↓
Compiler: ❌ "Error: missing 'direction' in window"
  ↓
LLM generates feature spec v2 (adds "direction": "backward")
  ↓
Compiler: ✅ Success
```

**Why this works:**
1. Error messages are structured and actionable
2. LLM maintains declarative spec (not debugging code)
3. Validation is deterministic (same spec → same result)
4. Fast iteration (no manual code review needed)

**Contrast with code generation:**
```
User: "I want volume imbalance over 5 minutes"
  ↓
LLM generates Python code v1
  ↓
Runtime: ❌ "AttributeError: 'LazyFrame' object has no attribute 'rolling_sum_by_time'"
  ↓
LLM generates Python code v2 (tries different API)
  ↓
Runtime: ❌ "TypeError: rolling_sum() got an unexpected keyword argument 'window_size'"
  ↓
LLM generates Python code v3 (tries yet another API)
  ↓
... (cycle continues)
```

## Technical Specification

### Feature DSL Schema (v1.0)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://pointline.dev/schemas/feature_dsl.v1.json",
  "title": "FeatureDSLV1",
  "type": "object",
  "required": ["feature_spec_version", "features"],
  "properties": {
    "feature_spec_version": {
      "const": "1.0"
    },
    "features": {
      "type": "array",
      "minItems": 1,
      "items": {
        "oneOf": [
          {"$ref": "#/$defs/ExpressionFeature"},
          {"$ref": "#/$defs/RollingWindowFeature"},
          {"$ref": "#/$defs/LagDiffFeature"},
          {"$ref": "#/$defs/RankFeature"}
        ]
      }
    },
    "label": {
      "description": "Label definition (can use forward direction)",
      "oneOf": [
        {"$ref": "#/$defs/ExpressionFeature"},
        {"$ref": "#/$defs/LagDiffFeature"},
        {"$ref": "#/$defs/RollingWindowLabel"}
      ]
    }
  },
  "$defs": {
    "ExpressionFeature": {
      "type": "object",
      "additionalProperties": false,
      "required": ["name", "type", "expression", "source_table"],
      "properties": {
        "name": {"type": "string", "minLength": 1},
        "type": {"const": "expression"},
        "expression": {"$ref": "#/$defs/Expression"},
        "source_table": {"type": "string"}
      }
    },
    "RollingWindowFeature": {
      "type": "object",
      "additionalProperties": false,
      "required": ["name", "type", "base_column", "aggregation", "window", "source_table"],
      "properties": {
        "name": {"type": "string", "minLength": 1},
        "type": {"const": "rolling_window"},
        "base_column": {"type": "string"},
        "aggregation": {
          "type": "string",
          "enum": ["sum", "mean", "std", "min", "max", "count"]
        },
        "window": {
          "type": "object",
          "additionalProperties": false,
          "required": ["size", "time_column", "direction"],
          "properties": {
            "size": {
              "type": "string",
              "pattern": "^[0-9]+(s|m|h|d)$",
              "description": "Duration: e.g., '5m', '1h', '30s'"
            },
            "time_column": {"type": "string", "default": "ts_local_us"},
            "direction": {
              "type": "string",
              "enum": ["backward"],
              "description": "Only backward allowed for features (PIT-safety)"
            },
            "min_periods": {"type": "integer", "minimum": 0, "default": 1}
          }
        },
        "source_table": {"type": "string"}
      }
    },
    "RollingWindowLabel": {
      "type": "object",
      "additionalProperties": false,
      "required": ["name", "type", "base_column", "aggregation", "window", "source_table"],
      "properties": {
        "name": {"type": "string", "minLength": 1},
        "type": {"const": "rolling_window"},
        "base_column": {"type": "string"},
        "aggregation": {
          "type": "string",
          "enum": ["sum", "mean", "std", "min", "max", "count"]
        },
        "window": {
          "type": "object",
          "additionalProperties": false,
          "required": ["size", "time_column", "direction"],
          "properties": {
            "size": {
              "type": "string",
              "pattern": "^[0-9]+(s|m|h|d)$"
            },
            "time_column": {"type": "string", "default": "ts_local_us"},
            "direction": {
              "type": "string",
              "enum": ["backward", "forward"],
              "description": "Labels may be backward or forward."
            },
            "min_periods": {"type": "integer", "minimum": 0, "default": 1}
          }
        },
        "source_table": {"type": "string"}
      }
    },
    "LagDiffFeature": {
      "type": "object",
      "additionalProperties": false,
      "required": ["name", "type", "base_column", "lag", "source_table"],
      "properties": {
        "name": {"type": "string", "minLength": 1},
        "type": {"const": "lag_diff"},
        "base_column": {"type": "string"},
        "lag": {
          "type": "object",
          "additionalProperties": false,
          "required": ["offset", "time_column", "direction"],
          "properties": {
            "offset": {
              "type": "string",
              "pattern": "^[0-9]+(s|m|h|d)$"
            },
            "time_column": {"type": "string", "default": "ts_local_us"},
            "direction": {
              "type": "string",
              "enum": ["backward", "forward"],
              "description": "backward=lag (safe), forward=lead (only for labels)"
            }
          }
        },
        "diff_type": {
          "type": "string",
          "enum": ["none", "absolute", "pct_change", "log_return"],
          "default": "none"
        },
        "source_table": {"type": "string"}
      }
    },
    "RankFeature": {
      "type": "object",
      "additionalProperties": false,
      "required": ["name", "type", "base_column", "window", "source_table"],
      "properties": {
        "name": {"type": "string", "minLength": 1},
        "type": {"const": "rank"},
        "base_column": {"type": "string"},
        "partition_by": {
          "type": "array",
          "items": {"type": "string"},
          "default": ["symbol_id"]
        },
        "window": {
          "type": "string",
          "pattern": "^[0-9]+(s|m|h|d)$"
        },
        "direction": {
          "type": "string",
          "enum": ["backward"],
          "description": "Only backward allowed (PIT-safety)"
        },
        "method": {
          "type": "string",
          "enum": ["average", "min", "max", "dense", "ordinal"],
          "default": "dense"
        },
        "source_table": {"type": "string"}
      }
    },
    "Expression": {
      "type": "object",
      "oneOf": [
        {
          "required": ["column"],
          "additionalProperties": false,
          "properties": {
            "column": {"type": "string"}
          }
        },
        {
          "required": ["literal"],
          "additionalProperties": false,
          "properties": {
            "literal": {"type": "number"}
          }
        },
        {
          "required": ["op", "args"],
          "additionalProperties": false,
          "properties": {
            "op": {
              "type": "string",
              "enum": [
                "add", "subtract", "multiply", "divide",
                "log", "exp", "abs", "sqrt", "power",
                "map", "when"
              ]
            },
            "args": {
              "type": "array",
              "items": {"$ref": "#/$defs/Expression"}
            },
            "mapping": {
              "type": "object",
              "description": "For 'map' operation"
            }
          }
        }
      ]
    }
  }
}
```

### Supported Primitives

#### 1. Expression (Algebraic Operations)

**Purpose**: Combine columns with arithmetic/mathematical operations.

**Supported operations**:
- Arithmetic: `add`, `subtract`, `multiply`, `divide`
- Mathematical: `log`, `exp`, `abs`, `sqrt`, `power`
- Mapping: `map` (value replacement), `when` (conditional)

**Example**:
```json
{
  "name": "mid_price",
  "type": "expression",
  "expression": {
    "op": "divide",
    "args": [
      {
        "op": "add",
        "args": [{"column": "ask_px"}, {"column": "bid_px"}]
      },
      {"literal": 2.0}
    ]
  },
  "source_table": "quotes"
}
```

**Compiles to**:
```python
pl.col("ask_px").add(pl.col("bid_px")).truediv(2.0).alias("mid_price")
```

#### 2. Rolling Window (Time-Based Aggregation)

**Purpose**: Compute statistics over sliding time windows.

**Supported aggregations**: `sum`, `mean`, `std`, `min`, `max`, `count`

**PIT-safety constraint**: `direction` MUST be `"backward"`.

**Example**:
```json
{
  "name": "volume_5m",
  "type": "rolling_window",
  "base_column": "qty",
  "aggregation": "sum",
  "window": {
    "size": "5m",
    "time_column": "ts_local_us",
    "direction": "backward",
    "min_periods": 1
  },
  "source_table": "trades"
}
```

**Compiles to**:
```python
pl.col("qty").rolling_sum_by("ts_local_us", window_size="5m").alias("volume_5m")
```

#### 3. Lag/Diff (Time Shifts and Returns)

**Purpose**: Compute lagged values and returns.

**Supported diff types**: `none`, `absolute`, `pct_change`, `log_return`

**PIT-safety constraint**: Features MUST use `direction="backward"` (labels can use `"forward"`).

**Example**:
```json
{
  "name": "return_10s",
  "type": "lag_diff",
  "base_column": "mid_price",
  "lag": {
    "offset": "10s",
    "time_column": "ts_local_us",
    "direction": "backward"
  },
  "diff_type": "log_return",
  "source_table": "quotes"
}
```

**Compiles to**:
```python
(
  pl.col("mid_price")
  / lag_value("mid_price", time_col="ts_local_us", offset="10s", direction="backward")
).log().alias("return_10s")
```

Where `lag_value(...)` is compiler-generated logic that performs a PIT-safe time-based lag
(implemented with deterministic as-of alignment, not raw row offsets).

#### 4. Rank (Cross-Sectional Ordering)

**Purpose**: Compute percentile rank within a partition.

**Supported methods**: `average`, `min`, `max`, `dense`, `ordinal`

**PIT-safety constraint**: `direction="backward"` only.

**Example**:
```json
{
  "name": "volume_rank_1h",
  "type": "rank",
  "base_column": "qty",
  "partition_by": ["symbol_id"],
  "window": "1h",
  "direction": "backward",
  "method": "dense",
  "source_table": "trades"
}
```

## PIT-Safety Guarantees

### Design Principle

**All features must be computable from past data only.**

**Deterministic ordering is mandatory before time-aware ops**:
`(ts_local_us, file_id, file_line_number)` ascending.

### Enforcement Mechanism

```python
# Compiler validation rules
SAFETY_RULES = {
    "rolling_window": {
        "direction": {
            "allowed_in_features": ["backward"],
            "allowed_in_labels": ["backward", "forward"]
        }
    },
    "lag_diff": {
        "direction": {
            "allowed_in_features": ["backward"],  # Lag only
            "allowed_in_labels": ["backward", "forward"]  # Lag or Lead
        }
    },
    "rank": {
        "direction": {
            "allowed_in_features": ["backward"],
            "allowed_in_labels": ["backward"]
        }
    }
}
```

### Validation Examples

**✅ Valid feature (backward only)**:
```json
{
  "name": "volume_5m",
  "type": "rolling_window",
  "window": {"size": "5m", "direction": "backward"}
}
```

**❌ Invalid feature (forward direction)**:
```json
{
  "name": "future_volume",
  "type": "rolling_window",
  "window": {"size": "5m", "direction": "forward"}
}
```

**Compiler error**:
```
PITViolationError: Feature 'future_volume' uses forward-looking window.
Features must only use past data (direction="backward").
```

**✅ Valid label (forward allowed)**:
```json
{
  "name": "forward_return_30s",
  "type": "lag_diff",
  "lag": {"offset": "30s", "direction": "forward"},
  "diff_type": "pct_change"
}
```

## Integration with Agent Architecture

### Workflow Overview

```
User Request (Natural Language)
        ↓
Planning Stage (LLM)
  - Generates feature_spec.json
  - Uses declarative primitives
        ↓
Feature Compiler (Deterministic)
  - Validates JSON schema
  - Checks PIT-safety
  - Translates to Polars expressions
        ↓
Execution Stage (Deterministic)
  - Applies compiled expressions
  - Computes features + labels
  - Runs backtest
        ↓
Quality Gates
  - Verify no lookahead
  - Check reproducibility
```

### Planning Stage (LLM)

**LLM Prompt Template**:
```python
planning_prompt = f"""
You are a quantitative researcher designing a trading experiment.

Objective: {input.objective}

Available data:
- Tables: {feasibility_results.tables}
- Symbols: {feasibility_results.symbols}
- Time range: {input.time_range}

Your task: Generate a feature specification using the Feature DSL.

Feature DSL Primitives:
1. expression: Algebraic combinations (add, subtract, multiply, divide, log, etc.)
2. rolling_window: Time-based aggregations (sum, mean, std, min, max, count)
3. lag_diff: Time shifts and returns (lag/lead + differencing)
4. rank: Cross-sectional ranking within windows

CRITICAL RULES:
1. ALL features must use direction="backward" (only past data)
2. Labels can use direction="forward" (predicting future)
3. Use ts_local_us as time_column (PIT timeline)
4. Window sizes for {input.persona_mode} mode: max {max_window}

Output JSON format:
{{
  "feature_spec_version": "1.0",
  "features": [
    {{
      "name": "feature_name",
      "type": "expression|rolling_window|lag_diff|rank",
      ...
    }}
  ],
  "label": {{
    "name": "label_name",
    "type": "lag_diff",
    ...
  }}
}}

Example for "test if bid-ask spread predicts returns":
{{
  "feature_spec_version": "1.0",
  "features": [
    {{
      "name": "spread",
      "type": "expression",
      "expression": {{
        "op": "subtract",
        "args": [{{"column": "ask_px"}}, {{"column": "bid_px"}}]
      }},
      "source_table": "quotes"
    }}
  ],
  "label": {{
    "name": "forward_return_5s",
    "type": "lag_diff",
    "base_column": "mid_price",
    "lag": {{"offset": "5s", "direction": "forward"}},
    "diff_type": "log_return",
    "source_table": "quotes"
  }}
}}

Now generate the feature spec for the objective above.
"""

llm_response = llm.generate(planning_prompt)
feature_spec = json.loads(llm_response)
```

### Feature Compiler Stage

```python
from pointline.agents.feature_compiler import FeatureCompiler, PITViolationError

compiler = FeatureCompiler(mode=input.persona_mode)

try:
    # Validate + compile
    compiled = compiler.compile(feature_spec)

    # Success: add to experiment spec
    experiment_spec["features"] = compiled.features
    experiment_spec["label"] = compiled.label

except PITViolationError as e:
    # PIT violation detected - reject immediately
    return Output(
        run={"status": "failed"},
        decision={
            "status": "reject",
            "rationale": f"Feature specification violates PIT-safety: {e}"
        },
        quality_gates={
            "lookahead_check": {
                "passed": False,
                "evidence": str(e)
            }
        }
    )

except ValueError as e:
    # Invalid spec (e.g., unsupported aggregation)
    # Return to LLM for refinement
    llm_refinement_prompt = f"""
The feature spec you generated has an error:

Error: {e}

Original spec:
{json.dumps(feature_spec, indent=2)}

Please fix the error and regenerate the feature spec.
"""

    # Retry with LLM refinement
    refined_spec = llm.generate(llm_refinement_prompt)
    feature_spec = json.loads(refined_spec)
    compiled = compiler.compile(feature_spec)  # Retry compilation
```

### Execution Stage

```python
# Load data
quotes = query.quotes(exchange, symbol, start, end, decoded=True).lazy()
trades = query.trades(exchange, symbol, start, end, decoded=True).lazy()

# Apply compiled features
features_df = compiled.apply_features(
    quotes.sort(["ts_local_us", "file_id", "file_line_number"])
)

# Apply compiled label
label_df = compiled.apply_label(
    quotes.sort(["ts_local_us", "file_id", "file_line_number"])
)

# PIT-safe join (as-of backward)
dataset = features_df.join_asof(
    label_df,
    on="ts_local_us",
    strategy="backward"
).collect()

# Now dataset has columns: [ts_local_us, symbol_id, spread, forward_return_5s, ...]
```

## Mode-Specific Constraints

### HFT Mode

```python
HFT_POLICY = {
    "max_feature_window": timedelta(minutes=30),
    "max_lag": timedelta(minutes=5),
    "allowed_aggregations": ["sum", "mean", "std", "min", "max", "count"],
    "require_microsecond_precision": True
}
```

**Enforced by compiler**:
```python
if mode == "HFT":
    if parse_duration(feature["window"]["size"]) > timedelta(minutes=30):
        raise ValueError(
            f"Feature '{feature['name']}': Window size {feature['window']['size']} "
            f"exceeds HFT limit of 30 minutes"
        )
```

### MFT Mode

```python
MFT_POLICY = {
    "max_feature_window": timedelta(days=30),
    "max_lag": timedelta(days=7),
    "allowed_aggregations": ["sum", "mean", "std", "min", "max", "count"],
    "require_regime_analysis": True
}
```

Note: `quantile` is planned for a later DSL version, not in v1.0.

## Error Handling and LLM Refinement

### Common Errors and Fixes

#### Error 1: Missing Required Field

**LLM generates**:
```json
{
  "name": "volume_5m",
  "type": "rolling_window",
  "window": {"size": "5m"}  // Missing "direction"
}
```

**Compiler error**:
```
ValidationError: Feature 'volume_5m': Missing required field 'direction' in window.
Required fields: size, time_column, direction
```

**LLM fixes**:
```json
{
  "name": "volume_5m",
  "type": "rolling_window",
  "window": {"size": "5m", "direction": "backward", "time_column": "ts_local_us"}
}
```

#### Error 2: Invalid Aggregation

**LLM generates**:
```json
{
  "type": "rolling_window",
  "aggregation": "median"  // Not supported yet
}
```

**Compiler error**:
```
ValueError: Unsupported aggregation 'median'.
Allowed: ['sum', 'mean', 'std', 'min', 'max', 'count']
```

**LLM fixes**:
```json
{
  "type": "rolling_window",
  "aggregation": "mean"  // Falls back to mean
}
```

#### Error 3: PIT Violation

**LLM generates**:
```json
{
  "name": "future_volume",
  "type": "rolling_window",
  "window": {"direction": "forward"}  // Forward-looking feature
}
```

**Compiler error**:
```
PITViolationError: Feature 'future_volume' uses forward-looking window.
Features must only use past data (direction="backward").
Did you mean to define a label instead?
```

**LLM fixes**:
```json
// Move to label section
"label": {
  "name": "future_volume",
  "type": "rolling_window",
  "window": {"direction": "forward"}  // OK for labels
}
```

## Performance Considerations

### Compilation Cost

**One-time cost per experiment**:
- Parse JSON: ~1ms
- Validate schema: ~5ms
- Check PIT-safety: ~10ms
- Generate Polars expressions: ~20ms

**Total**: ~40ms (negligible compared to data loading/computation)

### Runtime Performance

**Compiled Polars expressions are optimal**:
- No Python overhead (uses Rust engine)
- Parallelized automatically
- Memory-efficient (LazyFrame evaluation)

**Comparison**:
```python
# Declarative DSL (compiled to Polars)
df.with_columns([
  pl.col("qty").rolling_sum_by("ts_local_us", window_size="5m")
])
# → Rust engine, ~100MB/s throughput

# Python UDF (if LLM generated code)
df.with_columns([
  pl.col("qty").map_elements(lambda x: custom_rolling_sum(x))
])
# → Python overhead, ~10MB/s throughput (10x slower)
```

## Extension Points

### Adding New Primitives

```python
# In feature_compiler.py

class FeatureCompiler:
    def _compile_feature(self, feature: Dict) -> pl.Expr:
        feature_type = feature["type"]

        if feature_type == "expression":
            return self._compile_expression(feature)
        elif feature_type == "rolling_window":
            return self._compile_rolling(feature)
        elif feature_type == "lag_diff":
            return self._compile_lag_diff(feature)
        elif feature_type == "rank":
            return self._compile_rank(feature)

        # NEW: Add custom primitive
        elif feature_type == "ewma":  # Exponentially weighted moving average
            return self._compile_ewma(feature)

        else:
            raise ValueError(f"Unsupported feature type: {feature_type}")

    def _compile_ewma(self, feature: Dict) -> pl.Expr:
        """Compile EWMA feature."""
        base = pl.col(feature["base_column"])
        halflife = feature["halflife"]

        return base.ewm_mean(half_life=halflife).alias(feature["name"])
```

### Custom Functions

```python
# For complex features not covered by primitives
CUSTOM_FUNCTIONS = {
    "order_book_imbalance": compute_order_book_imbalance,
    "volume_weighted_spread": compute_vwap_spread,
}

# In DSL
{
  "type": "custom",
  "function_name": "order_book_imbalance",
  "params": {"levels": 5}
}

# Compiler calls pre-registered function
def _compile_custom(self, feature: Dict) -> pl.Expr:
    func_name = feature["function_name"]
    if func_name not in CUSTOM_FUNCTIONS:
        raise ValueError(f"Unknown custom function: {func_name}")

    func = CUSTOM_FUNCTIONS[func_name]
    return func(**feature["params"])
```

## Conclusion

### Why Declarative DSL Is The Sweet Spot

| Dimension | Score | Reasoning |
|-----------|-------|-----------|
| **LLM Success Rate** | ⭐⭐⭐⭐⭐ | LLMs excel at JSON generation |
| **Safety** | ⭐⭐⭐⭐⭐ | Structural PIT-safety validation |
| **Expressiveness** | ⭐⭐⭐⭐ | Covers 90% of quant features |
| **Performance** | ⭐⭐⭐⭐⭐ | Compiles to optimal Polars code |
| **Maintainability** | ⭐⭐⭐⭐ | Versionable, auditable, testable |
| **Error Recovery** | ⭐⭐⭐⭐⭐ | Clear errors → LLM self-correction |

### Implementation Roadmap

**Phase 1: Core Primitives** (Week 1-2)
- Implement FeatureCompiler with 4 core types
- JSON schema validation
- PIT-safety checks
- Unit tests

**Phase 2: Integration** (Week 3)
- Add to agent architecture as Stage 3.5
- LLM prompt templates
- Error handling and refinement loop

**Phase 3: Extensions** (Week 4+)
- Additional primitives (EWMA, quantile, etc.)
- Custom function registry
- Performance optimization
- Production monitoring

### Success Metrics

**Target KPIs**:
- LLM feature spec success rate: >90% on first attempt
- PIT violation detection: 100% (zero false negatives)
- Compilation time: <100ms per experiment
- Feature coverage: >90% of common quant features

This design provides the optimal balance of **LLM-friendliness**, **safety**, and **expressiveness** for automated quantitative research.
