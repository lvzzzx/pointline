# Liquidations + OI Feature Spec (Proposed)

This document proposes how to support liquidation-driven MFT features in the Feature DSL and Quant Agent pipeline.

## Objective

Enable PIT-safe feature generation for common crypto derivatives signals:
- liquidation pressure
- liquidation intensity
- liquidation imbalance (long vs short pressure)
- liquidation-to-open-interest ratios

## A. DSL Examples (Concrete)

All feature windows must be backward-looking. Labels may be forward-looking.

### A1) Liquidation Quantity and Count

```json
{
  "feature_spec_version": "1.0",
  "features": [
    {
      "name": "liq_qty_5m",
      "type": "rolling_window",
      "base_column": "qty_int",
      "aggregation": "sum",
      "window": {
        "size": "5m",
        "time_column": "ts_local_us",
        "direction": "backward"
      },
      "source_table": "liquidations"
    },
    {
      "name": "liq_count_5m",
      "type": "rolling_window",
      "base_column": "liq_id",
      "aggregation": "count",
      "window": {
        "size": "5m",
        "time_column": "ts_local_us",
        "direction": "backward"
      },
      "source_table": "liquidations"
    }
  ],
  "label": {
    "name": "fwd_mark_return_10m",
    "type": "lag_diff",
    "base_column": "mark_px",
    "lag": {
      "offset": "10m",
      "time_column": "ts_local_us",
      "direction": "forward"
    },
    "diff_type": "log_return",
    "source_table": "derivative_ticker"
  }
}
```

### A2) Long/Short Liquidation Imbalance

Requires side mapping (`side` enum -> sign) in expression map primitive.

```json
{
  "feature_spec_version": "1.0",
  "features": [
    {
      "name": "liq_signed_qty_5m",
      "type": "rolling_window",
      "base_column": "liq_signed_qty",
      "aggregation": "sum",
      "window": {
        "size": "5m",
        "time_column": "ts_local_us",
        "direction": "backward"
      },
      "source_table": "liquidations"
    },
    {
      "name": "liq_imbalance_5m",
      "type": "expression",
      "expression": {
        "op": "divide",
        "args": [
          {"column": "liq_signed_qty_5m"},
          {
            "op": "add",
            "args": [
              {"column": "liq_qty_5m"},
              {"literal": 1e-9}
            ]
          }
        ]
      },
      "source_table": "liquidations"
    }
  ]
}
```

Notes:
- `liq_signed_qty` is a compiler-generated derived column from `qty_int` and `side` mapping.
- Keep integer quantity until the final scaling/ratio stage where possible.

### A3) Liquidation-to-OI Pressure

PIT-align liquidation features to derivative ticker (`open_interest`) by `ts_local_us` with backward as-of join.

```json
{
  "feature_spec_version": "1.0",
  "features": [
    {
      "name": "liq_to_oi_5m",
      "type": "expression",
      "expression": {
        "op": "divide",
        "args": [
          {"column": "liq_qty_5m"},
          {
            "op": "add",
            "args": [
              {"column": "open_interest"},
              {"literal": 1e-9}
            ]
          }
        ]
      },
      "source_table": "joined_liq_ticker"
    }
  ]
}
```

## B. Required DSL Schema Changes

These are required to make liquidations first-class in the contract.

1. Add `liquidations` to allowed table enums wherever `source_table` is constrained.
2. Add a `derived_columns` section (optional) for safe side/sign mappings.
3. Add explicit `join_plan` section for cross-table feature composition.

### B1) Feature DSL additions (v1.1 suggestion)

```json
{
  "join_plan": [
    {
      "name": "joined_liq_ticker",
      "left_table": "liquidations",
      "right_table": "derivative_ticker",
      "on": "ts_local_us",
      "strategy": "backward",
      "partition_by": ["symbol_id"]
    }
  ],
  "derived_columns": [
    {
      "name": "liq_signed_qty",
      "source_table": "liquidations",
      "expression": {
        "op": "multiply",
        "args": [
          {"column": "qty_int"},
          {"op": "map", "column": "side", "mapping": {"0": 1, "1": -1}}
        ]
      }
    }
  ]
}
```

## C. Required Quant Agent Contract Changes

### C1) Input schema changes (`quant_research_input.v1.json` -> v1.1)

- Add `liquidations` to `data_requirements.tables` enum.
- Add optional `join_requirements` for cross-table features.
- Add optional `normalization` settings for integer scaling.

Example:

```json
{
  "data_requirements": {
    "tables": ["liquidations", "derivative_ticker", "trades"],
    "probe_window_minutes": 5,
    "min_probe_rows": 1
  }
}
```

### C2) Output schema changes (`quant_research_output.v1.json` -> v1.1)

- Add `join_evidence` under `data_feasibility`.
- Ensure `coverage_checks.table` enum includes `liquidations`.
- Add optional `normalization_evidence` in `quality_gates`.

## D. Required Research API Changes

To make this usable from quant workflows, add liquidations parity with existing query/core helpers.

1. `pointline/research/core.py`
- Add `load_liquidations(...)`.
- Optional: `load_liquidations_decoded(...)` (if decode helpers are available).

2. `pointline/research/query.py`
- Add `query.liquidations(exchange, symbol, start, end, ...)` with symbol auto-resolution.

3. `pointline/research/__init__.py`
- Export new core/query access points.

4. `pointline/research/discovery.py` + config metadata
- Include `liquidations` in supported table metadata for applicable exchanges.

5. Feature pipeline alignment
- Add a helper for PIT as-of join between liquidation streams and ticker/OI streams.

## E. PIT and Determinism Rules (Must-Have)

For liquidation/OI features:
- Use `ts_local_us` timeline by default.
- Sort deterministically before time ops: `ts_local_us`, `file_id`, `file_line_number`.
- Use backward as-of joins for cross-table alignment.
- For labels only, forward direction is allowed.

## F. Rollout Plan

1. Contract update (DSL + input/output schemas) as v1.1.
2. API parity (`load_liquidations` + `query.liquidations`).
3. Compiler support for `derived_columns` + `join_plan`.
4. Validation suite:
- PIT violation checks
- tie-break determinism checks
- feature regression tests on known windows
5. Add examples to docs and skill references.

## G. Acceptance Criteria

- Agent can generate and compile liquidation/OI specs without manual edits.
- All generated liquidation features pass PIT checks.
- End-to-end run produces stable values across repeated executions.
- Output includes feasibility evidence for both liquidations and ticker tables.
