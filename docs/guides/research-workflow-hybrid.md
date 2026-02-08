# Research Workflow Hybrid (v2)

`research.workflow(request)` orchestrates multi-stage research DAGs by composing existing pipeline modes:

- `event_joined`
- `tick_then_bar`
- `bar_then_feature`

This keeps `research.pipeline(request)` single-mode and adds a contract-first workflow layer for hybrid runs.

## When to use

Use `workflow(...)` when your hypothesis requires stage composition, for example:

1. tick-level microstructure extraction
2. bar-level normalization or transforms
3. event-level alignment for labels or cross-stream context

## Source references

Each stage source uses explicit references:

- `base:<source_name>`
- `artifact:<stage_id>:<output_name>`

## Fail-fast gates

Workflow executes stages in DAG order.
If a stage fails critical gates, the workflow stops immediately and returns `decision.status="reject"`.

## Minimal shape

```python
workflow_request = {
    "schema_version": "2.0",
    "request_id": "wf-001",
    "workflow_id": "hybrid-micro-001",
    "base_sources": [...],
    "stages": [...],
    "final_stage_id": "s3",
    "constraints": {"fail_fast": True},
    "artifacts": {"include_artifacts": False},
}
```

## Canonical template: OFI sum per bar -> rolling zscore -> event alignment

```python
from pointline.research import workflow

request = {
    "schema_version": "2.0",
    "request_id": "wf-ofi-z-001",
    "workflow_id": "ofi-zscore-hybrid",
    "base_sources": [
        {
            "name": "quotes",
            "inline_rows": [
                # quote ticks with ts_local_us/exchange_id/symbol_id and book top columns
            ],
        },
        {
            "name": "trades",
            "inline_rows": [
                # trade ticks for final event alignment/labels
            ],
        },
    ],
    "stages": [
        {
            "stage_id": "s1",
            "mode": "tick_then_bar",
            "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
            "spine": {
                "type": "clock",
                "step_ms": 60000,
                "start_ts_us": 0,
                "end_ts_us": 120000000,
            },
            "sources": [{"name": "quotes", "ref": "base:quotes"}],
            "operators": [
                {
                    "name": "ofi_1m",
                    "output_name": "ofi_1m",
                    "stage": "feature_then_aggregate",
                    "agg": "ofi_sum",
                    "source_column": "bid_px_int",
                    "required_columns": ["bid_px_int", "ask_px_int", "bid_sz_int", "ask_sz_int"],
                    "mode_allowlist": ["HFT"],
                    "pit_policy": {"feature_direction": "backward_only"},
                    "determinism_policy": {"required_sort": ["exchange_id", "symbol_id", "ts_local_us"]},
                    "impl_ref": "pointline.research.resample.aggregations.microstructure.compute_ofi",
                    "version": "2.0",
                }
            ],
            "labels": [],
            "evaluation": {"metrics": ["row_count"], "split_method": "fixed"},
            "constraints": {
                "pit_timeline": "ts_local_us",
                "forbid_lookahead": True,
                "cost_model": {"fees_bps": 1.0, "slippage_bps": 2.0},
            },
            "outputs": [{"name": "ofi_bars"}],
        },
        {
            "stage_id": "s2",
            "mode": "bar_then_feature",
            "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
            "spine": {
                "type": "clock",
                "step_ms": 60000,
                "start_ts_us": 0,
                "end_ts_us": 120000000,
            },
            "sources": [{"name": "ofi_bars", "ref": "artifact:s1:ofi_bars"}],
            "operators": [
                # Register a typed operator for rolling zscore in your registry,
                # then reference it here (example name shown below).
                {
                    "name": "ofi_z_20",
                    "output_name": "ofi_z_20",
                    "stage": "aggregate_then_feature",
                    "agg": "rolling_zscore_20",
                    "source_column": "ofi_1m_mean",
                    "required_columns": ["ofi_1m_mean"],
                    "mode_allowlist": ["MFT"],
                    "pit_policy": {"feature_direction": "backward_only"},
                    "determinism_policy": {
                        "required_sort": ["exchange_id", "symbol_id", "ts_local_us"],
                        "stateful": True,
                        "partition_by": ["exchange_id", "symbol_id"],
                    },
                    "impl_ref": "pointline.research.resample.aggregations.custom.rolling_zscore_20",
                    "version": "2.0",
                }
            ],
            "labels": [],
            "evaluation": {"metrics": ["row_count"], "split_method": "fixed"},
            "constraints": {
                "pit_timeline": "ts_local_us",
                "forbid_lookahead": True,
                "cost_model": {"fees_bps": 1.0, "slippage_bps": 2.0},
            },
            "outputs": [{"name": "ofi_norm"}],
        },
        {
            "stage_id": "s3",
            "mode": "event_joined",
            "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
            "spine": {"type": "trades", "source": "trades"},
            "sources": [
                {"name": "trades", "ref": "base:trades"},
                {"name": "ofi_norm", "ref": "artifact:s2:ofi_norm"},
            ],
            "operators": [],
            "labels": [],
            "evaluation": {"metrics": ["row_count"], "split_method": "fixed"},
            "constraints": {
                "pit_timeline": "ts_local_us",
                "forbid_lookahead": True,
                "cost_model": {"fees_bps": 1.0, "slippage_bps": 2.0},
            },
            "outputs": [{"name": "frame"}],
        },
    ],
    "final_stage_id": "s3",
    "constraints": {"fail_fast": True},
    "artifacts": {"include_artifacts": False},
}

output = workflow(request)
```

## Operational notes

1. Keep one canonical timeline policy (`ts_local_us`, half-open windows).
2. Keep forward-looking logic in labels only.
3. Partition stateful transforms by `exchange_id,symbol_id`.
4. Prefer registry-managed operators over ad-hoc transforms.
