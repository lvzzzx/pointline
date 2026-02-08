"""North-star research pipeline v2.

This module provides the contract-first execution path:
    research.pipeline(request) -> output
"""

from __future__ import annotations

import hashlib
import json
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from pointline.research import core as research_core
from pointline.research.contracts import (
    validate_quant_research_input_v2,
    validate_quant_research_output_v2,
)
from pointline.research.features.core import pit_align
from pointline.research.resample import AggregateConfig, AggregationRegistry, AggregationSpec
from pointline.research.resample.aggregate import aggregate
from pointline.research.resample.bucket_assignment import assign_to_buckets
from pointline.research.resample.rollups import (
    FeatureRollupRegistry,
    is_builtin_feature_rollup,
    normalize_feature_rollup_names,
)


class PipelineError(ValueError):
    """Raised for invalid v2 pipeline execution requests."""


@dataclass(frozen=True)
class _ExecutionArtifacts:
    coverage_checks: list[dict[str, Any]]
    probe_checks: list[dict[str, Any]]
    pit_violations: int
    unassigned_rows: int
    reproducibility_passed: bool
    output_hash: str
    rerun_output_hash: str
    reproducibility_evidence: str


def pipeline(request: dict[str, Any]) -> dict[str, Any]:
    """Execute research pipeline request (v2 contract)."""
    started_at = _utc_now_iso()
    validate_quant_research_input_v2(request)

    compiled = compile_request(request)
    frame, runtime = execute_compiled(compiled)
    gates = evaluate_quality_gates(compiled, runtime)
    metrics = compute_metrics(frame, compiled["evaluation"]["metrics"])
    status = "success"

    completed_at = _utc_now_iso()
    output = {
        "schema_version": "2.0",
        "request_id": compiled["request_id"],
        "run": {
            "run_id": compiled["run_id"],
            "started_at": started_at,
            "completed_at": completed_at,
            "status": status,
            "mode": compiled["mode"],
        },
        "resolved_plan": {
            "mode": compiled["mode"],
            "timeline": compiled["timeline"],
            "spine": compiled["spine"],
            "operators": compiled["operators"],
            "config_hash": compiled["config_hash"],
        },
        "data_feasibility": {
            "coverage_checks": runtime.coverage_checks,
            "probe_checks": runtime.probe_checks,
        },
        "quality_gates": gates,
        "results": {
            "row_count": frame.height,
            "columns": frame.columns,
            "metrics": metrics,
            "preview": frame.head(10).to_dicts(),
        },
        "decision": build_decision(gates, runtime),
        "artifacts": {
            "config_hash": compiled["config_hash"],
            "paths": emit_artifacts(compiled, gates, frame),
            "gate_metrics": {
                "pit_violations": runtime.pit_violations,
                "unassigned_rows": runtime.unassigned_rows,
                "forbid_lookahead": compiled["constraints"].get("forbid_lookahead", True),
                "cost_model": compiled["constraints"].get("cost_model", {}),
            },
        },
    }

    validate_quant_research_output_v2(output)
    return output


def compile_request(request: dict[str, Any]) -> dict[str, Any]:
    """Compile request into a normalized, decision-complete execution plan."""
    compiled = deepcopy(request)
    compiled.setdefault("timeline", {})
    compiled["timeline"].setdefault("time_col", "ts_local_us")
    compiled["timeline"].setdefault("timezone", "UTC")

    compiled.setdefault("spine", {})
    compiled["spine"].setdefault("type", "clock")
    compiled["spine"].setdefault("max_rows", 5_000_000)

    normalized_ops: list[dict[str, Any]] = []
    for op in compiled["operators"]:
        normalized_ops.append(_normalize_operator(op))
    compiled["operators"] = normalized_ops

    compiled["run_id"] = f"run-{uuid.uuid4().hex[:12]}"
    compiled["config_hash"] = _stable_hash(
        {
            "request_id": compiled["request_id"],
            "mode": compiled["mode"],
            "timeline": compiled["timeline"],
            "spine": compiled["spine"],
            "operators": compiled["operators"],
            "labels": compiled["labels"],
            "evaluation": compiled["evaluation"],
            "constraints": compiled["constraints"],
            "sources": _normalized_source_fingerprints(compiled["sources"]),
        }
    )

    return compiled


def execute_compiled(compiled: dict[str, Any]) -> tuple[pl.DataFrame, _ExecutionArtifacts]:
    """Execute a compiled request across all supported modes."""
    timeline_col = compiled["timeline"]["time_col"]
    sources, coverage_checks, probe_checks = _load_sources(compiled["sources"], timeline_col)
    return execute_compiled_with_sources(
        compiled,
        sources,
        coverage_checks=coverage_checks,
        probe_checks=probe_checks,
    )


def execute_compiled_with_sources(
    compiled: dict[str, Any],
    sources: dict[str, pl.LazyFrame],
    *,
    coverage_checks: list[dict[str, Any]] | None = None,
    probe_checks: list[dict[str, Any]] | None = None,
) -> tuple[pl.DataFrame, _ExecutionArtifacts]:
    """Execute a compiled request with pre-loaded source frames.

    This is used by higher-level orchestrators (for example workflow DAG execution)
    that resolve sources outside of the single-request input contract.
    """
    coverage_checks = coverage_checks or []
    probe_checks = probe_checks or []

    result, pit_violations, unassigned = _execute_mode(compiled, sources)
    result = _apply_labels(result, compiled.get("labels", []))
    frame = result.collect()

    # Mandatory reproducibility rerun check on identical compiled inputs.
    rerun_result, _, _ = _execute_mode(compiled, sources)
    rerun_result = _apply_labels(rerun_result, compiled.get("labels", []))
    rerun_frame = rerun_result.collect()

    output_hash = _hash_output_frame(frame)
    rerun_output_hash = _hash_output_frame(rerun_frame)
    reproducibility_passed = output_hash == rerun_output_hash
    reproducibility_evidence = (
        "Output hash matched deterministic rerun"
        if reproducibility_passed
        else (
            "Output hash mismatch on deterministic rerun: " f"{output_hash} != {rerun_output_hash}"
        )
    )

    return frame, _ExecutionArtifacts(
        coverage_checks=coverage_checks,
        probe_checks=probe_checks,
        pit_violations=pit_violations,
        unassigned_rows=unassigned,
        reproducibility_passed=reproducibility_passed,
        output_hash=output_hash,
        rerun_output_hash=rerun_output_hash,
        reproducibility_evidence=reproducibility_evidence,
    )


def _execute_mode(
    compiled: dict[str, Any],
    sources: dict[str, pl.LazyFrame],
) -> tuple[pl.LazyFrame, int, int]:
    mode = compiled["mode"]
    if mode == "bar_then_feature":
        return _execute_bar_then_feature(compiled, sources)
    if mode == "tick_then_bar":
        return _execute_tick_then_bar(compiled, sources)
    if mode == "event_joined":
        return _execute_event_joined(compiled, sources)
    raise PipelineError(f"Unsupported mode: {mode}")


def evaluate_quality_gates(
    compiled: dict[str, Any], runtime: _ExecutionArtifacts
) -> dict[str, Any]:
    """Evaluate mandatory quality gates."""
    violating_forward_ops = []
    for op in compiled["operators"]:
        feature_dir = op.get("pit_policy", {}).get("feature_direction")
        if feature_dir == "forward":
            violating_forward_ops.append(op["name"])

    forbid_lookahead = bool(compiled["constraints"].get("forbid_lookahead", True))
    lookahead_passed = len(violating_forward_ops) == 0 or not forbid_lookahead
    if not violating_forward_ops:
        lookahead_evidence = "All operators marked backward-only for features"
    elif not forbid_lookahead:
        lookahead_evidence = (
            "Forward feature operators present but allowed by "
            "constraints.forbid_lookahead=false: "
            f"{violating_forward_ops}"
        )
    else:
        lookahead_evidence = f"Forward feature operators found: {violating_forward_ops}"

    partition_violations = 0
    for op in compiled["operators"]:
        deterministic_policy = op.get("determinism_policy", {})
        if deterministic_policy.get("stateful"):
            partition_by = deterministic_policy.get("partition_by", [])
            required = {"exchange_id", "symbol_id"}
            if not required.issubset(set(partition_by)):
                partition_violations += 1

    pit_passed = runtime.pit_violations == 0
    partition_passed = partition_violations == 0
    reproducibility_passed = runtime.reproducibility_passed

    failed = []
    if not lookahead_passed:
        failed.append("lookahead_check")
    if not pit_passed:
        failed.append("pit_ordering_check")
    if not partition_passed:
        failed.append("partition_safety_check")
    if not reproducibility_passed:
        failed.append("reproducibility_check")

    return {
        "lookahead_check": {
            "passed": lookahead_passed,
            "evidence": lookahead_evidence,
            "forbid_lookahead": forbid_lookahead,
        },
        "pit_ordering_check": {
            "passed": pit_passed,
            "violations": runtime.pit_violations,
            "timeline": compiled["constraints"]["pit_timeline"],
        },
        "partition_safety_check": {
            "passed": partition_passed,
            "violations": partition_violations,
        },
        "reproducibility_check": {
            "passed": reproducibility_passed,
            "input_hash": compiled["config_hash"],
            "output_hash": runtime.output_hash,
            "rerun_output_hash": runtime.rerun_output_hash,
            "evidence": runtime.reproducibility_evidence,
        },
        "failed_gates": failed,
    }


def compute_metrics(frame: pl.DataFrame, metric_names: list[str]) -> list[dict[str, Any]]:
    """Compute lightweight built-in metrics for output payload."""
    metrics: list[dict[str, Any]] = []
    for name in metric_names:
        if name == "row_count":
            value = float(frame.height)
        elif name == "non_null_ratio":
            if frame.width == 0 or frame.height == 0:
                value = 0.0
            else:
                non_null = sum(
                    frame.select(pl.col(col).is_not_null().sum()).item() for col in frame.columns
                )
                value = float(non_null) / float(frame.height * frame.width)
        else:
            # For unsupported metrics, emit NaN-compatible sentinel as 0.0
            value = 0.0
        metrics.append({"name": name, "value": value})
    return metrics


def build_decision(gates: dict[str, Any], runtime: _ExecutionArtifacts) -> dict[str, Any]:
    """Build decision payload from gates and feasibility evidence."""
    probe_failed = any(not item["passed"] for item in runtime.probe_checks)
    if gates["failed_gates"]:
        status = "reject"
        rationale = f"Critical quality gates failed: {gates['failed_gates']}"
    elif probe_failed:
        status = "revise"
        rationale = "Probe checks indicate incomplete data coverage"
    else:
        status = "go"
        rationale = "All critical gates passed and probe checks are healthy"

    return {
        "status": status,
        "rationale": rationale,
        "risks": [
            "Data drift risk outside evaluated window",
        ],
        "next_actions": [
            "Review artifacts and metrics",
            "Promote to downstream evaluation only if decision status is go",
        ],
    }


def emit_artifacts(
    compiled: dict[str, Any], gates: dict[str, Any], frame: pl.DataFrame
) -> list[str]:
    """Emit run artifacts to disk when configured; return emitted paths."""
    artifacts_cfg = compiled.get("artifacts", {})
    if not artifacts_cfg.get("include_artifacts", True):
        return []

    output_dir = artifacts_cfg.get("output_dir")
    if not output_dir:
        return []

    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)
    run_dir = base / compiled["run_id"]
    run_dir.mkdir(parents=True, exist_ok=True)

    plan_path = run_dir / "resolved_plan.json"
    gate_path = run_dir / "quality_gates.json"
    preview_path = run_dir / "result_preview.json"

    plan_payload = {
        "request_id": compiled["request_id"],
        "mode": compiled["mode"],
        "timeline": compiled["timeline"],
        "spine": compiled["spine"],
        "operators": compiled["operators"],
        "constraints": compiled["constraints"],
        "config_hash": compiled["config_hash"],
    }

    with plan_path.open("w", encoding="utf-8") as f:
        json.dump(plan_payload, f, indent=2, sort_keys=True)
    with gate_path.open("w", encoding="utf-8") as f:
        json.dump(gates, f, indent=2, sort_keys=True)
    with preview_path.open("w", encoding="utf-8") as f:
        json.dump(frame.head(100).to_dicts(), f, indent=2, sort_keys=True)

    return [str(plan_path), str(gate_path), str(preview_path)]


def _load_sources(
    source_specs: list[dict[str, Any]], timeline_col: str
) -> tuple[dict[str, pl.LazyFrame], list[dict[str, Any]], list[dict[str, Any]]]:
    sources: dict[str, pl.LazyFrame] = {}
    coverage_checks: list[dict[str, Any]] = []
    probe_checks: list[dict[str, Any]] = []

    for spec in source_specs:
        name = spec["name"]
        try:
            lf = _load_source(spec, timeline_col)
            sources[name] = lf
            coverage_checks.append({"source": name, "available": True, "reason": None})

            row_count = int(lf.select(pl.len().alias("_n")).collect()["_n"][0])
            probe_checks.append(
                {
                    "source": name,
                    "row_count": row_count,
                    "passed": row_count > 0,
                }
            )
        except Exception as exc:  # pragma: no cover - defensive path
            coverage_checks.append({"source": name, "available": False, "reason": str(exc)})
            probe_checks.append({"source": name, "row_count": 0, "passed": False})

    return sources, coverage_checks, probe_checks


def load_sources(
    source_specs: list[dict[str, Any]], timeline_col: str
) -> tuple[dict[str, pl.LazyFrame], list[dict[str, Any]], list[dict[str, Any]]]:
    """Public wrapper for loading source specs into LazyFrames."""
    return _load_sources(source_specs, timeline_col)


def _load_source(spec: dict[str, Any], timeline_col: str) -> pl.LazyFrame:
    if "inline_rows" in spec:
        return pl.LazyFrame(spec["inline_rows"])

    return research_core.scan_table(
        spec["table"],
        symbol_id=spec["symbol_id"],
        start_ts_us=spec["start_ts_us"],
        end_ts_us=spec["end_ts_us"],
        ts_col=timeline_col,
        columns=spec.get("columns"),
    )


def load_source(spec: dict[str, Any], timeline_col: str) -> pl.LazyFrame:
    """Public wrapper for loading a single source spec."""
    return _load_source(spec, timeline_col)


def _execute_bar_then_feature(
    compiled: dict[str, Any], sources: dict[str, pl.LazyFrame]
) -> tuple[pl.LazyFrame, int, int]:
    _validate_mode_operator_stages(
        compiled,
        mode="bar_then_feature",
        allowed_stages={"aggregate_then_feature", "bar_feature"},
    )
    return _execute_bucketed_aggregation(compiled, sources)


def _execute_bucketed_aggregation(
    compiled: dict[str, Any], sources: dict[str, pl.LazyFrame]
) -> tuple[pl.LazyFrame, int, int]:
    source_name = _primary_source_name(compiled)
    source = sources[source_name]
    spine = _build_spine(compiled, source)

    bucketed = assign_to_buckets(source, spine)
    pit_violations = _count_pit_violations(bucketed)
    unassigned = _count_unassigned_rows(bucketed)

    bucketed = bucketed.filter(pl.col("bucket_ts").is_not_null())
    agg_config = _aggregate_config_from_operators(compiled)
    result = aggregate(bucketed, agg_config, spine=spine)
    return result, pit_violations, unassigned


def _execute_tick_then_bar(
    compiled: dict[str, Any], sources: dict[str, pl.LazyFrame]
) -> tuple[pl.LazyFrame, int, int]:
    # TODO(v2): diverge execution plan with explicit tick-level pre-feature stage graph.
    # Current implementation shares bucketing/aggregation engine with bar_then_feature,
    # but enforces tick_then_bar operator staging requirements.
    _validate_mode_operator_stages(
        compiled,
        mode="tick_then_bar",
        allowed_stages={"feature_then_aggregate", "aggregate_then_feature"},
        require_any={"feature_then_aggregate"},
    )
    return _execute_bucketed_aggregation(compiled, sources)


def _execute_event_joined(
    compiled: dict[str, Any], sources: dict[str, pl.LazyFrame]
) -> tuple[pl.LazyFrame, int, int]:
    if compiled["operators"]:
        raise PipelineError(
            "event_joined mode does not support operators in v2. "
            "Provide operators only for bar_then_feature or tick_then_bar."
        )

    primary = _primary_source_name(compiled)
    primary_lf = sources[primary]

    spine = primary_lf.select(["ts_local_us", "exchange_id", "symbol_id"]).sort(
        ["exchange_id", "symbol_id", "ts_local_us"]
    )
    others = {name: lf for name, lf in sources.items() if name != primary}
    aligned = pit_align(spine, others)
    return aligned, 0, 0


def _build_spine(compiled: dict[str, Any], source: pl.LazyFrame) -> pl.LazyFrame:
    spine_cfg = compiled["spine"]
    spine_type = spine_cfg["type"]

    if spine_type != "clock":
        # For non-clock in v2 bootstrap, use event timestamps as-is.
        return source.select(["ts_local_us", "exchange_id", "symbol_id"]).sort(
            ["exchange_id", "symbol_id", "ts_local_us"]
        )

    step_ms = int(spine_cfg.get("step_ms", 60_000))
    if step_ms <= 0:
        raise PipelineError("spine.step_ms must be positive")

    step_us = step_ms * 1_000

    if "start_ts_us" in spine_cfg and "end_ts_us" in spine_cfg:
        start_ts_us = int(spine_cfg["start_ts_us"])
        end_ts_us = int(spine_cfg["end_ts_us"])
    else:
        bounds = source.select(
            [
                pl.col("ts_local_us").min().alias("_min"),
                pl.col("ts_local_us").max().alias("_max"),
            ]
        ).collect()
        start_ts_us = int(bounds["_min"][0])
        end_ts_us = int(bounds["_max"][0])

    first_end = ((start_ts_us // step_us) + 1) * step_us
    timestamps: list[int] = []
    current = first_end
    while current <= end_ts_us:
        timestamps.append(current)
        current += step_us

    symbols = source.select(["exchange_id", "symbol_id"]).unique().collect()
    if not timestamps:
        return pl.LazyFrame(
            schema={
                "ts_local_us": pl.Int64,
                "exchange_id": symbols.schema.get("exchange_id", pl.Int64),
                "symbol_id": symbols.schema.get("symbol_id", pl.Int64),
            }
        )

    time_df = pl.DataFrame({"ts_local_us": timestamps})
    spine = symbols.lazy().join(time_df.lazy(), how="cross")
    return spine.sort(["exchange_id", "symbol_id", "ts_local_us"])


def _aggregate_config_from_operators(compiled: dict[str, Any]) -> AggregateConfig:
    aggregations: list[AggregationSpec] = []
    research_mode = _research_mode_from_mode(compiled["mode"])
    for op in compiled["operators"]:
        if "agg" not in op:
            continue

        # Typed-registry only policy.
        if op["agg"] not in AggregationRegistry._registry:
            raise PipelineError(f"Operator agg not registered: {op['agg']}")

        source_column = op.get("source_column")
        if not source_column:
            raise PipelineError(f"Operator requires source_column: {op['name']}")

        feature_rollups = op.get("feature_rollups")
        if feature_rollups and op.get("stage") != "feature_then_aggregate":
            raise PipelineError(
                f"feature_rollups is only valid for feature_then_aggregate operators: {op['name']}"
            )
        feature_rollup_params = op.get("feature_rollup_params")
        if feature_rollup_params and op.get("stage") != "feature_then_aggregate":
            raise PipelineError(
                "feature_rollup_params is only valid for feature_then_aggregate "
                f"operators: {op['name']}"
            )
        if feature_rollup_params and not feature_rollups:
            raise PipelineError(
                f"feature_rollup_params requires feature_rollups for operator: {op['name']}"
            )

        normalized_rollups = (
            normalize_feature_rollup_names(feature_rollups) if feature_rollups else None
        )
        if normalized_rollups:
            params_by_rollup = feature_rollup_params or {}
            unknown_param_rollups = sorted(set(params_by_rollup) - set(normalized_rollups))
            if unknown_param_rollups:
                raise PipelineError(
                    f"feature_rollup_params contains unknown rollups for {op['name']}: "
                    f"{unknown_param_rollups}"
                )
            for rollup in normalized_rollups:
                if is_builtin_feature_rollup(rollup):
                    continue
                if not FeatureRollupRegistry.exists(rollup):
                    raise PipelineError(f"Feature rollup not registered: {rollup}")
                FeatureRollupRegistry.validate_for_mode(rollup, research_mode)
                FeatureRollupRegistry.validate_semantic(rollup, op.get("semantic_type"))
                FeatureRollupRegistry.validate_params(rollup, params_by_rollup.get(rollup))

        aggregations.append(
            AggregationSpec(
                name=op.get("output_name", op["name"]),
                source_column=source_column,
                agg=op["agg"],
                semantic_type=op.get("semantic_type"),
                feature_rollups=normalized_rollups,
                feature_rollup_params=feature_rollup_params,
            )
        )

    if not aggregations:
        raise PipelineError("At least one aggregation operator with 'agg' is required")

    return AggregateConfig(
        by=["exchange_id", "symbol_id", "bucket_ts"],
        aggregations=aggregations,
        mode=compiled["mode"],
        research_mode=research_mode,
    )


def _apply_labels(lf: pl.LazyFrame, labels: list[dict[str, Any]]) -> pl.LazyFrame:
    if not labels:
        return lf

    out = lf.sort(["exchange_id", "symbol_id", "ts_local_us"])
    for label in labels:
        direction = label["direction"]
        horizon = int(label["horizon_bars"])
        group_by = label.get("group_by", ["exchange_id", "symbol_id"])
        source_col = label["source_column"]
        out_col = label["name"]

        if direction == "forward":
            expr = pl.col(source_col).shift(-horizon).over(group_by).alias(out_col)
        else:
            expr = pl.col(source_col).shift(horizon).over(group_by).alias(out_col)

        out = out.with_columns([expr])
    return out


def _normalize_operator(operator: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(operator)
    agg_name = normalized.get("agg")
    if not agg_name:
        return normalized

    meta = AggregationRegistry.get(agg_name)
    normalized.setdefault("stage", meta.stage)
    normalized.setdefault("required_columns", meta.required_columns)
    normalized.setdefault("mode_allowlist", meta.mode_allowlist)
    normalized.setdefault("pit_policy", meta.pit_policy)
    normalized.setdefault("determinism_policy", meta.determinism)
    normalized.setdefault("impl_ref", meta.impl_ref)
    normalized.setdefault("version", meta.version)
    normalized.setdefault("name", agg_name)
    normalized.setdefault("output_name", normalized["name"])

    required_contract_fields = [
        "name",
        "stage",
        "required_columns",
        "mode_allowlist",
        "pit_policy",
        "determinism_policy",
        "impl_ref",
        "version",
    ]
    missing = [field for field in required_contract_fields if field not in normalized]
    if missing:
        raise PipelineError(
            f"Operator contract missing fields for {normalized.get('name')}: {missing}"
        )

    return normalized


def _count_pit_violations(bucketed: pl.LazyFrame) -> int:
    candidate_col = (
        "bucket_ts_candidate" if "bucket_ts_candidate" in bucketed.columns else "bucket_ts"
    )
    violations = bucketed.filter(
        pl.col(candidate_col).is_not_null() & (pl.col("ts_local_us") >= pl.col(candidate_col))
    )
    return int(violations.select(pl.len().alias("_n")).collect()["_n"][0])


def _count_unassigned_rows(bucketed: pl.LazyFrame) -> int:
    missing = bucketed.filter(pl.col("bucket_ts").is_null())
    return int(missing.select(pl.len().alias("_n")).collect()["_n"][0])


def _research_mode_from_mode(mode: str) -> str:
    if mode == "bar_then_feature":
        return "MFT"
    if mode in {"event_joined", "tick_then_bar"}:
        return "HFT"
    raise PipelineError(f"Unknown mode: {mode}")


def _primary_source_name(compiled: dict[str, Any]) -> str:
    spine_source = compiled["spine"].get("source")
    if spine_source:
        return spine_source
    return compiled["sources"][0]["name"]


def _normalized_source_fingerprints(source_specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fingerprints = []
    for spec in source_specs:
        fp = {
            "name": spec["name"],
            "ref": spec.get("ref"),
            "table": spec.get("table"),
            "symbol_id": spec.get("symbol_id"),
            "start_ts_us": spec.get("start_ts_us"),
            "end_ts_us": spec.get("end_ts_us"),
            "columns": spec.get("columns", []),
        }
        if "inline_rows" in spec:
            fp["inline_rows_hash"] = _stable_hash(spec["inline_rows"])
        fingerprints.append(fp)
    return fingerprints


def _stable_hash(obj: Any) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _hash_output_frame(frame: pl.DataFrame) -> str:
    """Stable output hash for reproducibility checks."""
    payload = {
        "columns": frame.columns,
        "dtypes": [str(dtype) for dtype in frame.dtypes],
        "rows": frame.to_dicts(),
    }
    return _stable_hash(payload)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _validate_mode_operator_stages(
    compiled: dict[str, Any],
    *,
    mode: str,
    allowed_stages: set[str],
    require_any: set[str] | None = None,
) -> None:
    operator_stages = {op.get("stage") for op in compiled["operators"] if "stage" in op}
    disallowed = sorted(stage for stage in operator_stages if stage not in allowed_stages)
    if disallowed:
        raise PipelineError(f"{mode} does not allow operator stages: {disallowed}")

    if require_any:
        if not any(op.get("stage") in require_any for op in compiled["operators"]):
            raise PipelineError(
                f"{mode} requires at least one operator stage in {sorted(require_any)}"
            )
