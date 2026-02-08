"""Hybrid research workflow orchestration for pipeline v2.

This module composes the mode kernels behind `research.pipeline` into a
contract-first DAG execution path:
    research.workflow(request) -> output
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from pointline.research.contracts import (
    validate_quant_research_workflow_input_v2,
    validate_quant_research_workflow_output_v2,
)
from pointline.research.pipeline import (
    build_decision,
    compile_request,
    compute_metrics,
    evaluate_quality_gates,
    execute_compiled_with_sources,
    load_source,
)


class WorkflowError(ValueError):
    """Raised for invalid workflow requests or stage dependency graphs."""


def workflow(request: dict[str, Any]) -> dict[str, Any]:
    """Execute a hybrid workflow that composes multiple pipeline modes."""
    started_at = _utc_now_iso()
    validate_quant_research_workflow_input_v2(request)

    compiled = compile_workflow_request(request)
    artifact_store: dict[tuple[str, str], pl.DataFrame] = {}

    stage_runs: list[dict[str, Any]] = []
    lineage: list[dict[str, Any]] = []
    stage_artifacts: dict[str, list[str]] = {}
    all_artifact_paths: list[str] = []

    workflow_failed_gates: list[str] = []
    stage_gate_failures: list[dict[str, Any]] = []
    schema_errors: list[str] = []
    any_probe_failed = False

    for stage in compiled["stage_plan"]:
        stage_started = _utc_now_iso()
        stage_id = stage["stage_id"]

        try:
            (
                resolved_sources,
                coverage_checks,
                probe_checks,
            ) = _resolve_stage_sources(
                stage=stage,
                base_source_map=compiled["base_source_map"],
                artifact_store=artifact_store,
            )
            any_probe_failed = any_probe_failed or any(not item["passed"] for item in probe_checks)

            stage_request = _build_stage_request(compiled, stage)
            stage_compiled = compile_request(stage_request)
            frame, runtime = execute_compiled_with_sources(
                stage_compiled,
                resolved_sources,
                coverage_checks=coverage_checks,
                probe_checks=probe_checks,
            )
            gates = evaluate_quality_gates(stage_compiled, runtime)

            output_refs = _publish_stage_outputs(stage, frame, artifact_store, lineage)
            stage_paths = _emit_stage_artifacts(
                workflow_compiled=compiled,
                stage_id=stage_id,
                stage_compiled=stage_compiled,
                gates=gates,
                frame=frame,
            )
            stage_artifacts[stage_id] = stage_paths
            all_artifact_paths.extend(stage_paths)

            stage_status = "success" if not gates["failed_gates"] else "failed"
            stage_runs.append(
                {
                    "stage_id": stage_id,
                    "mode": stage["mode"],
                    "status": stage_status,
                    "started_at": stage_started,
                    "completed_at": _utc_now_iso(),
                    "config_hash": stage_compiled["config_hash"],
                    "row_count": frame.height,
                    "columns": frame.columns,
                    "gate_summary": {
                        "failed_gates": gates["failed_gates"],
                        "pit_violations": runtime.pit_violations,
                        "unassigned_rows": runtime.unassigned_rows,
                    },
                    "artifact_refs": output_refs,
                }
            )

            if gates["failed_gates"]:
                stage_gate_failures.append(
                    {"stage_id": stage_id, "failed_gates": gates["failed_gates"]}
                )
                workflow_failed_gates.extend(
                    f"{stage_id}:{gate_name}" for gate_name in gates["failed_gates"]
                )
                if compiled["constraints"]["fail_fast"]:
                    break

        except Exception as exc:
            schema_errors.append(str(exc))
            stage_runs.append(
                {
                    "stage_id": stage_id,
                    "mode": stage["mode"],
                    "status": "failed",
                    "started_at": stage_started,
                    "completed_at": _utc_now_iso(),
                    "config_hash": _stable_hash({"stage_id": stage_id, "mode": stage["mode"]}),
                    "row_count": 0,
                    "columns": [],
                    "gate_summary": {
                        "failed_gates": ["execution_error"],
                        "pit_violations": 0,
                        "unassigned_rows": 0,
                    },
                    "artifact_refs": [],
                }
            )
            stage_gate_failures.append({"stage_id": stage_id, "failed_gates": ["execution_error"]})
            workflow_failed_gates.append(f"{stage_id}:execution_error")
            break

    final_stage_id = compiled["final_stage_id"]
    final_ref = (final_stage_id, compiled["final_output_name"])
    final_frame = artifact_store.get(final_ref, pl.DataFrame())
    final_metrics = compute_metrics(final_frame, compiled["final_metrics"])

    quality_gates = _build_workflow_quality_gates(
        compiled=compiled,
        stage_runs=stage_runs,
        stage_gate_failures=stage_gate_failures,
        workflow_failed_gates=workflow_failed_gates,
        schema_errors=schema_errors,
        lineage=lineage,
    )

    last_runtime = _runtime_from_stage_runs(stage_runs)
    decision = build_decision(
        {
            "failed_gates": quality_gates["failed_gates"],
        },
        last_runtime,
    )
    if decision["status"] == "go" and any_probe_failed:
        decision["status"] = "revise"
        decision["rationale"] = (
            "Probe checks in one or more stages indicate incomplete data coverage"
        )

    completed_at = _utc_now_iso()
    output = {
        "schema_version": "2.0",
        "request_id": compiled["request_id"],
        "workflow_id": compiled["workflow_id"],
        "run": {
            "workflow_run_id": compiled["workflow_run_id"],
            "started_at": started_at,
            "completed_at": completed_at,
            "status": _workflow_status(workflow_failed_gates),
        },
        "resolved_plan": {
            "workflow_id": compiled["workflow_id"],
            "final_stage_id": final_stage_id,
            "stage_order": [stage["stage_id"] for stage in compiled["stage_plan"]],
            "stages": [
                {
                    "stage_id": stage["stage_id"],
                    "mode": stage["mode"],
                    "depends_on": sorted(stage["depends_on"]),
                    "outputs": [output_spec["name"] for output_spec in stage["outputs"]],
                }
                for stage in compiled["stage_plan"]
            ],
            "config_hash": compiled["config_hash"],
        },
        "stage_runs": stage_runs,
        "quality_gates": quality_gates,
        "results": {
            "final_stage_id": final_stage_id,
            "row_count": final_frame.height,
            "columns": final_frame.columns,
            "metrics": final_metrics,
            "preview": final_frame.head(10).to_dicts(),
        },
        "decision": decision,
        "artifacts": {
            "config_hash": compiled["config_hash"],
            "paths": all_artifact_paths,
            "lineage": lineage,
            "stage_artifacts": stage_artifacts,
        },
    }

    workflow_paths = _emit_workflow_artifacts(
        workflow_compiled=compiled,
        quality_gates=quality_gates,
        lineage=lineage,
        resolved_plan=output["resolved_plan"],
    )
    output["artifacts"]["paths"].extend(workflow_paths)

    validate_quant_research_workflow_output_v2(output)
    return output


def compile_workflow_request(request: dict[str, Any]) -> dict[str, Any]:
    """Compile workflow request into an executable DAG plan."""
    compiled = deepcopy(request)
    compiled["workflow_run_id"] = f"wf-{uuid.uuid4().hex[:12]}"

    base_source_map: dict[str, dict[str, Any]] = {}
    for source in compiled["base_sources"]:
        source_name = source["name"]
        if source_name in base_source_map:
            raise WorkflowError(f"Duplicate base source name: {source_name}")
        base_source_map[source_name] = deepcopy(source)

    stage_map: dict[str, dict[str, Any]] = {}
    order_index: dict[str, int] = {}
    for idx, stage in enumerate(compiled["stages"]):
        stage_id = stage["stage_id"]
        if stage_id in stage_map:
            raise WorkflowError(f"Duplicate stage_id: {stage_id}")
        output_names = [output["name"] for output in stage["outputs"]]
        if len(output_names) != len(set(output_names)):
            raise WorkflowError(f"Duplicate output names in stage: {stage_id}")

        source_names = [source["name"] for source in stage["sources"]]
        if len(source_names) != len(set(source_names)):
            raise WorkflowError(f"Duplicate source aliases in stage: {stage_id}")
        stage_map[stage_id] = deepcopy(stage)
        order_index[stage_id] = idx

    final_stage_id = compiled["final_stage_id"]
    if final_stage_id not in stage_map:
        raise WorkflowError(f"final_stage_id not found in stages: {final_stage_id}")

    stage_dependencies: dict[str, set[str]] = {stage_id: set() for stage_id in stage_map}
    for stage_id, stage in stage_map.items():
        for source_ref in stage["sources"]:
            ref_kind, ref_stage, ref_output = _parse_ref(source_ref["ref"])
            if ref_kind == "base":
                if ref_stage not in base_source_map:
                    raise WorkflowError(
                        f"Unknown base source in stage {stage_id}: {source_ref['ref']}"
                    )
                continue

            if ref_stage not in stage_map:
                raise WorkflowError(
                    f"Unknown artifact stage reference in stage {stage_id}: {source_ref['ref']}"
                )

            known_outputs = {output["name"] for output in stage_map[ref_stage]["outputs"]}
            if ref_output not in known_outputs:
                raise WorkflowError(
                    f"Unknown artifact output in stage {stage_id}: {source_ref['ref']}"
                )
            stage_dependencies[stage_id].add(ref_stage)

    ordered_stage_ids = _topological_sort(stage_dependencies, order_index)
    stage_plan = []
    for stage_id in ordered_stage_ids:
        stage = stage_map[stage_id]
        stage["depends_on"] = stage_dependencies[stage_id]
        stage_plan.append(stage)

    final_outputs = stage_map[final_stage_id]["outputs"]
    final_output_name = final_outputs[0]["name"]

    compiled["base_source_map"] = base_source_map
    compiled["stage_plan"] = stage_plan
    compiled["final_output_name"] = final_output_name
    compiled["final_metrics"] = stage_map[final_stage_id]["evaluation"]["metrics"]
    compiled["config_hash"] = _stable_hash(
        {
            "request_id": compiled["request_id"],
            "workflow_id": compiled["workflow_id"],
            "final_stage_id": compiled["final_stage_id"],
            "constraints": compiled["constraints"],
            "artifacts": compiled["artifacts"],
            "base_sources": _normalized_base_source_fingerprints(compiled["base_sources"]),
            "stages": [
                {
                    "stage_id": stage["stage_id"],
                    "mode": stage["mode"],
                    "timeline": stage["timeline"],
                    "spine": stage["spine"],
                    "sources": stage["sources"],
                    "operators": stage["operators"],
                    "labels": stage["labels"],
                    "evaluation": stage["evaluation"],
                    "constraints": stage["constraints"],
                    "outputs": stage["outputs"],
                }
                for stage in stage_plan
            ],
        }
    )
    return compiled


def _resolve_stage_sources(
    *,
    stage: dict[str, Any],
    base_source_map: dict[str, dict[str, Any]],
    artifact_store: dict[tuple[str, str], pl.DataFrame],
) -> tuple[dict[str, pl.LazyFrame], list[dict[str, Any]], list[dict[str, Any]]]:
    timeline_col = stage["timeline"]["time_col"]

    sources: dict[str, pl.LazyFrame] = {}
    coverage_checks: list[dict[str, Any]] = []
    probe_checks: list[dict[str, Any]] = []

    for source_ref in stage["sources"]:
        source_name = source_ref["name"]
        ref_kind, ref_stage, ref_output = _parse_ref(source_ref["ref"])
        selected_cols = source_ref.get("columns")

        if ref_kind == "base":
            spec = base_source_map[ref_stage]
            lf = load_source(spec, timeline_col)
            if selected_cols:
                lf = lf.select(selected_cols)
            if stage["mode"] == "event_joined":
                lf = _ensure_event_join_order_columns(lf)

            row_count = int(lf.select(pl.len().alias("_n")).collect()["_n"][0])
            sources[source_name] = lf
            coverage_checks.append({"source": source_name, "available": True, "reason": None})
            probe_checks.append(
                {
                    "source": source_name,
                    "row_count": row_count,
                    "passed": row_count > 0,
                }
            )
            continue

        artifact_key = (ref_stage, ref_output)
        if artifact_key not in artifact_store:
            raise WorkflowError(f"Artifact source not found for {source_ref['ref']}")

        frame = artifact_store[artifact_key]
        if selected_cols:
            missing = sorted(set(selected_cols) - set(frame.columns))
            if missing:
                raise WorkflowError(
                    f"Selected columns missing in artifact {source_ref['ref']}: {missing}"
                )
            frame = frame.select(selected_cols)
        if stage["mode"] == "event_joined":
            frame = _ensure_event_join_order_columns(frame.lazy()).collect()

        sources[source_name] = frame.lazy()
        coverage_checks.append({"source": source_name, "available": True, "reason": None})
        probe_checks.append(
            {
                "source": source_name,
                "row_count": frame.height,
                "passed": frame.height > 0,
            }
        )

    return sources, coverage_checks, probe_checks


def _build_stage_request(
    workflow_compiled: dict[str, Any], stage: dict[str, Any]
) -> dict[str, Any]:
    return {
        "schema_version": "2.0",
        "request_id": f"{workflow_compiled['request_id']}:{stage['stage_id']}",
        "mode": stage["mode"],
        "timeline": deepcopy(stage["timeline"]),
        "sources": [
            {
                "name": source["name"],
                "ref": source["ref"],
                "columns": source.get("columns", []),
            }
            for source in stage["sources"]
        ],
        "spine": deepcopy(stage["spine"]),
        "operators": deepcopy(stage["operators"]),
        "labels": deepcopy(stage["labels"]),
        "evaluation": deepcopy(stage["evaluation"]),
        "constraints": deepcopy(stage["constraints"]),
        "artifacts": {
            "include_artifacts": False,
        },
    }


def _publish_stage_outputs(
    stage: dict[str, Any],
    frame: pl.DataFrame,
    artifact_store: dict[tuple[str, str], pl.DataFrame],
    lineage: list[dict[str, Any]],
) -> list[str]:
    refs = []
    for output_spec in stage["outputs"]:
        output_name = output_spec["name"]
        artifact_store[(stage["stage_id"], output_name)] = frame.clone()

        refs.append(f"artifact:{stage['stage_id']}:{output_name}")
        lineage.append(
            {
                "stage_id": stage["stage_id"],
                "output_name": output_name,
                "row_count": frame.height,
                "columns": frame.columns,
            }
        )
    return refs


def _build_workflow_quality_gates(
    *,
    compiled: dict[str, Any],
    stage_runs: list[dict[str, Any]],
    stage_gate_failures: list[dict[str, Any]],
    workflow_failed_gates: list[str],
    schema_errors: list[str],
    lineage: list[dict[str, Any]],
) -> dict[str, Any]:
    executed_stage_ids = {stage_run["stage_id"] for stage_run in stage_runs}

    expected = {
        (stage["stage_id"], output_spec["name"])
        for stage in compiled["stage_plan"]
        if stage["stage_id"] in executed_stage_ids
        for output_spec in stage["outputs"]
    }
    actual = {(item["stage_id"], item["output_name"]) for item in lineage}
    missing = sorted(
        f"artifact:{stage_id}:{output_name}" for stage_id, output_name in expected - actual
    )

    return {
        "lineage_completeness_check": {
            "passed": not missing,
            "missing": missing,
        },
        "reference_resolution_check": {
            "passed": True,
            "errors": [],
        },
        "schema_compatibility_check": {
            "passed": len(schema_errors) == 0,
            "errors": schema_errors,
        },
        "stage_gate_failures": stage_gate_failures,
        "failed_gates": sorted(set(workflow_failed_gates)),
    }


def _emit_stage_artifacts(
    *,
    workflow_compiled: dict[str, Any],
    stage_id: str,
    stage_compiled: dict[str, Any],
    gates: dict[str, Any],
    frame: pl.DataFrame,
) -> list[str]:
    artifacts_cfg = workflow_compiled.get("artifacts", {})
    if not artifacts_cfg.get("include_artifacts", True):
        return []

    output_dir = artifacts_cfg.get("output_dir")
    if not output_dir:
        return []

    stage_dir = Path(output_dir) / workflow_compiled["workflow_run_id"] / "stages" / stage_id
    stage_dir.mkdir(parents=True, exist_ok=True)

    plan_path = stage_dir / "resolved_plan.json"
    gates_path = stage_dir / "quality_gates.json"
    preview_path = stage_dir / "result_preview.json"

    with plan_path.open("w", encoding="utf-8") as file_obj:
        json.dump(
            {
                "stage_id": stage_id,
                "mode": stage_compiled["mode"],
                "timeline": stage_compiled["timeline"],
                "spine": stage_compiled["spine"],
                "operators": stage_compiled["operators"],
                "config_hash": stage_compiled["config_hash"],
            },
            file_obj,
            indent=2,
            sort_keys=True,
        )

    with gates_path.open("w", encoding="utf-8") as file_obj:
        json.dump(gates, file_obj, indent=2, sort_keys=True)

    with preview_path.open("w", encoding="utf-8") as file_obj:
        json.dump(frame.head(100).to_dicts(), file_obj, indent=2, sort_keys=True)

    paths = [str(plan_path), str(gates_path), str(preview_path)]

    if artifacts_cfg.get("persist_stage_snapshots", False):
        snapshot_path = stage_dir / "result_full.json"
        with snapshot_path.open("w", encoding="utf-8") as file_obj:
            json.dump(frame.to_dicts(), file_obj, indent=2, sort_keys=True)
        paths.append(str(snapshot_path))

    return paths


def _emit_workflow_artifacts(
    *,
    workflow_compiled: dict[str, Any],
    quality_gates: dict[str, Any],
    lineage: list[dict[str, Any]],
    resolved_plan: dict[str, Any],
) -> list[str]:
    artifacts_cfg = workflow_compiled.get("artifacts", {})
    if not artifacts_cfg.get("include_artifacts", True):
        return []

    output_dir = artifacts_cfg.get("output_dir")
    if not output_dir:
        return []

    run_dir = Path(output_dir) / workflow_compiled["workflow_run_id"]
    run_dir.mkdir(parents=True, exist_ok=True)

    plan_path = run_dir / "workflow_resolved_plan.json"
    gates_path = run_dir / "workflow_quality_gates.json"
    lineage_path = run_dir / "workflow_lineage.json"

    with plan_path.open("w", encoding="utf-8") as file_obj:
        json.dump(resolved_plan, file_obj, indent=2, sort_keys=True)

    with gates_path.open("w", encoding="utf-8") as file_obj:
        json.dump(quality_gates, file_obj, indent=2, sort_keys=True)

    with lineage_path.open("w", encoding="utf-8") as file_obj:
        json.dump(lineage, file_obj, indent=2, sort_keys=True)

    return [str(plan_path), str(gates_path), str(lineage_path)]


def _topological_sort(
    dependencies: dict[str, set[str]],
    order_index: dict[str, int],
) -> list[str]:
    indegree = {stage_id: len(deps) for stage_id, deps in dependencies.items()}
    outgoing: dict[str, set[str]] = {stage_id: set() for stage_id in dependencies}

    for stage_id, deps in dependencies.items():
        for dep in deps:
            outgoing[dep].add(stage_id)

    ready = sorted(
        [stage_id for stage_id, degree in indegree.items() if degree == 0],
        key=lambda stage_id: order_index[stage_id],
    )
    queue = deque(ready)

    ordered: list[str] = []
    while queue:
        node = queue.popleft()
        ordered.append(node)

        children = sorted(outgoing[node], key=lambda stage_id: order_index[stage_id])
        for child in children:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if len(ordered) != len(dependencies):
        raise WorkflowError("Workflow stage graph contains a cycle")

    return ordered


def _parse_ref(ref: str) -> tuple[str, str, str | None]:
    if ref.startswith("base:"):
        return "base", ref.split(":", maxsplit=1)[1], None

    if ref.startswith("artifact:"):
        parts = ref.split(":")
        if len(parts) != 3:
            raise WorkflowError(f"Invalid artifact reference: {ref}")
        return "artifact", parts[1], parts[2]

    raise WorkflowError(f"Unsupported source reference: {ref}")


def _normalized_base_source_fingerprints(
    source_specs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    fingerprints = []
    for spec in source_specs:
        fp = {
            "name": spec["name"],
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


def _runtime_from_stage_runs(stage_runs: list[dict[str, Any]]) -> Any:
    probe_checks = [
        {
            "source": stage_run["stage_id"],
            "row_count": stage_run["row_count"],
            "passed": stage_run["row_count"] > 0,
        }
        for stage_run in stage_runs
    ]
    pit_violations = sum(stage_run["gate_summary"]["pit_violations"] for stage_run in stage_runs)
    unassigned_rows = sum(stage_run["gate_summary"]["unassigned_rows"] for stage_run in stage_runs)

    class RuntimeProxy:
        def __init__(self, probe: list[dict[str, Any]], pit: int, unassigned: int) -> None:
            self.probe_checks = probe
            self.pit_violations = pit
            self.unassigned_rows = unassigned

    return RuntimeProxy(probe_checks, pit_violations, unassigned_rows)


def _workflow_status(failed_gates: list[str]) -> str:
    return "failed" if failed_gates else "success"


def _ensure_event_join_order_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    missing_exprs = []
    schema_names = set(lf.collect_schema().names())
    if "file_id" not in schema_names:
        missing_exprs.append(pl.lit(0).cast(pl.Int64).alias("file_id"))
    if "file_line_number" not in schema_names:
        missing_exprs.append(pl.lit(0).cast(pl.Int64).alias("file_line_number"))
    if not missing_exprs:
        return lf
    return lf.with_columns(missing_exprs)


def _stable_hash(obj: Any) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
