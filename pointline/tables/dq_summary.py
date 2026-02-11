"""Schema and utilities for dq_summary in Polars.

This table stores per-run data quality summaries for silver tables. It is meant
for quick health checks and dashboards (row counts, null rates, duplicates,
freshness, and basic profiling).
"""

from __future__ import annotations

import json
import time
from datetime import date

import polars as pl

DQ_SUMMARY_SCHEMA: dict[str, pl.DataType] = {
    "run_id": pl.Int64,  # Unique run ID (microsecond timestamp)
    "table_name": pl.Utf8,
    "date": pl.Date,  # Optional partition date for the summary
    "row_count": pl.Int64,
    "null_counts": pl.Utf8,  # JSON dict: {column: null_count}
    "duplicate_rows": pl.Int64,
    "min_ts_us": pl.Int64,
    "max_ts_us": pl.Int64,
    "freshness_lag_sec": pl.Int64,  # now - max_ts_us in seconds (nullable)
    "status": pl.Utf8,  # "passed" or "failed"
    "issue_counts": pl.Utf8,  # JSON dict: {issue_name: count}
    "profile_stats": pl.Utf8,  # JSON dict of per-column stats
    "validated_at": pl.Int64,  # Timestamp in microseconds since epoch (UTC)
    "validation_duration_ms": pl.Int64,
}


def _json_dumps(value: dict | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def create_dq_summary_record(
    *,
    table_name: str,
    row_count: int,
    duplicate_rows: int,
    status: str,
    validation_duration_ms: int,
    date_partition: date | None = None,
    null_counts: dict[str, int] | None = None,
    min_ts_us: int | None = None,
    max_ts_us: int | None = None,
    freshness_lag_sec: int | None = None,
    issue_counts: dict[str, int] | None = None,
    profile_stats: dict[str, dict[str, float | int | None]] | None = None,
    run_id: int | None = None,
) -> pl.DataFrame:
    """Create a dq_summary record DataFrame."""
    if status not in ("passed", "failed"):
        raise ValueError(f"dq_summary status must be 'passed' or 'failed', got: {status}")

    run_id = run_id or int(time.time() * 1_000_000)
    validated_at = run_id

    return pl.DataFrame(
        {
            "run_id": [run_id],
            "table_name": [table_name],
            "date": [date_partition],
            "row_count": [row_count],
            "null_counts": [_json_dumps(null_counts)],
            "duplicate_rows": [duplicate_rows],
            "min_ts_us": [min_ts_us],
            "max_ts_us": [max_ts_us],
            "freshness_lag_sec": [freshness_lag_sec],
            "status": [status],
            "issue_counts": [_json_dumps(issue_counts)],
            "profile_stats": [_json_dumps(profile_stats)],
            "validated_at": [validated_at],
            "validation_duration_ms": [validation_duration_ms],
        },
        schema=DQ_SUMMARY_SCHEMA,
    )


def normalize_dq_summary_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical dq_summary schema and select only schema columns."""
    missing = [col for col in DQ_SUMMARY_SCHEMA if col not in df.columns]
    if missing:
        raise ValueError(f"dq_summary missing required columns: {missing}")

    casts = [pl.col(col).cast(dtype) for col, dtype in DQ_SUMMARY_SCHEMA.items()]
    return df.with_columns(casts).select(list(DQ_SUMMARY_SCHEMA.keys()))


def required_dq_summary_columns() -> tuple[str, ...]:
    """Columns required for a dq_summary DataFrame."""
    return tuple(DQ_SUMMARY_SCHEMA.keys())


# ---------------------------------------------------------------------------
# Schema registry registration
# ---------------------------------------------------------------------------
from pointline.schema_registry import register_schema as _register_schema  # noqa: E402

_register_schema("dq_summary", DQ_SUMMARY_SCHEMA, has_date=True)
