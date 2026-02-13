"""Canonical v2 control table specs."""

from __future__ import annotations

import polars as pl

from pointline.schemas.types import ColumnSpec, TableSpec

INGEST_MANIFEST = TableSpec(
    name="ingest_manifest",
    kind="control",
    column_specs=(
        ColumnSpec("file_id", pl.Int64),
        ColumnSpec("vendor", pl.Utf8),
        ColumnSpec("data_type", pl.Utf8),
        ColumnSpec("bronze_path", pl.Utf8),
        ColumnSpec("file_hash", pl.Utf8),
        ColumnSpec("status", pl.Utf8),
        ColumnSpec("rows_total", pl.Int64),
        ColumnSpec("rows_written", pl.Int64),
        ColumnSpec("rows_quarantined", pl.Int64),
        ColumnSpec("trading_date_min", pl.Date, nullable=True),
        ColumnSpec("trading_date_max", pl.Date, nullable=True),
        ColumnSpec("created_at_ts_us", pl.Int64),
        ColumnSpec("processed_at_ts_us", pl.Int64, nullable=True),
        ColumnSpec("status_reason", pl.Utf8, nullable=True),
    ),
    partition_by=(),
    business_keys=("vendor", "data_type", "bronze_path", "file_hash"),
    tie_break_keys=("file_id",),
    schema_version="v2",
)


VALIDATION_LOG = TableSpec(
    name="validation_log",
    kind="control",
    column_specs=(
        ColumnSpec("file_id", pl.Int64),
        ColumnSpec("rule_name", pl.Utf8),
        ColumnSpec("severity", pl.Utf8),
        ColumnSpec("logged_at_ts_us", pl.Int64),
        ColumnSpec("file_seq", pl.Int64, nullable=True),
        ColumnSpec("field_name", pl.Utf8, nullable=True),
        ColumnSpec("field_value", pl.Utf8, nullable=True),
        ColumnSpec("ts_event_us", pl.Int64, nullable=True),
        ColumnSpec("symbol", pl.Utf8, nullable=True),
        ColumnSpec("symbol_id", pl.Int64, nullable=True),
        ColumnSpec("message", pl.Utf8, nullable=True),
    ),
    partition_by=(),
    business_keys=("file_id", "rule_name", "logged_at_ts_us"),
    tie_break_keys=("file_id", "logged_at_ts_us", "file_seq"),
    schema_version="v2",
)


CONTROL_SPECS: tuple[TableSpec, ...] = (INGEST_MANIFEST, VALIDATION_LOG)
