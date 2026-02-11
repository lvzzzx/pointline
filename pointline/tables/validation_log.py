"""Schema and utilities for validation_log in Polars.

This module provides schema definition and helper functions for the validation_log table,
which tracks validation results for all silver table ingestions. This enables data quality
auditability and research reproducibility.

Example:
    import polars as pl
    from pointline.tables.validation_log import create_validation_record, VALIDATION_LOG_SCHEMA

    # Create validation record
    record = create_validation_record(
        file_id=123,
        table_name="trades",
        validation_status="passed",
        expected_rows=10000,
        ingested_rows=10000,
        missing_rows=0,
        extra_rows=0,
        mismatched_rows=0,
        mismatch_sample=None,
        validation_duration_ms=1234,
    )

    # Append to validation log
    record.write_delta("/lake/silver/validation_log", mode="append")
"""

from __future__ import annotations

import time

import polars as pl

# Schema definition for validation_log table
# Delta Lake Integer Type Limitations:
# - Use Int32 for file_id (matches ingest_manifest)
# - Use Int64 for timestamps (microseconds since epoch)
# - Use Int64 for row counts and validation_id
VALIDATION_LOG_SCHEMA: dict[str, pl.DataType] = {
    "validation_id": pl.Int64,  # Unique validation ID (microsecond timestamp)
    "file_id": pl.Int32,  # FK to ingest_manifest.file_id
    "table_name": pl.Utf8,  # e.g., "trades", "quotes", "book_snapshot_25"
    "validated_at": pl.Int64,  # Validation timestamp in microseconds since epoch (UTC)
    "validation_status": pl.Utf8,  # "passed", "failed", "ingested", "quarantined", "error"
    "expected_rows": pl.Int64,  # Rows from re-processing bronze file
    "ingested_rows": pl.Int64,  # Rows in silver table for this file_id
    "missing_rows": pl.Int64,  # Rows in expected but not in ingested (data loss)
    "extra_rows": pl.Int64,  # Rows in ingested but not in expected (duplicates)
    "mismatched_rows": pl.Int64,  # Rows present in both but with different values
    "mismatch_sample": pl.Utf8,  # JSON representation of mismatch sample (nullable)
    "validation_duration_ms": pl.Int64,  # Validation duration in milliseconds
    # Ingestion-specific columns (nullable, present only for ingestion records)
    "vendor": pl.Utf8,  # Vendor name (e.g., "tardis", "quant360")
    "data_type": pl.Utf8,  # Data type (e.g., "trades", "quotes")
    "exchange": pl.Utf8,  # Exchange name (e.g., "binance-futures")
    "date": pl.Date,  # Trading date
    "filtered_row_count": pl.Int64,  # Rows removed by quarantine/validation
    "filtered_symbol_count": pl.Int64,  # Symbols quarantined
    "error_message": pl.Utf8,  # Error message for failed ingestions
}


def create_validation_record(
    *,
    file_id: int,
    table_name: str,
    validation_status: str,
    expected_rows: int,
    ingested_rows: int,
    missing_rows: int,
    extra_rows: int,
    mismatched_rows: int,
    mismatch_sample: pl.DataFrame | None = None,
    validation_duration_ms: int,
) -> pl.DataFrame:
    """Create a validation record DataFrame.

    Args:
        file_id: File ID from ingest_manifest
        table_name: Name of the silver table being validated
        validation_status: "passed" or "failed"
        expected_rows: Row count from re-processing bronze
        ingested_rows: Row count in silver table
        missing_rows: Count of missing rows
        extra_rows: Count of extra rows
        mismatched_rows: Count of mismatched rows
        mismatch_sample: Optional DataFrame of mismatched rows
        validation_duration_ms: Validation duration in milliseconds

    Returns:
        DataFrame with one row matching VALIDATION_LOG_SCHEMA

    Raises:
        ValueError: If validation_status is not "passed" or "failed"
    """
    _VALID_STATUSES = ("passed", "failed", "ingested", "quarantined", "error")
    if validation_status not in _VALID_STATUSES:
        raise ValueError(
            f"validation_status must be one of {_VALID_STATUSES}, got: {validation_status}"
        )

    # Generate validation_id using current timestamp in microseconds
    validation_id = int(time.time() * 1_000_000)
    validated_at = validation_id  # Use same timestamp

    # Serialize mismatch_sample to JSON if provided
    mismatch_json = None
    if mismatch_sample is not None and not mismatch_sample.is_empty():
        try:
            mismatch_json = mismatch_sample.write_json()
        except Exception:
            # Fallback to string representation if JSON serialization fails
            mismatch_json = str(mismatch_sample)

    return pl.DataFrame(
        {
            "validation_id": [validation_id],
            "file_id": [file_id],
            "table_name": [table_name],
            "validated_at": [validated_at],
            "validation_status": [validation_status],
            "expected_rows": [expected_rows],
            "ingested_rows": [ingested_rows],
            "missing_rows": [missing_rows],
            "extra_rows": [extra_rows],
            "mismatched_rows": [mismatched_rows],
            "mismatch_sample": [mismatch_json],
            "validation_duration_ms": [validation_duration_ms],
            "vendor": [None],
            "data_type": [None],
            "exchange": [None],
            "date": [None],
            "filtered_row_count": [None],
            "filtered_symbol_count": [None],
            "error_message": [None],
        },
        schema=VALIDATION_LOG_SCHEMA,
    )


def create_ingestion_record(
    *,
    file_id: int,
    table_name: str,
    vendor: str,
    data_type: str,
    exchange: str | None = None,
    date: object | None = None,
    status: str,
    row_count: int,
    filtered_row_count: int = 0,
    filtered_symbol_count: int = 0,
    error_message: str | None = None,
    duration_ms: int,
) -> pl.DataFrame:
    """Create an ingestion record DataFrame for the validation_log.

    Args:
        file_id: File ID from ingest_manifest
        table_name: Target silver table name
        vendor: Vendor name (e.g., "tardis", "quant360")
        data_type: Data type (e.g., "trades", "quotes")
        exchange: Exchange name (nullable, may span multiple)
        date: Trading date (nullable)
        status: "ingested", "quarantined", or "error"
        row_count: Rows successfully ingested
        filtered_row_count: Rows removed by quarantine/validation
        filtered_symbol_count: Number of symbols quarantined
        error_message: Error message if status is "error" or "quarantined"
        duration_ms: Ingestion duration in milliseconds

    Returns:
        DataFrame with one row matching VALIDATION_LOG_SCHEMA
    """
    ingested_at_us = int(time.time() * 1_000_000)

    return pl.DataFrame(
        {
            "validation_id": [ingested_at_us],
            "file_id": [file_id],
            "table_name": [table_name],
            "validated_at": [ingested_at_us],
            "validation_status": [status],
            "expected_rows": [row_count + filtered_row_count],
            "ingested_rows": [row_count],
            "missing_rows": [0],
            "extra_rows": [0],
            "mismatched_rows": [0],
            "mismatch_sample": [None],
            "validation_duration_ms": [duration_ms],
            "vendor": [vendor],
            "data_type": [data_type],
            "exchange": [exchange],
            "date": [date],
            "filtered_row_count": [filtered_row_count],
            "filtered_symbol_count": [filtered_symbol_count],
            "error_message": [error_message],
        },
        schema=VALIDATION_LOG_SCHEMA,
    )


def normalize_validation_log_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical validation_log schema and select only schema columns.

    Args:
        df: DataFrame with validation_log columns

    Returns:
        DataFrame with normalized schema

    Raises:
        ValueError: If required columns are missing
    """
    # These columns are nullable (not required for all record types)
    optional_columns = {
        "mismatch_sample",
        "vendor",
        "data_type",
        "exchange",
        "date",
        "filtered_row_count",
        "filtered_symbol_count",
        "error_message",
    }

    missing_required = [
        col
        for col in VALIDATION_LOG_SCHEMA
        if col not in df.columns and col not in optional_columns
    ]
    if missing_required:
        raise ValueError(f"validation_log missing required columns: {missing_required}")

    # Cast columns to schema types
    casts = []
    for col, dtype in VALIDATION_LOG_SCHEMA.items():
        if col in df.columns:
            casts.append(pl.col(col).cast(dtype))
        elif col in optional_columns:
            # Fill missing optional columns with None
            casts.append(pl.lit(None, dtype=dtype).alias(col))
        else:
            raise ValueError(f"Required column {col} is missing")

    # Cast and select only schema columns
    return df.with_columns(casts).select(list(VALIDATION_LOG_SCHEMA.keys()))


def required_validation_log_columns() -> tuple[str, ...]:
    """Columns required for a validation_log DataFrame."""
    return tuple(VALIDATION_LOG_SCHEMA.keys())


# ---------------------------------------------------------------------------
# Schema registry registration
# ---------------------------------------------------------------------------
from pointline.schema_registry import register_schema as _register_schema  # noqa: E402

_register_schema("validation_log", VALIDATION_LOG_SCHEMA)
