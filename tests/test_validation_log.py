"""Tests for validation_log table and utilities."""

from pathlib import Path

import polars as pl
import pytest

from pointline.tables.validation_log import (
    VALIDATION_LOG_SCHEMA,
    create_validation_record,
    normalize_validation_log_schema,
    required_validation_log_columns,
)


def test_validation_log_schema():
    """Test that VALIDATION_LOG_SCHEMA has expected columns and types."""
    assert "validation_id" in VALIDATION_LOG_SCHEMA
    assert "file_id" in VALIDATION_LOG_SCHEMA
    assert "table_name" in VALIDATION_LOG_SCHEMA
    assert "validated_at" in VALIDATION_LOG_SCHEMA
    assert "validation_status" in VALIDATION_LOG_SCHEMA
    assert "expected_rows" in VALIDATION_LOG_SCHEMA
    assert "ingested_rows" in VALIDATION_LOG_SCHEMA
    assert "missing_rows" in VALIDATION_LOG_SCHEMA
    assert "extra_rows" in VALIDATION_LOG_SCHEMA
    assert "mismatched_rows" in VALIDATION_LOG_SCHEMA
    assert "mismatch_sample" in VALIDATION_LOG_SCHEMA
    assert "validation_duration_ms" in VALIDATION_LOG_SCHEMA

    # Check types
    assert VALIDATION_LOG_SCHEMA["validation_id"] == pl.Int64
    assert VALIDATION_LOG_SCHEMA["file_id"] == pl.Int32
    assert VALIDATION_LOG_SCHEMA["table_name"] == pl.Utf8
    assert VALIDATION_LOG_SCHEMA["validated_at"] == pl.Int64
    assert VALIDATION_LOG_SCHEMA["validation_status"] == pl.Utf8
    assert VALIDATION_LOG_SCHEMA["expected_rows"] == pl.Int64
    assert VALIDATION_LOG_SCHEMA["ingested_rows"] == pl.Int64
    assert VALIDATION_LOG_SCHEMA["missing_rows"] == pl.Int64
    assert VALIDATION_LOG_SCHEMA["extra_rows"] == pl.Int64
    assert VALIDATION_LOG_SCHEMA["mismatched_rows"] == pl.Int64
    assert VALIDATION_LOG_SCHEMA["mismatch_sample"] == pl.Utf8
    assert VALIDATION_LOG_SCHEMA["validation_duration_ms"] == pl.Int64


def test_create_validation_record_passed():
    """Test creating a validation record for a passed validation."""
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

    assert record.height == 1
    assert record.columns == list(VALIDATION_LOG_SCHEMA.keys())

    # Check values
    assert record.item(0, "file_id") == 123
    assert record.item(0, "table_name") == "trades"
    assert record.item(0, "validation_status") == "passed"
    assert record.item(0, "expected_rows") == 10000
    assert record.item(0, "ingested_rows") == 10000
    assert record.item(0, "missing_rows") == 0
    assert record.item(0, "extra_rows") == 0
    assert record.item(0, "mismatched_rows") == 0
    assert record.item(0, "mismatch_sample") is None
    assert record.item(0, "validation_duration_ms") == 1234

    # Check timestamps are set
    assert record.item(0, "validation_id") > 0
    assert record.item(0, "validated_at") > 0


def test_create_validation_record_failed():
    """Test creating a validation record for a failed validation."""
    mismatch_sample = pl.DataFrame({
        "file_id": [123, 123],
        "file_line_number": [10, 20],
        "price_int_exp": [50000000, 60000000],
        "price_int_ing": [50000001, 60000001],
    })

    record = create_validation_record(
        file_id=123,
        table_name="trades",
        validation_status="failed",
        expected_rows=10000,
        ingested_rows=10002,
        missing_rows=5,
        extra_rows=7,
        mismatched_rows=2,
        mismatch_sample=mismatch_sample,
        validation_duration_ms=2345,
    )

    assert record.height == 1
    assert record.item(0, "validation_status") == "failed"
    assert record.item(0, "missing_rows") == 5
    assert record.item(0, "extra_rows") == 7
    assert record.item(0, "mismatched_rows") == 2
    assert record.item(0, "mismatch_sample") is not None
    assert isinstance(record.item(0, "mismatch_sample"), str)


def test_create_validation_record_invalid_status():
    """Test that invalid validation_status raises ValueError."""
    with pytest.raises(ValueError, match="validation_status must be"):
        create_validation_record(
            file_id=123,
            table_name="trades",
            validation_status="invalid",
            expected_rows=10000,
            ingested_rows=10000,
            missing_rows=0,
            extra_rows=0,
            mismatched_rows=0,
            mismatch_sample=None,
            validation_duration_ms=1234,
        )


def test_normalize_validation_log_schema():
    """Test normalization of validation_log DataFrame."""
    df = pl.DataFrame({
        "validation_id": [1714557600000000],
        "file_id": [123],
        "table_name": ["trades"],
        "validated_at": [1714557600000000],
        "validation_status": ["passed"],
        "expected_rows": [10000],
        "ingested_rows": [10000],
        "missing_rows": [0],
        "extra_rows": [0],
        "mismatched_rows": [0],
        "mismatch_sample": [None],
        "validation_duration_ms": [1234],
        "extra_column": ["should be dropped"],
    })

    normalized = normalize_validation_log_schema(df)

    # Check that extra columns are dropped
    assert "extra_column" not in normalized.columns
    assert normalized.columns == list(VALIDATION_LOG_SCHEMA.keys())

    # Check types match schema
    for col, dtype in VALIDATION_LOG_SCHEMA.items():
        assert normalized[col].dtype == dtype


def test_normalize_validation_log_schema_missing_required():
    """Test that missing required columns raise ValueError."""
    df = pl.DataFrame({
        "validation_id": [1714557600000000],
        "file_id": [123],
        # Missing table_name
    })

    with pytest.raises(ValueError, match="missing required columns"):
        normalize_validation_log_schema(df)


def test_normalize_validation_log_schema_missing_optional():
    """Test that missing optional columns (mismatch_sample) are filled with None."""
    df = pl.DataFrame({
        "validation_id": [1714557600000000],
        "file_id": [123],
        "table_name": ["trades"],
        "validated_at": [1714557600000000],
        "validation_status": ["passed"],
        "expected_rows": [10000],
        "ingested_rows": [10000],
        "missing_rows": [0],
        "extra_rows": [0],
        "mismatched_rows": [0],
        # Missing mismatch_sample (optional)
        "validation_duration_ms": [1234],
    })

    normalized = normalize_validation_log_schema(df)

    assert "mismatch_sample" in normalized.columns
    assert normalized.item(0, "mismatch_sample") is None


def test_required_validation_log_columns():
    """Test that required_validation_log_columns returns all schema columns."""
    required = required_validation_log_columns()
    assert required == tuple(VALIDATION_LOG_SCHEMA.keys())
    assert len(required) == 12


def test_validation_record_persistence(tmp_path: Path):
    """Test writing and reading validation records to/from Delta table."""
    validation_log_path = tmp_path / "validation_log"

    # Create first validation record
    record1 = create_validation_record(
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

    # Write to Delta table
    record1.write_delta(str(validation_log_path), mode="overwrite")

    # Create second validation record
    record2 = create_validation_record(
        file_id=456,
        table_name="quotes",
        validation_status="failed",
        expected_rows=5000,
        ingested_rows=5005,
        missing_rows=2,
        extra_rows=7,
        mismatched_rows=3,
        mismatch_sample=None,
        validation_duration_ms=2345,
    )

    # Append to Delta table
    record2.write_delta(str(validation_log_path), mode="append")

    # Read back
    df = pl.read_delta(str(validation_log_path))

    assert df.height == 2
    assert df.filter(pl.col("file_id") == 123).height == 1
    assert df.filter(pl.col("file_id") == 456).height == 1
    assert df.filter(pl.col("validation_status") == "passed").height == 1
    assert df.filter(pl.col("validation_status") == "failed").height == 1


def test_validation_record_with_large_mismatch_sample():
    """Test handling of large mismatch samples."""
    # Create a large mismatch sample
    large_sample = pl.DataFrame({
        "file_id": list(range(100)),
        "file_line_number": list(range(100)),
        "price_int_exp": [50000000] * 100,
        "price_int_ing": [50000001] * 100,
    })

    record = create_validation_record(
        file_id=123,
        table_name="trades",
        validation_status="failed",
        expected_rows=10000,
        ingested_rows=10000,
        missing_rows=0,
        extra_rows=0,
        mismatched_rows=100,
        mismatch_sample=large_sample,
        validation_duration_ms=5000,
    )

    assert record.height == 1
    assert record.item(0, "mismatch_sample") is not None
    # Verify it's serialized as JSON string
    assert isinstance(record.item(0, "mismatch_sample"), str)
