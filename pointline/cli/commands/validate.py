"""Validation commands."""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import polars as pl

from pointline.cli.utils import (
    add_lineage,
    compare_expected_vs_ingested,
    compute_sha256,
    resolve_manifest_file_id,
)
from pointline.config import get_exchange_id, get_table_path
from pointline.io.protocols import BronzeFileMetadata
from pointline.io.vendors import get_vendor
from pointline.tables.quotes import (
    QUOTES_SCHEMA,
    normalize_quotes_schema,
)
from pointline.tables.quotes import (
    encode_fixed_point as encode_quotes_fixed_point,
)
from pointline.tables.trades import (
    TRADES_SCHEMA,
    normalize_trades_schema,
)
from pointline.tables.trades import (
    encode_fixed_point as encode_trades_fixed_point,
)
from pointline.tables.validation_log import create_validation_record

logger = logging.getLogger(__name__)


def cmd_validate_quotes(args: argparse.Namespace) -> int:
    """Validate raw quotes file against ingested table."""
    start_time_ms = int(time.time() * 1000)

    path = Path(args.file)
    if not path.exists():
        raise ValueError(f"File not found: {path}")

    # Infer vendor from bronze path (e.g., bronze/tardis/exchange=...)
    vendor = args.vendor if hasattr(args, "vendor") and args.vendor else None
    if not vendor:
        # Try to extract vendor from path
        path_parts = path.as_posix().split("/")
        if "bronze" in path_parts:
            bronze_idx = path_parts.index("bronze")
            if bronze_idx + 1 < len(path_parts):
                vendor = path_parts[bronze_idx + 1]
        if not vendor:
            vendor = "tardis"  # Default fallback

    if args.file_id is None:
        file_id = resolve_manifest_file_id(
            manifest_path=Path(args.manifest_path),
            bronze_root=Path(args.bronze_root),
            file_path=path,
            data_type="quotes",
        )
    else:
        file_id = args.file_id

    bronze_root = Path(args.bronze_root)
    try:
        bronze_rel = path.relative_to(bronze_root)
    except ValueError:
        bronze_rel = path.name

    stat = path.stat()
    meta = BronzeFileMetadata(
        vendor=vendor,
        data_type="quotes",
        bronze_file_path=str(bronze_rel),
        file_size_bytes=stat.st_size,
        last_modified_ts=int(stat.st_mtime * 1_000_000),
        sha256=compute_sha256(path),
        date=None,
        interval=None,
        extra=None,
    )

    parsed_df = get_vendor(vendor).read_and_parse(path, meta)
    if parsed_df.is_empty():
        print(f"Empty CSV file: {path}")
        return 0

    unique_exchanges = parsed_df["exchange"].unique().to_list()
    invalid_exchanges: list[str] = []
    for exchange in unique_exchanges:
        try:
            get_exchange_id(exchange)  # validate exchange is known
        except ValueError:
            invalid_exchanges.append(str(exchange))
    if invalid_exchanges:
        raise ValueError(f"Unknown exchanges: {sorted(set(invalid_exchanges))}")

    # Rename exchange_symbol -> symbol (canonical event table column)
    if "exchange_symbol" in parsed_df.columns and "symbol" not in parsed_df.columns:
        parsed_df = parsed_df.rename({"exchange_symbol": "symbol"})
    dim_symbol = pl.read_delta(str(get_table_path("dim_symbol")))
    encoded_df = encode_quotes_fixed_point(parsed_df, dim_symbol, unique_exchanges[0])
    expected_df = add_lineage(encoded_df, file_id)
    expected_df = normalize_quotes_schema(expected_df)

    ingested_lf = (
        pl.scan_delta(str(get_table_path("quotes")))
        .filter(pl.col("file_id") == file_id)
        .select(list(QUOTES_SCHEMA.keys()))
    )
    ingested_count = ingested_lf.select(pl.len()).collect().item()
    if ingested_count == 0:
        raise ValueError("No ingested rows found for file_id")

    key_cols = ["file_id", "file_line_number"]
    compare_cols = [col for col in QUOTES_SCHEMA if col not in key_cols]
    (
        expected_count,
        ingested_count,
        missing_count,
        extra_count,
        mismatch_count,
        mismatch_sample,
    ) = compare_expected_vs_ingested(
        expected=expected_df,
        ingested=ingested_lf.collect(streaming=True),
        key_cols=key_cols,
        compare_cols=compare_cols,
        limit=None if args.show_all else args.limit,
    )

    # Calculate validation duration
    end_time_ms = int(time.time() * 1000)
    validation_duration_ms = end_time_ms - start_time_ms

    # Determine validation status
    validation_status = (
        "passed" if (missing_count == 0 and extra_count == 0 and mismatch_count == 0) else "failed"
    )

    # Create validation record
    validation_record = create_validation_record(
        file_id=file_id,
        table_name="quotes",
        validation_status=validation_status,
        expected_rows=expected_count,
        ingested_rows=ingested_count,
        missing_rows=missing_count,
        extra_rows=extra_count,
        mismatched_rows=mismatch_count,
        mismatch_sample=mismatch_sample if mismatch_count > 0 else None,
        validation_duration_ms=validation_duration_ms,
    )

    # Persist validation record to validation_log table
    validation_log_path = get_table_path("validation_log")

    # Check if table exists; if not, create it with overwrite mode
    if not Path(validation_log_path).exists():
        validation_record.write_delta(
            str(validation_log_path),
            mode="overwrite",
        )
    else:
        try:
            validation_record.write_delta(
                str(validation_log_path),
                mode="append",
            )
        except Exception as e:
            # Log and re-raise any errors during append
            logger.error(f"Failed to append validation record to {validation_log_path}: {e}")
            raise

    print("Validation summary (quotes):")
    print(f"  expected rows: {expected_count}")
    print(f"  ingested rows: {ingested_count}")
    print(f"  missing in ingested: {missing_count}")
    print(f"  extra in ingested: {extra_count}")
    print(f"  mismatched rows: {mismatch_count}")
    print(f"  validation status: {validation_status}")
    print(f"  validation duration: {validation_duration_ms}ms")

    if mismatch_count > 0:
        print("\nMismatch sample:")
        print(mismatch_sample)

    return 1 if (args.exit_nonzero and (missing_count or extra_count or mismatch_count)) else 0


def cmd_validate_trades(args: argparse.Namespace) -> int:
    """Validate raw trades file against ingested table."""
    start_time_ms = int(time.time() * 1000)

    path = Path(args.file)
    if not path.exists():
        raise ValueError(f"File not found: {path}")

    # Infer vendor from bronze path (e.g., bronze/tardis/exchange=...)
    vendor = args.vendor if hasattr(args, "vendor") and args.vendor else None
    if not vendor:
        # Try to extract vendor from path
        path_parts = path.as_posix().split("/")
        if "bronze" in path_parts:
            bronze_idx = path_parts.index("bronze")
            if bronze_idx + 1 < len(path_parts):
                vendor = path_parts[bronze_idx + 1]
        if not vendor:
            vendor = "tardis"  # Default fallback

    if args.file_id is None:
        file_id = resolve_manifest_file_id(
            manifest_path=Path(args.manifest_path),
            bronze_root=Path(args.bronze_root),
            file_path=path,
            data_type="trades",
        )
    else:
        file_id = args.file_id

    bronze_root = Path(args.bronze_root)
    try:
        bronze_rel = path.relative_to(bronze_root)
    except ValueError:
        bronze_rel = path.name

    stat = path.stat()
    meta = BronzeFileMetadata(
        vendor=vendor,
        data_type="trades",
        bronze_file_path=str(bronze_rel),
        file_size_bytes=stat.st_size,
        last_modified_ts=int(stat.st_mtime * 1_000_000),
        sha256=compute_sha256(path),
        date=None,
        interval=None,
        extra=None,
    )

    parsed_df = get_vendor(vendor).read_and_parse(path, meta)
    if parsed_df.is_empty():
        print(f"Empty CSV file: {path}")
        return 0

    unique_exchanges = parsed_df["exchange"].unique().to_list()
    invalid_exchanges: list[str] = []
    for exchange in unique_exchanges:
        try:
            get_exchange_id(exchange)  # validate exchange is known
        except ValueError:
            invalid_exchanges.append(str(exchange))
    if invalid_exchanges:
        raise ValueError(f"Unknown exchanges: {sorted(set(invalid_exchanges))}")

    # Rename exchange_symbol -> symbol (canonical event table column)
    if "exchange_symbol" in parsed_df.columns and "symbol" not in parsed_df.columns:
        parsed_df = parsed_df.rename({"exchange_symbol": "symbol"})
    dim_symbol = pl.read_delta(str(get_table_path("dim_symbol")))
    encoded_df = encode_trades_fixed_point(parsed_df, dim_symbol, unique_exchanges[0])
    expected_df = add_lineage(encoded_df, file_id)
    expected_df = normalize_trades_schema(expected_df)

    ingested_lf = (
        pl.scan_delta(str(get_table_path("trades")))
        .filter(pl.col("file_id") == file_id)
        .select(list(TRADES_SCHEMA.keys()))
    )
    ingested_count = ingested_lf.select(pl.len()).collect().item()
    if ingested_count == 0:
        raise ValueError("No ingested rows found for file_id")

    key_cols = ["file_id", "file_line_number"]
    compare_cols = [col for col in TRADES_SCHEMA if col not in key_cols]
    (
        expected_count,
        ingested_count,
        missing_count,
        extra_count,
        mismatch_count,
        mismatch_sample,
    ) = compare_expected_vs_ingested(
        expected=expected_df,
        ingested=ingested_lf.collect(streaming=True),
        key_cols=key_cols,
        compare_cols=compare_cols,
        limit=None if args.show_all else args.limit,
    )

    # Calculate validation duration
    end_time_ms = int(time.time() * 1000)
    validation_duration_ms = end_time_ms - start_time_ms

    # Determine validation status
    validation_status = (
        "passed" if (missing_count == 0 and extra_count == 0 and mismatch_count == 0) else "failed"
    )

    # Create validation record
    validation_record = create_validation_record(
        file_id=file_id,
        table_name="trades",
        validation_status=validation_status,
        expected_rows=expected_count,
        ingested_rows=ingested_count,
        missing_rows=missing_count,
        extra_rows=extra_count,
        mismatched_rows=mismatch_count,
        mismatch_sample=mismatch_sample if mismatch_count > 0 else None,
        validation_duration_ms=validation_duration_ms,
    )

    # Persist validation record to validation_log table
    validation_log_path = get_table_path("validation_log")

    # Check if table exists; if not, create it with overwrite mode
    if not Path(validation_log_path).exists():
        validation_record.write_delta(
            str(validation_log_path),
            mode="overwrite",
        )
    else:
        try:
            validation_record.write_delta(
                str(validation_log_path),
                mode="append",
            )
        except Exception as e:
            # Log and re-raise any errors during append
            logger.error(f"Failed to append validation record to {validation_log_path}: {e}")
            raise

    print("Validation summary (trades):")
    print(f"  expected rows: {expected_count}")
    print(f"  ingested rows: {ingested_count}")
    print(f"  missing in ingested: {missing_count}")
    print(f"  extra in ingested: {extra_count}")
    print(f"  mismatched rows: {mismatch_count}")
    print(f"  validation status: {validation_status}")
    print(f"  validation duration: {validation_duration_ms}ms")

    if mismatch_count > 0:
        print("\nMismatch sample:")
        print(mismatch_sample)

    return 1 if (args.exit_nonzero and (missing_count or extra_count or mismatch_count)) else 0
