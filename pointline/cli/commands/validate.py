"""Validation commands."""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import polars as pl

from pointline.cli.utils import (
    add_lineage,
    add_metadata,
    compare_expected_vs_ingested,
    infer_bronze_metadata,
    parse_date_arg,
    read_bronze_csv,
    resolve_manifest_file_id,
)
from pointline.config import get_exchange_id, get_table_path, normalize_exchange
from pointline.io.vendors import get_parser
from pointline.tables.quotes import (
    QUOTES_SCHEMA,
    normalize_quotes_schema,
)
from pointline.tables.quotes import (
    encode_fixed_point as encode_quotes_fixed_point,
)
from pointline.tables.quotes import (
    resolve_symbol_ids as resolve_quotes_symbol_ids,
)
from pointline.tables.trades import (
    TRADES_SCHEMA,
    normalize_trades_schema,
)
from pointline.tables.trades import (
    encode_fixed_point as encode_trades_fixed_point,
)
from pointline.tables.trades import (
    resolve_symbol_ids as resolve_trades_symbol_ids,
)
from pointline.tables.validation_log import create_validation_record

logger = logging.getLogger(__name__)


def cmd_validate_quotes(args: argparse.Namespace) -> int:
    start_time_ms = int(time.time() * 1000)

    path = Path(args.file)
    if not path.exists():
        raise ValueError(f"File not found: {path}")

    inferred = infer_bronze_metadata(path)
    exchange = args.exchange or inferred.get("exchange")
    symbol = args.symbol or inferred.get("symbol")
    date_str = args.date or inferred.get("date")

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

    if not exchange or not symbol or not date_str:
        raise ValueError(
            "validate quotes: --exchange, --symbol, and --date required (or inferable from path)"
        )

    exchange = normalize_exchange(exchange)
    exchange_id = get_exchange_id(exchange)
    file_date = parse_date_arg(date_str)
    if file_date is None:
        raise ValueError("validate quotes: --date required")

    if args.file_id is None:
        file_id = resolve_manifest_file_id(
            manifest_path=Path(args.manifest_path),
            bronze_root=Path(args.bronze_root),
            file_path=path,
            exchange=exchange,
            data_type="quotes",
            symbol=symbol,
            file_date=file_date,
        )
    else:
        file_id = args.file_id

    raw_df = read_bronze_csv(path)
    if raw_df.is_empty():
        print(f"Empty CSV file: {path}")
        return 0

    # Get vendor-specific parser
    parse_quotes = get_parser(vendor, "quotes")
    parsed_df = parse_quotes(raw_df)
    dim_symbol = pl.read_delta(str(get_table_path("dim_symbol")))
    resolved_df = resolve_quotes_symbol_ids(parsed_df, dim_symbol, exchange_id, symbol)
    encoded_df = encode_quotes_fixed_point(resolved_df, dim_symbol)
    expected_df = add_metadata(add_lineage(encoded_df, file_id), exchange, exchange_id)
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
    start_time_ms = int(time.time() * 1000)

    path = Path(args.file)
    if not path.exists():
        raise ValueError(f"File not found: {path}")

    inferred = infer_bronze_metadata(path)
    exchange = args.exchange or inferred.get("exchange")
    symbol = args.symbol or inferred.get("symbol")
    date_str = args.date or inferred.get("date")

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

    if not exchange or not symbol or not date_str:
        raise ValueError(
            "validate trades: --exchange, --symbol, and --date required (or inferable from path)"
        )

    exchange = normalize_exchange(exchange)
    exchange_id = get_exchange_id(exchange)
    file_date = parse_date_arg(date_str)
    if file_date is None:
        raise ValueError("validate trades: --date required")

    if args.file_id is None:
        file_id = resolve_manifest_file_id(
            manifest_path=Path(args.manifest_path),
            bronze_root=Path(args.bronze_root),
            file_path=path,
            exchange=exchange,
            data_type="trades",
            symbol=symbol,
            file_date=file_date,
        )
    else:
        file_id = args.file_id

    raw_df = read_bronze_csv(path)
    if raw_df.is_empty():
        print(f"Empty CSV file: {path}")
        return 0

    # Get vendor-specific parser
    parse_trades = get_parser(vendor, "trades")
    parsed_df = parse_trades(raw_df)
    dim_symbol = pl.read_delta(str(get_table_path("dim_symbol")))
    resolved_df = resolve_trades_symbol_ids(parsed_df, dim_symbol, exchange_id, symbol)
    encoded_df = encode_trades_fixed_point(resolved_df, dim_symbol)
    expected_df = add_metadata(add_lineage(encoded_df, file_id), exchange, exchange_id)
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
