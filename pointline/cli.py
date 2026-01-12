"""Command-line interface for Pointline."""

from __future__ import annotations

import argparse
import gc
import gzip
import inspect
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Iterable, Sequence

import polars as pl

from pointline.config import LAKE_ROOT, get_exchange_id, get_table_path, normalize_exchange, TABLE_PATHS
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.local_source import LocalBronzeSource
from pointline.io.protocols import BronzeFileMetadata, IngestionResult
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.vendor.tardis import build_updates_from_instruments, TardisClient, download_tardis_datasets
from pointline import research
from pointline.l2_state_checkpoint import build_state_checkpoints_delta
from pointline.services.dim_symbol_service import DimSymbolService
from pointline.services.trades_service import TradesIngestionService
from pointline.services.quotes_service import QuotesIngestionService
from pointline.services.book_snapshots_service import BookSnapshotsIngestionService
from pointline.services.l2_updates_service import L2UpdatesIngestionService
from pointline.services.derivative_ticker_service import DerivativeTickerIngestionService
from pointline.registry import find_symbol, resolve_symbol
from pointline.quotes import (
    QUOTES_SCHEMA,
    encode_fixed_point as encode_quotes_fixed_point,
    normalize_quotes_schema,
    parse_tardis_quotes_csv,
    resolve_symbol_ids as resolve_quotes_symbol_ids,
)
from pointline.trades import (
    TRADES_SCHEMA,
    encode_fixed_point as encode_trades_fixed_point,
    normalize_trades_schema,
    parse_tardis_trades_csv,
    resolve_symbol_ids as resolve_trades_symbol_ids,
)


def _sorted_files(files: Iterable[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
    return sorted(
        files,
        key=lambda f: (f.exchange, f.data_type, f.symbol, f.date.isoformat(), f.bronze_file_path),
    )


TABLE_PARTITIONS = {
    "trades": ["exchange", "date"],
    "quotes": ["exchange", "date"],
    "book_snapshot_25": ["exchange", "date"],
    "l2_updates": ["exchange", "date", "symbol_id"],
    "l2_state_checkpoint": ["exchange", "date", "symbol_id"],
}


def _print_files(files: Sequence[BronzeFileMetadata]) -> None:
    for f in files:
        print(
            " | ".join(
                [
                    f"exchange={f.exchange}",
                    f"type={f.data_type}",
                    f"symbol={f.symbol}",
                    f"date={f.date.isoformat()}",
                    f"path={f.bronze_file_path}",
                ]
            )
    )


def _cmd_ingest_discover(args: argparse.Namespace) -> int:
    bronze_root = Path(args.bronze_root)
    source = LocalBronzeSource(bronze_root)
    files = list(source.list_files(args.glob))

    # Filter by data type if specified
    if args.data_type:
        files = [f for f in files if f.data_type == args.data_type]

    if args.pending_only:
        manifest_repo = DeltaManifestRepository(Path(args.manifest_path))
        files = manifest_repo.filter_pending(files)

    files = _sorted_files(files)
    label = "pending files" if args.pending_only else "files"
    print(f"{label}: {len(files)}")
    _print_files(files)
    return 0


def _compute_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _parse_date_arg(value: str | None) -> date | None:
    if value is None:
        return None
    from datetime import datetime as _dt

    try:
        return _dt.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {value} (expected YYYY-MM-DD)") from exc


def _resolve_manifest_file_id(
    *,
    manifest_path: Path,
    bronze_root: Path,
    file_path: Path,
    exchange: str,
    data_type: str,
    symbol: str,
    file_date: date,
) -> int:
    manifest_repo = DeltaManifestRepository(manifest_path)
    if not file_path.exists():
        raise ValueError(f"File not found: {file_path}")

    try:
        bronze_rel = file_path.relative_to(bronze_root)
    except ValueError as exc:
        raise ValueError(
            f"File is not under bronze root: {file_path} (bronze_root={bronze_root})"
        ) from exc

    sha256 = _compute_sha256(file_path)
    manifest_df = manifest_repo.read_all()
    if manifest_df.is_empty():
        raise ValueError("manifest is empty; cannot resolve file_id")

    matches = manifest_df.filter(
        (pl.col("exchange") == exchange)
        & (pl.col("data_type") == data_type)
        & (pl.col("symbol") == symbol)
        & (pl.col("date") == file_date)
        & (pl.col("bronze_file_name") == str(bronze_rel))
        & (pl.col("sha256") == sha256)
    )
    if matches.is_empty():
        raise ValueError("No manifest record found for file path + sha256")

    return matches.item(0, "file_id")


def _add_lineage(df: pl.DataFrame, file_id: int) -> pl.DataFrame:
    if "file_line_number" in df.columns:
        file_line_number = pl.col("file_line_number").cast(pl.Int32)
    else:
        file_line_number = pl.int_range(1, df.height + 1, dtype=pl.Int32)
    ingest_seq = pl.int_range(1, df.height + 1, dtype=pl.Int32)
    return df.with_columns(
        [
            pl.lit(file_id, dtype=pl.Int32).alias("file_id"),
            file_line_number.alias("file_line_number"),
            ingest_seq.alias("ingest_seq"),
        ]
    )


def _add_metadata(df: pl.DataFrame, exchange: str, exchange_id: int) -> pl.DataFrame:
    result = df.with_columns(
        [
            pl.lit(exchange, dtype=pl.Utf8).alias("exchange"),
            pl.lit(exchange_id, dtype=pl.Int16).alias("exchange_id"),
        ]
    )
    return result.with_columns(
        [
            pl.from_epoch(pl.col("ts_local_us"), time_unit="us")
            .cast(pl.Date)
            .alias("date"),
        ]
    )


def _compare_expected_vs_ingested(
    *,
    expected: pl.DataFrame,
    ingested: pl.DataFrame,
    key_cols: list[str],
    compare_cols: list[str],
    limit: int | None,
) -> tuple[int, int, int, int, int, pl.DataFrame]:
    expected_marked = expected.select(key_cols + compare_cols).with_columns(
        pl.lit(True).alias("_present_exp")
    )
    ingested_marked = ingested.select(key_cols + compare_cols).with_columns(
        pl.lit(True).alias("_present_ing")
    )

    exp_renames = {col: f"{col}_exp" for col in compare_cols}
    ing_renames = {col: f"{col}_ing" for col in compare_cols}
    expected_marked = expected_marked.rename(exp_renames)
    ingested_marked = ingested_marked.rename(ing_renames)

    joined = expected_marked.join(ingested_marked, on=key_cols, how="outer")

    missing_in_ingested = joined.filter(
        pl.col("_present_exp").is_not_null() & pl.col("_present_ing").is_null()
    )
    extra_in_ingested = joined.filter(
        pl.col("_present_exp").is_null() & pl.col("_present_ing").is_not_null()
    )

    comparisons = [
        pl.col(f"{col}_exp").eq_missing(pl.col(f"{col}_ing")) for col in compare_cols
    ]
    all_equal = pl.all_horizontal(comparisons)
    mismatched = joined.filter(
        pl.col("_present_exp").is_not_null()
        & pl.col("_present_ing").is_not_null()
        & ~all_equal
    )

    mismatch_sample = mismatched.select(
        key_cols + [f"{col}_exp" for col in compare_cols] + [f"{col}_ing" for col in compare_cols]
    )
    if limit is not None:
        mismatch_sample = mismatch_sample.head(limit)

    return (
        expected.height,
        ingested.height,
        missing_in_ingested.height,
        extra_in_ingested.height,
        mismatched.height,
        mismatch_sample,
    )


def _cmd_validate_quotes(args: argparse.Namespace) -> int:
    path = Path(args.file)
    if not path.exists():
        raise ValueError(f"File not found: {path}")

    inferred = _infer_bronze_metadata(path)
    exchange = args.exchange or inferred.get("exchange")
    symbol = args.symbol or inferred.get("symbol")
    date_str = args.date or inferred.get("date")
    if not exchange or not symbol or not date_str:
        raise ValueError(
            "validate quotes: --exchange, --symbol, and --date required (or inferable from path)"
        )

    exchange = normalize_exchange(exchange)
    exchange_id = get_exchange_id(exchange)
    file_date = _parse_date_arg(date_str)
    if file_date is None:
        raise ValueError("validate quotes: --date required")

    if args.file_id is None:
        file_id = _resolve_manifest_file_id(
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

    raw_df = _read_bronze_csv(path)
    if raw_df.is_empty():
        print(f"Empty CSV file: {path}")
        return 0

    parsed_df = parse_tardis_quotes_csv(raw_df)
    dim_symbol = pl.read_delta(str(get_table_path("dim_symbol")))
    resolved_df = resolve_quotes_symbol_ids(parsed_df, dim_symbol, exchange_id, symbol)
    encoded_df = encode_quotes_fixed_point(resolved_df, dim_symbol)
    expected_df = _add_metadata(
        _add_lineage(encoded_df, file_id), exchange, exchange_id
    )
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
    compare_cols = [col for col in QUOTES_SCHEMA.keys() if col not in key_cols]
    (
        expected_count,
        ingested_count,
        missing_count,
        extra_count,
        mismatch_count,
        mismatch_sample,
    ) = _compare_expected_vs_ingested(
        expected=expected_df,
        ingested=ingested_lf.collect(streaming=True),
        key_cols=key_cols,
        compare_cols=compare_cols,
        limit=None if args.show_all else args.limit,
    )

    print("Validation summary (quotes):")
    print(f"  expected rows: {expected_count}")
    print(f"  ingested rows: {ingested_count}")
    print(f"  missing in ingested: {missing_count}")
    print(f"  extra in ingested: {extra_count}")
    print(f"  mismatched rows: {mismatch_count}")

    if mismatch_count > 0:
        print("\nMismatch sample:")
        print(mismatch_sample)

    return 1 if (args.exit_nonzero and (missing_count or extra_count or mismatch_count)) else 0


def _cmd_validate_trades(args: argparse.Namespace) -> int:
    path = Path(args.file)
    if not path.exists():
        raise ValueError(f"File not found: {path}")

    inferred = _infer_bronze_metadata(path)
    exchange = args.exchange or inferred.get("exchange")
    symbol = args.symbol or inferred.get("symbol")
    date_str = args.date or inferred.get("date")
    if not exchange or not symbol or not date_str:
        raise ValueError(
            "validate trades: --exchange, --symbol, and --date required (or inferable from path)"
        )

    exchange = normalize_exchange(exchange)
    exchange_id = get_exchange_id(exchange)
    file_date = _parse_date_arg(date_str)
    if file_date is None:
        raise ValueError("validate trades: --date required")

    if args.file_id is None:
        file_id = _resolve_manifest_file_id(
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

    raw_df = _read_bronze_csv(path)
    if raw_df.is_empty():
        print(f"Empty CSV file: {path}")
        return 0

    parsed_df = parse_tardis_trades_csv(raw_df)
    dim_symbol = pl.read_delta(str(get_table_path("dim_symbol")))
    resolved_df = resolve_trades_symbol_ids(parsed_df, dim_symbol, exchange_id, symbol)
    encoded_df = encode_trades_fixed_point(resolved_df, dim_symbol)
    expected_df = _add_metadata(
        _add_lineage(encoded_df, file_id), exchange, exchange_id
    )
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
    compare_cols = [col for col in TRADES_SCHEMA.keys() if col not in key_cols]
    (
        expected_count,
        ingested_count,
        missing_count,
        extra_count,
        mismatch_count,
        mismatch_sample,
    ) = _compare_expected_vs_ingested(
        expected=expected_df,
        ingested=ingested_lf.collect(streaming=True),
        key_cols=key_cols,
        compare_cols=compare_cols,
        limit=None if args.show_all else args.limit,
    )

    print("Validation summary (trades):")
    print(f"  expected rows: {expected_count}")
    print(f"  ingested rows: {ingested_count}")
    print(f"  missing in ingested: {missing_count}")
    print(f"  extra in ingested: {extra_count}")
    print(f"  mismatched rows: {mismatch_count}")

    if mismatch_count > 0:
        print("\nMismatch sample:")
        print(mismatch_sample)

    return 1 if (args.exit_nonzero and (missing_count or extra_count or mismatch_count)) else 0


def _create_ingestion_service(data_type: str, manifest_repo):
    """Create the appropriate ingestion service based on data type."""
    dim_symbol_repo = BaseDeltaRepository(get_table_path("dim_symbol"))
    
        # Map bronze layer data_type to canonical table name
        # Uses Tardis naming (book_snapshot_25) for consistency
    if data_type == "trades":
        repo = BaseDeltaRepository(
            get_table_path("trades"),
            partition_by=["exchange", "date"]
        )
        return TradesIngestionService(repo, dim_symbol_repo, manifest_repo)
    elif data_type == "quotes":
        repo = BaseDeltaRepository(
            get_table_path("quotes"),
            partition_by=["exchange", "date"]
        )
        return QuotesIngestionService(repo, dim_symbol_repo, manifest_repo)
    elif data_type == "book_snapshot_25":
        repo = BaseDeltaRepository(
            get_table_path("book_snapshot_25"),
            partition_by=["exchange", "date"]
        )
        return BookSnapshotsIngestionService(repo, dim_symbol_repo, manifest_repo)
    elif data_type == "derivative_ticker":
        repo = BaseDeltaRepository(
            get_table_path("derivative_ticker"),
            partition_by=["exchange", "date"]
        )
        return DerivativeTickerIngestionService(repo, dim_symbol_repo, manifest_repo)
    elif data_type == "incremental_book_L2":
        repo = BaseDeltaRepository(
            get_table_path("l2_updates"),
            partition_by=["exchange", "date", "symbol_id"]
        )
        return L2UpdatesIngestionService(repo, dim_symbol_repo, manifest_repo)
    else:
        raise ValueError(f"Unsupported data type: {data_type}")


def _parse_partition_filters(items: Sequence[str]) -> dict[str, object]:
    filters: dict[str, object] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid partition filter: {item}")
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if key == "date":
            from datetime import datetime

            try:
                filters[key] = datetime.strptime(value, "%Y-%m-%d").date()
                continue
            except ValueError as exc:
                raise ValueError(f"Invalid date format for {key}: {value}") from exc
        if value.lstrip("-").isdigit():
            filters[key] = int(value)
        else:
            filters[key] = value
    return filters


def _read_bronze_csv(path: Path) -> pl.DataFrame:
    """Read a bronze CSV file with line numbers preserved."""
    read_options = {
        "infer_schema_length": 10000,
        "try_parse_dates": False,
    }
    if "row_index_name" in inspect.signature(pl.read_csv).parameters:
        read_options["row_index_name"] = "file_line_number"
        read_options["row_index_offset"] = 2
    else:
        read_options["row_count_name"] = "file_line_number"
        read_options["row_count_offset"] = 2

    try:
        if path.suffix == ".gz" or str(path).endswith(".csv.gz"):
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                return pl.read_csv(handle, **read_options)
        return pl.read_csv(path, **read_options)
    except pl.exceptions.NoDataError:
        return pl.DataFrame()


def _infer_bronze_metadata(path: Path) -> dict[str, str]:
    match = re.search(
        r"exchange=([^/]+)/type=([^/]+)/date=([^/]+)/symbol=([^/]+)/",
        path.as_posix(),
    )
    if not match:
        return {}
    exchange, data_type, date_str, symbol = match.groups()
    return {
        "exchange": exchange,
        "data_type": data_type,
        "date": date_str,
        "symbol": symbol,
    }


def _cmd_delta_optimize(args: argparse.Namespace) -> int:
    try:
        filters = _parse_partition_filters(args.partition)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 2

    if not filters:
        print("Error: at least one --partition KEY=VALUE is required")
        return 2

    partition_by = TABLE_PARTITIONS.get(args.table)
    repo = BaseDeltaRepository(get_table_path(args.table), partition_by=partition_by)
    z_order = [s.strip() for s in args.zorder.split(",")] if args.zorder else None
    try:
        metrics = repo.optimize_partition(
            filters=filters,
            target_file_size=args.target_file_size,
            z_order=z_order,
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1

    if not metrics or metrics.get("totalConsideredFiles", 0) == 0:
        print("No rows matched the partition filters; nothing to optimize.")
        return 0

    predicate = " AND ".join(f"{k}={v}" for k, v in filters.items())
    print(f"Optimized {args.table} where {predicate}")
    print(
        f"filesRemoved={metrics.get('numFilesRemoved')}, "
        f"filesAdded={metrics.get('numFilesAdded')}, "
        f"totalConsideredFiles={metrics.get('totalConsideredFiles')}"
    )
    return 0


def _cmd_delta_vacuum(args: argparse.Namespace) -> int:
    partition_by = TABLE_PARTITIONS.get(args.table)
    repo = BaseDeltaRepository(get_table_path(args.table), partition_by=partition_by)
    dry_run = not args.execute
    try:
        removed = repo.vacuum(
            retention_hours=args.retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=not args.no_retention_check,
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1

    if dry_run:
        print(f"Vacuum dry run: {len(removed)} files would be removed.")
    else:
        print(f"Vacuum complete: {len(removed)} files removed.")
    return 0


def _cmd_ingest_run(args: argparse.Namespace) -> int:
    """Run ingestion for pending bronze files."""
    bronze_root = Path(args.bronze_root)
    source = LocalBronzeSource(bronze_root)
    manifest_repo = DeltaManifestRepository(Path(args.manifest_path))
    
    # Discover files
    files = list(source.list_files(args.glob))
    
    # Filter by data type if specified
    if args.data_type:
        files = [f for f in files if f.data_type == args.data_type]
    
    # Filter pending files
    if not args.force:
        files = manifest_repo.filter_pending(files)
    
    if not files:
        print("No files to ingest.")
        return 0
    
    # Filter quarantined if retry requested
    if args.retry_quarantined:
        manifest_df = manifest_repo.read_all()
        quarantined = manifest_df.filter(pl.col("status") == "quarantined")
        if not quarantined.is_empty():
            quarantined_paths = set(quarantined["bronze_file_name"].to_list())
            files = [f for f in files if f.bronze_file_path in quarantined_paths]
        else:
            print("No quarantined files to retry.")
            return 0
    
    files = _sorted_files(files)
    print(f"Ingesting {len(files)} file(s)...")
    
    # Process each file
    success_count = 0
    failed_count = 0
    quarantined_count = 0
    
    # Group files by data_type to reuse services
    files_by_type = {}
    for file_meta in files:
        if file_meta.data_type not in files_by_type:
            files_by_type[file_meta.data_type] = []
        files_by_type[file_meta.data_type].append(file_meta)
    
    for data_type, type_files in files_by_type.items():
        try:
            service = _create_ingestion_service(data_type, manifest_repo)
        except ValueError as e:
            print(f"Error: {e}")
            for file_meta in type_files:
                print(f"✗ {file_meta.bronze_file_path}: Unsupported data type")
                failed_count += 1
            continue
        
        for file_meta in type_files:
            if args.data_type and file_meta.data_type != args.data_type:
                continue
            
            # Resolve file_id
            file_id = manifest_repo.resolve_file_id(file_meta)
            
            # Ingest file
            result = service.ingest_file(file_meta, file_id, bronze_root=bronze_root)

            # Optional post-ingest validation (sampled)
            if (
                args.validate
                and result.error_message is None
                and hasattr(service, "validate_ingested")
            ):
                ok, message = service.validate_ingested(
                    file_meta,
                    file_id,
                    bronze_root=bronze_root,
                    sample_size=args.validate_sample_size,
                    seed=args.validate_seed,
                )
                if not ok:
                    result = IngestionResult(
                        row_count=result.row_count,
                        ts_local_min_us=result.ts_local_min_us,
                        ts_local_max_us=result.ts_local_max_us,
                        error_message=message,
                    )
            
            # Update manifest
            if result.error_message:
                if "missing_symbol" in result.error_message or "invalid_validity_window" in result.error_message:
                    status = "quarantined"
                    quarantined_count += 1
                else:
                    status = "failed"
                    failed_count += 1
            else:
                status = "success"
                success_count += 1
            
            manifest_repo.update_status(file_id, status, file_meta, result)
            
            # Print result
            if status == "success":
                print(f"✓ {file_meta.bronze_file_path}: {result.row_count} rows")
            elif status == "quarantined":
                print(f"⚠ {file_meta.bronze_file_path}: QUARANTINED - {result.error_message}")
            else:
                print(f"✗ {file_meta.bronze_file_path}: FAILED - {result.error_message}")
            
            # Force garbage collection to free memory between large files
            gc.collect()
    
    print(f"\nSummary: {success_count} succeeded, {failed_count} failed, {quarantined_count} quarantined")
    return 0 if failed_count == 0 else 1


def _cmd_manifest_show(args: argparse.Namespace) -> int:
    manifest_repo = DeltaManifestRepository(Path(args.manifest_path))
    df = manifest_repo.read_all()

    if df.is_empty():
        print("manifest: empty")
        return 0

    # Apply filters if provided
    if args.status:
        df = df.filter(pl.col("status") == args.status)
    if args.exchange:
        df = df.filter(pl.col("exchange") == args.exchange)
    if args.data_type:
        df = df.filter(pl.col("data_type") == args.data_type)
    if args.symbol:
        df = df.filter(pl.col("symbol") == args.symbol)

    if df.is_empty():
        print("manifest: no matching records")
        return 0

    # Show detailed view or summary
    if args.detailed:
        # Show detailed file information
        if args.limit:
            df = df.head(args.limit)

        # Select relevant columns for display
        display_cols = [
            "file_id",
            "exchange",
            "data_type",
            "symbol",
            "date",
            "status",
            "row_count",
            "bronze_file_name",
        ]
        if args.show_errors:
            display_cols.append("error_message")

        available_cols = [col for col in display_cols if col in df.columns]
        display_df = df.select(available_cols)

        print(f"manifest entries ({df.height} total):")
        print(display_df)
    else:
        # Show summary view
        summary = (
            df.group_by("status")
            .agg(pl.len().alias("count"))
            .sort("status")
        )
        print("manifest status counts:")
        for row in summary.iter_rows(named=True):
            print(f"  {row['status']}: {row['count']}")

        # Additional summary stats
        if "exchange" in df.columns:
            exchange_summary = (
                df.group_by("exchange")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
            )
            if exchange_summary.height > 0:
                print("\nexchange counts:")
                for row in exchange_summary.iter_rows(named=True):
                    print(f"  {row['exchange']}: {row['count']}")

        print(f"\ntotal rows: {df.height}")
    
    return 0


def _cmd_manifest_backfill_sha256(args: argparse.Namespace) -> int:
    manifest_path = Path(args.manifest_path)
    bronze_root = Path(args.bronze_root)
    manifest_repo = DeltaManifestRepository(manifest_path)
    df = manifest_repo.read_all()

    if df.is_empty():
        print("manifest: empty")
        return 0

    if "sha256" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("sha256"))

    candidates = df.filter(pl.col("sha256").is_null() | (pl.col("sha256") == ""))
    if candidates.is_empty():
        print("manifest: no rows missing sha256")
        return 0

    if args.limit:
        candidates = candidates.head(args.limit)

    total = candidates.height
    updated_rows: list[dict[str, object]] = []
    missing_files: list[str] = []

    for row in candidates.iter_rows(named=True):
        bronze_name = row.get("bronze_file_name")
        if bronze_name is None:
            missing_files.append("<missing bronze_file_name>")
            continue

        path = bronze_root / str(bronze_name)
        if not path.exists():
            missing_files.append(str(path))
            continue

        sha256 = _compute_sha256(path)
        updated = dict(row)
        updated["sha256"] = sha256
        updated_rows.append(updated)

    if args.dry_run:
        print("manifest sha256 backfill dry-run:")
        print(f"  candidates: {total}")
        print(f"  would update: {len(updated_rows)}")
        print(f"  missing files: {len(missing_files)}")
        if missing_files:
            sample = missing_files[: min(10, len(missing_files))]
            print(f"  missing sample: {sample}")
        return 0

    if not updated_rows:
        print("manifest: no rows updated (all missing files)")
        return 0

    batch_size = max(1, args.batch_size)
    for start in range(0, len(updated_rows), batch_size):
        batch = updated_rows[start : start + batch_size]
        batch_df = pl.DataFrame(batch, schema=df.schema)
        manifest_repo.merge(batch_df, keys=["file_id"])

    print("manifest sha256 backfill complete:")
    print(f"  candidates: {total}")
    print(f"  updated: {len(updated_rows)}")
    print(f"  missing files: {len(missing_files)}")
    if missing_files:
        sample = missing_files[: min(10, len(missing_files))]
        print(f"  missing sample: {sample}")
    return 0


def _read_updates(path: Path) -> pl.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pl.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pl.read_parquet(path)
    raise SystemExit(f"Unsupported update file format: {path}")


def _parse_effective_ts(value: str | None) -> int:
    if value is None or value.lower() == "now":
        return int(time.time() * 1_000_000)
    try:
        return int(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid --effective-ts value: {value}") from exc


def _cmd_dim_symbol_upsert(args: argparse.Namespace) -> int:
    updates = _read_updates(Path(args.file))
    repo = BaseDeltaRepository(Path(args.table_path))
    service = DimSymbolService(repo)
    service.update(updates)
    print(f"dim_symbol updated: {updates.height} rows")
    return 0


def _cmd_download(args: argparse.Namespace) -> int:
    """Download Tardis datasets into the Bronze layer."""
    data_types = [item.strip() for item in args.data_types.split(",") if item.strip()]
    symbols = [item.strip() for item in args.symbols.split(",") if item.strip()]

    if not data_types or not symbols:
        print("Error: data-types and symbols must be non-empty")
        return 1

    try:
        download_tardis_datasets(
            exchange=args.exchange,
            data_types=data_types,
            symbols=symbols,
            from_date=args.from_date,
            to_date=args.to_date,
            format=args.format,
            api_key=args.api_key,
            download_dir=args.download_dir,
            filename_template=args.filename_template,
            concurrency=args.concurrency,
            http_proxy=args.http_proxy,
        )
        print("Download complete.")
        return 0
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}")
        return 2


def _cmd_dim_symbol_sync(args: argparse.Namespace) -> int:
    """
    Proposed sync command that handles history rebuilds.
    In a real app, this would fetch from an API. Here we read from --source.
    """
    if args.source == "api":
        if not args.exchange:
            print("Error: --exchange is required when --source=api")
            return 1

        try:
            filter_payload = json.loads(args.filter) if args.filter else None
        except json.JSONDecodeError as exc:
            print(f"Error: invalid --filter JSON: {exc}")
            return 1

        effective_ts = _parse_effective_ts(args.effective_ts)
        api_key = args.api_key or os.getenv("TARDIS_API_KEY", "")

        client = TardisClient(api_key=api_key)
        instruments = client.fetch_instruments(
            args.exchange,
            symbol=args.symbol,
            filter_payload=filter_payload,
        )
        updates = build_updates_from_instruments(
            instruments,
            exchange=args.exchange,
            effective_ts=effective_ts,
            rebuild=args.rebuild,
        )
    else:
        source_path = Path(args.source)
        if not source_path.exists():
            print(f"Error: source {source_path} not found")
            return 2

        # In this mock-up, we'll assume the file contains the flattened history
        # as described in the 'dim-symbol-sync.md' document.
        updates = _read_updates(source_path)
    
    repo = BaseDeltaRepository(Path(args.table_path))
    service = DimSymbolService(repo)
    
    if args.rebuild:
        print(f"Rebuilding history for {updates.select('exchange_symbol').n_unique()} symbols...")
        service.rebuild(updates)
    else:
        print("Applying incremental updates...")
        service.update(updates)
        
    print("Sync complete.")
    return 0


def _parse_symbol_id_single(value: str | None) -> int | None:
    if not value:
        return None
    items = [int(part.strip()) for part in value.split(",") if part.strip()]
    if not items:
        return None
    if len(items) != 1:
        raise ValueError("symbol_id must be a single value")
    return items[0]



def _cmd_l2_state_checkpoint_build(args: argparse.Namespace) -> int:
    try:
        symbol_id = _parse_symbol_id_single(args.symbol_id)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 2

    if symbol_id is None:
        print("Error: symbol_id is required for l2-state-checkpoint")
        return 2

    exchange, exchange_id, _ = resolve_symbol(symbol_id)

    rows_written = build_state_checkpoints_delta(
        updates_path=get_table_path("l2_updates"),
        output_path=get_table_path("l2_state_checkpoint"),
        exchange=exchange,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        start_date=args.start_date,
        end_date=args.end_date,
        checkpoint_every_us=args.checkpoint_every_us,
        checkpoint_every_updates=args.checkpoint_every_updates,
        validate_monotonic=args.validate_monotonic,
        assume_sorted=args.assume_sorted,
    )
    print(f"l2_state_checkpoint: wrote {rows_written} row(s)")
    return 0


def _cmd_symbol_search(args: argparse.Namespace) -> int:
    df = find_symbol(
        query=args.query,
        exchange=args.exchange,
        base_asset=args.base_asset,
        quote_asset=args.quote_asset,
    )

    if df.is_empty():
        print("No matching symbols found.")
        return 0

    print(f"Found {df.height} matching symbols:")
    
    # Configure display options
    with pl.Config(tbl_rows=100, tbl_cols=20, fmt_float="full"):
        # Select key columns for display
        cols = [
            "symbol_id", "exchange", "exchange_symbol", 
            "base_asset", "quote_asset", "asset_type",
            "tick_size", "lot_size", "price_increment", "amount_increment", "contract_size",
            "valid_from_ts", "valid_until_ts"
        ]
        # Only select columns that exist in the dataframe (in case registry schema evolves)
        display_cols = [c for c in cols if c in df.columns]
        print(df.select(display_cols))

    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pointline", description="Pointline data lake CLI")
    subparsers = parser.add_subparsers(dest="command")

    # --- Symbol Registry ---
    symbol = subparsers.add_parser("symbol", help="Symbol registry utilities")
    symbol_sub = symbol.add_subparsers(dest="symbol_command")

    symbol_search = symbol_sub.add_parser("search", help="Search for symbols")
    symbol_search.add_argument("query", nargs="?", help="Fuzzy search term")
    symbol_search.add_argument("--exchange", help="Filter by exchange")
    symbol_search.add_argument("--base-asset", help="Filter by base asset")
    symbol_search.add_argument("--quote-asset", help="Filter by quote asset")
    symbol_search.set_defaults(func=_cmd_symbol_search)

    download = subparsers.add_parser("download", help="Download Tardis datasets to Bronze layer")
    download.add_argument("--exchange", required=True, help="Exchange name (e.g., binance)")
    download.add_argument(
        "--data-types",
        required=True,
        help="Comma-separated list of data types (e.g., trades,quotes,book_snapshot_25)",
    )
    download.add_argument(
        "--symbols",
        required=True,
        help="Comma-separated list of symbols (e.g., BTCUSDT,ETHUSDT)",
    )
    download.add_argument(
        "--from-date",
        required=True,
        help="Start date YYYY-MM-DD (inclusive)",
    )
    download.add_argument(
        "--to-date",
        required=True,
        help="End date YYYY-MM-DD (non-inclusive)",
    )
    download.add_argument(
        "--format",
        default="csv",
        help="Dataset format (default: csv)",
    )
    download.add_argument(
        "--download-dir",
        default=str(LAKE_ROOT),
        help=f"Root directory for downloads (default: {LAKE_ROOT})",
    )
    download.add_argument(
        "--filename-template",
        required=True,
        help="Template with {exchange},{data_type},{date},{symbol},{format}",
    )
    download.add_argument(
        "--api-key",
        default=os.getenv("TARDIS_API_KEY", ""),
        help="Tardis API key (or set TARDIS_API_KEY)",
    )
    download.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of concurrent downloads (default: 5)",
    )
    download.add_argument("--http-proxy", default=None, help="HTTP proxy URL (optional)")
    download.set_defaults(func=_cmd_download)

    ingest = subparsers.add_parser("ingest", help="Ingestion ledger utilities")
    ingest_sub = ingest.add_subparsers(dest="ingest_command")

    ingest_discover = ingest_sub.add_parser("discover", help="Discover bronze files")
    ingest_discover.add_argument(
        "--bronze-root",
        default=str(LAKE_ROOT / "tardis"),
        help="Bronze root path (default: LAKE_ROOT/tardis)",
    )
    ingest_discover.add_argument(
        "--glob",
        default="**/*.csv.gz",
        help="Glob pattern for bronze files",
    )
    ingest_discover.add_argument(
        "--manifest-path",
        default=str(get_table_path("ingest_manifest")),
        help="Path to the ingest manifest table",
    )
    ingest_discover.add_argument(
        "--data-type",
        help="Filter by data type (e.g., trades, quotes, book_snapshot_25)",
    )
    ingest_discover.add_argument(
        "--pending-only",
        action="store_true",
        help="Only show files not yet marked success in the manifest",
    )
    ingest_discover.set_defaults(func=_cmd_ingest_discover)

    ingest_run = ingest_sub.add_parser("run", help="Run ingestion for pending files")
    ingest_run.add_argument(
        "--bronze-root",
        default=str(LAKE_ROOT / "tardis"),
        help="Bronze root path (default: LAKE_ROOT/tardis)",
    )
    ingest_run.add_argument(
        "--glob",
        default="**/*.csv.gz",
        help="Glob pattern for bronze files",
    )
    ingest_run.add_argument(
        "--manifest-path",
        default=str(get_table_path("ingest_manifest")),
        help="Path to the ingest manifest table",
    )
    ingest_run.add_argument(
        "--data-type",
        help="Filter by data type (e.g., trades, quotes, book_snapshot_25).",
    )
    ingest_run.add_argument(
        "--force",
        action="store_true",
        help="Re-ingest files even if already marked as success",
    )
    ingest_run.add_argument(
        "--retry-quarantined",
        action="store_true",
        help="Retry ingestion for quarantined files",
    )
    ingest_run.add_argument(
        "--validate",
        action="store_true",
        help="Validate ingested rows against raw file (sampled)",
    )
    ingest_run.add_argument(
        "--validate-sample-size",
        type=int,
        default=2000,
        help="Number of rows to sample for post-ingest validation (default: 2000)",
    )
    ingest_run.add_argument(
        "--validate-seed",
        type=int,
        default=0,
        help="Random seed for validation sampling (default: 0)",
    )
    ingest_run.set_defaults(func=_cmd_ingest_run)

    validate = subparsers.add_parser("validate", help="Validate raw files")
    validate_sub = validate.add_subparsers(dest="validate_command")

    validate_quotes = validate_sub.add_parser(
        "quotes", help="Validate raw quotes file against ingested table"
    )
    validate_quotes.add_argument("--file", required=True, help="Path to the raw quotes CSV file")
    validate_quotes.add_argument(
        "--file-id",
        type=int,
        default=None,
        help="File ID to validate (if omitted, resolve via manifest)",
    )
    validate_quotes.add_argument(
        "--date",
        default=None,
        help="File date (YYYY-MM-DD), inferred from path when possible",
    )
    validate_quotes.add_argument(
        "--exchange",
        default=None,
        help="Exchange name (defaults to value inferred from path)",
    )
    validate_quotes.add_argument(
        "--symbol",
        default=None,
        help="Exchange symbol (defaults to value inferred from path)",
    )
    validate_quotes.add_argument(
        "--bronze-root",
        default=str(LAKE_ROOT / "tardis"),
        help="Bronze root used to resolve manifest paths (default: LAKE_ROOT/tardis)",
    )
    validate_quotes.add_argument(
        "--manifest-path",
        default=str(get_table_path("ingest_manifest")),
        help="Path to ingest_manifest (default: LAKE_ROOT/silver/ingest_manifest)",
    )
    validate_quotes.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max invalid rows to print (default: 20)",
    )
    validate_quotes.add_argument(
        "--show-all",
        action="store_true",
        help="Print all mismatched rows",
    )
    validate_quotes.add_argument(
        "--exit-nonzero",
        action="store_true",
        help="Exit with status 1 if invalid rows are found",
    )
    validate_quotes.set_defaults(func=_cmd_validate_quotes)

    validate_trades = validate_sub.add_parser(
        "trades", help="Validate raw trades file against ingested table"
    )
    validate_trades.add_argument("--file", required=True, help="Path to the raw trades CSV file")
    validate_trades.add_argument(
        "--file-id",
        type=int,
        default=None,
        help="File ID to validate (if omitted, resolve via manifest)",
    )
    validate_trades.add_argument(
        "--date",
        default=None,
        help="File date (YYYY-MM-DD), inferred from path when possible",
    )
    validate_trades.add_argument(
        "--exchange",
        default=None,
        help="Exchange name (defaults to value inferred from path)",
    )
    validate_trades.add_argument(
        "--symbol",
        default=None,
        help="Exchange symbol (defaults to value inferred from path)",
    )
    validate_trades.add_argument(
        "--bronze-root",
        default=str(LAKE_ROOT / "tardis"),
        help="Bronze root used to resolve manifest paths (default: LAKE_ROOT/tardis)",
    )
    validate_trades.add_argument(
        "--manifest-path",
        default=str(get_table_path("ingest_manifest")),
        help="Path to ingest_manifest (default: LAKE_ROOT/silver/ingest_manifest)",
    )
    validate_trades.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max mismatched rows to print (default: 20)",
    )
    validate_trades.add_argument(
        "--show-all",
        action="store_true",
        help="Print all mismatched rows",
    )
    validate_trades.add_argument(
        "--exit-nonzero",
        action="store_true",
        help="Exit with status 1 if mismatches are found",
    )
    validate_trades.set_defaults(func=_cmd_validate_trades)

    delta = subparsers.add_parser("delta", help="Delta Lake maintenance utilities")
    delta_sub = delta.add_subparsers(dest="delta_command")

    delta_optimize = delta_sub.add_parser("optimize", help="Compact files for a partition")
    delta_optimize.add_argument(
        "--table",
        required=True,
        choices=sorted(TABLE_PATHS.keys()),
        help="Table name to optimize",
    )
    delta_optimize.add_argument(
        "--partition",
        action="append",
        default=[],
        help="Partition filter in KEY=VALUE form (repeatable)",
    )
    delta_optimize.add_argument(
        "--target-file-size",
        type=int,
        default=None,
        help="Target file size in bytes (optional)",
    )
    delta_optimize.add_argument(
        "--zorder",
        default=None,
        help="Comma-separated columns to Z-order (default: symbol_id,ts_local_us when present)",
    )
    delta_optimize.set_defaults(func=_cmd_delta_optimize)

    delta_vacuum = delta_sub.add_parser("vacuum", help="Remove unreferenced files")
    delta_vacuum.add_argument(
        "--table",
        required=True,
        choices=sorted(TABLE_PATHS.keys()),
        help="Table name to vacuum",
    )
    delta_vacuum.add_argument(
        "--retention-hours",
        type=int,
        required=True,
        help="Retention window in hours",
    )
    delta_vacuum.add_argument(
        "--execute",
        action="store_true",
        help="Actually remove files (default is dry-run)",
    )
    delta_vacuum.add_argument(
        "--no-retention-check",
        action="store_true",
        help="Disable retention duration enforcement",
    )
    delta_vacuum.set_defaults(func=_cmd_delta_vacuum)

    manifest = subparsers.add_parser("manifest", help="Ingestion manifest utilities")
    manifest_sub = manifest.add_subparsers(dest="manifest_command")

    manifest_show = manifest_sub.add_parser("show", help="Show manifest information")
    manifest_show.add_argument(
        "--manifest-path",
        default=str(get_table_path("ingest_manifest")),
        help="Path to the ingest manifest table",
    )
    manifest_show.add_argument(
        "--detailed",
        action="store_true",
        help="Show detailed file information instead of summary",
    )
    manifest_show.add_argument(
        "--status",
        help="Filter by status (success, failed, pending, quarantined)",
    )
    manifest_show.add_argument(
        "--exchange",
        help="Filter by exchange name",
    )
    manifest_show.add_argument(
        "--data-type",
        help="Filter by data type (e.g., trades, quotes, book_snapshot_25)",
    )
    manifest_show.add_argument(
        "--symbol",
        help="Filter by symbol",
    )
    manifest_show.add_argument(
        "--limit",
        type=int,
        help="Limit number of rows shown (detailed mode only)",
    )
    manifest_show.add_argument(
        "--show-errors",
        action="store_true",
        help="Include error messages in detailed output",
    )
    manifest_show.set_defaults(func=_cmd_manifest_show)

    manifest_backfill = manifest_sub.add_parser(
        "backfill-sha256", help="Backfill sha256 for manifest rows"
    )
    manifest_backfill.add_argument(
        "--manifest-path",
        default=str(get_table_path("ingest_manifest")),
        help="Path to the ingest manifest table",
    )
    manifest_backfill.add_argument(
        "--bronze-root",
        default=str(LAKE_ROOT / "tardis"),
        help="Bronze root containing raw files (default: LAKE_ROOT/tardis)",
    )
    manifest_backfill.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows per merge batch (default: 500)",
    )
    manifest_backfill.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of rows to backfill (optional)",
    )
    manifest_backfill.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing",
    )
    manifest_backfill.set_defaults(func=_cmd_manifest_backfill_sha256)

    dim_symbol = subparsers.add_parser("dim-symbol", help="dim_symbol operations")
    dim_symbol_sub = dim_symbol.add_subparsers(dest="dim_symbol_command")

    dim_symbol_upsert = dim_symbol_sub.add_parser("upsert", help="Upsert dim_symbol updates")
    dim_symbol_upsert.add_argument("--file", required=True, help="CSV or Parquet updates file")
    dim_symbol_upsert.add_argument(
        "--table-path",
        default=str(get_table_path("dim_symbol")),
        help="Path to the dim_symbol Delta table",
    )
    dim_symbol_upsert.set_defaults(func=_cmd_dim_symbol_upsert)

    dim_symbol_sync = dim_symbol_sub.add_parser("sync", help="Sync dim_symbol from a source")
    dim_symbol_sync.add_argument(
        "--source",
        required=True,
        help="Metadata source file, or 'api' to fetch from Tardis",
    )
    dim_symbol_sync.add_argument("--exchange", help="Exchange name for Tardis API source")
    dim_symbol_sync.add_argument(
        "--symbol",
        help="Instrument symbol/id for Tardis API source (optional)",
    )
    dim_symbol_sync.add_argument(
        "--filter",
        help="JSON filter payload for the Tardis API source",
    )
    dim_symbol_sync.add_argument(
        "--api-key",
        default=os.getenv("TARDIS_API_KEY", ""),
        help="Tardis API key (or set TARDIS_API_KEY)",
    )
    dim_symbol_sync.add_argument(
        "--effective-ts",
        default="now",
        help="Unix timestamp in microseconds to use if availableSince missing",
    )
    dim_symbol_sync.add_argument(
        "--table-path",
        default=str(get_table_path("dim_symbol")),
        help="Path to the dim_symbol Delta table",
    )
    dim_symbol_sync.add_argument(
        "--rebuild",
        action="store_true",
        help="Perform a full history rebuild for the symbols in the source",
    )
    dim_symbol_sync.set_defaults(func=_cmd_dim_symbol_sync)

    gold = subparsers.add_parser("gold", help="Gold table build utilities")
    gold_sub = gold.add_subparsers(dest="gold_command")

    l2_state_checkpoint = gold_sub.add_parser(
        "l2-state-checkpoint",
        help="Build gold.l2_state_checkpoint from silver.l2_updates",
    )
    l2_state_checkpoint.add_argument(
        "--symbol-id",
        required=True,
        help="Single symbol_id value (required)",
    )
    l2_state_checkpoint.add_argument(
        "--start-date",
        required=True,
        help="Start date YYYY-MM-DD (inclusive)",
    )
    l2_state_checkpoint.add_argument(
        "--end-date",
        required=True,
        help="End date YYYY-MM-DD (inclusive)",
    )
    l2_state_checkpoint.add_argument(
        "--checkpoint-every-us",
        type=int,
        default=60_000_000,
        help="Emit a checkpoint at this time cadence in microseconds (default: 60_000_000)",
    )
    l2_state_checkpoint.add_argument(
        "--checkpoint-every-updates",
        type=int,
        default=10_000,
        help="Emit a checkpoint after this many updates (default: 10_000)",
    )
    l2_state_checkpoint.add_argument(
        "--validate-monotonic",
        action="store_true",
        help="Fail if updates are not strictly ordered by replay key",
    )
    l2_state_checkpoint.add_argument(
        "--assume-sorted",
        action="store_true",
        help=(
            "Skip global sort and assume updates are already ordered by "
            "ts_local_us, ingest_seq, file_line_number"
        ),
    )
    l2_state_checkpoint.set_defaults(func=_cmd_l2_state_checkpoint_build)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
