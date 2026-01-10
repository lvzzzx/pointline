"""Command-line interface for Pointline."""

from __future__ import annotations

import argparse
import gc
import json
import os
import time
from pathlib import Path
from typing import Iterable, Sequence

import polars as pl

from pointline.config import LAKE_ROOT, get_exchange_id, get_exchange_name, get_table_path
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
from pointline.registry import find_symbol, resolve_symbol


def _sorted_files(files: Iterable[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
    return sorted(
        files,
        key=lambda f: (f.exchange, f.data_type, f.symbol, f.date.isoformat(), f.bronze_file_path),
    )


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
    elif data_type == "incremental_book_L2":
        repo = BaseDeltaRepository(
            get_table_path("l2_updates"),
            partition_by=["exchange", "date", "symbol_id"]
        )
        return L2UpdatesIngestionService(repo, dim_symbol_repo, manifest_repo)
    else:
        raise ValueError(f"Unsupported data type: {data_type}")


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
