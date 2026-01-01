"""Command-line interface for Pointline."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Iterable, Sequence

import polars as pl

from pointline.config import LAKE_ROOT, get_table_path
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.local_source import LocalBronzeSource
from pointline.io.protocols import BronzeFileMetadata
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.vendor.tardis import build_updates_from_instruments, TardisClient, download_tardis_datasets
from pointline.services.dim_symbol_service import DimSymbolService


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

    if args.pending_only:
        manifest_repo = DeltaManifestRepository(Path(args.manifest_path))
        files = manifest_repo.filter_pending(files)

    files = _sorted_files(files)
    label = "pending files" if args.pending_only else "files"
    print(f"{label}: {len(files)}")
    _print_files(files)
    return 0


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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pointline", description="Pointline data lake CLI")
    subparsers = parser.add_subparsers(dest="command")

    download = subparsers.add_parser("download", help="Download Tardis datasets to Bronze layer")
    download.add_argument("--exchange", required=True, help="Exchange name (e.g., binance)")
    download.add_argument(
        "--data-types",
        required=True,
        help="Comma-separated list of data types (e.g., trades,quotes)",
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
        "--pending-only",
        action="store_true",
        help="Only show files not yet marked success in the manifest",
    )
    ingest_discover.set_defaults(func=_cmd_ingest_discover)

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
        help="Filter by data type (e.g., trades, quotes)",
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
