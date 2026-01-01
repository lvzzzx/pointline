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
from pointline.io.vendor.tardis import build_updates_from_instruments, TardisClient
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

    summary = (
        df.group_by("status")
        .agg(pl.len().alias("count"))
        .sort("status")
    )
    print("manifest status counts:")
    for row in summary.iter_rows(named=True):
        print(f"{row['status']}: {row['count']}")
    print(f"total rows: {df.height}")
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
        print(f"Applying incremental updates...")
        service.update(updates)
        
    print("Sync complete.")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pointline", description="Pointline data lake CLI")
    subparsers = parser.add_subparsers(dest="command")

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

    manifest_show = manifest_sub.add_parser("show", help="Show manifest status counts")
    manifest_show.add_argument(
        "--manifest-path",
        default=str(get_table_path("ingest_manifest")),
        help="Path to the ingest manifest table",
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
