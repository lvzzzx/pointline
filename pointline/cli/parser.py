"""Argument parser for the Pointline CLI."""

from __future__ import annotations

import argparse
import os
import sys
from typing import Callable

from pointline.cli.commands.delta import cmd_delta_optimize, cmd_delta_vacuum
from pointline.cli.commands.dim_asset_stats import (
    cmd_dim_asset_stats_backfill,
    cmd_dim_asset_stats_sync,
)
from pointline.cli.commands.dim_symbol import cmd_dim_symbol_sync, cmd_dim_symbol_upsert
from pointline.cli.commands.download import cmd_download
from pointline.cli.commands.gold import cmd_l2_state_checkpoint_build
from pointline.cli.commands.ingest import cmd_ingest_discover, cmd_ingest_run
from pointline.cli.commands.manifest import cmd_manifest_backfill_sha256, cmd_manifest_show
from pointline.cli.commands.symbol import cmd_symbol_search
from pointline.cli.commands.validate import cmd_validate_quotes, cmd_validate_trades
from pointline.config import LAKE_ROOT, TABLE_PATHS, get_table_path


def _deprecated(func: Callable[[argparse.Namespace], int], message: str):
    def wrapper(args: argparse.Namespace) -> int:
        print(f"Deprecation: {message}", file=sys.stderr)
        return func(args)

    return wrapper


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pointline", description="Pointline data lake CLI")
    subparsers = parser.add_subparsers(dest="command")

    # --- Symbol ---
    symbol = subparsers.add_parser("symbol", help="Symbol registry utilities")
    symbol_sub = symbol.add_subparsers(dest="symbol_command")

    symbol_search = symbol_sub.add_parser("search", help="Search for symbols")
    symbol_search.add_argument("query", nargs="?", help="Fuzzy search term")
    symbol_search.add_argument("--exchange", help="Filter by exchange")
    symbol_search.add_argument("--base-asset", help="Filter by base asset")
    symbol_search.add_argument("--quote-asset", help="Filter by quote asset")
    symbol_search.set_defaults(func=cmd_symbol_search)

    symbol_sync = symbol_sub.add_parser("sync", help="Sync dim_symbol from a source")
    symbol_sync.add_argument(
        "--source",
        required=True,
        help="Metadata source file, or 'api' to fetch from Tardis",
    )
    symbol_sync.add_argument("--exchange", help="Exchange name for Tardis API source")
    symbol_sync.add_argument(
        "--symbol",
        help="Instrument symbol/id for Tardis API source (optional)",
    )
    symbol_sync.add_argument(
        "--filter",
        help="JSON filter payload for the Tardis API source",
    )
    symbol_sync.add_argument(
        "--api-key",
        default=os.getenv("TARDIS_API_KEY", ""),
        help="Tardis API key (or set TARDIS_API_KEY)",
    )
    symbol_sync.add_argument(
        "--effective-ts",
        default="now",
        help="Unix timestamp in microseconds to use if availableSince missing",
    )
    symbol_sync.add_argument(
        "--table-path",
        default=str(get_table_path("dim_symbol")),
        help="Path to the dim_symbol Delta table",
    )
    symbol_sync.add_argument(
        "--rebuild",
        action="store_true",
        help="Perform a full history rebuild for the symbols in the source",
    )
    symbol_sync.set_defaults(func=cmd_dim_symbol_sync)

    # --- Bronze ---
    bronze = subparsers.add_parser("bronze", help="Bronze layer utilities")
    bronze_sub = bronze.add_subparsers(dest="bronze_command")

    bronze_download = bronze_sub.add_parser(
        "download", help="Download Tardis datasets to Bronze layer"
    )
    bronze_download.add_argument("--exchange", required=True, help="Exchange name (e.g., binance)")
    bronze_download.add_argument(
        "--data-types",
        required=True,
        help="Comma-separated list of data types (e.g., trades,quotes,book_snapshot_25)",
    )
    bronze_download.add_argument(
        "--symbols",
        required=True,
        help="Comma-separated list of symbols (e.g., BTCUSDT,ETHUSDT)",
    )
    bronze_download.add_argument(
        "--start-date",
        "--from-date",
        dest="from_date",
        required=True,
        help="Start date YYYY-MM-DD (inclusive)",
    )
    bronze_download.add_argument(
        "--end-date",
        "--to-date",
        dest="to_date",
        required=True,
        help="End date YYYY-MM-DD (non-inclusive)",
    )
    bronze_download.add_argument(
        "--format",
        default="csv",
        help="Dataset format (default: csv)",
    )
    bronze_download.add_argument(
        "--download-dir",
        default=str(LAKE_ROOT),
        help=f"Root directory for downloads (default: {LAKE_ROOT})",
    )
    bronze_download.add_argument(
        "--filename-template",
        required=True,
        help="Template with {exchange},{data_type},{date},{symbol},{format}",
    )
    bronze_download.add_argument(
        "--api-key",
        default=os.getenv("TARDIS_API_KEY", ""),
        help="Tardis API key (or set TARDIS_API_KEY)",
    )
    bronze_download.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of concurrent downloads (default: 5)",
    )
    bronze_download.add_argument("--http-proxy", default=None, help="HTTP proxy URL (optional)")
    bronze_download.set_defaults(func=cmd_download)

    bronze_discover = bronze_sub.add_parser("discover", help="Discover bronze files")
    bronze_discover.add_argument(
        "--bronze-root",
        default=str(LAKE_ROOT / "tardis"),
        help="Bronze root path (default: LAKE_ROOT/tardis)",
    )
    bronze_discover.add_argument(
        "--glob",
        default="**/*.csv.gz",
        help="Glob pattern for bronze files",
    )
    bronze_discover.add_argument(
        "--manifest-path",
        default=str(get_table_path("ingest_manifest")),
        help="Path to the ingest manifest table",
    )
    bronze_discover.add_argument(
        "--data-type",
        help="Filter by data type (e.g., trades, quotes, book_snapshot_25)",
    )
    bronze_discover.add_argument(
        "--pending-only",
        action="store_true",
        help="Only show files not yet marked success in the manifest",
    )
    bronze_discover.set_defaults(func=cmd_ingest_discover)

    bronze_ingest = bronze_sub.add_parser("ingest", help="Run ingestion for pending files")
    bronze_ingest.add_argument(
        "--bronze-root",
        default=str(LAKE_ROOT / "tardis"),
        help="Bronze root path (default: LAKE_ROOT/tardis)",
    )
    bronze_ingest.add_argument(
        "--glob",
        default="**/*.csv.gz",
        help="Glob pattern for bronze files",
    )
    bronze_ingest.add_argument(
        "--manifest-path",
        default=str(get_table_path("ingest_manifest")),
        help="Path to the ingest manifest table",
    )
    bronze_ingest.add_argument(
        "--data-type",
        help="Filter by data type (e.g., trades, quotes, book_snapshot_25).",
    )
    bronze_ingest.add_argument(
        "--force",
        action="store_true",
        help="Re-ingest files even if already marked as success",
    )
    bronze_ingest.add_argument(
        "--retry-quarantined",
        action="store_true",
        help="Retry ingestion for quarantined files",
    )
    bronze_ingest.add_argument(
        "--validate",
        action="store_true",
        help="Validate ingested rows against raw file (sampled)",
    )
    bronze_ingest.add_argument(
        "--validate-sample-size",
        type=int,
        default=2000,
        help="Number of rows to sample for post-ingest validation (default: 2000)",
    )
    bronze_ingest.add_argument(
        "--validate-seed",
        type=int,
        default=0,
        help="Random seed for validation sampling (default: 0)",
    )
    bronze_ingest.set_defaults(func=cmd_ingest_run)

    # --- Silver ---
    silver = subparsers.add_parser("silver", help="Silver layer utilities")
    silver_sub = silver.add_subparsers(dest="silver_command")

    silver_validate = silver_sub.add_parser("validate", help="Validate raw files")
    silver_validate_sub = silver_validate.add_subparsers(dest="silver_validate_command")

    silver_validate_quotes = silver_validate_sub.add_parser(
        "quotes", help="Validate raw quotes file against ingested table"
    )
    silver_validate_quotes.add_argument(
        "--file", required=True, help="Path to the raw quotes CSV file"
    )
    silver_validate_quotes.add_argument(
        "--file-id",
        type=int,
        default=None,
        help="File ID to validate (if omitted, resolve via manifest)",
    )
    silver_validate_quotes.add_argument(
        "--date",
        default=None,
        help="File date (YYYY-MM-DD), inferred from path when possible",
    )
    silver_validate_quotes.add_argument(
        "--exchange",
        default=None,
        help="Exchange name (defaults to value inferred from path)",
    )
    silver_validate_quotes.add_argument(
        "--symbol",
        default=None,
        help="Exchange symbol (defaults to value inferred from path)",
    )
    silver_validate_quotes.add_argument(
        "--bronze-root",
        default=str(LAKE_ROOT / "tardis"),
        help="Bronze root used to resolve manifest paths (default: LAKE_ROOT/tardis)",
    )
    silver_validate_quotes.add_argument(
        "--manifest-path",
        default=str(get_table_path("ingest_manifest")),
        help="Path to ingest_manifest (default: LAKE_ROOT/silver/ingest_manifest)",
    )
    silver_validate_quotes.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max invalid rows to print (default: 20)",
    )
    silver_validate_quotes.add_argument(
        "--show-all",
        action="store_true",
        help="Print all mismatched rows",
    )
    silver_validate_quotes.add_argument(
        "--exit-nonzero",
        action="store_true",
        help="Exit with status 1 if invalid rows are found",
    )
    silver_validate_quotes.set_defaults(func=cmd_validate_quotes)

    silver_validate_trades = silver_validate_sub.add_parser(
        "trades", help="Validate raw trades file against ingested table"
    )
    silver_validate_trades.add_argument(
        "--file", required=True, help="Path to the raw trades CSV file"
    )
    silver_validate_trades.add_argument(
        "--file-id",
        type=int,
        default=None,
        help="File ID to validate (if omitted, resolve via manifest)",
    )
    silver_validate_trades.add_argument(
        "--date",
        default=None,
        help="File date (YYYY-MM-DD), inferred from path when possible",
    )
    silver_validate_trades.add_argument(
        "--exchange",
        default=None,
        help="Exchange name (defaults to value inferred from path)",
    )
    silver_validate_trades.add_argument(
        "--symbol",
        default=None,
        help="Exchange symbol (defaults to value inferred from path)",
    )
    silver_validate_trades.add_argument(
        "--bronze-root",
        default=str(LAKE_ROOT / "tardis"),
        help="Bronze root used to resolve manifest paths (default: LAKE_ROOT/tardis)",
    )
    silver_validate_trades.add_argument(
        "--manifest-path",
        default=str(get_table_path("ingest_manifest")),
        help="Path to ingest_manifest (default: LAKE_ROOT/silver/ingest_manifest)",
    )
    silver_validate_trades.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max mismatched rows to print (default: 20)",
    )
    silver_validate_trades.add_argument(
        "--show-all",
        action="store_true",
        help="Print all mismatched rows",
    )
    silver_validate_trades.add_argument(
        "--exit-nonzero",
        action="store_true",
        help="Exit with status 1 if mismatches are found",
    )
    silver_validate_trades.set_defaults(func=cmd_validate_trades)

    # --- Manifest ---
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
    manifest_show.set_defaults(func=cmd_manifest_show)

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
    manifest_backfill.set_defaults(func=cmd_manifest_backfill_sha256)

    # --- Assets ---
    assets = subparsers.add_parser("assets", help="Asset stats utilities")
    assets_sub = assets.add_subparsers(dest="assets_command")

    assets_sync = assets_sub.add_parser("sync", help="Sync dim_asset_stats for a date")
    assets_sync.add_argument(
        "--date",
        required=True,
        help="Date to sync (YYYY-MM-DD)",
    )
    assets_sync.add_argument(
        "--base-assets",
        help=(
            "Comma-separated list of base assets to sync (e.g., BTC,ETH,SOL). "
            "If omitted, syncs all assets from dim_symbol"
        ),
    )
    assets_sync.add_argument(
        "--table-path",
        default=str(get_table_path("dim_asset_stats")),
        help="Path to the dim_asset_stats Delta table",
    )
    assets_sync.add_argument(
        "--api-key",
        default=os.getenv("COINGECKO_API_KEY", ""),
        help="CoinGecko API key (optional, for higher rate limits)",
    )
    assets_sync.set_defaults(func=cmd_dim_asset_stats_sync)

    assets_backfill = assets_sub.add_parser(
        "backfill", help="Backfill historical dim_asset_stats for a date range"
    )
    assets_backfill.add_argument(
        "--start-date",
        required=True,
        help="Start date YYYY-MM-DD (inclusive)",
    )
    assets_backfill.add_argument(
        "--end-date",
        required=True,
        help="End date YYYY-MM-DD (inclusive)",
    )
    assets_backfill.add_argument(
        "--base-assets",
        help=(
            "Comma-separated list of base assets to sync (e.g., BTC,ETH,SOL). "
            "If omitted, syncs all assets from dim_symbol"
        ),
    )
    assets_backfill.add_argument(
        "--table-path",
        default=str(get_table_path("dim_asset_stats")),
        help="Path to the dim_asset_stats Delta table",
    )
    assets_backfill.add_argument(
        "--api-key",
        default=os.getenv("COINGECKO_API_KEY", ""),
        help="CoinGecko API key (optional, for higher rate limits)",
    )
    assets_backfill.set_defaults(func=cmd_dim_asset_stats_backfill)

    # --- Delta ---
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
    delta_optimize.set_defaults(func=cmd_delta_optimize)

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
    delta_vacuum.set_defaults(func=cmd_delta_vacuum)

    # --- Gold ---
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
        help=(
            "Emit a checkpoint at this time cadence in microseconds (default: 60_000_000)"
        ),
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
    l2_state_checkpoint.set_defaults(func=cmd_l2_state_checkpoint_build)

    # --- Legacy paths (deprecated) ---
    legacy_download = subparsers.add_parser(
        "download", help="DEPRECATED: use 'bronze download'"
    )
    legacy_download.add_argument("--exchange", required=True, help="Exchange name (e.g., binance)")
    legacy_download.add_argument(
        "--data-types",
        required=True,
        help="Comma-separated list of data types (e.g., trades,quotes,book_snapshot_25)",
    )
    legacy_download.add_argument(
        "--symbols",
        required=True,
        help="Comma-separated list of symbols (e.g., BTCUSDT,ETHUSDT)",
    )
    legacy_download.add_argument(
        "--from-date",
        "--start-date",
        dest="from_date",
        required=True,
        help="Start date YYYY-MM-DD (inclusive)",
    )
    legacy_download.add_argument(
        "--to-date",
        "--end-date",
        dest="to_date",
        required=True,
        help="End date YYYY-MM-DD (non-inclusive)",
    )
    legacy_download.add_argument(
        "--format",
        default="csv",
        help="Dataset format (default: csv)",
    )
    legacy_download.add_argument(
        "--download-dir",
        default=str(LAKE_ROOT),
        help=f"Root directory for downloads (default: {LAKE_ROOT})",
    )
    legacy_download.add_argument(
        "--filename-template",
        required=True,
        help="Template with {exchange},{data_type},{date},{symbol},{format}",
    )
    legacy_download.add_argument(
        "--api-key",
        default=os.getenv("TARDIS_API_KEY", ""),
        help="Tardis API key (or set TARDIS_API_KEY)",
    )
    legacy_download.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of concurrent downloads (default: 5)",
    )
    legacy_download.add_argument("--http-proxy", default=None, help="HTTP proxy URL (optional)")
    legacy_download.set_defaults(
        func=_deprecated(cmd_download, "Use 'pointline bronze download' instead.")
    )

    ingest = subparsers.add_parser("ingest", help="DEPRECATED: use 'bronze' commands")
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
    ingest_discover.set_defaults(
        func=_deprecated(cmd_ingest_discover, "Use 'pointline bronze discover' instead.")
    )

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
    ingest_run.set_defaults(
        func=_deprecated(cmd_ingest_run, "Use 'pointline bronze ingest' instead.")
    )

    validate = subparsers.add_parser("validate", help="DEPRECATED: use 'silver validate' commands")
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
    validate_quotes.set_defaults(
        func=_deprecated(cmd_validate_quotes, "Use 'pointline silver validate quotes' instead.")
    )

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
    validate_trades.set_defaults(
        func=_deprecated(cmd_validate_trades, "Use 'pointline silver validate trades' instead.")
    )

    dim_symbol = subparsers.add_parser(
        "dim-symbol", help="DEPRECATED: use 'symbol sync'"
    )
    dim_symbol_sub = dim_symbol.add_subparsers(dest="dim_symbol_command")

    dim_symbol_upsert = dim_symbol_sub.add_parser("upsert", help="Upsert dim_symbol updates")
    dim_symbol_upsert.add_argument("--file", required=True, help="CSV or Parquet updates file")
    dim_symbol_upsert.add_argument(
        "--table-path",
        default=str(get_table_path("dim_symbol")),
        help="Path to the dim_symbol Delta table",
    )
    dim_symbol_upsert.set_defaults(
        func=_deprecated(cmd_dim_symbol_upsert, "Use 'pointline symbol sync' instead.")
    )

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
    dim_symbol_sync.set_defaults(
        func=_deprecated(cmd_dim_symbol_sync, "Use 'pointline symbol sync' instead.")
    )

    dim_asset_stats = subparsers.add_parser(
        "dim-asset-stats", help="DEPRECATED: use 'assets'"
    )
    dim_asset_stats_sub = dim_asset_stats.add_subparsers(dest="dim_asset_stats_command")

    dim_asset_stats_sync = dim_asset_stats_sub.add_parser(
        "sync", help="Sync dim_asset_stats for a date"
    )
    dim_asset_stats_sync.add_argument(
        "--date",
        required=True,
        help="Date to sync (YYYY-MM-DD)",
    )
    dim_asset_stats_sync.add_argument(
        "--base-assets",
        help=(
            "Comma-separated list of base assets to sync (e.g., BTC,ETH,SOL). "
            "If omitted, syncs all assets from dim_symbol"
        ),
    )
    dim_asset_stats_sync.add_argument(
        "--table-path",
        default=str(get_table_path("dim_asset_stats")),
        help="Path to the dim_asset_stats Delta table",
    )
    dim_asset_stats_sync.add_argument(
        "--api-key",
        default=os.getenv("COINGECKO_API_KEY", ""),
        help="CoinGecko API key (optional, for higher rate limits)",
    )
    dim_asset_stats_sync.set_defaults(
        func=_deprecated(cmd_dim_asset_stats_sync, "Use 'pointline assets sync' instead.")
    )

    dim_asset_stats_backfill = dim_asset_stats_sub.add_parser(
        "backfill", help="Backfill historical dim_asset_stats for a date range"
    )
    dim_asset_stats_backfill.add_argument(
        "--start-date",
        required=True,
        help="Start date YYYY-MM-DD (inclusive)",
    )
    dim_asset_stats_backfill.add_argument(
        "--end-date",
        required=True,
        help="End date YYYY-MM-DD (inclusive)",
    )
    dim_asset_stats_backfill.add_argument(
        "--base-assets",
        help=(
            "Comma-separated list of base assets to sync (e.g., BTC,ETH,SOL). "
            "If omitted, syncs all assets from dim_symbol"
        ),
    )
    dim_asset_stats_backfill.add_argument(
        "--table-path",
        default=str(get_table_path("dim_asset_stats")),
        help="Path to the dim_asset_stats Delta table",
    )
    dim_asset_stats_backfill.add_argument(
        "--api-key",
        default=os.getenv("COINGECKO_API_KEY", ""),
        help="CoinGecko API key (optional, for higher rate limits)",
    )
    dim_asset_stats_backfill.set_defaults(
        func=_deprecated(
            cmd_dim_asset_stats_backfill, "Use 'pointline assets backfill' instead."
        )
    )

    return parser
