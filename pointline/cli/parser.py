"""Argument parser for the Pointline CLI."""

from __future__ import annotations

import argparse
import os

from pointline.cli.commands.bronze_reorganize import cmd_bronze_reorganize
from pointline.cli.commands.config import cmd_config_set, cmd_config_show
from pointline.cli.commands.delta import cmd_delta_optimize, cmd_delta_vacuum
from pointline.cli.commands.dim_asset_stats import (
    cmd_dim_asset_stats_backfill,
    cmd_dim_asset_stats_sync,
)
from pointline.cli.commands.dim_symbol import cmd_dim_symbol_sync, cmd_dim_symbol_sync_tushare
from pointline.cli.commands.download import cmd_download
from pointline.cli.commands.dq import cmd_dq_report, cmd_dq_run, cmd_dq_summary, dq_table_choices
from pointline.cli.commands.ingest import cmd_ingest_discover, cmd_ingest_run
from pointline.cli.commands.manifest import cmd_manifest_backfill_sha256, cmd_manifest_show
from pointline.cli.commands.symbol import cmd_symbol_search
from pointline.cli.commands.validate import cmd_validate_quotes, cmd_validate_trades
from pointline.cli.commands.validation import cmd_validation_show, cmd_validation_stats
from pointline.config import BRONZE_ROOT, TABLE_PATHS, get_bronze_root, get_table_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pointline", description="Pointline data lake CLI")
    subparsers = parser.add_subparsers(dest="command")

    # --- Config ---
    config = subparsers.add_parser("config", help="User configuration utilities")
    config_sub = config.add_subparsers(dest="config_command")

    config_show = config_sub.add_parser("show", help="Show resolved configuration")
    config_show.set_defaults(func=cmd_config_show)

    config_set = config_sub.add_parser("set", help="Set configuration values")
    config_set.add_argument("--lake-root", required=True, help="Root path to the data lake")
    config_set.set_defaults(func=cmd_config_set)

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

    symbol_sync_tushare = symbol_sub.add_parser(
        "sync-tushare", help="Sync Chinese stocks from Tushare to dim_symbol"
    )
    symbol_sync_tushare.add_argument(
        "--exchange",
        required=True,
        choices=["szse", "sse", "all"],
        help="Exchange to sync (szse=Shenzhen, sse=Shanghai, all=both)",
    )
    symbol_sync_tushare.add_argument(
        "--include-delisted",
        action="store_true",
        help="Include delisted stocks",
    )
    symbol_sync_tushare.add_argument(
        "--token",
        default=os.getenv("TUSHARE_TOKEN", ""),
        help="Tushare API token (or set TUSHARE_TOKEN env var)",
    )
    symbol_sync_tushare.add_argument(
        "--table-path",
        default=str(get_table_path("dim_symbol")),
        help="Path to the dim_symbol Delta table",
    )
    symbol_sync_tushare.set_defaults(func=cmd_dim_symbol_sync_tushare)

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
        dest="from_date",
        required=True,
        help="Start date YYYY-MM-DD (inclusive)",
    )
    bronze_download.add_argument(
        "--end-date",
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
        default=str(get_bronze_root("tardis")),
        help="Root directory for downloads (default: LAKE_ROOT/bronze/tardis)",
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

    bronze_reorganize = bronze_sub.add_parser(
        "reorganize", help="Reorganize vendor archives into Hive-partitioned bronze layout"
    )
    bronze_reorganize.add_argument(
        "--source-dir",
        required=True,
        help="Source directory containing vendor archives (e.g., .7z files)",
    )
    bronze_reorganize.add_argument(
        "--bronze-root",
        required=True,
        help="Bronze root directory (target for reorganized files)",
    )
    bronze_reorganize.add_argument(
        "--vendor",
        default="quant360",
        help="Vendor name (default: quant360)",
    )
    bronze_reorganize.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without executing",
    )
    bronze_reorganize.set_defaults(func=cmd_bronze_reorganize)

    bronze_discover = bronze_sub.add_parser("discover", help="Discover bronze files")
    bronze_discover.add_argument(
        "--bronze-root",
        default=str(BRONZE_ROOT),
        help="Bronze root path (default: LAKE_ROOT/bronze)",
    )
    bronze_discover.add_argument(
        "--vendor",
        help="Vendor name (used to construct bronze-root as LAKE_ROOT/bronze/{vendor})",
    )
    bronze_discover.add_argument(
        "--manifest-path",
        default=str(get_table_path("ingest_manifest")),
        help="Path to ingest_manifest (default: LAKE_ROOT/silver/ingest_manifest)",
    )
    bronze_discover.add_argument(
        "--glob",
        default="**/*.csv.gz",
        help="Glob pattern for bronze files",
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
    bronze_discover.add_argument(
        "--no-prehook",
        action="store_true",
        help="Skip vendor-specific prehooks (e.g., archive reorganization)",
    )
    bronze_discover.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of files to display (default: 100, use 0 for all)",
    )
    bronze_discover.set_defaults(func=cmd_ingest_discover)

    bronze_ingest = bronze_sub.add_parser("ingest", help="Run ingestion for pending files")
    bronze_ingest.add_argument(
        "--bronze-root",
        default=str(BRONZE_ROOT),
        help="Bronze root path (default: LAKE_ROOT/bronze)",
    )
    bronze_ingest.add_argument(
        "--vendor",
        help="Vendor name (used to construct bronze-root as LAKE_ROOT/bronze/{vendor})",
    )
    bronze_ingest.add_argument(
        "--glob",
        default="**/*.csv.gz",
        help="Glob pattern for bronze files",
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
    bronze_ingest.add_argument(
        "--no-prehook",
        action="store_true",
        help="Skip vendor-specific prehooks (e.g., archive reorganization)",
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
        default=str(get_bronze_root("tardis")),
        help="Bronze root used to resolve manifest paths (default: LAKE_ROOT/bronze/tardis)",
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
        default=str(get_bronze_root("tardis")),
        help="Bronze root used to resolve manifest paths (default: LAKE_ROOT/bronze/tardis)",
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
        default=str(get_bronze_root("tardis")),
        help="Bronze root containing raw files (default: LAKE_ROOT/bronze/tardis)",
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

    # --- Validation ---
    validation = subparsers.add_parser("validation", help="Validation log utilities")
    validation_sub = validation.add_subparsers(dest="validation_command")

    validation_show = validation_sub.add_parser("show", help="Show validation log information")
    validation_show.add_argument(
        "--validation-log-path",
        default=str(get_table_path("validation_log")),
        help="Path to the validation log table",
    )
    validation_show.add_argument(
        "--detailed",
        action="store_true",
        help="Show detailed validation records instead of summary",
    )
    validation_show.add_argument(
        "--status",
        choices=["passed", "failed"],
        help="Filter by validation status",
    )
    validation_show.add_argument(
        "--table",
        help="Filter by table name (e.g., trades, quotes)",
    )
    validation_show.add_argument(
        "--failed-only",
        action="store_true",
        help="Show only failed validations",
    )
    validation_show.add_argument(
        "--passed-only",
        action="store_true",
        help="Show only passed validations",
    )
    validation_show.add_argument(
        "--limit",
        type=int,
        help="Limit number of records shown (detailed mode only)",
    )
    validation_show.add_argument(
        "--show-mismatches",
        action="store_true",
        help="Show mismatch samples for failed validations (detailed mode only)",
    )
    validation_show.set_defaults(func=cmd_validation_show)

    validation_stats = validation_sub.add_parser("stats", help="Show validation statistics")
    validation_stats.add_argument(
        "--validation-log-path",
        default=str(get_table_path("validation_log")),
        help="Path to the validation log table",
    )
    validation_stats.set_defaults(func=cmd_validation_stats)

    # --- Data Quality ---
    dq = subparsers.add_parser("dq", help="Data quality summaries")
    dq_sub = dq.add_subparsers(dest="dq_command")

    dq_run = dq_sub.add_parser("run", help="Run data quality checks")
    dq_run.add_argument(
        "--table",
        default="all",
        choices=dq_table_choices(),
        help="Table to check (default: all)",
    )
    dq_run.add_argument(
        "--date",
        default=None,
        help="Optional partition date (YYYY-MM-DD)",
    )
    dq_run.add_argument(
        "--start-date",
        default=None,
        help="Start date for partitioned runs (YYYY-MM-DD)",
    )
    dq_run.add_argument(
        "--end-date",
        default=None,
        help="End date for partitioned runs (YYYY-MM-DD)",
    )
    dq_run.add_argument(
        "--max-dates",
        type=int,
        default=None,
        help="Max partitions to scan (newest-first) in partitioned runs",
    )
    dq_run.add_argument(
        "--partitioned",
        action="store_true",
        help="Run partition-by-partition using ingest_manifest dates",
    )
    dq_run.add_argument(
        "--progress",
        action="store_true",
        help="Print progress per partition",
    )
    dq_run.add_argument(
        "--no-rollup",
        action="store_true",
        help="Skip aggregate rollup row for partitioned runs",
    )
    dq_run.add_argument(
        "--dq-summary-path",
        default=str(get_table_path("dq_summary")),
        help="Path to dq_summary table (default: LAKE_ROOT/silver/dq_summary)",
    )
    dq_run.add_argument(
        "--no-write",
        action="store_true",
        help="Skip persisting results to dq_summary",
    )
    dq_run.set_defaults(func=cmd_dq_run)

    dq_report = dq_sub.add_parser("report", help="Show data quality summaries")
    dq_report.add_argument(
        "--table",
        required=True,
        choices=dq_table_choices()[1:],
        help="Table to report",
    )
    dq_report.add_argument(
        "--date",
        default=None,
        help="Optional partition date (YYYY-MM-DD)",
    )
    dq_report.add_argument(
        "--latest",
        action="store_true",
        help="Show only the latest record (default)",
    )
    dq_report.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Limit records when not using --latest (default: 10)",
    )
    dq_report.add_argument(
        "--dq-summary-path",
        default=str(get_table_path("dq_summary")),
        help="Path to dq_summary table (default: LAKE_ROOT/silver/dq_summary)",
    )
    dq_report.set_defaults(func=cmd_dq_report)

    dq_summary = dq_sub.add_parser("summary", help="Show human-friendly DQ summary")
    dq_summary.add_argument(
        "--table",
        required=True,
        choices=dq_table_choices()[1:],
        help="Table to summarize",
    )
    dq_summary.add_argument(
        "--recent",
        type=int,
        default=30,
        help="Number of recent partitions to summarize (default: 30)",
    )
    dq_summary.add_argument(
        "--dq-summary-path",
        default=str(get_table_path("dq_summary")),
        help="Path to dq_summary table (default: LAKE_ROOT/silver/dq_summary)",
    )
    dq_summary.set_defaults(func=cmd_dq_summary)

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

    return parser
