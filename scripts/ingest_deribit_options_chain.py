"""Ingest Deribit options_chain bronze file into the data lake.

Usage:
    python scripts/ingest_deribit_options_chain.py --root ~/data/lake
    python scripts/ingest_deribit_options_chain.py --silver-root /path/to/silver --bronze-root /path/to/bronze/tardis

This script ingests a Tardis options_chain file into Silver Delta tables.
"""

from __future__ import annotations

import argparse
import hashlib
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path

import polars as pl

from pointline.cli._config import resolve_bronze_root, resolve_root, resolve_silver_root
from pointline.dim_symbol import bootstrap
from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata
from pointline.storage.delta import DeltaEventStore, DeltaManifestStore
from pointline.vendors.tardis import get_tardis_parser

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_BRONZE_FILE = Path(
    "exchange=deribit/type=options_chain/date=2020-09-01"
    "/symbol=OPTIONS/deribit_options_chain_2020-09-01_OPTIONS.csv.gz"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest a Deribit options_chain bronze file.")
    parser.add_argument(
        "--root", type=Path, default=None, help="Lake root (POINTLINE_ROOT fallback)."
    )
    parser.add_argument(
        "--bronze-root", type=Path, default=None, help="Bronze root for Tardis files."
    )
    parser.add_argument(
        "--silver-root", type=Path, default=None, help="Silver root output directory."
    )
    parser.add_argument(
        "--bronze-file",
        type=Path,
        default=DEFAULT_BRONZE_FILE,
        help=(
            "Bronze file path. If relative, it is resolved from bronze root. "
            "Default points to a sample Deribit options_chain file."
        ),
    )
    return parser.parse_args()


def file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def _partition_value(path: Path, key: str) -> str | None:
    prefix = f"{key}="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix) :]
    return None


def build_meta(path: Path, *, bronze_root: Path) -> BronzeFileMetadata:
    """Build BronzeFileMetadata from a Tardis grouped-symbol bronze file."""
    try:
        rel_path = str(path.relative_to(bronze_root))
    except ValueError as exc:
        raise SystemExit(f"error: bronze file must be under {bronze_root}") from exc

    date_raw = _partition_value(path, "date")
    if date_raw is None:
        raise SystemExit("error: bronze file path must include date=YYYY-MM-DD partition")
    try:
        trading_date = date.fromisoformat(date_raw)
    except ValueError as exc:
        raise SystemExit(f"error: invalid date partition '{date_raw}' in bronze path") from exc

    grouped_symbol = _partition_value(path, "symbol")
    if grouped_symbol is None:
        raise SystemExit("error: bronze file path must include symbol=<GROUP> partition")

    stat = path.stat()
    return BronzeFileMetadata(
        vendor="tardis",
        data_type="options_chain",
        bronze_file_path=rel_path,
        file_size_bytes=stat.st_size,
        last_modified_ts=int(stat.st_mtime * 1_000_000),
        sha256=file_sha256(path),
        date=trading_date,
        extra={"grouped_symbol": grouped_symbol},
    )


def bootstrap_dim_symbol_from_csv(path: Path) -> pl.DataFrame:
    """Build a minimal dim_symbol from the unique (exchange, symbol) pairs in the CSV."""
    raw = pl.read_csv(path, columns=["exchange", "symbol"], n_rows=None)
    unique_pairs = raw.unique(subset=["exchange", "symbol"]).sort(["exchange", "symbol"])

    # Use a timestamp before the data range for full coverage
    effective_ts_us = 1_500_000_000_000_000  # ~2017-07-14, well before 2020-09-01

    snapshot = unique_pairs.with_columns(
        pl.col("exchange").str.strip_chars().str.to_lowercase(),
        pl.col("symbol").str.strip_chars().alias("exchange_symbol"),
        pl.col("symbol").str.strip_chars().alias("canonical_symbol"),
        pl.lit("option").alias("market_type"),
        pl.lit(None, dtype=pl.Utf8).alias("base_asset"),
        pl.lit(None, dtype=pl.Utf8).alias("quote_asset"),
        pl.lit(None, dtype=pl.Int64).alias("tick_size"),
        pl.lit(None, dtype=pl.Int64).alias("lot_size"),
        pl.lit(None, dtype=pl.Int64).alias("contract_size"),
    ).drop("symbol")

    return bootstrap(snapshot, effective_ts_us)


def make_parser(path: Path) -> Callable[[BronzeFileMetadata], pl.DataFrame]:
    """Return a parser callable compatible with ingest_file(parser=...)."""

    def parser(meta: BronzeFileMetadata) -> pl.DataFrame:
        stream_parser = get_tardis_parser(meta.data_type)
        raw = pl.read_csv(path)
        return stream_parser(raw)

    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    root = resolve_root(args.root)
    bronze_root = resolve_bronze_root(args.bronze_root, root=root, vendor="tardis").expanduser()
    silver_root = resolve_silver_root(args.silver_root, root=root).expanduser()

    bronze_file = args.bronze_file.expanduser()
    if not bronze_file.is_absolute():
        bronze_file = bronze_root / bronze_file
    if not bronze_file.exists():
        raise SystemExit(f"error: bronze file not found: {bronze_file}")

    print(f"Bronze root: {bronze_root}")
    print(f"Bronze file: {bronze_file}")
    print(f"Silver root: {silver_root}")
    silver_root.mkdir(parents=True, exist_ok=True)

    # 1. Build metadata
    print("\n[1/4] Building BronzeFileMetadata...")
    t0 = time.time()
    meta = build_meta(bronze_file, bronze_root=bronze_root)
    print(f"  sha256: {meta.sha256[:16]}...")
    print(f"  size: {meta.file_size_bytes / 1024 / 1024:.1f} MB")
    print(f"  ({time.time() - t0:.1f}s)")

    # 2. Bootstrap dim_symbol
    print("\n[2/4] Bootstrapping dim_symbol from CSV symbols...")
    t0 = time.time()
    dim_symbol_df = bootstrap_dim_symbol_from_csv(bronze_file)
    print(f"  {dim_symbol_df.height} symbol entries")
    print(f"  ({time.time() - t0:.1f}s)")

    # 3. Set up stores
    print("\n[3/4] Initializing Delta stores...")
    manifest_store = DeltaManifestStore(table_path=silver_root / "ingest_manifest")
    event_store = DeltaEventStore(silver_root=silver_root)

    # 4. Ingest
    print("\n[4/4] Running ingest_file()...")
    t0 = time.time()
    result = ingest_file(
        meta,
        parser=make_parser(bronze_file),
        manifest_repo=manifest_store,
        writer=event_store,
        dim_symbol_df=dim_symbol_df,
    )
    elapsed = time.time() - t0

    # Report
    print(f"\n{'=' * 60}")
    print(f"Status:           {result.status}")
    print(f"Rows total:       {result.row_count:,}")
    print(f"Rows written:     {result.rows_written:,}")
    print(f"Rows quarantined: {result.rows_quarantined:,}")
    print(f"File ID:          {result.file_id}")
    print(f"Trading dates:    {result.trading_date_min} → {result.trading_date_max}")
    print(f"Elapsed:          {elapsed:.1f}s")
    if elapsed > 0:
        print(f"Throughput:       {result.row_count / elapsed:,.0f} rows/s")
    else:
        print("Throughput:       N/A")
    if result.skipped:
        print("  (skipped — already ingested)")
    if result.error_message:
        print(f"  Error: {result.error_message}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
