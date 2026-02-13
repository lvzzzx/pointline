"""Example: Ingest a Tardis grouped-symbol bronze file into the data lake.

Usage:
    python examples/ingest_tardis_grouped.py

This script demonstrates ingesting a real Tardis SPOT trades file into
Silver Delta tables using the v2 pipeline. It:

1. Builds BronzeFileMetadata from the bronze file path.
2. Bootstraps a minimal dim_symbol from the CSV's unique symbols
   (since we don't have a Tardis symbol dimension yet).
3. Runs ingest_file() with Delta-backed manifest + event stores.
4. Reports results.
"""

from __future__ import annotations

import hashlib
import time
from datetime import date
from pathlib import Path

import polars as pl

from pointline.dim_symbol import bootstrap
from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata
from pointline.storage.delta import DeltaEventStore, DeltaManifestStore
from pointline.vendors.tardis import get_tardis_parser

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BRONZE_FILE = Path(
    "/Users/zjx/data/lake/bronze/tardis"
    "/exchange=binance/type=trades/date=2020-09-01"
    "/symbol=SPOT/binance_trades_2020-09-01_SPOT.csv.gz"
)

LAKE_ROOT = Path("/Users/zjx/data/lake")
SILVER_ROOT = LAKE_ROOT / "silver"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def build_meta(path: Path) -> BronzeFileMetadata:
    """Build BronzeFileMetadata from a Tardis grouped-symbol bronze file."""
    bronze_root = LAKE_ROOT / "bronze" / "tardis"
    rel_path = str(path.relative_to(bronze_root))
    stat = path.stat()
    return BronzeFileMetadata(
        vendor="tardis",
        data_type="trades",
        bronze_file_path=rel_path,
        file_size_bytes=stat.st_size,
        last_modified_ts=int(stat.st_mtime * 1_000_000),
        sha256=file_sha256(path),
        date=date(2020, 9, 1),
        extra={"grouped_symbol": "SPOT"},
    )


def bootstrap_dim_symbol_from_csv(path: Path) -> pl.DataFrame:
    """Build a minimal dim_symbol from the unique (exchange, symbol) pairs in the CSV.

    This is a bootstrap shortcut — in production, dim_symbol should be populated
    from a proper symbol metadata source (e.g. Tardis /exchanges API or exchange
    reference data).
    """
    raw = pl.read_csv(path, columns=["exchange", "symbol"], n_rows=None)
    unique_pairs = raw.unique(subset=["exchange", "symbol"]).sort(["exchange", "symbol"])

    # Use a timestamp before the data range for full coverage
    effective_ts_us = 1_500_000_000_000_000  # ~2017-07-14, well before 2020-09-01

    snapshot = unique_pairs.with_columns(
        pl.col("exchange").str.strip_chars().str.to_lowercase(),
        pl.col("symbol").str.strip_chars().alias("exchange_symbol"),
        pl.col("symbol").str.strip_chars().alias("canonical_symbol"),
        pl.lit("spot").alias("market_type"),
        pl.lit(None, dtype=pl.Utf8).alias("base_asset"),
        pl.lit(None, dtype=pl.Utf8).alias("quote_asset"),
        pl.lit(None, dtype=pl.Int64).alias("tick_size"),
        pl.lit(None, dtype=pl.Int64).alias("lot_size"),
        pl.lit(None, dtype=pl.Int64).alias("contract_size"),
    ).drop("symbol")

    return bootstrap(snapshot, effective_ts_us)


def make_parser(path: Path):
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
    print(f"Bronze file: {BRONZE_FILE}")
    print(f"Silver root: {SILVER_ROOT}")
    SILVER_ROOT.mkdir(parents=True, exist_ok=True)

    # 1. Build metadata
    print("\n[1/4] Building BronzeFileMetadata...")
    t0 = time.time()
    meta = build_meta(BRONZE_FILE)
    print(f"  sha256: {meta.sha256[:16]}...")
    print(f"  size: {meta.file_size_bytes / 1024 / 1024:.1f} MB")
    print(f"  ({time.time() - t0:.1f}s)")

    # 2. Bootstrap dim_symbol
    print("\n[2/4] Bootstrapping dim_symbol from CSV symbols...")
    t0 = time.time()
    dim_symbol_df = bootstrap_dim_symbol_from_csv(BRONZE_FILE)
    print(f"  {dim_symbol_df.height} symbol entries")
    print(f"  ({time.time() - t0:.1f}s)")

    # 3. Set up stores
    print("\n[3/4] Initializing Delta stores...")
    manifest_store = DeltaManifestStore(table_path=SILVER_ROOT / "ingest_manifest")
    event_store = DeltaEventStore(silver_root=SILVER_ROOT)

    # 4. Ingest
    print("\n[4/4] Running ingest_file()...")
    t0 = time.time()
    result = ingest_file(
        meta,
        parser=make_parser(BRONZE_FILE),
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
    print(f"Throughput:       {result.row_count / elapsed:,.0f} rows/s")
    if result.skipped:
        print("  (skipped — already ingested)")
    if result.error_message:
        print(f"  Error: {result.error_message}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
