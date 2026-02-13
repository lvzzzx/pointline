"""Example: Batch-ingest Quant360 per-symbol bronze files for a date partition.

Usage:
    python examples/ingest_quant360_batch.py

Ingests all files under:
    ~/data/lake/bronze/quant360/exchange=sse/type=tick_new/date=2024-01-02/

Each symbol directory contains one CSV.gz file. The script:
1. Discovers all symbol files via glob.
2. Loads dim_symbol from Silver for PIT coverage.
3. Runs ingest_file() for each file with force=True (re-ingest failed files).
4. Reports aggregate results.
"""

from __future__ import annotations

import hashlib
import time
from datetime import date
from pathlib import Path

import polars as pl

from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata
from pointline.storage.delta import DeltaEventStore, DeltaManifestStore
from pointline.vendors.quant360.dispatch import get_quant360_stream_parser

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LAKE_ROOT = Path("/Users/zjx/data/lake")
BRONZE_ROOT = LAKE_ROOT / "bronze" / "quant360"
SILVER_ROOT = LAKE_ROOT / "silver"

EXCHANGE = "sse"
DATA_TYPE = "tick_new"
TRADING_DATE = date(2024, 1, 2)

PARTITION_DIR = (
    BRONZE_ROOT / f"exchange={EXCHANGE}" / f"type={DATA_TYPE}" / f"date={TRADING_DATE.isoformat()}"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def discover_symbol_files(partition_dir: Path) -> list[tuple[str, Path]]:
    """Discover (symbol, csv_path) pairs from a Quant360 date partition."""
    results = []
    for symbol_dir in sorted(partition_dir.iterdir()):
        if not symbol_dir.is_dir() or not symbol_dir.name.startswith("symbol="):
            continue
        symbol = symbol_dir.name.removeprefix("symbol=")
        csvs = list(symbol_dir.glob("*.csv.gz"))
        if csvs:
            results.append((symbol, csvs[0]))
    return results


def build_meta(csv_path: Path, *, symbol: str) -> BronzeFileMetadata:
    rel_path = str(csv_path.relative_to(BRONZE_ROOT))
    stat = csv_path.stat()
    return BronzeFileMetadata(
        vendor="quant360",
        data_type=DATA_TYPE,
        bronze_file_path=rel_path,
        file_size_bytes=stat.st_size,
        last_modified_ts=int(stat.st_mtime * 1_000_000),
        sha256=file_sha256(csv_path),
        date=TRADING_DATE,
    )


def make_parser(csv_path: Path, *, exchange: str, symbol: str):
    """Return a parser callable for ingest_file(parser=...)."""
    stream_parser = get_quant360_stream_parser(DATA_TYPE)

    def parser(meta: BronzeFileMetadata) -> pl.DataFrame:
        raw = pl.read_csv(csv_path)
        return stream_parser(raw, exchange=exchange, symbol=symbol)

    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print(f"Partition: {PARTITION_DIR}")
    print(f"Silver:    {SILVER_ROOT}")

    # 1. Discover files
    files = discover_symbol_files(PARTITION_DIR)
    print(f"\nDiscovered {len(files)} symbol files")

    # 2. Load dim_symbol
    dim_symbol_df = pl.read_delta(str(SILVER_ROOT / "dim_symbol"))
    sse_count = dim_symbol_df.filter(pl.col("exchange") == EXCHANGE).height
    print(f"dim_symbol: {dim_symbol_df.height} total, {sse_count} {EXCHANGE}")

    # 3. Set up stores
    manifest_store = DeltaManifestStore(table_path=SILVER_ROOT / "ingest_manifest")
    event_store = DeltaEventStore(silver_root=SILVER_ROOT)

    # 4. Batch ingest
    print(f"\nIngesting {len(files)} files (force=True to re-ingest failed)...\n")
    t0 = time.time()
    total_rows = 0
    total_written = 0
    total_quarantined = 0
    succeeded = 0
    failed = 0
    skipped = 0

    for i, (symbol, csv_path) in enumerate(files):
        meta = build_meta(csv_path, symbol=symbol)
        result = ingest_file(
            meta,
            parser=make_parser(csv_path, exchange=EXCHANGE, symbol=symbol),
            manifest_repo=manifest_store,
            writer=event_store,
            dim_symbol_df=dim_symbol_df,
            force=True,
        )

        total_rows += result.row_count
        total_written += result.rows_written
        total_quarantined += result.rows_quarantined

        if result.skipped:
            skipped += 1
        elif result.status == "success":
            succeeded += 1
        else:
            failed += 1
            if failed <= 5:
                print(f"  FAILED {symbol}: {result.failure_reason} — {result.error_message}")

        if (i + 1) % 500 == 0:
            elapsed = time.time() - t0
            print(f"  [{i + 1}/{len(files)}] {succeeded} ok, {failed} fail, {elapsed:.0f}s elapsed")

    elapsed = time.time() - t0

    # Report
    print(f"\n{'=' * 60}")
    print(f"Exchange:         {EXCHANGE}")
    print(f"Data type:        {DATA_TYPE}")
    print(f"Trading date:     {TRADING_DATE}")
    print(f"Files processed:  {len(files)}")
    print(f"  Succeeded:      {succeeded}")
    print(f"  Failed:         {failed}")
    print(f"  Skipped:        {skipped}")
    print(f"Rows total:       {total_rows:,}")
    print(f"Rows written:     {total_written:,}")
    print(f"Rows quarantined: {total_quarantined:,}")
    print(f"Elapsed:          {elapsed:.1f}s")
    if total_rows > 0:
        print(f"Throughput:       {total_rows / elapsed:,.0f} rows/s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
