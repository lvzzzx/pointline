#!/usr/bin/env python3
"""Ingest extracted Quant360 partition files through the v2 ingestion pipeline.

Example (your test path):
    uv run python scripts/ingest_v2_quant360_partition.py \
      --source-dir ~/data/lake/bronze/quant360/exchange=sse/type=order_new \
      --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from time import perf_counter

import polars as pl

from pointline.io.protocols import BronzeFileMetadata
from pointline.v2.ingestion.pipeline import ingest_file
from pointline.v2.storage.delta import (
    DeltaDimensionStore,
    DeltaEventStore,
    DeltaManifestStore,
    DeltaQuarantineStore,
)
from pointline.v2.vendors.quant360 import get_quant360_stream_parser

_PARTITION_RE = re.compile(
    r"exchange=(?P<exchange>[^/]+)/type=(?P<data_type>[^/]+)/date=(?P<trading_date>\d{4}-\d{2}-\d{2})/symbol=(?P<symbol>[^/]+)/"
)


@dataclass(frozen=True)
class FileJob:
    path: Path
    rel_path: str
    exchange: str
    data_type: str
    symbol: str
    trading_date: date


def _sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _infer_bronze_root(source_dir: Path) -> Path:
    resolved = source_dir.expanduser().resolve()
    for idx, part in enumerate(resolved.parts):
        if "=" in part:
            if idx == 0:
                return resolved
            return Path(*resolved.parts[:idx])
    return resolved


def _infer_silver_root(bronze_root: Path) -> Path:
    # Prefer the common layout: <lake>/bronze/<vendor>.
    if bronze_root.parent.name == "bronze":
        return bronze_root.parent.parent / "silver"
    return bronze_root.parent / "silver"


def _discover_jobs(source_dir: Path, bronze_root: Path, *, limit: int | None) -> list[FileJob]:
    files = sorted(source_dir.glob("date=*/symbol=*/*.csv.gz"))
    if limit is not None:
        files = files[:limit]

    jobs: list[FileJob] = []
    for file_path in files:
        rel = file_path.resolve().relative_to(bronze_root.resolve()).as_posix()
        match = _PARTITION_RE.search(rel + "/")
        if match is None:
            raise ValueError(f"Unexpected partition layout: {file_path}")

        groups = match.groupdict()
        jobs.append(
            FileJob(
                path=file_path,
                rel_path=rel,
                exchange=groups["exchange"],
                data_type=groups["data_type"],
                symbol=groups["symbol"],
                trading_date=date.fromisoformat(groups["trading_date"]),
            )
        )
    return jobs


def _read_raw_csv(path: Path) -> pl.DataFrame:
    try:
        return pl.read_csv(path, infer_schema_length=10_000, try_parse_dates=False)
    except pl.exceptions.NoDataError:
        return pl.DataFrame()


def _make_parser(job: FileJob):
    stream_parser = get_quant360_stream_parser(job.data_type)

    def _parser(_meta: BronzeFileMetadata) -> pl.DataFrame:
        raw = _read_raw_csv(job.path)
        if raw.is_empty():
            return raw
        return stream_parser(raw, exchange=job.exchange, symbol=job.symbol)

    return _parser


def _build_meta(job: FileJob) -> BronzeFileMetadata:
    stat = job.path.stat()
    return BronzeFileMetadata(
        vendor="quant360",
        data_type=job.data_type,
        bronze_file_path=job.rel_path,
        file_size_bytes=int(stat.st_size),
        last_modified_ts=int(stat.st_mtime_ns // 1_000),
        sha256=_sha256(job.path),
        date=job.trading_date,
    )


def _print_file_result(index: int, total: int, job: FileJob, status: str, detail: str) -> None:
    print(f"[{index}/{total}] {status:<11} {job.rel_path}  {detail}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest extracted Quant360 partition files through v2 pipeline.",
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("~/data/lake/bronze/quant360/exchange=sse/type=order_new"),
        help="Partition directory to ingest (e.g., exchange=sse/type=order_new).",
    )
    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=None,
        help="Bronze root (defaults to inferred root before exchange=...).",
    )
    parser.add_argument(
        "--silver-root",
        type=Path,
        default=None,
        help="Silver root (defaults to <lake>/silver when inferable).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only ingest first N files (sorted by path).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force ingest even if manifest has success records for file identities.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run full transform/validation without writing manifest/events/quarantine.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    started = perf_counter()

    source_dir = args.source_dir.expanduser().resolve()
    if not source_dir.exists():
        raise SystemExit(f"source directory does not exist: {source_dir}")

    bronze_root = (
        args.bronze_root.expanduser().resolve()
        if args.bronze_root is not None
        else _infer_bronze_root(source_dir)
    )
    silver_root = (
        args.silver_root.expanduser().resolve()
        if args.silver_root is not None
        else _infer_silver_root(bronze_root)
    )

    jobs = _discover_jobs(source_dir, bronze_root, limit=args.limit)
    if not jobs:
        print(f"No files found under: {source_dir}")
        return 0

    print(f"Source dir : {source_dir}")
    print(f"Bronze root: {bronze_root}")
    print(f"Silver root: {silver_root}")
    print(f"Files      : {len(jobs)}")
    print(f"Dry run    : {args.dry_run}")
    print()

    manifest_store = DeltaManifestStore(silver_root / "ingest_manifest")
    event_store = DeltaEventStore(silver_root=silver_root)
    quarantine_store = DeltaQuarantineStore(silver_root=silver_root)
    dim_store = DeltaDimensionStore(silver_root=silver_root)
    dim_symbol_df = dim_store.load_dim_symbol()
    if dim_symbol_df.is_empty():
        print(
            "Warning: dim_symbol is empty. PIT coverage will quarantine all rows "
            "(expected for dry-run smoke tests)."
        )
        print()

    success = 0
    quarantined = 0
    failed = 0
    skipped = 0
    total_written = 0
    total_quarantined = 0

    for idx, job in enumerate(jobs, start=1):
        meta = _build_meta(job)
        parser = _make_parser(job)

        result = ingest_file(
            meta,
            parser=parser,
            manifest_repo=manifest_store,
            writer=event_store,
            dim_symbol_df=dim_symbol_df,
            quarantine_store=quarantine_store,
            force=args.force,
            dry_run=args.dry_run,
        )

        if result.skipped:
            skipped += 1
        elif result.status == "success":
            success += 1
        elif result.status == "quarantined":
            quarantined += 1
        else:
            failed += 1

        total_written += result.rows_written
        total_quarantined += result.rows_quarantined

        detail = (
            f"rows={result.row_count} written={result.rows_written} "
            f"quarantined={result.rows_quarantined}"
        )
        if result.failure_reason:
            detail += f" reason={result.failure_reason}"
        if result.error_message:
            detail += f" error={result.error_message}"
        _print_file_result(idx, len(jobs), job, result.status.upper(), detail)

    elapsed = perf_counter() - started
    print()
    print("Summary")
    print(f"- success    : {success}")
    print(f"- quarantined: {quarantined}")
    print(f"- failed     : {failed}")
    print(f"- skipped    : {skipped}")
    print(f"- rows_written_total    : {total_written}")
    print(f"- rows_quarantined_total: {total_quarantined}")
    print(f"- elapsed_sec           : {elapsed:.2f}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
