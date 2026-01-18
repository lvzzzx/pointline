"""Shared helpers for CLI commands."""

from __future__ import annotations

import gzip
import hashlib
import inspect
import re
import time
from pathlib import Path
from typing import Iterable, Sequence

import polars as pl

from pointline.config import get_exchange_id, normalize_exchange
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.protocols import BronzeFileMetadata


def sorted_files(files: Iterable[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
    return sorted(
        files,
        key=lambda f: (
            f.vendor,
            f.exchange,
            f.data_type,
            f.symbol,
            f.date.isoformat(),
            f.bronze_file_path,
        ),
    )


def print_files(files: Sequence[BronzeFileMetadata]) -> None:
    for f in files:
        print(
            " | ".join(
                [
                    f"vendor={f.vendor}",
                    f"exchange={f.exchange}",
                    f"type={f.data_type}",
                    f"symbol={f.symbol}",
                    f"date={f.date.isoformat()}",
                    f"path={f.bronze_file_path}",
                ]
            )
        )


def compute_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def parse_date_arg(value: str | None) -> date | None:
    if value is None:
        return None
    from datetime import datetime as _dt

    try:
        return _dt.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {value} (expected YYYY-MM-DD)") from exc


def resolve_manifest_file_id(
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
    if bronze_root.name != "bronze":
        vendor = bronze_root.name
    else:
        vendor = bronze_rel.parts[0] if bronze_rel.parts else "unknown"

    sha256 = compute_sha256(file_path)
    manifest_df = manifest_repo.read_all()
    if manifest_df.is_empty():
        raise ValueError("manifest is empty; cannot resolve file_id")

    matches = manifest_df.filter(
        (pl.col("vendor") == vendor)
        & (pl.col("exchange") == exchange)
        & (pl.col("data_type") == data_type)
        & (pl.col("symbol") == symbol)
        & (pl.col("date") == file_date)
        & (pl.col("bronze_file_name") == str(bronze_rel))
        & (pl.col("sha256") == sha256)
    )
    if matches.is_empty():
        raise ValueError("No manifest record found for file path + sha256")

    return matches.item(0, "file_id")


def add_lineage(df: pl.DataFrame, file_id: int) -> pl.DataFrame:
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


def add_metadata(df: pl.DataFrame, exchange: str, exchange_id: int) -> pl.DataFrame:
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


def compare_expected_vs_ingested(
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


def parse_partition_filters(items: Sequence[str]) -> dict[str, object]:
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


def read_bronze_csv(path: Path) -> pl.DataFrame:
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


def infer_bronze_metadata(path: Path) -> dict[str, str]:
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


def read_updates(path: Path) -> pl.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pl.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pl.read_parquet(path)
    raise SystemExit(f"Unsupported update file format: {path}")


def parse_effective_ts(value: str | None) -> int:
    if value is None or value.lower() == "now":
        return int(time.time() * 1_000_000)
    try:
        return int(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid --effective-ts value: {value}") from exc


def parse_symbol_id_single(value: str | None) -> int | None:
    if not value:
        return None
    items = [int(part.strip()) for part in value.split(",") if part.strip()]
    if not items:
        return None
    if len(items) != 1:
        raise ValueError("symbol_id must be a single value")
    return items[0]


def normalize_exchange_arg(exchange: str | None) -> tuple[str, int] | tuple[None, None]:
    if exchange is None:
        return None, None
    exchange = normalize_exchange(exchange)
    return exchange, get_exchange_id(exchange)
