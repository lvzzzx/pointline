"""Shared helpers for CLI commands."""

from __future__ import annotations

import gzip
import hashlib
import inspect
import re
import time
from collections.abc import Iterable, Sequence
from datetime import date
from pathlib import Path

import polars as pl

from pointline.config import get_exchange_id, normalize_exchange
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.protocols import BronzeFileMetadata


def sorted_files(files: Iterable[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
    """Sort bronze files by vendor, data type, date, and path.

    Args:
        files: Iterable of bronze file metadata.

    Returns:
        Sorted list of bronze file metadata.
    """
    return sorted(
        files,
        key=lambda f: (
            f.vendor,
            f.data_type,
            f.date.isoformat() if f.date else "",
            f.bronze_file_path,
        ),
    )


def print_files(files: Sequence[BronzeFileMetadata], *, limit: int | None = 100) -> None:
    """
    Print bronze file metadata.

    Args:
        files: Sequence of file metadata
        limit: Maximum number of files to print. If None, prints all files.
              Default is 100 to avoid flooding the terminal.
    """
    total = len(files)
    files_to_print = files if limit is None else files[:limit]

    for f in files_to_print:
        print(
            " | ".join(
                [
                    f"vendor={f.vendor}",
                    f"type={f.data_type}",
                    f"date={f.date.isoformat()}" if f.date else "date=<none>",
                    f"path={f.bronze_file_path}",
                ]
            )
        )

    if limit is not None and total > limit:
        print(f"... and {total - limit} more files (showing first {limit})")
        print("Tip: Use --limit to adjust output, or --limit 0 to show all files")


def compute_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Compute SHA256 hash of a file.

    Args:
        path: Path to the file.
        chunk_size: Size of chunks to read. Default is 1MB.

    Returns:
        Hex digest of the SHA256 hash.
    """
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def parse_date_arg(value: str | None) -> date | None:
    """Parse a date string in YYYY-MM-DD format.

    Args:
        value: Date string to parse, or None.

    Returns:
        Parsed date, or None if input was None.

    Raises:
        ValueError: If the date format is invalid.
    """
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
    data_type: str,
) -> int:
    """Resolve file ID from the manifest based on file path and SHA256 hash.

    Args:
        manifest_path: Path to the manifest table.
        bronze_root: Root directory for bronze files.
        file_path: Path to the file to look up.
        data_type: Data type of the file.

    Returns:
        Resolved file ID.

    Raises:
        ValueError: If file not found, not under bronze root, or no manifest record exists.
    """
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
        & (pl.col("data_type") == data_type)
        & (pl.col("bronze_file_name") == str(bronze_rel))
        & (pl.col("sha256") == sha256)
    )
    if matches.is_empty():
        raise ValueError("No manifest record found for file path + sha256")

    return matches.item(0, "file_id")


def add_lineage(df: pl.DataFrame, file_id: int) -> pl.DataFrame:
    """Add file_id and file_line_number columns for lineage tracking.

    Args:
        df: Input DataFrame.
        file_id: File ID to associate with the records.

    Returns:
        DataFrame with lineage columns added.
    """
    if "file_line_number" in df.columns:
        file_line_number = pl.col("file_line_number").cast(pl.Int32)
    else:
        file_line_number = pl.int_range(1, df.height + 1, dtype=pl.Int32)
    return df.with_columns(
        [
            pl.lit(file_id, dtype=pl.Int32).alias("file_id"),
            file_line_number.alias("file_line_number"),
        ]
    )


def add_metadata(df: pl.DataFrame, exchange: str, exchange_id: int | None = None) -> pl.DataFrame:
    """Add exchange metadata and derived date column to the DataFrame.

    Args:
        df: Input DataFrame with ts_local_us column.
        exchange: Exchange name.
        exchange_id: Deprecated, ignored. Kept for call-site compatibility.

    Returns:
        DataFrame with exchange and date columns added.
    """
    result = df.with_columns(
        [
            pl.lit(exchange, dtype=pl.Utf8).alias("exchange"),
        ]
    )
    return result.with_columns(
        [
            pl.from_epoch(pl.col("ts_local_us"), time_unit="us").cast(pl.Date).alias("date"),
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
    """Compare expected data against ingested data for validation.

    Args:
        expected: Expected DataFrame from the source file.
        ingested: Ingested DataFrame from the table.
        key_cols: Columns to use as join keys.
        compare_cols: Columns to compare for equality.
        limit: Maximum number of mismatched rows to return.

    Returns:
        Tuple of (expected_count, ingested_count, missing_count, extra_count,
                  mismatch_count, mismatch_sample).
    """
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

    comparisons = [pl.col(f"{col}_exp").eq_missing(pl.col(f"{col}_ing")) for col in compare_cols]
    all_equal = pl.all_horizontal(comparisons)
    mismatched = joined.filter(
        pl.col("_present_exp").is_not_null() & pl.col("_present_ing").is_not_null() & ~all_equal
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
    """Parse partition filter strings into a dictionary.

    Supports date values (parsed as dates) and integer values (parsed as int).

    Args:
        items: Sequence of "key=value" strings.

    Returns:
        Dictionary of filter key to parsed value.

    Raises:
        ValueError: If a filter string is invalid or date format is wrong.
    """
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
    """Infer bronze file metadata from its path.

    Expects path format: .../exchange={exchange}/type={type}/date={date}/symbol={symbol}/...

    Args:
        path: Path to the bronze file.

    Returns:
        Dictionary with exchange, data_type, date, and symbol if matched, empty otherwise.
    """
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
    """Read updates from a CSV or Parquet file.

    Args:
        path: Path to the update file.

    Returns:
        DataFrame containing the updates.

    Raises:
        SystemExit: If the file format is unsupported.
    """
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pl.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pl.read_parquet(path)
    raise SystemExit(f"Unsupported update file format: {path}")


def parse_effective_ts(value: str | None) -> int:
    """Parse effective timestamp from string or use current time.

    Args:
        value: Timestamp string (microseconds since epoch) or "now".

    Returns:
        Timestamp in microseconds since epoch.

    Raises:
        SystemExit: If the value cannot be parsed as an integer.
    """
    if value is None or value.lower() == "now":
        return int(time.time() * 1_000_000)
    try:
        return int(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid --effective-ts value: {value}") from exc


def normalize_exchange_arg(exchange: str | None) -> tuple[str, int] | tuple[None, None]:
    """Normalize exchange name and return with its ID.

    Args:
        exchange: Exchange name to normalize, or None.

    Returns:
        Tuple of (normalized_exchange, exchange_id) or (None, None) if input is None.
    """
    if exchange is None:
        return None, None
    exchange = normalize_exchange(exchange)
    return exchange, get_exchange_id(exchange)
