"""Reorganize Quant360 SZSE L3 archives into Hive-partitioned bronze layout.

This module extracts .7z archives and reorganizes per-symbol CSV files into
a Hive-style partitioned structure for ETL ingestion.
"""

from __future__ import annotations

import gzip
import re
import subprocess
from datetime import date, datetime
from pathlib import Path


def parse_quant360_filename(filename: str) -> dict[str, str] | None:
    """
    Parse Quant360 archive filename to extract metadata.

    Expected formats:
    - order_new_STK_SZ_20240930.7z
    - tick_new_STK_SZ_20240930.7z

    Returns:
        dict with keys: data_type, market, exchange, date_str
        None if filename doesn't match expected pattern
    """
    pattern = r"^(order|tick)_new_STK_(SZ|SH)_(\d{8})\.7z$"
    match = re.match(pattern, filename)
    if not match:
        return None

    data_type, exchange_code, date_str = match.groups()

    # Map exchange code to our exchange names
    exchange_map = {
        "SZ": "szse",  # Shenzhen Stock Exchange
        "SH": "sse",  # Shanghai Stock Exchange
    }

    return {
        "data_type": data_type,  # "order" or "tick"
        "market": "STK",  # Stock market
        "exchange": exchange_map[exchange_code],
        "date_str": date_str,  # YYYYMMDD
    }


def parse_quant360_date(date_str: str) -> date:
    """Parse YYYYMMDD string to date object."""
    return datetime.strptime(date_str, "%Y%m%d").date()


def list_archive_contents(archive_path: Path) -> list[str]:
    """
    List CSV files in 7z archive.

    Returns:
        List of file paths within archive (e.g., "order_new_STK_SZ_20240930/000001.csv")
    """
    result = subprocess.run(
        ["7z", "l", "-slt", str(archive_path)],
        capture_output=True,
        text=True,
        check=True,
    )

    # Parse 7z output to get file paths
    files = []
    current_path = None

    for line in result.stdout.split("\n"):
        if line.startswith("Path = "):
            path = line.split(" = ", 1)[1]
            if path.endswith(".csv"):
                current_path = path
        elif line.startswith("Attributes = ") and current_path:
            # Check if it's a file (not directory)
            attrs = line.split(" = ", 1)[1]
            if "D" not in attrs:
                files.append(current_path)
            current_path = None

    return files


def extract_and_compress_file(
    archive_path: Path, file_path_in_archive: str, output_path: Path
) -> None:
    """
    Extract a single file from .7z archive and write as gzipped CSV.

    Args:
        archive_path: Path to .7z archive
        file_path_in_archive: Path within archive (e.g., "order_new_STK_SZ_20240930/000001.csv")
        output_path: Target path for compressed file (e.g., ".../000001.csv.gz")
    """
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Extract to stdout and compress directly
    extract_proc = subprocess.Popen(
        ["7z", "e", "-so", str(archive_path), file_path_in_archive],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )

    # Write compressed output
    with gzip.open(output_path, "wb") as gz_file:
        # Stream data to avoid loading entire file in memory
        chunk_size = 1024 * 1024  # 1MB chunks
        while True:
            chunk = extract_proc.stdout.read(chunk_size)
            if not chunk:
                break
            gz_file.write(chunk)

    extract_proc.wait()
    if extract_proc.returncode != 0:
        raise RuntimeError(
            f"7z extraction failed for {file_path_in_archive} with code {extract_proc.returncode}"
        )


def reorganize_archive(
    archive_path: Path,
    bronze_root: Path,
    vendor: str = "quant360",
    dry_run: bool = False,
) -> int:
    """
    Reorganize a single Quant360 .7z archive into Hive-partitioned structure.

    Args:
        archive_path: Path to .7z archive
        bronze_root: Root bronze directory (e.g., ~/data/lake/bronze)
        vendor: Vendor name (default: "quant360")
        dry_run: If True, only print actions without executing

    Returns:
        Number of files reorganized

    Target structure:
        bronze_root/
        └── {vendor}/
            └── exchange={exchange}/
                └── type=l3_{data_type}s/
                    └── date={YYYY-MM-DD}/
                        └── symbol={XXXXXX}/
                            └── {XXXXXX}.csv.gz
    """
    # Parse archive filename
    metadata = parse_quant360_filename(archive_path.name)
    if not metadata:
        raise ValueError(
            f"Invalid Quant360 filename format: {archive_path.name}. "
            f"Expected: (order|tick)_new_STK_(SZ|SH)_YYYYMMDD.7z"
        )

    exchange = metadata["exchange"]
    data_type = metadata["data_type"]  # "order" or "tick"
    date_obj = parse_quant360_date(metadata["date_str"])
    date_str = date_obj.isoformat()  # YYYY-MM-DD

    # Map data_type to table type
    type_map = {
        "order": "l3_orders",
        "tick": "l3_ticks",
    }
    table_type = type_map[data_type]

    print(f"Processing {archive_path.name}")
    print(f"  Exchange: {exchange}")
    print(f"  Type: {table_type}")
    print(f"  Date: {date_str}")

    # List files in archive
    csv_files = list_archive_contents(archive_path)
    print(f"  Found {len(csv_files)} CSV files")

    if dry_run:
        print("  [DRY RUN] Would extract and reorganize these files:")
        for csv_file in csv_files[:5]:  # Show first 5
            symbol = Path(csv_file).stem  # Extract symbol from filename
            target = (
                bronze_root
                / vendor
                / f"exchange={exchange}"
                / f"type={table_type}"
                / f"date={date_str}"
                / f"symbol={symbol}"
                / f"{symbol}.csv.gz"
            )
            print(f"    {csv_file} -> {target}")
        if len(csv_files) > 5:
            print(f"    ... and {len(csv_files) - 5} more files")
        return len(csv_files)

    # Extract and reorganize each CSV
    for i, csv_file in enumerate(csv_files, 1):
        symbol = Path(csv_file).stem  # Extract symbol from filename (e.g., "000001")

        # Build target path
        target_path = (
            bronze_root
            / vendor
            / f"exchange={exchange}"
            / f"type={table_type}"
            / f"date={date_str}"
            / f"symbol={symbol}"
            / f"{symbol}.csv.gz"
        )

        # Skip if already exists
        if target_path.exists():
            print(f"  [{i}/{len(csv_files)}] Skipping {symbol} (already exists)")
            continue

        print(f"  [{i}/{len(csv_files)}] Extracting {symbol}...", end=" ", flush=True)
        extract_and_compress_file(archive_path, csv_file, target_path)
        print(f"✓ {target_path}")

    print(f"✓ Completed {archive_path.name}")
    return len(csv_files)


def reorganize_quant360_archives(
    source_dir: Path | str,
    bronze_root: Path | str,
    vendor: str = "quant360",
    pattern: str = "*_new_STK_*.7z",
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Reorganize all Quant360 archives in a directory.

    Args:
        source_dir: Directory containing .7z archives
        bronze_root: Root bronze directory
        vendor: Vendor name (default: "quant360")
        pattern: Glob pattern to match archives (default: "*_new_STK_*.7z")
        dry_run: If True, only print actions without executing

    Returns:
        dict mapping archive names to file counts
    """
    source_dir = Path(source_dir)
    bronze_root = Path(bronze_root)

    # Find all matching archives
    archives = list(source_dir.glob(pattern))
    if not archives:
        print(f"No archives found matching {pattern} in {source_dir}")
        return {}

    print(f"Found {len(archives)} archives to process")
    print()

    results = {}
    for archive in sorted(archives):
        try:
            count = reorganize_archive(archive, bronze_root, vendor, dry_run)
            results[archive.name] = count
            print()
        except Exception as e:
            print(f"✗ Error processing {archive.name}: {e}")
            print()
            results[archive.name] = -1

    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    total_files = sum(c for c in results.values() if c > 0)
    successful = sum(1 for c in results.values() if c > 0)
    failed = sum(1 for c in results.values() if c < 0)

    for archive_name, count in sorted(results.items()):
        status = "✓" if count > 0 else "✗"
        print(f"{status} {archive_name}: {count if count > 0 else 'FAILED'} files")

    print()
    print(f"Processed {successful} archives successfully, {failed} failed")
    print(f"Total files reorganized: {total_files}")

    return results
