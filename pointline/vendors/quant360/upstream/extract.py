"""Archive extraction helpers for Quant360 upstream adapter."""

from __future__ import annotations

import gzip
import os
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory

import py7zr

from pointline.vendors.quant360.upstream.discover import plan_members
from pointline.vendors.quant360.upstream.models import ArchiveJob, MemberJob


class ExtractionError(Exception):
    """Raised when archive extraction fails or produces unexpected results."""

    pass


def _extract_all(job: ArchiveJob, extract_root: Path) -> set[str]:
    """Extract entire archive using 7z CLI if available, fallback to py7zr.

    Returns:
        Set of extracted member paths (relative to extract_root)
    """
    if seven_zip := shutil.which("7z"):
        subprocess.run(
            [seven_zip, "x", str(job.archive_path), f"-o{extract_root}", "-bso0", "-bsp0"],
            check=True,
            capture_output=True,
        )
    else:
        with py7zr.SevenZipFile(job.archive_path, mode="r") as archive:
            archive.extract(path=extract_root)

    # Collect all extracted CSV files (case-insensitive suffix match).
    extracted: set[str] = set()
    for path in extract_root.rglob("*"):
        if not path.is_file() or path.suffix.lower() != ".csv":
            continue
        rel_path = path.relative_to(extract_root)
        extracted.add(str(rel_path))
    return extracted


def _gzip_all_csvs(extract_root: Path) -> None:
    """Gzip all CSV files in-place under extract_root.

    Prefer CLI gzip for speed, fallback to Python gzip path-by-path.
    """
    csv_files = sorted(
        [
            path
            for path in extract_root.rglob("*")
            if path.is_file() and path.suffix.lower() == ".csv"
        ]
    )
    if not csv_files:
        return

    for csv_path in csv_files:
        os.chmod(csv_path, 0o600)

    if gzip_bin := shutil.which("gzip"):
        # Avoid overly long argv by batching.
        batch_size = 500
        for i in range(0, len(csv_files), batch_size):
            batch = csv_files[i : i + batch_size]
            subprocess.run(
                [gzip_bin, "-f", *[str(path) for path in batch]],
                check=True,
                capture_output=True,
            )
        return

    for csv_path in csv_files:
        payload = csv_path.read_bytes()
        gz_path = csv_path.with_name(f"{csv_path.name}.gz")
        with gz_path.open("wb") as f, gzip.GzipFile(filename="", mode="wb", fileobj=f) as gz:
            gz.write(payload)
        csv_path.unlink(missing_ok=True)


def iter_members(
    job: ArchiveJob,
    *,
    member_jobs: list[MemberJob] | None = None,
    expected_members: list[str] | None = None,
) -> Iterator[tuple[MemberJob, Path]]:
    """Iterate over all members as staged .csv.gz files.

    The archive is extracted once to a temp directory, all CSV members are gzipped
    in place, and each planned member yields `(member_job, gz_path)`.
    """
    planned = member_jobs if member_jobs is not None else plan_members(job)
    expected = (
        expected_members if expected_members is not None else [m.member_path for m in planned]
    )
    expected_set = set(expected)

    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        extracted = _extract_all(job, root)

        missing = expected_set - extracted
        if missing:
            raise ExtractionError(
                f"Archive extraction incomplete: {len(missing)} expected members not found. "
                f"Archive: {job.archive_path}, Missing: {sorted(missing)[:5]}..."
            )

        _gzip_all_csvs(root)

        for member_job in planned:
            gz_path = root / f"{member_job.member_path}.gz"
            if not gz_path.exists():
                raise ExtractionError(
                    f"Gzipped member missing after extraction: {member_job.member_path}"
                )
            os.chmod(gz_path, 0o600)
            yield member_job, gz_path
