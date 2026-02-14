"""Archive extraction helpers for Quant360 upstream adapter."""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory

import py7zr

from pointline.vendors.quant360.upstream.discover import plan_members
from pointline.vendors.quant360.upstream.models import ArchiveJob, MemberJob, MemberPayload


class ExtractionError(Exception):
    """Raised when archive extraction fails or produces unexpected results."""

    pass


def extract_member(member_job: MemberJob) -> MemberPayload:
    """Extract a single member from an archive."""
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        with py7zr.SevenZipFile(member_job.archive_job.archive_path, mode="r") as archive:
            archive.extract(path=root, targets=[member_job.member_path])

        extracted = root / member_job.member_path
        if not extracted.exists():
            raise ExtractionError(f"Member not found: {member_job.member_path}")

        os.chmod(extracted, 0o600)
        return MemberPayload(
            member_job=member_job,
            csv_bytes=extracted.read_bytes(),
        )


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

    # Collect all extracted CSV files
    extracted: set[str] = set()
    for path in extract_root.rglob("*.csv"):
        # Get relative path from extract_root
        rel_path = path.relative_to(extract_root)
        extracted.add(str(rel_path))
    return extracted


def iter_members(
    job: ArchiveJob,
    *,
    member_jobs: list[MemberJob] | None = None,
    expected_members: list[str] | None = None,
) -> Iterator[MemberPayload]:
    """Iterate over all members in an archive (extracts once, yields all).

    Args:
        job: The archive job to process
        member_jobs: Pre-planned member jobs (optional)
        expected_members: List of expected member paths for validation (optional)

    Raises:
        ExtractionError: If extracted files don't match expected members
    """
    planned = member_jobs if member_jobs is not None else plan_members(job)
    expected = (
        expected_members if expected_members is not None else [m.member_path for m in planned]
    )
    expected_set = set(expected)

    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        extracted = _extract_all(job, root)

        # Validate extraction: all expected members must be present
        missing = expected_set - extracted
        if missing:
            raise ExtractionError(
                f"Archive extraction incomplete: {len(missing)} expected members not found. "
                f"Archive: {job.archive_path}, Missing: {sorted(missing)[:5]}..."
            )

        # Yield payloads for all planned members
        for member_job in planned:
            extracted_path = root / member_job.member_path
            if not extracted_path.exists():
                raise ExtractionError(f"Member missing after extraction: {member_job.member_path}")

            os.chmod(extracted_path, 0o600)
            yield MemberPayload(
                member_job=member_job,
                csv_bytes=extracted_path.read_bytes(),
            )
