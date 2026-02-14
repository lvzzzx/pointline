"""Archive discovery and member planning for Quant360 upstream adapter."""

from __future__ import annotations

from pathlib import Path

import py7zr

from pointline.vendors.quant360.filenames import (
    parse_archive_filename,
    parse_symbol_from_member_path,
)
from pointline.vendors.quant360.upstream.models import ArchiveJob, MemberJob
from pointline.vendors.quant360.upstream.utils import file_sha256


def discover_archives(source_dir: Path) -> list[ArchiveJob]:
    """Discover all .7z archives in source directory."""
    jobs: list[ArchiveJob] = []
    for path in sorted(source_dir.glob("*.7z")):
        meta = parse_archive_filename(path.name)
        jobs.append(
            ArchiveJob(
                archive_path=path,
                archive_meta=meta,
                archive_sha256=file_sha256(path),
            )
        )
    return sorted(
        jobs,
        key=lambda j: (
            j.archive_meta.trading_date.isoformat(),
            j.archive_meta.exchange,
            j.archive_meta.stream_type,
            j.archive_path.name,
        ),
    )


def list_csv_members(job: ArchiveJob) -> list[str]:
    """List all CSV members in an archive."""
    with py7zr.SevenZipFile(job.archive_path, mode="r") as archive:
        return sorted(name for name in archive.getnames() if name.lower().endswith(".csv"))


def plan_members(job: ArchiveJob) -> list[MemberJob]:
    """Plan member extraction jobs for an archive."""
    members: list[MemberJob] = []
    for member_path in list_csv_members(job):
        symbol = parse_symbol_from_member_path(member_path)
        members.append(
            MemberJob(
                archive_job=job,
                member_path=member_path,
                symbol=symbol,
            )
        )
    return sorted(members, key=lambda m: m.member_path)
