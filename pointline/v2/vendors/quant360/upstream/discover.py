"""Archive discovery and member planning for Quant360 upstream adapter."""

from __future__ import annotations

import hashlib
from pathlib import Path

import py7zr

from pointline.v2.vendors.quant360.filenames import (
    parse_archive_filename,
    parse_symbol_from_member_path,
)
from pointline.v2.vendors.quant360.upstream.models import Quant360ArchiveJob, Quant360MemberJob


def _compute_file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def discover_quant360_archives(source_dir: Path) -> list[Quant360ArchiveJob]:
    jobs: list[Quant360ArchiveJob] = []
    for archive_path in sorted(source_dir.glob("*.7z")):
        archive_meta = parse_archive_filename(archive_path.name)
        jobs.append(
            Quant360ArchiveJob(
                archive_path=archive_path,
                archive_meta=archive_meta,
                archive_sha256=_compute_file_sha256(archive_path),
            )
        )
    return sorted(
        jobs,
        key=lambda job: (
            job.archive_meta.trading_date.isoformat(),
            job.archive_meta.exchange,
            job.archive_meta.stream_type,
            job.archive_path.name,
        ),
    )


def list_archive_csv_members(job: Quant360ArchiveJob) -> list[str]:
    with py7zr.SevenZipFile(job.archive_path, mode="r") as archive:
        return sorted(name for name in archive.getnames() if name.lower().endswith(".csv"))


def plan_archive_members(job: Quant360ArchiveJob) -> list[Quant360MemberJob]:
    members: list[Quant360MemberJob] = []
    for member_path in list_archive_csv_members(job):
        symbol = parse_symbol_from_member_path(member_path)
        members.append(
            Quant360MemberJob(
                archive_job=job,
                member_path=member_path,
                symbol=symbol,
            )
        )
    return sorted(members, key=lambda member: member.member_path)
