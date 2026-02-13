"""Archive extraction helpers for Quant360 upstream adapter."""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory

import py7zr

from pointline.vendors.quant360.upstream.discover import plan_archive_members
from pointline.vendors.quant360.upstream.models import (
    Quant360ArchiveJob,
    Quant360MemberJob,
    Quant360MemberPayload,
)


def extract_member_payload(member_job: Quant360MemberJob) -> Quant360MemberPayload:
    with TemporaryDirectory() as tmpdir:
        extract_root = Path(tmpdir)
        with py7zr.SevenZipFile(member_job.archive_job.archive_path, mode="r") as archive:
            archive.extract(path=extract_root, targets=[member_job.member_path])
        extracted_path = extract_root / member_job.member_path
        if not extracted_path.exists():
            raise ValueError(
                f"Archive member not found after extraction: {member_job.member_path} "
                f"(archive={member_job.archive_job.archive_path})"
            )
        os.chmod(extracted_path, 0o600)
        return Quant360MemberPayload(
            member_job=member_job,
            csv_bytes=extracted_path.read_bytes(),
        )


def _extract_archive_once(job: Quant360ArchiveJob, extract_root: Path) -> None:
    seven_zip = shutil.which("7z")
    if seven_zip is not None:
        subprocess.run(
            [seven_zip, "x", str(job.archive_path), f"-o{extract_root}", "-bso0", "-bsp0"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return

    with py7zr.SevenZipFile(job.archive_path, mode="r") as archive:
        archive.extract(path=extract_root)


def iter_archive_members(
    job: Quant360ArchiveJob,
    *,
    member_jobs: list[Quant360MemberJob] | None = None,
) -> Iterator[Quant360MemberPayload]:
    planned = member_jobs if member_jobs is not None else plan_archive_members(job)
    with TemporaryDirectory() as tmpdir:
        extract_root = Path(tmpdir)
        _extract_archive_once(job, extract_root)
        for member_job in planned:
            extracted_path = extract_root / member_job.member_path
            if not extracted_path.exists():
                raise ValueError(
                    f"Archive member missing after archive extraction: {member_job.member_path} "
                    f"(archive={job.archive_path})"
                )
            os.chmod(extracted_path, 0o600)
            yield Quant360MemberPayload(
                member_job=member_job,
                csv_bytes=extracted_path.read_bytes(),
            )
