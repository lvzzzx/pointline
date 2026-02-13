"""Archive extraction helpers for Quant360 upstream adapter."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory

import py7zr

from pointline.v2.vendors.quant360.upstream.discover import plan_archive_members
from pointline.v2.vendors.quant360.upstream.models import (
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


def iter_archive_members(job: Quant360ArchiveJob) -> Iterator[Quant360MemberPayload]:
    for member_job in plan_archive_members(job):
        yield extract_member_payload(member_job)
