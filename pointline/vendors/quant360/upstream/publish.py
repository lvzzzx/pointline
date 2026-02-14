"""Publish extracted Quant360 members to stable extracted Bronze layout."""

from __future__ import annotations

import errno
import os
import shutil
from pathlib import Path

from pointline.vendors.quant360.upstream.models import MemberJob, PublishedFile
from pointline.vendors.quant360.upstream.utils import file_sha256


def build_rel_path(member_job: MemberJob) -> Path:
    """Build relative path for a member in Bronze layout."""
    date_str = member_job.trading_date.isoformat()
    return (
        Path(f"exchange={member_job.exchange}")
        / f"type={member_job.data_type}"
        / f"date={date_str}"
        / f"symbol={member_job.symbol}"
        / f"{member_job.symbol}.csv.gz"
    )


def publish(
    member_job: MemberJob,
    *,
    gz_path: Path,
    bronze_root: Path,
) -> PublishedFile:
    """Publish a pre-gzipped member file to Bronze (overwrites if exists)."""
    rel_path = build_rel_path(member_job)
    output_path = bronze_root / rel_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    already_existed = output_path.exists()
    try:
        os.replace(gz_path, output_path)
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        shutil.copy2(gz_path, output_path)
        gz_path.unlink(missing_ok=True)

    return PublishedFile(
        bronze_rel_path=str(rel_path),
        output_path=output_path,
        output_sha256=file_sha256(output_path),
        file_size_bytes=output_path.stat().st_size,
        data_type=member_job.data_type,
        exchange=member_job.exchange,
        symbol=member_job.symbol,
        trading_date=member_job.trading_date,
        already_exists=already_existed,
    )
