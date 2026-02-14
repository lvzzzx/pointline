"""Publish extracted Quant360 members to stable extracted Bronze layout."""

from __future__ import annotations

import gzip
import os
from pathlib import Path

from pointline.vendors.quant360.upstream.models import MemberPayload, PublishedFile
from pointline.vendors.quant360.upstream.utils import file_sha256


def build_rel_path(payload: MemberPayload) -> Path:
    """Build relative path for a member in Bronze layout."""
    job = payload.member_job
    date_str = job.trading_date.isoformat()
    return (
        Path(f"exchange={job.exchange}")
        / f"type={job.data_type}"
        / f"date={date_str}"
        / f"symbol={job.symbol}"
        / f"{job.symbol}.csv.gz"
    )


def publish(
    payload: MemberPayload,
    *,
    bronze_root: Path,
) -> PublishedFile:
    """Publish a member payload to Bronze (overwrites if exists)."""
    rel_path = build_rel_path(payload)
    output_path = bronze_root / rel_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    already_existed = output_path.exists()
    tmp = output_path.with_suffix(".tmp")
    try:
        with gzip.open(tmp, mode="wb") as f:
            f.write(payload.csv_bytes)
        os.replace(tmp, output_path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)

    return PublishedFile(
        bronze_rel_path=str(rel_path),
        output_path=output_path,
        output_sha256=file_sha256(output_path),
        file_size_bytes=output_path.stat().st_size,
        data_type=payload.member_job.data_type,
        exchange=payload.member_job.exchange,
        symbol=payload.member_job.symbol,
        trading_date=payload.member_job.trading_date,
        already_exists=already_existed,
    )
