"""Publish extracted Quant360 members to stable extracted Bronze layout."""

from __future__ import annotations

import gzip
import hashlib
import os
from pathlib import Path

from pointline.v2.vendors.quant360.upstream.models import (
    Quant360MemberPayload,
    Quant360PublishedFile,
)


def _compute_file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def build_bronze_relative_path(member_payload: Quant360MemberPayload) -> Path:
    member_job = member_payload.member_job
    date_str = member_job.trading_date.isoformat()
    return (
        Path(f"exchange={member_job.exchange}")
        / f"type={member_job.data_type}"
        / f"date={date_str}"
        / f"symbol={member_job.symbol}"
        / f"{member_job.symbol}.csv.gz"
    )


def publish_member_payload(
    payload: Quant360MemberPayload,
    *,
    bronze_root: Path,
) -> Quant360PublishedFile:
    rel_path = build_bronze_relative_path(payload)
    output_path = bronze_root / rel_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        return Quant360PublishedFile(
            bronze_rel_path=str(rel_path),
            output_path=output_path,
            output_sha256=_compute_file_sha256(output_path),
            file_size_bytes=output_path.stat().st_size,
            data_type=payload.member_job.data_type,
            exchange=payload.member_job.exchange,
            symbol=payload.member_job.symbol,
            trading_date=payload.member_job.trading_date,
            already_exists=True,
        )

    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        with gzip.open(tmp_path, mode="wb") as handle:
            handle.write(payload.csv_bytes)
        os.replace(tmp_path, output_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)

    return Quant360PublishedFile(
        bronze_rel_path=str(rel_path),
        output_path=output_path,
        output_sha256=_compute_file_sha256(output_path),
        file_size_bytes=output_path.stat().st_size,
        data_type=payload.member_job.data_type,
        exchange=payload.member_job.exchange,
        symbol=payload.member_job.symbol,
        trading_date=payload.member_job.trading_date,
    )
