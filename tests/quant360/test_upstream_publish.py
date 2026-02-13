from __future__ import annotations

import gzip
from datetime import date
from pathlib import Path

from pointline.vendors.quant360.types import Quant360ArchiveMeta
from pointline.vendors.quant360.upstream.models import (
    Quant360ArchiveJob,
    Quant360MemberJob,
    Quant360MemberPayload,
)
from pointline.vendors.quant360.upstream.publish import publish_member_payload


def _payload(csv_content: bytes) -> Quant360MemberPayload:
    archive_meta = Quant360ArchiveMeta(
        source_filename="order_new_STK_SZ_20240102.7z",
        stream_type="order_new",
        market="STK",
        exchange="szse",
        trading_date=date(2024, 1, 2),
        canonical_data_type="cn_order_events",
    )
    archive_job = Quant360ArchiveJob(
        archive_path=Path("/tmp/order_new_STK_SZ_20240102.7z"),
        archive_meta=archive_meta,
        archive_sha256="c" * 64,
    )
    member_job = Quant360MemberJob(
        archive_job=archive_job,
        member_path="order_new_STK_SZ_20240102/000001.csv",
        symbol="000001",
    )
    return Quant360MemberPayload(member_job=member_job, csv_bytes=csv_content)


def test_publish_member_payload_writes_expected_path_and_content(tmp_path: Path) -> None:
    payload = _payload(b"col1,col2\n1,2\n")
    published = publish_member_payload(payload, bronze_root=tmp_path)

    assert published.bronze_rel_path == (
        "exchange=szse/type=order_new/date=2024-01-02/symbol=000001/000001.csv.gz"
    )
    assert published.output_path.exists()
    assert published.already_exists is False

    with gzip.open(published.output_path, mode="rb") as handle:
        assert handle.read() == b"col1,col2\n1,2\n"


def test_publish_member_payload_does_not_overwrite_existing_file(tmp_path: Path) -> None:
    first = publish_member_payload(_payload(b"x,y\n1,2\n"), bronze_root=tmp_path)
    second = publish_member_payload(_payload(b"x,y\n9,9\n"), bronze_root=tmp_path)

    assert first.output_path == second.output_path
    assert second.already_exists is True

    with gzip.open(first.output_path, mode="rb") as handle:
        assert handle.read() == b"x,y\n1,2\n"


def test_publish_handoff_to_bronze_metadata(tmp_path: Path) -> None:
    published = publish_member_payload(_payload(b"c,d\n7,8\n"), bronze_root=tmp_path)
    meta = published.to_bronze_file_metadata()
    assert meta.vendor == "quant360"
    assert meta.data_type == "order_new"
    assert meta.bronze_file_path == published.bronze_rel_path
    assert meta.file_size_bytes == published.file_size_bytes
    assert meta.sha256 == published.output_sha256
    assert meta.date == date(2024, 1, 2)
