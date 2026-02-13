from __future__ import annotations

from datetime import date
from pathlib import Path

from pointline.v2.vendors.quant360.types import Quant360ArchiveMeta
from pointline.v2.vendors.quant360.upstream.contracts import (
    LEDGER_STATUS_FAILED,
    LEDGER_STATUS_PENDING,
    LEDGER_STATUS_SUCCESS,
)
from pointline.v2.vendors.quant360.upstream.models import (
    Quant360ArchiveJob,
    Quant360ArchiveKey,
    Quant360MemberJob,
)


def test_archive_key_string_is_stable() -> None:
    key = Quant360ArchiveKey(
        source_filename="order_new_STK_SZ_20240102.7z",
        archive_sha256="d" * 64,
    )
    assert key.as_string() == "order_new_STK_SZ_20240102.7z:" + ("d" * 64)


def test_member_job_properties_follow_archive_meta() -> None:
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
        archive_sha256="b" * 64,
    )
    member_job = Quant360MemberJob(
        archive_job=archive_job,
        member_path="order_new_STK_SZ_20240102/000001.csv",
        symbol="000001",
    )
    assert member_job.data_type == "order_new"
    assert member_job.exchange == "szse"
    assert member_job.trading_date == date(2024, 1, 2)


def test_ledger_status_values_are_explicit() -> None:
    assert LEDGER_STATUS_PENDING == "pending"
    assert LEDGER_STATUS_SUCCESS == "success"
    assert LEDGER_STATUS_FAILED == "failed"
