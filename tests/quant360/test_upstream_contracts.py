from __future__ import annotations

from datetime import date
from pathlib import Path

from pointline.vendors.quant360.types import Quant360ArchiveMeta
from pointline.vendors.quant360.upstream.ledger import STATUS_FAILED, STATUS_SUCCESS
from pointline.vendors.quant360.upstream.models import (
    ArchiveJob,
    ArchiveKey,
    ArchiveState,
    MemberJob,
)


def test_archive_key_string_is_stable() -> None:
    key = ArchiveKey(
        source_filename="order_new_STK_SZ_20240102.7z",
        archive_sha256="d" * 64,
    )
    assert str(key) == "order_new_STK_SZ_20240102.7z:" + ("d" * 64)


def test_member_job_properties_follow_archive_meta() -> None:
    archive_meta = Quant360ArchiveMeta(
        source_filename="order_new_STK_SZ_20240102.7z",
        stream_type="order_new",
        market="STK",
        exchange="szse",
        trading_date=date(2024, 1, 2),
        canonical_data_type="cn_order_events",
    )
    archive_job = ArchiveJob(
        archive_path=Path("/tmp/order_new_STK_SZ_20240102.7z"),
        archive_meta=archive_meta,
        archive_sha256="b" * 64,
    )
    member_job = MemberJob(
        archive_job=archive_job,
        member_path="order_new_STK_SZ_20240102/000001.csv",
        symbol="000001",
    )
    assert member_job.data_type == "order_new"
    assert member_job.exchange == "szse"
    assert member_job.trading_date == date(2024, 1, 2)


def test_ledger_status_values_are_explicit() -> None:
    assert STATUS_SUCCESS == "success"
    assert STATUS_FAILED == "failed"


def test_archive_state_tracks_counts() -> None:
    key = ArchiveKey(
        source_filename="order_new_STK_SZ_20240102.7z",
        archive_sha256="a" * 64,
    )
    state = ArchiveState(
        archive_key=key,
        status=STATUS_SUCCESS,
        member_count=10,
        published_count=10,
    )
    assert state.member_count == 10
    assert state.published_count == 10
