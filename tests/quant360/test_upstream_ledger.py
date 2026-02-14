from __future__ import annotations

from pathlib import Path

from pointline.vendors.quant360.upstream.ledger import STATUS_FAILED, STATUS_SUCCESS, Ledger
from pointline.vendors.quant360.upstream.models import ArchiveKey, ArchiveState


def test_ledger_persists_success_skip_state(tmp_path: Path) -> None:
    ledger_path = tmp_path / "ledger" / "quant360_upstream.json"
    key = ArchiveKey(
        source_filename="order_new_STK_SZ_20240102.7z",
        archive_sha256="a" * 64,
    )

    ledger = Ledger(ledger_path)
    ledger.load()
    assert ledger.is_success(key) is False

    ledger.set_state(
        ArchiveState(
            archive_key=key,
            status=STATUS_SUCCESS,
            member_count=2,
            published_count=2,
        )
    )
    ledger.save()

    reloaded = Ledger(ledger_path)
    reloaded.load()
    assert reloaded.is_success(key) is True
    assert reloaded.is_failed(key) is False


def test_ledger_failed_state_is_not_skipped(tmp_path: Path) -> None:
    ledger_path = tmp_path / "ledger.json"
    key = ArchiveKey(
        source_filename="tick_new_STK_SH_20240102.7z",
        archive_sha256="c" * 64,
    )
    ledger = Ledger(ledger_path)
    ledger.load()

    ledger.set_state(
        ArchiveState(
            archive_key=key,
            status=STATUS_FAILED,
            member_count=1,
            published_count=0,
            failure_reason="publish_error",
            error_message="boom",
        )
    )
    ledger.save()

    reloaded = Ledger(ledger_path)
    reloaded.load()
    assert reloaded.is_success(key) is False
    assert reloaded.is_failed(key) is True


def test_ledger_can_retrieve_previous_state(tmp_path: Path) -> None:
    ledger_path = tmp_path / "ledger.json"
    key = ArchiveKey(
        source_filename="order_new_STK_SZ_20240102.7z",
        archive_sha256="a" * 64,
    )

    ledger = Ledger(ledger_path)
    ledger.load()
    assert ledger.get_state(key) is None

    state = ArchiveState(
        archive_key=key,
        status=STATUS_FAILED,
        member_count=5,
        published_count=3,
        failure_reason="extract_error",
        error_message="member missing",
    )
    ledger.set_state(state)

    retrieved = ledger.get_state(key)
    assert retrieved is not None
    assert retrieved.status == STATUS_FAILED
    assert retrieved.member_count == 5
    assert retrieved.published_count == 3
