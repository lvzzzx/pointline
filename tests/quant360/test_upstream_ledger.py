from __future__ import annotations

from pathlib import Path

from pointline.vendors.quant360.upstream.contracts import (
    LEDGER_STATUS_FAILED,
    LEDGER_STATUS_SUCCESS,
)
from pointline.vendors.quant360.upstream.ledger import Quant360UpstreamLedger
from pointline.vendors.quant360.upstream.models import (
    Quant360ArchiveKey,
    Quant360LedgerRecord,
)


def test_ledger_persists_success_skip_state(tmp_path: Path) -> None:
    ledger_path = tmp_path / "ledger" / "quant360_upstream.json"
    key = Quant360ArchiveKey(
        source_filename="order_new_STK_SZ_20240102.7z",
        archive_sha256="a" * 64,
    )

    ledger = Quant360UpstreamLedger(ledger_path)
    ledger.load()
    assert ledger.should_skip(key) is False
    ledger.mark_success(
        Quant360LedgerRecord(
            archive_key=key,
            status=LEDGER_STATUS_SUCCESS,
            updated_at_us=1,
            member_count=2,
            published_count=2,
        )
    )
    ledger.save()

    reloaded = Quant360UpstreamLedger(ledger_path)
    reloaded.load()
    assert reloaded.should_skip(key) is True


def test_ledger_failed_state_is_not_skippable(tmp_path: Path) -> None:
    ledger_path = tmp_path / "ledger.json"
    key = Quant360ArchiveKey(
        source_filename="tick_new_STK_SH_20240102.7z",
        archive_sha256="c" * 64,
    )
    ledger = Quant360UpstreamLedger(ledger_path)
    ledger.load()
    ledger.mark_failure(
        Quant360LedgerRecord(
            archive_key=key,
            status=LEDGER_STATUS_FAILED,
            updated_at_us=2,
            failure_reason="publish_error",
            error_message="boom",
            member_count=1,
            published_count=0,
        )
    )
    ledger.save()

    reloaded = Quant360UpstreamLedger(ledger_path)
    reloaded.load()
    assert reloaded.should_skip(key) is False
