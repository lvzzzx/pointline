from __future__ import annotations

from pathlib import Path

from pointline.v2.vendors.quant360.upstream.contracts import (
    LEDGER_STATUS_FAILED,
    LEDGER_STATUS_SUCCESS,
)
from pointline.v2.vendors.quant360.upstream.ledger import Quant360UpstreamLedger
from pointline.v2.vendors.quant360.upstream.models import Quant360LedgerRecord, Quant360MemberKey


def test_ledger_persists_success_skip_state(tmp_path: Path) -> None:
    ledger_path = tmp_path / "ledger" / "quant360_upstream.json"
    key = Quant360MemberKey(
        archive_sha256="a" * 64,
        member_path="order_new_STK_SZ_20240102/000001.csv",
    )

    ledger = Quant360UpstreamLedger(ledger_path)
    ledger.load()
    assert ledger.should_skip(key) is False
    ledger.mark_success(
        Quant360LedgerRecord(
            member_key=key,
            status=LEDGER_STATUS_SUCCESS,
            updated_at_us=1,
            bronze_rel_path="exchange=szse/type=order_new/date=2024-01-02/symbol=000001/000001.csv.gz",
            output_sha256="b" * 64,
        )
    )
    ledger.save()

    reloaded = Quant360UpstreamLedger(ledger_path)
    reloaded.load()
    assert reloaded.should_skip(key) is True


def test_ledger_failed_state_is_not_skippable(tmp_path: Path) -> None:
    ledger_path = tmp_path / "ledger.json"
    key = Quant360MemberKey(
        archive_sha256="c" * 64,
        member_path="tick_new_STK_SH_20240102/600000.csv",
    )
    ledger = Quant360UpstreamLedger(ledger_path)
    ledger.load()
    ledger.mark_failure(
        Quant360LedgerRecord(
            member_key=key,
            status=LEDGER_STATUS_FAILED,
            updated_at_us=2,
            failure_reason="publish_error",
            error_message="boom",
        )
    )
    ledger.save()

    reloaded = Quant360UpstreamLedger(ledger_path)
    reloaded.load()
    assert reloaded.should_skip(key) is False
