"""JSON ledger for Quant360 upstream idempotency and recovery."""

from __future__ import annotations

import json
from pathlib import Path
from time import time_ns

from pointline.vendors.quant360.upstream.contracts import (
    LEDGER_SCHEMA_VERSION,
    LEDGER_STATUS_FAILED,
    LEDGER_STATUS_SUCCESS,
)
from pointline.vendors.quant360.upstream.models import Quant360ArchiveKey, Quant360LedgerRecord


def _now_us() -> int:
    return time_ns() // 1_000


def _record_to_dict(record: Quant360LedgerRecord) -> dict[str, object]:
    return {
        "source_filename": record.archive_key.source_filename,
        "archive_sha256": record.archive_key.archive_sha256,
        "status": record.status,
        "updated_at_us": record.updated_at_us,
        "failure_reason": record.failure_reason,
        "error_message": record.error_message,
        "member_count": record.member_count,
        "published_count": record.published_count,
    }


def _record_from_dict(data: dict[str, object]) -> Quant360LedgerRecord:
    return Quant360LedgerRecord(
        archive_key=Quant360ArchiveKey(
            source_filename=str(data["source_filename"]),
            archive_sha256=str(data["archive_sha256"]),
        ),
        status=str(data["status"]),
        updated_at_us=int(data["updated_at_us"]),
        failure_reason=(
            str(data["failure_reason"]) if data.get("failure_reason") is not None else None
        ),
        error_message=str(data["error_message"]) if data.get("error_message") is not None else None,
        member_count=int(data["member_count"]) if data.get("member_count") is not None else None,
        published_count=(
            int(data["published_count"]) if data.get("published_count") is not None else None
        ),
    )


class Quant360UpstreamLedger:
    """Local JSON state store keyed by archive identity."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._records: dict[str, Quant360LedgerRecord] = {}

    def load(self) -> None:
        if not self.path.exists():
            self._records = {}
            return

        payload = json.loads(self.path.read_text(encoding="utf-8"))
        raw_records = payload.get("records", {})
        if not isinstance(raw_records, dict):
            raise ValueError("Invalid Quant360 upstream ledger format: records must be an object")

        parsed: dict[str, Quant360LedgerRecord] = {}
        for key, value in raw_records.items():
            if not isinstance(value, dict):
                raise ValueError(f"Invalid ledger record for key {key!r}")
            parsed[key] = _record_from_dict(value)
        self._records = parsed

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        payload = {
            "version": LEDGER_SCHEMA_VERSION,
            "updated_at_us": _now_us(),
            "records": {
                key: _record_to_dict(record) for key, record in sorted(self._records.items())
            },
        }
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp_path.replace(self.path)

    def should_skip(self, key: Quant360ArchiveKey) -> bool:
        record = self._records.get(key.as_string())
        return record is not None and record.status == LEDGER_STATUS_SUCCESS

    def mark_success(self, record: Quant360LedgerRecord) -> None:
        if record.status != LEDGER_STATUS_SUCCESS:
            raise ValueError(f"mark_success requires status={LEDGER_STATUS_SUCCESS!r}")
        self._records[record.archive_key.as_string()] = record

    def mark_failure(self, record: Quant360LedgerRecord) -> None:
        if record.status != LEDGER_STATUS_FAILED:
            raise ValueError(f"mark_failure requires status={LEDGER_STATUS_FAILED!r}")
        self._records[record.archive_key.as_string()] = record
