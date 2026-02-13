"""JSON ledger for Quant360 upstream idempotency and recovery."""

from __future__ import annotations

import json
from pathlib import Path
from time import time_ns

from pointline.v2.vendors.quant360.upstream.contracts import (
    LEDGER_SCHEMA_VERSION,
    LEDGER_STATUS_FAILED,
    LEDGER_STATUS_SUCCESS,
)
from pointline.v2.vendors.quant360.upstream.models import Quant360LedgerRecord, Quant360MemberKey


def _now_us() -> int:
    return time_ns() // 1_000


def _record_to_dict(record: Quant360LedgerRecord) -> dict[str, object]:
    return {
        "archive_sha256": record.member_key.archive_sha256,
        "member_path": record.member_key.member_path,
        "status": record.status,
        "updated_at_us": record.updated_at_us,
        "failure_reason": record.failure_reason,
        "error_message": record.error_message,
        "bronze_rel_path": record.bronze_rel_path,
        "output_sha256": record.output_sha256,
    }


def _record_from_dict(data: dict[str, object]) -> Quant360LedgerRecord:
    return Quant360LedgerRecord(
        member_key=Quant360MemberKey(
            archive_sha256=str(data["archive_sha256"]),
            member_path=str(data["member_path"]),
        ),
        status=str(data["status"]),
        updated_at_us=int(data["updated_at_us"]),
        failure_reason=(
            str(data["failure_reason"]) if data.get("failure_reason") is not None else None
        ),
        error_message=str(data["error_message"]) if data.get("error_message") is not None else None,
        bronze_rel_path=str(data["bronze_rel_path"])
        if data.get("bronze_rel_path") is not None
        else None,
        output_sha256=str(data["output_sha256"]) if data.get("output_sha256") is not None else None,
    )


class Quant360UpstreamLedger:
    """Local JSON state store keyed by archive content hash + member path."""

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

    def should_skip(self, key: Quant360MemberKey) -> bool:
        record = self._records.get(key.as_string())
        return record is not None and record.status == LEDGER_STATUS_SUCCESS

    def mark_success(self, record: Quant360LedgerRecord) -> None:
        if record.status != LEDGER_STATUS_SUCCESS:
            raise ValueError(f"mark_success requires status={LEDGER_STATUS_SUCCESS!r}")
        self._records[record.member_key.as_string()] = record

    def mark_failure(self, record: Quant360LedgerRecord) -> None:
        if record.status != LEDGER_STATUS_FAILED:
            raise ValueError(f"mark_failure requires status={LEDGER_STATUS_FAILED!r}")
        self._records[record.member_key.as_string()] = record
