"""JSON ledger for Quant360 upstream idempotency and recovery."""

from __future__ import annotations

import json
from pathlib import Path
from time import time_ns

from pointline.vendors.quant360.upstream.models import ArchiveKey, ArchiveState

LEDGER_VERSION = 1
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"


def _now_us() -> int:
    return time_ns() // 1_000


class Ledger:
    """Local JSON state store keyed by archive identity."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._states: dict[str, ArchiveState] = {}

    def load(self) -> None:
        if not self.path.exists():
            self._states = {}
            return

        data = json.loads(self.path.read_text(encoding="utf-8"))
        self._states = {
            k: ArchiveState(
                archive_key=ArchiveKey(
                    source_filename=str(v["source_filename"]),
                    archive_sha256=str(v["archive_sha256"]),
                ),
                status=str(v["status"]),
                member_count=int(v["member_count"]),
                published_count=int(v["published_count"]),
                updated_at_us=int(v["updated_at_us"]),
                failure_reason=v.get("failure_reason"),
                error_message=v.get("error_message"),
            )
            for k, v in data.get("records", {}).items()
        }

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        payload = {
            "version": LEDGER_VERSION,
            "updated_at_us": _now_us(),
            "records": {
                k: {
                    "source_filename": s.archive_key.source_filename,
                    "archive_sha256": s.archive_key.archive_sha256,
                    "status": s.status,
                    "member_count": s.member_count,
                    "published_count": s.published_count,
                    "updated_at_us": s.updated_at_us,
                    "failure_reason": s.failure_reason,
                    "error_message": s.error_message,
                }
                for k, s in sorted(self._states.items())
            },
        }
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def is_success(self, key: ArchiveKey) -> bool:
        """Check if archive was successfully processed."""
        state = self._states.get(str(key))
        return state is not None and state.status == STATUS_SUCCESS

    def is_failed(self, key: ArchiveKey) -> bool:
        """Check if archive previously failed."""
        state = self._states.get(str(key))
        return state is not None and state.status == STATUS_FAILED

    def get_state(self, key: ArchiveKey) -> ArchiveState | None:
        """Get previous state for an archive."""
        return self._states.get(str(key))

    def set_state(self, state: ArchiveState) -> None:
        """Store state for an archive."""
        self._states[str(state.archive_key)] = state
