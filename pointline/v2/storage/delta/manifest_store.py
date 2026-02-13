"""Delta-backed manifest store for v2 ingestion."""

from __future__ import annotations

from pathlib import Path
from time import time_ns

import filelock
import polars as pl

from pointline.io.protocols import BronzeFileMetadata
from pointline.schemas.control import INGEST_MANIFEST
from pointline.schemas.types import (
    INGEST_STATUS_FAILED,
    INGEST_STATUS_PENDING,
    INGEST_STATUS_SUCCESS,
    INGEST_STATUS_VALUES,
)
from pointline.v2.ingestion.models import IngestionResult
from pointline.v2.storage.contracts import ManifestStore
from pointline.v2.storage.delta._utils import (
    normalize_to_spec,
    overwrite_delta,
    read_delta_or_empty,
)
from pointline.v2.storage.models import ManifestIdentity


def _now_us() -> int:
    return time_ns() // 1_000


class _FileIdCounter:
    """Monotonic file_id allocator backed by a local file and lock."""

    def __init__(self, counter_path: Path) -> None:
        self.counter_path = counter_path
        self.lock_path = counter_path.with_suffix(counter_path.suffix + ".lock")
        self.counter_path.parent.mkdir(parents=True, exist_ok=True)

    def next_id(self, *, existing_max: int) -> int:
        lock = filelock.FileLock(self.lock_path, timeout=30)
        with lock:
            current = 0
            if self.counter_path.exists():
                raw = self.counter_path.read_text(encoding="utf-8").strip()
                if raw:
                    current = int(raw)
            base = max(current, existing_max)
            next_id = base + 1
            self.counter_path.write_text(str(next_id), encoding="utf-8")
            return next_id


class DeltaManifestStore(ManifestStore):
    """v2-owned manifest persistence on Delta Lake."""

    def __init__(self, table_path: Path) -> None:
        self.table_path = table_path
        self.spec = INGEST_MANIFEST
        self._lock = filelock.FileLock(
            str(table_path.parent / ".v2_manifest_identity.lock"),
            timeout=30,
        )
        self._counter = _FileIdCounter(table_path.parent / ".v2_manifest_file_id")

    def _read(self) -> pl.DataFrame:
        return read_delta_or_empty(self.table_path, spec=self.spec)

    def _write(self, df: pl.DataFrame) -> None:
        normalized = normalize_to_spec(df, self.spec)
        overwrite_delta(self.table_path, df=normalized, partition_by=self.spec.partition_by)

    def _identity_filter(self, meta: BronzeFileMetadata) -> pl.Expr:
        identity = ManifestIdentity.from_meta(meta)
        return (
            (pl.col("vendor") == identity.vendor)
            & (pl.col("data_type") == identity.data_type)
            & (pl.col("bronze_path") == identity.bronze_path)
            & (pl.col("file_hash") == identity.file_hash)
        )

    def _existing_created_at(self, df: pl.DataFrame, file_id: int) -> int | None:
        if df.is_empty():
            return None
        existing = df.filter(pl.col("file_id") == file_id)
        if existing.is_empty():
            return None
        value = existing.item(0, "created_at_ts_us")
        return int(value) if value is not None else None

    def resolve_file_id(self, meta: BronzeFileMetadata) -> int:
        with self._lock:
            manifest = self._read()
            existing = manifest.filter(self._identity_filter(meta))
            if not existing.is_empty():
                return int(existing.sort("file_id").item(0, "file_id"))

            existing_max = 0
            if not manifest.is_empty():
                max_val = manifest.select(pl.col("file_id").max()).item()
                if max_val is not None:
                    existing_max = int(max_val)

            file_id = self._counter.next_id(existing_max=existing_max)
            now_us = _now_us()
            pending = pl.DataFrame(
                {
                    "file_id": [file_id],
                    "vendor": [meta.vendor],
                    "data_type": [meta.data_type],
                    "bronze_path": [meta.bronze_file_path],
                    "file_hash": [meta.sha256],
                    "status": [INGEST_STATUS_PENDING],
                    "rows_total": [None],
                    "rows_written": [None],
                    "rows_quarantined": [None],
                    "trading_date_min": [None],
                    "trading_date_max": [None],
                    "created_at_ts_us": [now_us],
                    "processed_at_ts_us": [None],
                    "status_reason": [None],
                },
                schema=self.spec.to_polars(),
            )
            updated = (
                pending if manifest.is_empty() else pl.concat([manifest, pending], how="vertical")
            )
            self._write(updated.sort("file_id"))
            return file_id

    def filter_pending(self, candidates: list[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
        if not candidates:
            return []

        manifest = self._read()
        if manifest.is_empty():
            return candidates

        success = manifest.filter(pl.col("status") == INGEST_STATUS_SUCCESS)
        if success.is_empty():
            return candidates

        success_keys = set(
            success.select(["vendor", "data_type", "bronze_path", "file_hash"]).iter_rows()
        )

        pending: list[BronzeFileMetadata] = []
        for candidate in candidates:
            key = (
                candidate.vendor,
                candidate.data_type,
                candidate.bronze_file_path,
                candidate.sha256,
            )
            if key not in success_keys:
                pending.append(candidate)
        return pending

    def update_status(
        self,
        file_id: int,
        status: str,
        meta: BronzeFileMetadata,
        result: IngestionResult | None = None,
    ) -> None:
        if status not in INGEST_STATUS_VALUES:
            raise ValueError(f"Unsupported ingest status {status!r}")

        with self._lock:
            manifest = self._read()
            now_us = _now_us()
            created_at = self._existing_created_at(manifest, file_id) or now_us

            status_reason: str | None = None
            if result is not None:
                status_reason = result.failure_reason
                if status_reason is None and status == INGEST_STATUS_FAILED:
                    status_reason = result.error_message or "unknown_error"
            if status_reason is None and status == INGEST_STATUS_FAILED:
                status_reason = "unknown_error"

            row = pl.DataFrame(
                {
                    "file_id": [file_id],
                    "vendor": [meta.vendor],
                    "data_type": [meta.data_type],
                    "bronze_path": [meta.bronze_file_path],
                    "file_hash": [meta.sha256],
                    "status": [status],
                    "rows_total": [result.row_count if result else None],
                    "rows_written": [result.rows_written if result else None],
                    "rows_quarantined": [result.rows_quarantined if result else None],
                    "trading_date_min": [result.trading_date_min if result else None],
                    "trading_date_max": [result.trading_date_max if result else None],
                    "created_at_ts_us": [created_at],
                    "processed_at_ts_us": [now_us],
                    "status_reason": [status_reason],
                },
                schema=self.spec.to_polars(),
            )

            if manifest.is_empty():
                updated = row
            else:
                without_current = manifest.filter(pl.col("file_id") != file_id)
                updated = (
                    row
                    if without_current.is_empty()
                    else pl.concat([without_current, row], how="vertical")
                )
            self._write(updated.sort("file_id"))
