import logging
import struct
import time
from pathlib import Path

import polars as pl
from deltalake import DeltaTable

from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.protocols import BronzeFileMetadata, IngestionResult

logger = logging.getLogger(__name__)

# Manifest schema (single source of truth)
MANIFEST_SCHEMA: dict[str, pl.DataType] = {
    "file_id": pl.Int32,
    "vendor": pl.Utf8,
    "data_type": pl.Utf8,
    "bronze_file_name": pl.Utf8,
    "sha256": pl.Utf8,
    "file_size_bytes": pl.Int64,
    "last_modified_ts": pl.Int64,
    "date": pl.Date,
    "status": pl.Utf8,
    "created_at_us": pl.Int64,
    "processed_at_us": pl.Int64,
    "row_count": pl.Int64,
    "ts_local_min_us": pl.Int64,
    "ts_local_max_us": pl.Int64,
    "error_message": pl.Utf8,
}


class _FileIdCounter:
    """Cross-platform monotonic counter backed by a binary file.

    Stores a single 4-byte little-endian integer outside the Delta table directory.
    Uses advisory file locking that works on both POSIX and Windows.
    """

    def __init__(self, counter_path: Path):
        self.counter_path = counter_path
        self.counter_path.parent.mkdir(parents=True, exist_ok=True)

    def next_id(self) -> int:
        """Atomically read, increment, and return the next file_id."""
        import filelock

        lock_path = self.counter_path.with_suffix(".lock")
        lock = filelock.FileLock(lock_path, timeout=30)

        with lock:
            if self.counter_path.exists():
                raw = self.counter_path.read_bytes()
                current = struct.unpack("<i", raw)[0] if len(raw) >= 4 else 0
            else:
                current = 0

            next_val = current + 1
            self.counter_path.write_bytes(struct.pack("<i", next_val))
            return next_val

    def sync_from_manifest(self, manifest_df: pl.DataFrame) -> None:
        """Sync counter to be at least max(file_id) from existing manifest."""
        if manifest_df.is_empty():
            return

        max_id = manifest_df.select(pl.col("file_id").max()).item()
        if max_id is None:
            return

        import filelock

        lock_path = self.counter_path.with_suffix(".lock")
        lock = filelock.FileLock(lock_path, timeout=30)

        with lock:
            if self.counter_path.exists():
                raw = self.counter_path.read_bytes()
                current = struct.unpack("<i", raw)[0] if len(raw) >= 4 else 0
            else:
                current = 0

            if max_id > current:
                self.counter_path.write_bytes(struct.pack("<i", max_id))


class DeltaManifestRepository(BaseDeltaRepository):
    """
    Implementation of IngestionManifestRepository using Delta Lake.

    Uses a cross-platform file counter for monotonic file_id assignment
    instead of fcntl-based locking.
    """

    def __init__(self, table_path: str | Path):
        super().__init__(table_path)
        self._ensure_initialized()

        # Counter file lives alongside (not inside) the Delta table directory
        counter_dir = Path(self.table_path).parent
        self._counter = _FileIdCounter(counter_dir / ".file_id_counter")

    def _ensure_initialized(self):
        """Creates the manifest table with correct schema if it doesn't exist."""
        if not Path(self.table_path).exists():
            df = pl.DataFrame(schema=MANIFEST_SCHEMA)
            self.write_full(df)
            return

    def resolve_file_id(self, meta: BronzeFileMetadata) -> int:
        """Gets existing ID or mints a new one based on identity key.

        Identity: (vendor, data_type, bronze_file_name, sha256)

        Persists 'pending' state if minting new ID to ensure stability.

        Returns:
            file_id: Integer file ID for this bronze file
        """
        try:
            DeltaTable(self.table_path)
            df = pl.read_delta(self.table_path)
        except Exception:
            df = pl.DataFrame(schema=MANIFEST_SCHEMA)

        # Check existing by composite key
        existing = df.filter(
            (pl.col("vendor") == meta.vendor)
            & (pl.col("data_type") == meta.data_type)
            & (pl.col("bronze_file_name") == meta.bronze_file_path)
            & (pl.col("sha256") == meta.sha256)
        )

        if not existing.is_empty():
            return existing.item(0, "file_id")

        # Ensure counter is synced with manifest (handles first run or counter reset)
        self._counter.sync_from_manifest(df)

        # Mint new ID using cross-platform counter
        next_id = self._counter.next_id()

        current_ts_us = int(time.time() * 1_000_000)

        pending_record = pl.DataFrame(
            {
                "file_id": [next_id],
                "vendor": [meta.vendor],
                "data_type": [meta.data_type],
                "bronze_file_name": [meta.bronze_file_path],
                "sha256": [meta.sha256],
                "file_size_bytes": [meta.file_size_bytes],
                "last_modified_ts": [meta.last_modified_ts],
                "date": [meta.date],
                "status": ["pending"],
                "created_at_us": [current_ts_us],
                "processed_at_us": [None],
                "row_count": [None],
                "ts_local_min_us": [None],
                "ts_local_max_us": [None],
                "error_message": [None],
            },
            schema=MANIFEST_SCHEMA,
        )

        self.append(pending_record)
        return next_id

    def filter_pending(self, candidates: list[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
        """Returns only files that need processing (efficient batch anti-join).

        Uses strict manifest key when SHA256 is present:
        (vendor, data_type, bronze_file_name, sha256).

        For candidates with missing SHA256 (e.g., fast discovery), falls back to
        (vendor, data_type, bronze_file_name, file_size_bytes, last_modified_ts).
        """
        if not candidates:
            return []

        try:
            manifest_df = pl.read_delta(self.table_path)
        except Exception:
            return candidates

        if manifest_df.is_empty():
            return candidates

        success_manifest = manifest_df.filter(pl.col("status") == "success")
        if success_manifest.is_empty():
            return candidates

        strict_success_keys = set(
            success_manifest.select(
                ["vendor", "data_type", "bronze_file_name", "sha256"]
            ).iter_rows()
        )
        fallback_success_keys = set(
            success_manifest.select(
                [
                    "vendor",
                    "data_type",
                    "bronze_file_name",
                    "file_size_bytes",
                    "last_modified_ts",
                ]
            ).iter_rows()
        )

        pending: list[BronzeFileMetadata] = []
        for candidate in candidates:
            if candidate.sha256:
                strict_key = (
                    candidate.vendor,
                    candidate.data_type,
                    candidate.bronze_file_path,
                    candidate.sha256,
                )
                if strict_key in strict_success_keys:
                    continue
            else:
                fallback_key = (
                    candidate.vendor,
                    candidate.data_type,
                    candidate.bronze_file_path,
                    candidate.file_size_bytes,
                    candidate.last_modified_ts,
                )
                if fallback_key in fallback_success_keys:
                    continue

            pending.append(candidate)

        return pending

    def update_status(
        self,
        file_id: int,
        status: str,
        meta: BronzeFileMetadata,
        result: IngestionResult | None = None,
    ) -> None:
        """Records success/failure with new schema."""
        row_count = result.row_count if result else None
        min_ts = result.ts_local_min_us if result else None
        max_ts = result.ts_local_max_us if result else None
        err_msg = result.error_message if result else None

        if status == "failed" and err_msg is None:
            err_msg = "Unknown error"

        processed_ts_us = int(time.time() * 1_000_000)
        created_at_us = processed_ts_us
        try:
            current = pl.read_delta(self.table_path).filter(pl.col("file_id") == file_id)
            if not current.is_empty():
                existing_created = current.item(0, "created_at_us")
                if existing_created is not None:
                    created_at_us = int(existing_created)
        except Exception:
            pass

        update_df = pl.DataFrame(
            {
                "file_id": [file_id],
                "vendor": [meta.vendor],
                "data_type": [meta.data_type],
                "bronze_file_name": [meta.bronze_file_path],
                "sha256": [meta.sha256],
                "file_size_bytes": [meta.file_size_bytes],
                "last_modified_ts": [meta.last_modified_ts],
                "date": [meta.date],
                "status": [status],
                "created_at_us": [created_at_us],
                "processed_at_us": [processed_ts_us],
                "row_count": [row_count],
                "ts_local_min_us": [min_ts],
                "ts_local_max_us": [max_ts],
                "error_message": [err_msg],
            },
            schema=MANIFEST_SCHEMA,
        )

        self.merge(update_df, keys=["file_id"])


# ---------------------------------------------------------------------------
# Schema registry registration
# ---------------------------------------------------------------------------
from pointline.schema_registry import register_schema as _register_schema  # noqa: E402

_register_schema("ingest_manifest", MANIFEST_SCHEMA, has_date=True)
