import fcntl
from pathlib import Path

import polars as pl
from deltalake import DeltaTable

from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.protocols import BronzeFileMetadata, IngestionResult


class DeltaManifestRepository(BaseDeltaRepository):
    """
    Implementation of IngestionManifestRepository using Delta Lake.
    """

    def __init__(self, table_path: str | Path):
        super().__init__(table_path)
        self._ensure_initialized()

    def _ensure_initialized(self):
        """Creates the manifest table with correct schema if it doesn't exist."""
        if not Path(self.table_path).exists():
            # Define schema according to flexible bronze layer architecture
            # Delta Lake doesn't support UInt32, stores as Int32
            # Schema definition is single source of truth
            #
            # Primary identity: (vendor, data_type, bronze_file_name, sha256)
            # Optional fields: date (nullable for non-symbol data)
            schema = {
                "file_id": pl.Int32,
                "vendor": pl.Utf8,
                "data_type": pl.Utf8,
                "bronze_file_name": pl.Utf8,  # Renamed from bronze_file_path
                "sha256": pl.Utf8,
                "file_size_bytes": pl.Int64,
                "last_modified_ts": pl.Int64,
                "date": pl.Date,  # NOW NULLABLE
                "status": pl.Utf8,
                "created_at_us": pl.Int64,  # When discovered
                "processed_at_us": pl.Int64,  # When ingested (nullable)
                "row_count": pl.Int64,
                "ts_local_min_us": pl.Int64,
                "ts_local_max_us": pl.Int64,
                "error_message": pl.Utf8,
            }
            df = pl.DataFrame(schema=schema)
            self.write_full(df)
            return

        # No migration logic - clean break with new schema
        # Users should run scripts/reset_manifest.py before deploying new code

    def resolve_file_id(self, meta: BronzeFileMetadata) -> int:
        """Gets existing ID or mints a new one based on new identity key.

        Identity: (vendor, data_type, bronze_file_name, sha256)

        Persists 'pending' state if minting new ID to ensure stability.
        Uses file locking to prevent race conditions when multiple processes
        mint IDs concurrently.

        Returns:
            file_id: Integer file ID for this bronze file
        """
        try:
            DeltaTable(self.table_path)
            # Use SQL-like filter for efficiency if supported by engine,
            # but for robust Polars interop:
            df = pl.read_delta(self.table_path)
        except Exception:
            # Should exist due to _ensure_initialized, but safety net
            df = pl.DataFrame()

        # 1. Check existing (outside lock - read-only operation)
        # Filter by new composite key: (vendor, data_type, bronze_file_name, sha256)
        existing = df.filter(
            (pl.col("vendor") == meta.vendor)
            & (pl.col("data_type") == meta.data_type)
            & (pl.col("bronze_file_name") == meta.bronze_file_path)
            & (pl.col("sha256") == meta.sha256)
        )

        if not existing.is_empty():
            return existing.item(0, "file_id")

        # 2. Mint New ID (with file lock to prevent concurrent minting)
        lock_file = Path(self.table_path) / ".file_id_lock"
        lock_file.parent.mkdir(parents=True, exist_ok=True)

        with open(lock_file, "w") as lockf:
            # Acquire exclusive lock (blocks until available)
            fcntl.flock(lockf.fileno(), fcntl.LOCK_EX)

            try:
                # Re-read inside lock (another process may have added it)
                df = pl.read_delta(self.table_path)
                existing = df.filter(
                    (pl.col("vendor") == meta.vendor)
                    & (pl.col("data_type") == meta.data_type)
                    & (pl.col("bronze_file_name") == meta.bronze_file_path)
                    & (pl.col("sha256") == meta.sha256)
                )

                if not existing.is_empty():
                    return existing.item(0, "file_id")

                # Compute next ID
                if df.is_empty():
                    next_id = 1
                else:
                    # Handle nulls if any
                    max_id = df.select(pl.col("file_id").max()).item()
                    next_id = (max_id if max_id is not None else 0) + 1

                # 3. Reserve (Write Pending)
                # We write a minimal record to reserve the ID
                # Schema definition is single source of truth - use explicit schema with Int32
                import time

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
                        "date": [meta.date],  # Can be NULL
                        "status": ["pending"],
                        "created_at_us": [current_ts_us],
                        "processed_at_us": [None],
                        "row_count": [None],
                        "ts_local_min_us": [None],
                        "ts_local_max_us": [None],
                        "error_message": [None],
                    },
                    schema={
                        "file_id": pl.Int32,
                        "vendor": pl.Utf8,
                        "data_type": pl.Utf8,
                        "bronze_file_name": pl.Utf8,
                        "sha256": pl.Utf8,
                        "file_size_bytes": pl.Int64,
                        "last_modified_ts": pl.Int64,
                        "date": pl.Date,  # Nullable
                        "status": pl.Utf8,
                        "created_at_us": pl.Int64,
                        "processed_at_us": pl.Int64,  # Nullable
                        "row_count": pl.Int64,  # Nullable
                        "ts_local_min_us": pl.Int64,  # Nullable
                        "ts_local_max_us": pl.Int64,  # Nullable
                        "error_message": pl.Utf8,  # Nullable
                    },
                )

                self.append(pending_record)
                return next_id

            finally:
                # Release lock (happens automatically when file closes, but explicit for clarity)
                fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)

    def filter_pending(self, candidates: list[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
        """Returns only files that need processing (efficient batch anti-join).

        Uses new manifest key: (vendor, data_type, bronze_file_name, sha256)
        """
        if not candidates:
            return []

        try:
            manifest_df = pl.read_delta(self.table_path)
        except Exception:
            return candidates

        if manifest_df.is_empty():
            return candidates

        # Convert candidates to DataFrame for anti-join with new key
        cand_df = pl.DataFrame(
            [
                {
                    "vendor": c.vendor,
                    "data_type": c.data_type,
                    "bronze_file_name": c.bronze_file_path,
                    "sha256": c.sha256,
                    "_idx": idx,  # Preserve order for mapping back
                }
                for idx, c in enumerate(candidates)
            ]
        )

        # Filter criteria: Status must be 'success'
        # We want to keep candidates that do NOT have a matching 'success' record
        success_manifest = manifest_df.filter(pl.col("status") == "success")

        # Join keys (new composite key)
        join_keys = ["vendor", "data_type", "bronze_file_name", "sha256"]

        # Anti-join: Keep candidates that don't match strict success criteria
        pending_df = cand_df.join(success_manifest, on=join_keys, how="anti")

        # Convert back to objects using preserved indices
        pending_indices = set(pending_df["_idx"].to_list())

        return [c for idx, c in enumerate(candidates) if idx in pending_indices]

    def update_status(
        self,
        file_id: int,
        status: str,
        meta: BronzeFileMetadata,
        result: IngestionResult | None = None,
    ) -> None:
        """Records success/failure with new schema."""
        # Create the updated row
        row_count = result.row_count if result else None
        min_ts = result.ts_local_min_us if result else None
        max_ts = result.ts_local_max_us if result else None
        err_msg = result.error_message if result else None

        # If error passed directly or via result
        if status == "failed" and err_msg is None:
            err_msg = "Unknown error"

        import time

        processed_ts_us = int(time.time() * 1_000_000)

        update_df = pl.DataFrame(
            {
                "file_id": [file_id],
                "vendor": [meta.vendor],
                "data_type": [meta.data_type],
                "bronze_file_name": [meta.bronze_file_path],
                "sha256": [meta.sha256],
                "file_size_bytes": [meta.file_size_bytes],
                "last_modified_ts": [meta.last_modified_ts],
                "date": [meta.date],  # Can be NULL
                "status": [status],
                "created_at_us": [None],  # Preserve existing created_at_us (will be merged)
                "processed_at_us": [processed_ts_us],
                "row_count": [row_count],
                "ts_local_min_us": [min_ts],
                "ts_local_max_us": [max_ts],
                "error_message": [err_msg],
            },
            schema={
                "file_id": pl.Int32,
                "vendor": pl.Utf8,
                "data_type": pl.Utf8,
                "bronze_file_name": pl.Utf8,
                "sha256": pl.Utf8,
                "file_size_bytes": pl.Int64,
                "last_modified_ts": pl.Int64,
                "date": pl.Date,  # Nullable
                "status": pl.Utf8,
                "created_at_us": pl.Int64,  # Nullable (preserve existing)
                "processed_at_us": pl.Int64,  # Nullable
                "row_count": pl.Int64,  # Nullable
                "ts_local_min_us": pl.Int64,  # Nullable
                "ts_local_max_us": pl.Int64,  # Nullable
                "error_message": pl.Utf8,  # Nullable
            },
        )

        # Schema definition is single source of truth - no dynamic schema reading
        # We use merge to update the existing 'pending' (or previous failed) record
        self.merge(update_df, keys=["file_id"])
