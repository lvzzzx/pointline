from typing import List, Optional
import polars as pl
from pathlib import Path
from deltalake import DeltaTable, WriterProperties

from pointline.io.protocols import IngestionManifestRepository, BronzeFileMetadata, IngestionResult
from pointline.io.base_repository import BaseDeltaRepository
from pointline.config import STORAGE_OPTIONS

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
            # Define schema according to design.md
            # Delta Lake doesn't support UInt32, stores as Int32
            # Schema definition is single source of truth
            schema = {
                "file_id": pl.Int32,
                "vendor": pl.Utf8,
                "exchange": pl.Utf8,
                "data_type": pl.Utf8,
                "symbol": pl.Utf8,
                "date": pl.Date,
                "bronze_file_name": pl.Utf8,
                "file_size_bytes": pl.Int64,
                "last_modified_ts": pl.Int64,
                "sha256": pl.Utf8,
                "row_count": pl.Int64,
                "ts_local_min_us": pl.Int64,
                "ts_local_max_us": pl.Int64,
                "ts_exch_min_us": pl.Int64,
                "ts_exch_max_us": pl.Int64,
                "ingested_at": pl.Int64,
                "status": pl.Utf8,
                "error_message": pl.Utf8,
            }
            df = pl.DataFrame(schema=schema)
            self.write_full(df)
            return

        try:
            df = pl.read_delta(self.table_path)
        except Exception:
            return

        if "vendor" not in df.columns:
            df = df.with_columns(pl.lit("tardis").alias("vendor"))
            ordered_cols = [
                "file_id",
                "vendor",
                "exchange",
                "data_type",
                "symbol",
                "date",
                "bronze_file_name",
                "file_size_bytes",
                "last_modified_ts",
                "sha256",
                "row_count",
                "ts_local_min_us",
                "ts_local_max_us",
                "ts_exch_min_us",
                "ts_exch_max_us",
                "ingested_at",
                "status",
                "error_message",
            ]
            existing_cols = [col for col in ordered_cols if col in df.columns]
            df = df.select(existing_cols)
            self.write_full(df)

    def resolve_file_id(self, meta: BronzeFileMetadata) -> int:
        """
        Gets existing ID or mints a new one.
        Persists 'pending' state if minting new ID to ensure stability.
        """
        try:
            dt = DeltaTable(self.table_path)
            # Use SQL-like filter for efficiency if supported by engine, 
            # but for robust Polars interop:
            df = pl.read_delta(self.table_path)
        except Exception:
            # Should exist due to _ensure_initialized, but safety net
            df = pl.DataFrame()

        # 1. Check existing
        # Filter by composite unique key
        existing = df.filter(
            (pl.col("vendor") == meta.vendor) &
            (pl.col("exchange") == meta.exchange) &
            (pl.col("data_type") == meta.data_type) &
            (pl.col("symbol") == meta.symbol) &
            (pl.col("date") == meta.date) &
            (pl.col("bronze_file_name") == meta.bronze_file_path) &
            (pl.col("sha256") == meta.sha256)
        )

        if not existing.is_empty():
            return existing.item(0, "file_id")

        # 2. Mint New ID
        if df.is_empty():
            next_id = 1
        else:
            # Handle nulls if any
            max_id = df.select(pl.col("file_id").max()).item()
            next_id = (max_id if max_id is not None else 0) + 1

        # 3. Reserve (Write Pending)
        # We write a minimal record to reserve the ID
        # Schema definition is single source of truth - use explicit schema with Int32
        pending_record = pl.DataFrame({
            "file_id": [next_id],
            "vendor": [meta.vendor],
            "exchange": [meta.exchange],
            "data_type": [meta.data_type],
            "symbol": [meta.symbol],
            "date": [meta.date],
            "bronze_file_name": [meta.bronze_file_path],
            "file_size_bytes": [meta.file_size_bytes],
            "last_modified_ts": [meta.last_modified_ts],
            "sha256": [meta.sha256],
            "row_count": [0],
            "ts_local_min_us": [0],
            "ts_local_max_us": [0],
            "ts_exch_min_us": [0],
            "ts_exch_max_us": [0],
            "ingested_at": [0],
            "status": ["pending"],
            "error_message": [None],
        }, schema={
            "file_id": pl.Int32,  # Delta Lake doesn't support UInt32, stores as Int32
            "vendor": pl.Utf8,
            "exchange": pl.Utf8,
            "data_type": pl.Utf8,
            "symbol": pl.Utf8,
            "date": pl.Date,
            "bronze_file_name": pl.Utf8,
            "file_size_bytes": pl.Int64,
            "last_modified_ts": pl.Int64,
            "sha256": pl.Utf8,  # Nullable string
            "row_count": pl.Int64,
            "ts_local_min_us": pl.Int64,
            "ts_local_max_us": pl.Int64,
            "ts_exch_min_us": pl.Int64,
            "ts_exch_max_us": pl.Int64,
            "ingested_at": pl.Int64,
            "status": pl.Utf8,
            "error_message": pl.Utf8,  # Nullable string
            })

        self.append(pending_record)
        return next_id

    def filter_pending(self, candidates: List[BronzeFileMetadata]) -> List[BronzeFileMetadata]:
        if not candidates:
            return []

        try:
            manifest_df = pl.read_delta(self.table_path)
        except Exception:
            return candidates

        if manifest_df.is_empty():
            return candidates

        # Convert candidates to DataFrame for anti-join
        cand_df = pl.DataFrame([
            {
                "vendor": c.vendor,
                "exchange": c.exchange,
                "data_type": c.data_type,
                "symbol": c.symbol,
                "date": c.date,
                "bronze_file_name": c.bronze_file_path,
                "file_size_bytes": c.file_size_bytes,
                "last_modified_ts": c.last_modified_ts,
                "sha256": c.sha256,
            } for c in candidates
        ])

        # Filter criteria:
        # Status must be 'success'
        # sha256 matches
        
        # We want to keep candidates that do NOT have a matching 'success' record
        
        success_manifest = manifest_df.filter(pl.col("status") == "success")
        
        # Join keys
        join_keys = [
            "vendor",
            "exchange",
            "data_type",
            "symbol",
            "date",
            "bronze_file_name",
            "sha256",
        ]
        
        # Anti-join: Keep candidates that don't match strict success criteria
        pending_df = cand_df.join(success_manifest, on=join_keys, how="anti")
        
        # Convert back to objects
        pending_keys = set(
            pending_df.select(["vendor", "bronze_file_name", "sha256"]).iter_rows()
        )

        return [
            c
            for c in candidates
            if (c.vendor, c.bronze_file_path, c.sha256) in pending_keys
        ]

    def update_status(self, 
                      file_id: int, 
                      status: str, 
                      meta: BronzeFileMetadata, 
                      result: Optional[IngestionResult] = None) -> None:
        
        # Create the updated row
        row_count = result.row_count if result else 0
        min_ts = result.ts_local_min_us if result else 0
        max_ts = result.ts_local_max_us if result else 0
        err_msg = result.error_message if result else None
        
        # If error passed directly or via result
        if status == "failed" and err_msg is None:
             err_msg = "Unknown error"

        import time
        current_ts = int(time.time() * 1_000_000)

        update_df = pl.DataFrame({
            "file_id": [file_id],
            "vendor": [meta.vendor],
            "exchange": [meta.exchange],
            "data_type": [meta.data_type],
            "symbol": [meta.symbol],
            "date": [meta.date],
            "bronze_file_name": [meta.bronze_file_path],
            "file_size_bytes": [meta.file_size_bytes],
            "last_modified_ts": [meta.last_modified_ts],
            "sha256": [meta.sha256],
            "row_count": [row_count],
            "ts_local_min_us": [min_ts],
            "ts_local_max_us": [max_ts],
            "ts_exch_min_us": [0],
            "ts_exch_max_us": [0],
            "ingested_at": [current_ts],
            "status": [status],
            "error_message": [err_msg],
        }, schema={
            "file_id": pl.Int32,  # Delta Lake doesn't support UInt32, stores as Int32
            "vendor": pl.Utf8,
            "exchange": pl.Utf8,
            "data_type": pl.Utf8,
            "symbol": pl.Utf8,
            "date": pl.Date,
            "bronze_file_name": pl.Utf8,
            "file_size_bytes": pl.Int64,
            "last_modified_ts": pl.Int64,
            "sha256": pl.Utf8,
            "row_count": pl.Int64,
            "ts_local_min_us": pl.Int64,
            "ts_local_max_us": pl.Int64,
            "ts_exch_min_us": pl.Int64,
            "ts_exch_max_us": pl.Int64,
            "ingested_at": pl.Int64,
            "status": pl.Utf8,
            "error_message": pl.Utf8,
        })
        
        # Schema definition is single source of truth - no dynamic schema reading
        # We use merge to update the existing 'pending' (or previous failed) record
        self.merge(update_df, keys=["file_id"])
