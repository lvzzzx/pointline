"""Trades ingestion service orchestrating the Bronze → Silver pipeline."""

from __future__ import annotations

import gzip
import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from pointline.config import EXCHANGE_MAP, LAKE_ROOT
from pointline.dim_symbol import check_coverage
from pointline.io.protocols import (
    BronzeFileMetadata,
    IngestionManifestRepository,
    IngestionResult,
    TableRepository,
)
from pointline.services.base_service import BaseService
from pointline.trades import (
    encode_fixed_point,
    normalize_trades_schema,
    parse_tardis_trades_csv,
    resolve_symbol_ids,
    validate_trades,
)

logger = logging.getLogger(__name__)


class TradesIngestionService(BaseService):
    """
    Orchestrates trades ingestion from Bronze CSV files to Silver Delta tables.
    
    Handles:
    - CSV parsing and validation
    - Quarantine checks (symbol metadata coverage)
    - Symbol ID resolution
    - Fixed-point encoding
    - Delta Lake append operations
    """

    def __init__(
        self,
        repo: TableRepository,
        dim_symbol_repo: TableRepository,
        manifest_repo: IngestionManifestRepository,
    ):
        """
        Initialize the trades ingestion service.
        
        Args:
            repo: Repository for the trades Silver table
            dim_symbol_repo: Repository for dim_symbol table
            manifest_repo: Repository for ingestion manifest
        """
        self.repo = repo
        self.dim_symbol_repo = dim_symbol_repo
        self.manifest_repo = manifest_repo

    def validate(self, data: pl.DataFrame) -> pl.DataFrame:
        """Validate trades data schema and quality."""
        return validate_trades(data)

    def compute_state(self, valid_data: pl.DataFrame) -> pl.DataFrame:
        """
        Transform validated trades data to final Silver format.
        
        Note: This is called by the BaseService.update() template method.
        For ingestion from files, use ingest_file() instead.
        """
        # For direct updates (not from file), assume data is already transformed
        return normalize_trades_schema(valid_data)

    def write(self, result: pl.DataFrame) -> None:
        """Append trades data to the Delta table."""
        if result.is_empty():
            logger.warning("write: skipping empty DataFrame")
            return
        
        # Use append for immutable event data
        if hasattr(self.repo, "append"):
            self.repo.append(result)
        else:
            # Fallback to merge if append not available
            # For trades, we could use a composite key, but append is preferred
            raise NotImplementedError("Repository must support append() for trades")

    def ingest_file(
        self,
        meta: BronzeFileMetadata,
        file_id: int,
        *,
        bronze_root: Path | None = None,
    ) -> IngestionResult:
        """
        Ingest a single trades CSV file from Bronze to Silver.
        
        Args:
            meta: Metadata about the bronze file
            file_id: File ID from manifest repository
            bronze_root: Root path for bronze files (default: LAKE_ROOT/tardis)
            
        Returns:
            IngestionResult with row count and timestamp ranges
        """
        if bronze_root is None:
            bronze_root = LAKE_ROOT / "tardis"
        bronze_path = bronze_root / meta.bronze_file_path
        
        if not bronze_path.exists():
            error_msg = f"Bronze file not found: {bronze_path}"
            logger.error(error_msg)
            return IngestionResult(
                row_count=0,
                ts_local_min_us=0,
                ts_local_max_us=0,
                error_message=error_msg,
            )
        
        try:
            # 1. Read bronze CSV file
            raw_df = self._read_bronze_csv(bronze_path)
            
            if raw_df.is_empty():
                logger.warning(f"Empty CSV file: {bronze_path}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=None,
                )
            
            # 2. Parse CSV
            parsed_df = parse_tardis_trades_csv(raw_df)
            
            # 3. Load dim_symbol for quarantine check
            dim_symbol = self.dim_symbol_repo.read_all()
            
            # 4. Quarantine check
            exchange_id = self._resolve_exchange_id(meta.exchange)
            is_valid, error_msg = self._check_quarantine(
                meta, dim_symbol, exchange_id, parsed_df
            )
            
            if not is_valid:
                logger.warning(f"File quarantined: {meta.bronze_file_path} - {error_msg}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=error_msg,
                )
            
            # 5. Resolve symbol IDs
            resolved_df = resolve_symbol_ids(
                parsed_df,
                dim_symbol,
                exchange_id,
                meta.symbol,
                ts_col="ts_local_us",
            )
            
            # 6. Encode fixed-point
            encoded_df = encode_fixed_point(resolved_df, dim_symbol)
            
            # 7. Add lineage columns
            lineage_df = self._add_lineage(encoded_df, file_id)
            
            # 8. Add exchange_id and date
            final_df = self._add_metadata(lineage_df, exchange_id)
            
            # 9. Normalize schema
            normalized_df = normalize_trades_schema(final_df)
            
            # 10. Validate
            validated_df = self.validate(normalized_df)
            
            if validated_df.is_empty():
                logger.warning(f"No valid rows after validation: {bronze_path}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message="All rows filtered by validation",
                )
            
            # 11. Append to Delta table
            self.write(validated_df)
            
            # 12. Compute result stats
            ts_min = validated_df["ts_local_us"].min()
            ts_max = validated_df["ts_local_us"].max()
            
            logger.info(
                f"Ingested {validated_df.height} trades from {meta.bronze_file_path} "
                f"(ts range: {ts_min} - {ts_max})"
            )
            
            return IngestionResult(
                row_count=validated_df.height,
                ts_local_min_us=ts_min,
                ts_local_max_us=ts_max,
                error_message=None,
            )
            
        except Exception as e:
            error_msg = f"Error ingesting {bronze_path}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return IngestionResult(
                row_count=0,
                ts_local_min_us=0,
                ts_local_max_us=0,
                error_message=error_msg,
            )

    def _read_bronze_csv(self, path: Path) -> pl.DataFrame:
        """Read a bronze CSV file, handling gzip compression."""
        if path.suffix == ".gz" or str(path).endswith(".csv.gz"):
            with gzip.open(path, "rt", encoding="utf-8") as f:
                return pl.read_csv(f)
        else:
            return pl.read_csv(path)

    def _resolve_exchange_id(self, exchange: str) -> int:
        """Resolve exchange name to exchange_id."""
        if exchange not in EXCHANGE_MAP:
            raise ValueError(
                f"Exchange '{exchange}' not in EXCHANGE_MAP; update pointline.config"
            )
        return EXCHANGE_MAP[exchange]

    def _check_quarantine(
        self,
        meta: BronzeFileMetadata,
        dim_symbol: pl.DataFrame,
        exchange_id: int,
        parsed_df: pl.DataFrame,
    ) -> tuple[bool, str]:
        """
        Check if file should be quarantined based on symbol metadata coverage.
        
        Returns:
            (is_valid, error_message) tuple
        """
        # Calculate UTC day boundaries
        from datetime import timedelta
        
        file_date = meta.date
        day_start = datetime.combine(file_date, datetime.min.time(), tzinfo=timezone.utc)
        day_start_us = int(day_start.timestamp() * 1_000_000)
        
        # Next day start (exclusive end)
        day_end = datetime.combine(
            file_date + timedelta(days=1),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        day_end_us = int(day_end.timestamp() * 1_000_000)
        
        # Check coverage
        has_coverage = check_coverage(
            dim_symbol,
            exchange_id,
            meta.symbol,
            day_start_us,
            day_end_us,
        )
        
        if not has_coverage:
            # Determine specific reason
            rows = dim_symbol.filter(
                (pl.col("exchange_id") == exchange_id)
                & (pl.col("exchange_symbol") == meta.symbol)
            )
            
            if rows.is_empty():
                return False, "missing_symbol"
            else:
                return False, "invalid_validity_window"
        
        return True, ""

    def _add_lineage(self, df: pl.DataFrame, file_id: int) -> pl.DataFrame:
        """Add lineage tracking columns: file_id, file_line_number, and ingest_seq.
        
        ingest_seq is derived from file_line_number for deterministic ordering
        within the source file.
        """
        file_line_number = pl.int_range(1, df.height + 1, dtype=pl.UInt32)
        return df.with_columns([
            pl.lit(file_id, dtype=pl.UInt32).alias("file_id"),
            file_line_number.alias("file_line_number"),
            file_line_number.alias("ingest_seq"),  # Use line number as ingest sequence
        ])

    def _add_metadata(self, df: pl.DataFrame, exchange_id: int) -> pl.DataFrame:
        """Add exchange_id and date columns."""
        # Use Int16 to match dim_symbol schema (dim_symbol uses Int16, not UInt16)
        result = df.with_columns([
            pl.lit(exchange_id, dtype=pl.Int16).alias("exchange_id"),
        ])
        
        # Derive date from ts_local_us (microseconds since epoch)
        result = result.with_columns([
            pl.from_epoch(pl.col("ts_local_us"), time_unit="us")
            .cast(pl.Date)
            .alias("date"),
        ])
        
        return result
