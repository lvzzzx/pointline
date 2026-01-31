"""SZSE L3 orders ingestion service orchestrating the Bronze → Silver pipeline."""

from __future__ import annotations

import gzip
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from pointline.config import get_bronze_root, get_exchange_id, get_exchange_timezone, normalize_exchange
from pointline.dim_symbol import check_coverage
from pointline.io.protocols import (
    BronzeFileMetadata,
    IngestionManifestRepository,
    IngestionResult,
    TableRepository,
)
from pointline.services.base_service import BaseService
from pointline.tables.szse_l3_orders import (
    encode_fixed_point,
    normalize_szse_l3_orders_schema,
    parse_quant360_orders_csv,
    resolve_symbol_ids,
    validate_szse_l3_orders,
)

logger = logging.getLogger(__name__)


class SzseL3OrdersIngestionService(BaseService):
    """
    Orchestrates SZSE L3 order ingestion from Bronze CSV files to Silver Delta tables.

    Handles:
    - CSV parsing and validation (Quant360 format)
    - Quarantine checks (symbol metadata coverage)
    - Symbol ID resolution
    - Fixed-point encoding (lot-based for Chinese stocks)
    - Delta Lake append operations
    """

    def __init__(
        self,
        repo: TableRepository,
        dim_symbol_repo: TableRepository,
        manifest_repo: IngestionManifestRepository,
    ):
        """
        Initialize the SZSE L3 orders ingestion service.

        Args:
            repo: Repository for the szse_l3_orders Silver table
            dim_symbol_repo: Repository for dim_symbol table
            manifest_repo: Repository for ingestion manifest
        """
        self.repo = repo
        self.dim_symbol_repo = dim_symbol_repo
        self.manifest_repo = manifest_repo

    def validate(self, data: pl.DataFrame) -> pl.DataFrame:
        """Validate SZSE L3 orders data schema and quality."""
        return validate_szse_l3_orders(data)

    def compute_state(self, valid_data: pl.DataFrame) -> pl.DataFrame:
        """
        Transform validated orders data to final Silver format.

        Note: This is called by the BaseService.update() template method.
        For ingestion from files, use ingest_file() instead.
        """
        # For direct updates (not from file), assume data is already transformed
        return normalize_szse_l3_orders_schema(valid_data)

    def write(self, result: pl.DataFrame) -> None:
        """Append orders data to the Delta table."""
        if result.is_empty():
            logger.warning("write: skipping empty DataFrame")
            return

        # Use append for immutable event data
        if hasattr(self.repo, "append"):
            self.repo.append(result)
        else:
            raise NotImplementedError("Repository must support append() for szse_l3_orders")

    def ingest_file(
        self,
        meta: BronzeFileMetadata,
        file_id: int,
        *,
        bronze_root: Path | None = None,
    ) -> IngestionResult:
        """
        Ingest a single SZSE L3 orders CSV file from Bronze to Silver.

        Args:
            meta: Metadata about the bronze file
            file_id: File ID from manifest repository
            bronze_root: Root path for bronze files (default: LAKE_ROOT/bronze/quant360)

        Returns:
            IngestionResult with row count and timestamp ranges
        """
        if bronze_root is None:
            bronze_root = get_bronze_root("quant360")
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

            # 2. Parse CSV (Quant360 format)
            parsed_df = parse_quant360_orders_csv(raw_df)

            # 3. Load dim_symbol for quarantine check
            dim_symbol = self.dim_symbol_repo.read_all()

            # 4. Quarantine check
            exchange_id = self._resolve_exchange_id(meta.exchange)
            is_valid, error_msg = self._check_quarantine(meta, dim_symbol, exchange_id, parsed_df)

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

            # 6. Encode fixed-point (lot-based for Chinese stocks)
            encoded_df = encode_fixed_point(resolved_df, dim_symbol)

            # 7. Add lineage columns
            lineage_df = self._add_lineage(encoded_df, file_id)

            # 8. Add exchange, exchange_id and date
            normalized_exchange = normalize_exchange(meta.exchange)
            final_df = self._add_metadata(lineage_df, normalized_exchange, exchange_id)

            # 9. Normalize schema
            normalized_df = normalize_szse_l3_orders_schema(final_df)

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
                f"Ingested {validated_df.height} orders from {meta.bronze_file_path} "
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
        read_options = {
            "infer_schema_length": 10000,
            "try_parse_dates": False,
        }
        # Preserve raw CSV line numbers (1-based data rows + header)
        import inspect

        if "row_index_name" in inspect.signature(pl.read_csv).parameters:
            read_options["row_index_name"] = "file_line_number"
            read_options["row_index_offset"] = 2
        else:
            read_options["row_count_name"] = "file_line_number"
            read_options["row_count_offset"] = 2

        try:
            if path.suffix == ".gz" or str(path).endswith(".csv.gz"):
                with gzip.open(path, "rt", encoding="utf-8") as f:
                    return pl.read_csv(f, **read_options)
            else:
                return pl.read_csv(path, **read_options)
        except pl.exceptions.NoDataError:
            return pl.DataFrame()

    def _resolve_exchange_id(self, exchange: str) -> int:
        """Resolve exchange name to exchange_id using canonical mapping."""
        return get_exchange_id(exchange)

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
                (pl.col("exchange_id") == exchange_id) & (pl.col("exchange_symbol") == meta.symbol)
            )

            if rows.is_empty():
                return False, "missing_symbol"
            else:
                return False, "invalid_validity_window"

        return True, ""

    def _add_lineage(self, df: pl.DataFrame, file_id: int) -> pl.DataFrame:
        """Add lineage tracking columns: file_id and file_line_number."""
        # Use Int32 to match Delta Lake storage
        if "file_line_number" in df.columns:
            file_line_number = pl.col("file_line_number").cast(pl.Int32)
        else:
            file_line_number = pl.int_range(1, df.height + 1, dtype=pl.Int32)
        return df.with_columns(
            [
                pl.lit(file_id, dtype=pl.Int32).alias("file_id"),
                file_line_number.alias("file_line_number"),
            ]
        )

    def _add_metadata(self, df: pl.DataFrame, exchange: str, exchange_id: int) -> pl.DataFrame:
        """
        Add exchange, exchange_id, and exchange-local date columns.

        The date column is derived from ts_local_us in the exchange's local timezone,
        ensuring that one trading day maps to exactly one partition.

        For SZSE (Asia/Shanghai timezone):
        - 2024-09-30 00:30 CST → date=2024-09-30
        - 2024-09-30 23:30 CST → date=2024-09-30

        For crypto (UTC timezone):
        - 2024-09-30 00:30 UTC → date=2024-09-30
        - 2024-09-30 23:30 UTC → date=2024-09-30
        """
        # Add exchange (string) for partitioning and human readability
        # Add exchange_id (Int16) for joins and compression
        result = df.with_columns(
            [
                pl.lit(exchange, dtype=pl.Utf8).alias("exchange"),
                pl.lit(exchange_id, dtype=pl.Int16).alias("exchange_id"),
            ]
        )

        # Derive date from ts_local_us in exchange-local timezone
        # This ensures one trading day = one partition (no UTC boundary splits)
        exchange_tz = get_exchange_timezone(exchange)
        result = result.with_columns(
            [
                pl.from_epoch(pl.col("ts_local_us"), time_unit="us")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone(exchange_tz)
                .dt.date()
                .alias("date"),
            ]
        )

        return result
