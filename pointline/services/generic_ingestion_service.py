"""Generic ingestion service supporting multiple vendors per table.

This module provides a vendor-agnostic ingestion service that uses runtime parser
dispatch to handle different vendor formats without hardcoded coupling.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from pointline.config import get_bronze_root, get_exchange_id
from pointline.dim_symbol import check_coverage
from pointline.io.protocols import (
    BronzeFileMetadata,
    IngestionManifestRepository,
    IngestionResult,
    TableRepository,
)
from pointline.io.vendors import get_vendor
from pointline.services.base_service import BaseService

logger = logging.getLogger(__name__)


@dataclass
class TableStrategy:
    """Table-specific functions (encoding, validation, normalization, resolution).

    This encapsulates all the table-specific domain logic while keeping the
    ingestion pipeline vendor-agnostic.

    Attributes:
        encode_fixed_point: Convert float prices/quantities to fixed-point integers
        validate: Apply quality checks and filter invalid rows
        normalize_schema: Cast to canonical schema and select only schema columns
        resolve_symbol_ids: Map exchange symbols to symbol_ids with SCD Type 2 handling
        normalize_symbol: Normalize exchange symbol for dim_symbol matching (optional)
        ts_col: Timestamp column name for symbol resolution (default: ts_local_us)
    """

    encode_fixed_point: Callable[[pl.DataFrame, pl.DataFrame], pl.DataFrame]
    validate: Callable[[pl.DataFrame], pl.DataFrame]
    normalize_schema: Callable[[pl.DataFrame], pl.DataFrame]
    resolve_symbol_ids: Callable[
        [pl.DataFrame, pl.DataFrame, int | None, str | None, str], pl.DataFrame
    ]  # (df, dim_symbol, exchange_id, symbol, ts_col) -> df
    ts_col: str = "ts_local_us"  # Timestamp column for symbol resolution


class GenericIngestionService(BaseService):
    """Vendor-agnostic ingestion service with runtime parser selection.

    This service implements the standard ingestion pipeline with column-based metadata:
    1. Vendor reads + parses file (adds metadata columns)
    2. Validate required metadata columns
    3. Map exchange -> exchange_id
    4. Load dim_symbol and perform quarantine checks
    5. Resolve symbol IDs (SCD Type 2 as-of join)
    6. Encode fixed-point (price/qty → integers)
    7. Add lineage columns (file_id, file_line_number)
    8. Normalize schema (enforce canonical schema)
    9. Validate (quality checks)
    10. Append to Delta table
    11. Return IngestionResult

    Only step 1 (read_and_parse) varies by vendor. All other steps are standardized.
    """

    def __init__(
        self,
        table_name: str,
        table_strategy: TableStrategy,
        repo: TableRepository,
        dim_symbol_repo: TableRepository,
        manifest_repo: IngestionManifestRepository,
    ):
        """Initialize the generic ingestion service.

        Args:
            table_name: Name of the table (e.g., "trades", "quotes", "l3_orders")
            table_strategy: Table-specific functions for this data type
            repo: Repository for the target Silver table
            dim_symbol_repo: Repository for dim_symbol table
            manifest_repo: Repository for ingestion manifest
        """
        self.table_name = table_name
        self.strategy = table_strategy
        self.repo = repo
        self.dim_symbol_repo = dim_symbol_repo
        self.manifest_repo = manifest_repo

    def validate(self, data: pl.DataFrame) -> pl.DataFrame:
        """Validate data using table-specific validation logic."""
        return self.strategy.validate(data)

    def compute_state(self, valid_data: pl.DataFrame) -> pl.DataFrame:
        """Transform validated data to final Silver format.

        Note: This is called by the BaseService.update() template method.
        For ingestion from files, use ingest_file() instead.
        """
        return self.strategy.normalize_schema(valid_data)

    def write(self, result: pl.DataFrame) -> None:
        """Append data to the Delta table."""
        if result.is_empty():
            logger.warning("write: skipping empty DataFrame")
            return

        # Use append for immutable event data
        if hasattr(self.repo, "append"):
            self.repo.append(result)
        else:
            raise NotImplementedError("Repository must support append() for event data")

    def ingest_file(
        self,
        meta: BronzeFileMetadata,
        file_id: int,
        *,
        bronze_root: Path | None = None,
    ) -> IngestionResult:
        """Ingest a single CSV file from Bronze to Silver with runtime vendor dispatch.

        Args:
            meta: Metadata about the bronze file (includes vendor field)
            file_id: File ID from manifest repository
            bronze_root: Root path for bronze files (default: auto-detected from vendor)

        Returns:
            IngestionResult with row count and timestamp ranges
        """
        # Validate required metadata for this table
        table_module = importlib.import_module(f"pointline.tables.{self.table_name}")
        required_fields = {
            "vendor",
            "data_type",
            "bronze_file_path",
            "file_size_bytes",
            "last_modified_ts",
            "sha256",
        }
        required_fields |= getattr(table_module, "REQUIRED_METADATA_FIELDS", set())

        missing_fields = []
        for field in required_fields:
            if getattr(meta, field, None) is None:
                missing_fields.append(field)

        if missing_fields:
            error_msg = (
                f"Bronze file missing required metadata for table '{self.table_name}': "
                f"{missing_fields}. File: {meta.bronze_file_path}, "
                f"Vendor: {meta.vendor}, Data Type: {meta.data_type}"
            )
            logger.error(error_msg)
            return IngestionResult(
                row_count=0,
                ts_local_min_us=0,
                ts_local_max_us=0,
                error_message=error_msg,
            )

        # Detect vendor from metadata and get bronze root
        vendor = meta.vendor
        if bronze_root is None:
            bronze_root = get_bronze_root(vendor)
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
            # 1. Vendor reads and parses file (returns DataFrame with metadata columns)
            df = get_vendor(vendor).read_and_parse(bronze_path, meta)

            if df.is_empty():
                logger.info(f"Empty file (no data): {bronze_path}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=None,
                )

            # 2. Validate required metadata columns
            required_cols = ["exchange", "exchange_symbol", "date", "file_line_number"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                error_msg = (
                    f"Vendor output missing required columns: {missing_cols}. "
                    "Vendors must add metadata columns during read_and_parse()."
                )
                logger.error(error_msg)
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=error_msg,
                )
            null_cols = [col for col in required_cols if df[col].null_count() > 0]
            if null_cols:
                error_msg = f"Required metadata columns contain nulls: {null_cols}"
                logger.error(error_msg)
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=error_msg,
                )

            # 3. Map exchange -> exchange_id (vectorized)
            unique_exchanges = df["exchange"].unique().to_list()
            exchange_map: dict[str, int] = {}
            invalid_exchanges: list[str] = []
            for exchange in unique_exchanges:
                try:
                    exchange_map[exchange] = get_exchange_id(exchange)
                except ValueError:
                    invalid_exchanges.append(str(exchange))

            if invalid_exchanges:
                error_msg = f"Unknown exchanges: {sorted(set(invalid_exchanges))}"
                logger.error(error_msg)
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=error_msg,
                )

            df = df.with_columns(
                pl.col("exchange").map_dict(exchange_map).cast(pl.Int16).alias("exchange_id")
            )

            # 4. Load dim_symbol for quarantine check
            dim_symbol = self.dim_symbol_repo.read_all()

            # 5. Quarantine check per (exchange_id, exchange_symbol, date)
            unique_pairs = df.select(["exchange_id", "exchange_symbol", "date"]).unique()
            quarantined_pairs: list[tuple[int, str, date]] = []
            original_row_count = df.height

            for row in unique_pairs.iter_rows(named=True):
                exchange_id = row["exchange_id"]
                symbol = row["exchange_symbol"]
                trading_date = row["date"]
                is_valid, error_msg = self._check_quarantine(
                    dim_symbol, exchange_id, symbol, trading_date
                )
                if not is_valid:
                    logger.warning(
                        "Symbol quarantined: exchange_id=%s, symbol=%s, date=%s, reason=%s",
                        exchange_id,
                        symbol,
                        trading_date,
                        error_msg,
                    )
                    quarantined_pairs.append((exchange_id, symbol, trading_date))
                    df = df.filter(
                        ~(
                            (pl.col("exchange_id") == exchange_id)
                            & (pl.col("exchange_symbol") == symbol)
                            & (pl.col("date") == trading_date)
                        )
                    )

            filtered_row_count = original_row_count - df.height
            filtered_symbol_count = len(quarantined_pairs)

            if df.is_empty():
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message="All symbols quarantined",
                    partial_ingestion=True,
                    filtered_symbol_count=filtered_symbol_count,
                    filtered_row_count=filtered_row_count,
                )

            if filtered_symbol_count:
                logger.warning(
                    "Partial ingestion: %s symbol-date pairs filtered, %s rows dropped",
                    filtered_symbol_count,
                    filtered_row_count,
                )

            # 6. Resolve symbol IDs (SCD Type 2 as-of join using columns)
            resolved_df = self.strategy.resolve_symbol_ids(
                df,
                dim_symbol,
                exchange_id=None,
                exchange_symbol=None,
                ts_col=self.strategy.ts_col,
            )

            # 7. Encode fixed-point (price/qty → integers)
            encoded_df = self.strategy.encode_fixed_point(resolved_df, dim_symbol)

            # 8. Add lineage columns (file_id, file_line_number)
            lineage_df = self._add_lineage(encoded_df, file_id)

            # 9. Normalize schema (enforce canonical schema)
            normalized_df = self.strategy.normalize_schema(lineage_df)

            # 10. Validate (quality checks)
            validated_df = self.validate(normalized_df)

            filtered_by_validation = normalized_df.height - validated_df.height
            if filtered_by_validation:
                filtered_row_count += filtered_by_validation
                if validated_df.is_empty():
                    logger.warning(f"No valid rows after validation: {bronze_path}")
                    return IngestionResult(
                        row_count=0,
                        ts_local_min_us=0,
                        ts_local_max_us=0,
                        error_message="All rows filtered by validation",
                        partial_ingestion=True,
                        filtered_symbol_count=filtered_symbol_count,
                        filtered_row_count=filtered_row_count,
                    )

            # 11. Append to Delta table
            self.write(validated_df)

            # 12. Compute result stats
            ts_min = validated_df[self.strategy.ts_col].min()
            ts_max = validated_df[self.strategy.ts_col].max()

            logger.info(
                f"Ingested {validated_df.height} rows from {meta.bronze_file_path} "
                f"[vendor={vendor}, data_type={meta.data_type}] "
                f"(ts range: {ts_min} - {ts_max})"
            )

            return IngestionResult(
                row_count=validated_df.height,
                ts_local_min_us=ts_min,
                ts_local_max_us=ts_max,
                error_message=None,
                partial_ingestion=filtered_row_count > 0,
                filtered_symbol_count=filtered_symbol_count,
                filtered_row_count=filtered_row_count,
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

    def _check_quarantine(
        self,
        dim_symbol: pl.DataFrame,
        exchange_id: int,
        exchange_symbol: str,
        trading_date: date,
    ) -> tuple[bool, str]:
        """Check if file should be quarantined based on symbol metadata coverage.

        Args:
            dim_symbol: Symbol dimension table
            exchange_id: Resolved exchange ID
            exchange_symbol: Normalized symbol for dim_symbol matching
            trading_date: Trading date for coverage check

        Returns:
            (is_valid, error_message) tuple
        """
        # Calculate UTC day boundaries
        day_start = datetime.combine(trading_date, datetime.min.time(), tzinfo=timezone.utc)
        day_start_us = int(day_start.timestamp() * 1_000_000)

        # Next day start (exclusive end)
        day_end = datetime.combine(
            trading_date + timedelta(days=1),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        day_end_us = int(day_end.timestamp() * 1_000_000)

        # Check coverage using normalized symbol
        has_coverage = check_coverage(
            dim_symbol,
            exchange_id,
            exchange_symbol,
            day_start_us,
            day_end_us,
        )

        if not has_coverage:
            # Determine specific reason (also use normalized symbol)
            rows = dim_symbol.filter(
                (pl.col("exchange_id") == exchange_id)
                & (pl.col("exchange_symbol") == exchange_symbol)
            )

            if rows.is_empty():
                return False, "missing_symbol"
            else:
                return False, "invalid_validity_window"

        return True, ""

    def _add_lineage(self, df: pl.DataFrame, file_id: int) -> pl.DataFrame:
        """Add lineage tracking columns: file_id and file_line_number."""
        # Use Int32 to match Delta Lake storage (Delta Lake doesn't support UInt32)
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
