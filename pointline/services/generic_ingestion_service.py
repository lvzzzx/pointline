"""Generic ingestion service supporting multiple vendors per table.

This module provides a vendor-agnostic ingestion service that uses runtime parser
dispatch to handle different vendor formats without hardcoded coupling.
"""

from __future__ import annotations

import gzip
import importlib
import logging
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from pointline.config import (
    get_bronze_root,
    get_exchange_id,
    get_exchange_timezone,
    normalize_exchange,
)
from pointline.dim_symbol import check_coverage
from pointline.io.protocols import (
    BronzeFileMetadata,
    IngestionManifestRepository,
    IngestionResult,
    TableRepository,
)
from pointline.io.vendors import get_parser
from pointline.services.base_service import BaseService

logger = logging.getLogger(__name__)

# Registry of headerless CSV formats: (vendor, data_type) → column_names
# Files without headers need explicit column names to prevent data loss
HEADERLESS_FORMATS: dict[tuple[str, str], list[str]] = {
    ("binance_vision", "klines"): [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trade_count",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "ignore",
    ],
}


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
    """

    encode_fixed_point: Callable[[pl.DataFrame, pl.DataFrame], pl.DataFrame]
    validate: Callable[[pl.DataFrame], pl.DataFrame]
    normalize_schema: Callable[[pl.DataFrame], pl.DataFrame]
    resolve_symbol_ids: Callable[
        [pl.DataFrame, pl.DataFrame, int, str, str], pl.DataFrame
    ]  # (df, dim_symbol, exchange_id, symbol, ts_col) -> df
    normalize_symbol: Callable[[str, str], str] = lambda symbol, exchange: symbol
    # Default: identity function (no normalization)
    # Signature: (symbol: str, exchange: str) -> normalized_symbol: str


class GenericIngestionService(BaseService):
    """Vendor-agnostic ingestion service with runtime parser selection.

    This service implements the standard 12-step ingestion pipeline:
    1. Read bronze CSV file (handling gzip)
    2. Parse CSV (vendor-specific, runtime dispatch)
    3. Load dim_symbol for quarantine check
    4. Quarantine check (symbol coverage validation)
    5. Resolve symbol IDs (SCD Type 2 as-of join)
    6. Encode fixed-point (price/qty → integers)
    7. Add lineage columns (file_id, file_line_number)
    8. Add metadata (exchange, exchange_id, date)
    9. Normalize schema (enforce canonical schema)
    10. Validate (quality checks)
    11. Append to Delta table
    12. Return IngestionResult

    Only step 2 (parsing) varies by vendor. All other steps are standardized.
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
        required_fields = getattr(table_module, "REQUIRED_METADATA_FIELDS", set())

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
            # 1. Read bronze CSV file (with headerless format detection)
            raw_df = self._read_bronze_csv(bronze_path, meta)

            if raw_df.is_empty():
                logger.warning(f"Empty CSV file: {bronze_path}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=None,
                )

            # 2. Parse CSV (VENDOR-SPECIFIC - Runtime dispatch)
            try:
                parser = get_parser(vendor, meta.data_type)
            except KeyError:
                error_msg = f"No parser registered for vendor={vendor}, data_type={meta.data_type}"
                logger.error(error_msg)
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=error_msg,
                )

            parsed_df = parser(raw_df)

            # 3. Load dim_symbol for quarantine check
            dim_symbol = self.dim_symbol_repo.read_all()

            # 4. Quarantine check (using normalized symbol for dim_symbol matching)
            # Normalize symbol once for both quarantine check and symbol resolution
            exchange_id = self._resolve_exchange_id(meta.exchange)
            normalized_symbol = self.strategy.normalize_symbol(meta.symbol, meta.exchange)
            is_valid, error_msg = self._check_quarantine(
                meta, dim_symbol, exchange_id, parsed_df, normalized_symbol
            )

            if not is_valid:
                logger.warning(f"File quarantined: {meta.bronze_file_path} - {error_msg}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=error_msg,
                )

            # 5. Resolve symbol IDs (SCD Type 2 as-of join using normalized symbol)
            resolved_df = self.strategy.resolve_symbol_ids(
                parsed_df,
                dim_symbol,
                exchange_id,
                normalized_symbol,
                ts_col="ts_local_us",
            )

            # 6. Encode fixed-point (price/qty → integers)
            encoded_df = self.strategy.encode_fixed_point(resolved_df, dim_symbol)

            # 7. Add lineage columns (file_id, file_line_number)
            lineage_df = self._add_lineage(encoded_df, file_id)

            # 8. Add metadata (exchange, exchange_id, date)
            normalized_exchange = normalize_exchange(meta.exchange)
            final_df = self._add_metadata(lineage_df, normalized_exchange, exchange_id)

            # 9. Normalize schema (enforce canonical schema)
            normalized_df = self.strategy.normalize_schema(final_df)

            # 10. Validate (quality checks)
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
                f"Ingested {validated_df.height} rows from {meta.bronze_file_path} "
                f"[vendor={vendor}, data_type={meta.data_type}] "
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

    def _read_bronze_csv(self, path: Path, meta: BronzeFileMetadata) -> pl.DataFrame:
        """Read a bronze CSV file, handling ZIP and gzip compression and headerless formats.

        Args:
            path: Path to the CSV file (may be .csv, .csv.gz, or .zip)
            meta: Bronze file metadata (needed to detect headerless formats)

        Returns:
            DataFrame with raw CSV data

        Note:
            Some vendors (e.g., Binance klines) provide headerless CSVs. Without proper
            handling, polars will consume the first data row as column names, causing
            data loss. This method checks HEADERLESS_FORMATS registry and configures
            has_header=False with explicit column names when needed.

            ZIP files: Assumes a single CSV file inside the ZIP archive.

            Row numbering: For headerless CSVs, row numbers start at 1 (first data row).
            For CSVs with headers, row numbers start at 2 (first data row after header).
        """
        read_options = {
            "infer_schema_length": 10000,
            "try_parse_dates": False,
        }

        # Check if this is a headerless format
        key = (meta.vendor.lower(), meta.data_type.lower())
        is_headerless = key in HEADERLESS_FORMATS

        if is_headerless:
            read_options["has_header"] = False
            read_options["new_columns"] = HEADERLESS_FORMATS[key]

        # Note: For headerless CSVs, we add row index AFTER reading to avoid
        # column name conflicts with new_columns parameter
        add_row_index_after = is_headerless

        if not add_row_index_after:
            # For CSVs with headers, add row index during read
            import inspect

            if "row_index_name" in inspect.signature(pl.read_csv).parameters:
                read_options["row_index_name"] = "file_line_number"
                read_options["row_index_offset"] = 2  # Skip header row
            else:
                read_options["row_count_name"] = "file_line_number"
                read_options["row_count_offset"] = 2

        try:
            if path.suffix == ".zip":
                with zipfile.ZipFile(path) as zf:
                    # Find first CSV file in ZIP
                    csv_name = next(
                        (name for name in zf.namelist() if name.endswith(".csv")),
                        None,
                    )
                    if csv_name is None:
                        logger.warning(f"No CSV file found in ZIP archive: {path}")
                        return pl.DataFrame()
                    with zf.open(csv_name) as handle:
                        df = pl.read_csv(handle, **read_options)
            elif path.suffix == ".gz" or str(path).endswith(".csv.gz"):
                with gzip.open(path, "rt", encoding="utf-8") as f:
                    df = pl.read_csv(f, **read_options)
            else:
                df = pl.read_csv(path, **read_options)

            # Add row index after reading for headerless CSVs
            if add_row_index_after and not df.is_empty():
                df = df.with_row_index(name="file_line_number", offset=1)

            return df

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
        normalized_symbol: str,
    ) -> tuple[bool, str]:
        """Check if file should be quarantined based on symbol metadata coverage.

        Args:
            meta: Bronze file metadata
            dim_symbol: Symbol dimension table
            exchange_id: Resolved exchange ID
            parsed_df: Parsed DataFrame (unused, for future validation)
            normalized_symbol: Normalized symbol for dim_symbol matching

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

        # Check coverage using normalized symbol
        has_coverage = check_coverage(
            dim_symbol,
            exchange_id,
            normalized_symbol,
            day_start_us,
            day_end_us,
        )

        if not has_coverage:
            # Determine specific reason (also use normalized symbol)
            rows = dim_symbol.filter(
                (pl.col("exchange_id") == exchange_id)
                & (pl.col("exchange_symbol") == normalized_symbol)
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

    def _add_metadata(self, df: pl.DataFrame, exchange: str, exchange_id: int) -> pl.DataFrame:
        """Add exchange, exchange_id, and exchange-local date columns.

        The date column is derived from ts_local_us in the exchange's local timezone,
        ensuring that one trading day maps to exactly one partition.

        Raises:
            ValueError: If exchange is not registered in EXCHANGE_TIMEZONES
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
        # Validate exchange has explicit timezone mapping (fail fast to prevent mispartitioning)
        try:
            exchange_tz = get_exchange_timezone(exchange, strict=True)
        except ValueError as e:
            raise ValueError(
                f"Cannot add metadata for exchange '{exchange}' (exchange_id={exchange_id}): {e}"
            ) from e
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
