"""Generic ingestion service supporting multiple vendors per table.

This module provides a vendor-agnostic ingestion service that uses runtime parser
dispatch to handle different vendor formats without hardcoded coupling.
"""

from __future__ import annotations

import importlib
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from pointline.config import (
    TABLE_ALLOWED_EXCHANGES,
    get_bronze_root,
    get_exchange_id,
    get_exchange_timezone,
    get_table_path,
)

# get_exchange_id is still imported for quarantine checks against dim_symbol
from pointline.dim_symbol import check_coverage
from pointline.io.protocols import (
    BronzeFileMetadata,
    IngestionManifestRepository,
    IngestionResult,
    TableRepository,
)
from pointline.io.vendors import get_vendor
from pointline.services.base_service import BaseService
from pointline.tables.validation_log import create_ingestion_record

logger = logging.getLogger(__name__)


@dataclass
class TableStrategy:
    """Table-specific functions (encoding, validation, normalization).

    This encapsulates all the table-specific domain logic while keeping the
    ingestion pipeline vendor-agnostic.

    Attributes:
        encode_fixed_point: Convert float prices/quantities to fixed-point integers (df, dim_symbol, exchange)
        validate: Apply quality checks and filter invalid rows
        normalize_schema: Cast to canonical schema and select only schema columns
    """

    encode_fixed_point: Callable[[pl.DataFrame, pl.DataFrame, str], pl.DataFrame]
    validate: Callable[[pl.DataFrame], pl.DataFrame]
    normalize_schema: Callable[[pl.DataFrame], pl.DataFrame]


class GenericIngestionService(BaseService):
    """Vendor-agnostic ingestion service with runtime parser selection.

    This service implements the standard ingestion pipeline with column-based metadata:
    1. Vendor reads + parses file (adds metadata columns)
    2. Validate required metadata columns; rename exchange_symbol → symbol
    3. Validate exchange names; check table-level exchange restrictions
    4. Load dim_symbol and perform quarantine checks
    5. Encode fixed-point (price/qty → integers)
    6. Add lineage columns (file_id, file_line_number)
    7. Normalize schema (enforce canonical schema)
    8. Validate (quality checks)
    9. Append to Delta table
    10. Return IngestionResult

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

    def write(self, result: pl.DataFrame, *, use_merge: bool = False) -> None:
        """Append data to the Delta table.

        Default is append-only for performance. MERGE is available as opt-in
        for tables that need upsert semantics or explicit crash-recovery dedup.

        Args:
            result: DataFrame to write.
            use_merge: If True, use MERGE on lineage keys for idempotent writes.
                      Default False (append-only, ~2-5x faster).
        """
        if result.is_empty():
            logger.warning("write: skipping empty DataFrame")
            return

        if use_merge:
            lineage_keys = ["file_id", "file_line_number"]
            if all(col in result.columns for col in lineage_keys) and hasattr(self.repo, "merge"):
                self.repo.merge(result, keys=lineage_keys)
                return

        if hasattr(self.repo, "append"):
            self.repo.append(result)
            return

        raise NotImplementedError("Repository must support append() for event data")

    def ingest_file(
        self,
        meta: BronzeFileMetadata,
        file_id: int,
        *,
        bronze_root: Path | None = None,
        dry_run: bool = False,
        idempotent_write: bool = False,
    ) -> IngestionResult:
        """Ingest a single CSV file from Bronze to Silver with runtime vendor dispatch.

        Args:
            meta: Metadata about the bronze file (includes vendor field)
            file_id: File ID from manifest repository
            bronze_root: Root path for bronze files (default: auto-detected from vendor)
            dry_run: Walk the full pipeline but skip write. No side effects.
            idempotent_write: If True, write via MERGE on lineage keys to make retries safe.

        Returns:
            IngestionResult with row count and timestamp ranges
        """
        start_time_ms = int(time.time() * 1000)

        # Validate required metadata for this table
        table_module_name = self.table_name
        if self.table_name == "book_snapshot_25":
            table_module_name = "book_snapshots"
        elif self.table_name.startswith("kline_"):
            table_module_name = "klines"
        table_module = importlib.import_module(f"pointline.tables.{table_module_name}")
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
            result = IngestionResult(
                row_count=0,
                ts_local_min_us=0,
                ts_local_max_us=0,
                error_message=error_msg,
                failure_reason="missing_required_metadata",
            )
            if not dry_run:
                self._write_validation_log(meta, file_id, result, start_time_ms)
            return result

        # Detect vendor from metadata and get bronze root
        vendor = meta.vendor
        if bronze_root is None:
            bronze_root = get_bronze_root(vendor)
        bronze_path = bronze_root / meta.bronze_file_path

        if not bronze_path.exists():
            error_msg = f"Bronze file not found: {bronze_path}"
            logger.error(error_msg)
            result = IngestionResult(
                row_count=0,
                ts_local_min_us=0,
                ts_local_max_us=0,
                error_message=error_msg,
                failure_reason="missing_bronze_file",
            )
            if not dry_run:
                self._write_validation_log(meta, file_id, result, start_time_ms)
            return result

        try:
            # 1. Vendor reads and parses file (returns DataFrame with metadata columns)
            df = get_vendor(vendor).read_and_parse(bronze_path, meta)

            if df.is_empty():
                logger.info(f"Empty file (no data): {bronze_path}")
                result = IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=None,
                )
                if not dry_run:
                    self._write_validation_log(meta, file_id, result, start_time_ms)
                return result

            # 2. Validate required metadata columns
            required_cols = ["exchange", "exchange_symbol", "date", "file_line_number"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                error_msg = (
                    f"Vendor output missing required columns: {missing_cols}. "
                    "Vendors must add metadata columns during read_and_parse()."
                )
                logger.error(error_msg)
                result = IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=error_msg,
                    failure_reason="vendor_missing_required_columns",
                )
                if not dry_run:
                    self._write_validation_log(meta, file_id, result, start_time_ms)
                return result
            null_cols = [col for col in required_cols if df[col].null_count() > 0]
            if null_cols:
                error_msg = f"Required metadata columns contain nulls: {null_cols}"
                logger.error(error_msg)
                result = IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=error_msg,
                    failure_reason="vendor_null_required_columns",
                )
                if not dry_run:
                    self._write_validation_log(meta, file_id, result, start_time_ms)
                return result

            # 3. Validate exchange names
            unique_exchanges = df["exchange"].unique().to_list()
            invalid_exchanges: list[str] = []
            for exchange in unique_exchanges:
                try:
                    get_exchange_id(exchange)  # validates exchange is known
                except ValueError:
                    invalid_exchanges.append(str(exchange))

            if invalid_exchanges:
                error_msg = f"Unknown exchanges: {sorted(set(invalid_exchanges))}"
                logger.error(error_msg)
                result = IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=error_msg,
                    failure_reason="unknown_exchange",
                )
                if not dry_run:
                    self._write_validation_log(meta, file_id, result, start_time_ms)
                return result

            # 3b. Check table-level exchange restrictions (e.g., l3_* → szse/sse only)
            allowed = TABLE_ALLOWED_EXCHANGES.get(self.table_name)
            if allowed is not None:
                disallowed = sorted(set(unique_exchanges) - allowed)
                if disallowed:
                    error_msg = (
                        f"Table '{self.table_name}' is restricted to exchanges "
                        f"{sorted(allowed)}, got: {disallowed}"
                    )
                    logger.error(error_msg)
                    result = IngestionResult(
                        row_count=0,
                        ts_local_min_us=0,
                        ts_local_max_us=0,
                        error_message=error_msg,
                        failure_reason="exchange_not_allowed",
                    )
                    if not dry_run:
                        self._write_validation_log(meta, file_id, result, start_time_ms)
                    return result

            # 2b. Rename exchange_symbol → symbol (canonical event table column)
            df = df.rename({"exchange_symbol": "symbol"})

            # 4. Validate date partition alignment against exchange-local timezone
            ts_col = "ts_bucket_start_us" if "ts_bucket_start_us" in df.columns else "ts_local_us"
            date_alignment_ok, date_alignment_error = self._validate_date_partition_alignment(
                df, ts_col=ts_col
            )
            if not date_alignment_ok:
                result = IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=date_alignment_error,
                    failure_reason="date_partition_mismatch",
                )
                if not dry_run:
                    self._write_validation_log(meta, file_id, result, start_time_ms)
                return result

            # 5. Load dim_symbol for quarantine check
            dim_symbol = self.dim_symbol_repo.read_all()

            # 6. Vectorized quarantine check
            df, filtered_row_count, filtered_symbol_count = self._check_quarantine_vectorized(
                df, dim_symbol
            )

            if df.is_empty():
                result = IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message="All symbols quarantined",
                    failure_reason="all_symbols_quarantined",
                    partial_ingestion=True,
                    filtered_symbol_count=filtered_symbol_count,
                    filtered_row_count=filtered_row_count,
                )
                if not dry_run:
                    self._write_validation_log(meta, file_id, result, start_time_ms)
                return result

            if filtered_symbol_count:
                logger.warning(
                    "Partial ingestion: %s symbol-date pairs filtered, %s rows dropped",
                    filtered_symbol_count,
                    filtered_row_count,
                )

            # 7. Encode fixed-point (price/qty → integers)
            # Use first exchange from data — files are single-exchange by convention
            exchange = unique_exchanges[0]
            encoded_df = self.strategy.encode_fixed_point(df, dim_symbol, exchange)

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
                    result = IngestionResult(
                        row_count=0,
                        ts_local_min_us=0,
                        ts_local_max_us=0,
                        error_message="All rows filtered by validation",
                        failure_reason="all_rows_filtered_by_validation",
                        partial_ingestion=True,
                        filtered_symbol_count=filtered_symbol_count,
                        filtered_row_count=filtered_row_count,
                    )
                    if not dry_run:
                        self._write_validation_log(meta, file_id, result, start_time_ms)
                    return result

            # 11. Append to Delta table (skip in dry-run mode)
            if dry_run:
                logger.info(
                    f"DRY RUN: would ingest {validated_df.height} rows from "
                    f"{meta.bronze_file_path} [vendor={vendor}, data_type={meta.data_type}]"
                )
            else:
                self.write(validated_df, use_merge=idempotent_write)

            # 12. Compute result stats
            ts_min = validated_df[ts_col].min()
            ts_max = validated_df[ts_col].max()

            logger.info(
                f"Ingested {validated_df.height} rows from {meta.bronze_file_path} "
                f"[vendor={vendor}, data_type={meta.data_type}] "
                f"(ts range: {ts_min} - {ts_max})"
            )

            result = IngestionResult(
                row_count=validated_df.height,
                ts_local_min_us=ts_min,
                ts_local_max_us=ts_max,
                error_message=None,
                partial_ingestion=filtered_row_count > 0,
                filtered_symbol_count=filtered_symbol_count,
                filtered_row_count=filtered_row_count,
            )
            if not dry_run:
                self._write_validation_log(meta, file_id, result, start_time_ms)
            return result

        except Exception as e:
            error_msg = f"Error ingesting {bronze_path}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            result = IngestionResult(
                row_count=0,
                ts_local_min_us=0,
                ts_local_max_us=0,
                error_message=error_msg,
                failure_reason="ingest_exception",
            )
            if not dry_run:
                self._write_validation_log(meta, file_id, result, start_time_ms)
            return result

    def _check_quarantine_vectorized(
        self,
        df: pl.DataFrame,
        dim_symbol: pl.DataFrame,
    ) -> tuple[pl.DataFrame, int, int]:
        """Vectorized quarantine check using anti-join against dim_symbol coverage.

        Replaces the previous row-by-row loop with a single vectorized operation.

        Args:
            df: Input DataFrame with exchange, symbol, date columns
            dim_symbol: Symbol dimension table

        Returns:
            (filtered_df, filtered_row_count, filtered_symbol_count) tuple
        """
        # 1. Build unique (exchange, symbol, date) pairs
        unique_pairs = df.select(["exchange", "symbol", "date"]).unique()

        if unique_pairs.is_empty():
            return df, 0, 0

        # 2. Compute day boundaries per exchange timezone (vectorized by exchange)
        exchange_tz_cache: dict[str, ZoneInfo] = {}
        exchange_id_cache: dict[str, int] = {}
        day_starts: list[int] = []
        day_ends: list[int] = []

        for row in unique_pairs.iter_rows(named=True):
            exchange_name = row["exchange"]
            trading_date = row["date"]

            if exchange_name not in exchange_tz_cache:
                tz_name = get_exchange_timezone(str(exchange_name), strict=True)
                exchange_tz_cache[exchange_name] = ZoneInfo(tz_name)
                exchange_id_cache[exchange_name] = get_exchange_id(str(exchange_name))

            tz = exchange_tz_cache[exchange_name]
            day_start = datetime.combine(trading_date, datetime.min.time(), tzinfo=tz)
            day_end = datetime.combine(
                trading_date + timedelta(days=1), datetime.min.time(), tzinfo=tz
            )
            day_starts.append(int(day_start.astimezone(timezone.utc).timestamp() * 1_000_000))
            day_ends.append(int(day_end.astimezone(timezone.utc).timestamp() * 1_000_000))

        unique_pairs = unique_pairs.with_columns(
            pl.Series("day_start_us", day_starts, dtype=pl.Int64),
            pl.Series("day_end_us", day_ends, dtype=pl.Int64),
        )

        # 3. Evaluate full-window coverage per (exchange, symbol, date).
        # Derive exchange_id from exchange name for dim_symbol lookup.
        # A pair is covered only if dim_symbol has contiguous coverage for the
        # entire trading-day window [day_start_us, day_end_us).
        dim_subset = dim_symbol.select(
            ["exchange_id", "exchange_symbol", "valid_from_ts", "valid_until_ts"]
        )

        coverage_flags = [
            check_coverage(
                dim_subset,
                exchange_id_cache[row["exchange"]],
                row["symbol"],
                row["day_start_us"],
                row["day_end_us"],
            )
            for row in unique_pairs.iter_rows(named=True)
        ]
        quarantined = (
            unique_pairs.with_columns(pl.Series("is_covered", coverage_flags, dtype=pl.Boolean))
            .filter(~pl.col("is_covered"))
            .drop(["day_start_us", "day_end_us", "is_covered"])
        )

        filtered_symbol_count = quarantined.height

        if filtered_symbol_count == 0:
            return df, 0, 0

        # Log quarantined symbols
        for row in quarantined.iter_rows(named=True):
            logger.warning(
                "Symbol quarantined: exchange=%s, symbol=%s, date=%s",
                row["exchange"],
                row["symbol"],
                row["date"],
            )

        # 5. Filter DataFrame in one pass using anti-join
        filtered_df = df.join(
            quarantined.select(["exchange", "symbol", "date"]),
            on=["exchange", "symbol", "date"],
            how="anti",
        )

        filtered_row_count = df.height - filtered_df.height
        return filtered_df, filtered_row_count, filtered_symbol_count

    def _validate_date_partition_alignment(
        self, df: pl.DataFrame, *, ts_col: str
    ) -> tuple[bool, str | None]:
        """Validate that row timestamps align with exchange-local date partitions.

        Date partitions are authoritative metadata. Every row timestamp must fall into
        the exchange-local day window represented by its `(exchange, date)` pair.
        """
        if df.is_empty():
            return True, None

        required = {"exchange", "date", ts_col}
        missing = sorted(required - set(df.columns))
        if missing:
            return False, (
                f"Date partition validation requires columns {sorted(required)}; missing {missing}"
            )

        unique_pairs = df.select(["exchange", "date"]).unique()
        tz_cache: dict[str, ZoneInfo] = {}
        boundaries: list[dict[str, object]] = []

        for row in unique_pairs.iter_rows(named=True):
            exchange_name = str(row["exchange"])
            trading_date = row["date"]
            if exchange_name not in tz_cache:
                tz_name = get_exchange_timezone(exchange_name, strict=True)
                tz_cache[exchange_name] = ZoneInfo(tz_name)

            tz = tz_cache[exchange_name]
            day_start_local = datetime.combine(trading_date, datetime.min.time(), tzinfo=tz)
            day_end_local = datetime.combine(
                trading_date + timedelta(days=1), datetime.min.time(), tzinfo=tz
            )
            boundaries.append(
                {
                    "exchange": exchange_name,
                    "date": trading_date,
                    "_day_start_us": int(
                        day_start_local.astimezone(timezone.utc).timestamp() * 1_000_000
                    ),
                    "_day_end_us": int(
                        day_end_local.astimezone(timezone.utc).timestamp() * 1_000_000
                    ),
                }
            )

        boundary_df = pl.DataFrame(
            boundaries,
            schema={
                "exchange": pl.Utf8,
                "date": pl.Date,
                "_day_start_us": pl.Int64,
                "_day_end_us": pl.Int64,
            },
        )

        checked = df.join(boundary_df, on=["exchange", "date"], how="left")
        invalid_rows = checked.filter(
            pl.col(ts_col).is_null()
            | pl.col("_day_start_us").is_null()
            | pl.col("_day_end_us").is_null()
            | (pl.col(ts_col) < pl.col("_day_start_us"))
            | (pl.col(ts_col) >= pl.col("_day_end_us"))
        )

        if invalid_rows.is_empty():
            return True, None

        sample_cols = ["exchange", "date", ts_col]
        if "file_line_number" in invalid_rows.columns:
            sample_cols.append("file_line_number")
        sample = invalid_rows.select(sample_cols).head(5).to_dicts()

        error_message = (
            "Date partition mismatch: "
            f"{invalid_rows.height} row(s) outside exchange-local day window for '{ts_col}'. "
            f"sample={sample}"
        )
        return False, error_message

    def _check_quarantine(
        self,
        dim_symbol: pl.DataFrame,
        exchange_id: int,
        exchange_symbol: str,
        trading_date: date,
        exchange: str | None = None,
    ) -> tuple[bool, str]:
        """Check if a single symbol-date pair should be quarantined.

        Retained for backward compatibility with unit tests and direct callers.
        The main ingestion pipeline uses _check_quarantine_vectorized instead.

        Args:
            dim_symbol: Symbol dimension table
            exchange_id: Resolved exchange ID
            exchange_symbol: Normalized symbol for dim_symbol matching
            trading_date: Trading date for coverage check
            exchange: Exchange name for timezone resolution (optional)

        Returns:
            (is_valid, error_message) tuple
        """
        exchange_name = exchange
        if exchange_name is None:
            exchange_name = next(
                (
                    val
                    for val in dim_symbol.filter(pl.col("exchange_id") == exchange_id)
                    .select("exchange")
                    .drop_nulls()
                    .unique()
                    .get_column("exchange")
                    .to_list()
                ),
                "UTC",
            )

        tz_name = get_exchange_timezone(str(exchange_name), strict=True)
        tz = ZoneInfo(tz_name)

        day_start_local = datetime.combine(trading_date, datetime.min.time(), tzinfo=tz)
        day_start_us = int(day_start_local.astimezone(timezone.utc).timestamp() * 1_000_000)

        day_end_local = datetime.combine(
            trading_date + timedelta(days=1),
            datetime.min.time(),
            tzinfo=tz,
        )
        day_end_us = int(day_end_local.astimezone(timezone.utc).timestamp() * 1_000_000)

        has_coverage = check_coverage(
            dim_symbol,
            exchange_id,
            exchange_symbol,
            day_start_us,
            day_end_us,
        )

        if not has_coverage:
            rows = dim_symbol.filter(
                (pl.col("exchange_id") == exchange_id)
                & (pl.col("exchange_symbol") == exchange_symbol)
            )

            if rows.is_empty():
                return False, "missing_symbol"
            else:
                return False, "invalid_validity_window"

        return True, ""

    def _write_validation_log(
        self,
        meta: BronzeFileMetadata,
        file_id: int,
        result: IngestionResult,
        start_time_ms: int,
    ) -> None:
        """Write an ingestion record to the validation_log table.

        Best-effort: logs errors but does not raise on failure.
        """
        duration_ms = int(time.time() * 1000) - start_time_ms

        _QUARANTINE_REASONS = {
            "all_symbols_quarantined",
            "missing_symbol",
            "invalid_validity_window",
        }

        if result.failure_reason in _QUARANTINE_REASONS:
            status = "quarantined"
        elif result.failure_reason is not None or result.error_message is not None:
            status = "error"
        else:
            status = "ingested"

        # Extract exchange from bronze path (best effort)
        exchange = None
        for part in Path(meta.bronze_file_path).parts:
            if part.startswith("exchange="):
                exchange = part.split("=", 1)[1]
                break

        try:
            record = create_ingestion_record(
                file_id=file_id,
                table_name=self.table_name,
                vendor=meta.vendor,
                data_type=meta.data_type,
                exchange=exchange,
                date=meta.date,
                status=status,
                row_count=result.row_count,
                filtered_row_count=result.filtered_row_count,
                filtered_symbol_count=result.filtered_symbol_count,
                error_message=result.error_message,
                duration_ms=duration_ms,
            )

            log_path = get_table_path("validation_log")
            if log_path.exists():
                record.write_delta(str(log_path), mode="append")
            else:
                record.write_delta(str(log_path), mode="overwrite")

        except Exception:
            logger.debug("Failed to write validation_log record", exc_info=True)

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
