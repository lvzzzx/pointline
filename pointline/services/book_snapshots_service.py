"""Book snapshots ingestion service orchestrating the Bronze → Silver pipeline."""

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
from pointline.tables.book_snapshots import (
    encode_fixed_point,
    normalize_book_snapshots_schema,
    parse_tardis_book_snapshots_csv,
    resolve_symbol_ids,
    validate_book_snapshots,
)

logger = logging.getLogger(__name__)


class BookSnapshotsIngestionService(BaseService):
    """
    Orchestrates book snapshots ingestion from Bronze CSV files to Silver Delta tables.

    Handles:
    - CSV parsing and validation
    - Quarantine checks (symbol metadata coverage)
    - Symbol ID resolution
    - Fixed-point encoding (for arrays)
    - Delta Lake append operations
    """

    def __init__(
        self,
        repo: TableRepository,
        dim_symbol_repo: TableRepository,
        manifest_repo: IngestionManifestRepository,
    ):
        """
        Initialize the book snapshots ingestion service.

        Args:
            repo: Repository for the book_snapshot_25 Silver table
            dim_symbol_repo: Repository for dim_symbol table
            manifest_repo: Repository for ingestion manifest
        """
        self.repo = repo
        self.dim_symbol_repo = dim_symbol_repo
        self.manifest_repo = manifest_repo

    def validate(self, data: pl.DataFrame) -> pl.DataFrame:
        """Validate book snapshots data schema and quality."""
        return validate_book_snapshots(data)

    def compute_state(self, valid_data: pl.DataFrame) -> pl.DataFrame:
        """
        Transform validated book snapshots data to final Silver format.

        Note: This is called by the BaseService.update() template method.
        For ingestion from files, use ingest_file() instead.
        """
        # For direct updates (not from file), assume data is already transformed
        return normalize_book_snapshots_schema(valid_data)

    def write(self, result: pl.DataFrame) -> None:
        """Append book snapshots data to the Delta table."""
        if result.is_empty():
            logger.warning("write: skipping empty DataFrame")
            return

        if not hasattr(self.repo, "append"):
            raise NotImplementedError("Repository must support append() for book_snapshot_25")
        self.repo.append(result)

    def ingest_file(
        self,
        meta: BronzeFileMetadata,
        file_id: int,
        *,
        bronze_root: Path | None = None,
    ) -> IngestionResult:
        """
        Ingest a single book snapshots CSV file from Bronze to Silver.

        Args:
            meta: Metadata about the bronze file
            file_id: File ID from manifest repository
            bronze_root: Root path for bronze files (default: LAKE_ROOT/bronze/tardis)

        Returns:
            IngestionResult with row count and timestamp ranges
        """
        if bronze_root is None:
            bronze_root = get_bronze_root("tardis")
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
            parsed_df = parse_tardis_book_snapshots_csv(raw_df)

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

            # 6. Encode fixed-point
            encoded_df = encode_fixed_point(resolved_df, dim_symbol)

            # 7. Add lineage columns
            lineage_df = self._add_lineage(encoded_df, file_id)

            # 8. Add exchange, exchange_id and date
            normalized_exchange = normalize_exchange(meta.exchange)
            final_df = self._add_metadata(lineage_df, normalized_exchange, exchange_id)

            # 9. Normalize schema
            normalized_df = normalize_book_snapshots_schema(final_df)

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
                f"Ingested {validated_df.height} book snapshots from {meta.bronze_file_path} "
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

    def validate_ingested(
        self,
        meta: BronzeFileMetadata,
        file_id: int,
        *,
        bronze_root: Path | None = None,
        sample_size: int = 2000,
        seed: int = 0,
    ) -> tuple[bool, str]:
        """Validate ingested rows against raw file (sampled).

        Compares encoded list columns for a sample of rows joined by file_line_number.
        Returns (ok, message). Message is empty if ok.
        """
        if bronze_root is None:
            bronze_root = get_bronze_root("tardis")
        bronze_path = bronze_root / meta.bronze_file_path

        raw_df = self._read_bronze_csv(bronze_path)
        if raw_df.is_empty():
            return True, ""

        parsed_df = parse_tardis_book_snapshots_csv(raw_df)
        dim_symbol = self.dim_symbol_repo.read_all()
        exchange_id = self._resolve_exchange_id(meta.exchange)

        resolved_df = resolve_symbol_ids(
            parsed_df,
            dim_symbol,
            exchange_id,
            meta.symbol,
            ts_col="ts_local_us",
        )
        encoded_df = encode_fixed_point(resolved_df, dim_symbol)

        raw_encoded = encoded_df.select(
            [
                "file_line_number",
                "bids_px",
                "bids_sz",
                "asks_px",
                "asks_sz",
            ]
        ).with_columns(pl.col("file_line_number").cast(pl.Int32))

        if raw_encoded.is_empty():
            return True, ""

        if sample_size > 0 and raw_encoded.height > sample_size:
            raw_sample = raw_encoded.sample(n=sample_size, seed=seed)
        else:
            raw_sample = raw_encoded

        ingested = (
            pl.scan_delta(self.repo.table_path)
            .filter(pl.col("file_id") == file_id)
            .select(
                [
                    "file_line_number",
                    "bids_px",
                    "bids_sz",
                    "asks_px",
                    "asks_sz",
                ]
            )
            .collect()
            .with_columns(pl.col("file_line_number").cast(pl.Int32))
        )

        if ingested.is_empty():
            return False, "post_ingest_validation_failed: no rows found for file_id"

        missing = raw_sample.join(ingested, on="file_line_number", how="anti")
        joined = raw_sample.join(ingested, on="file_line_number", how="inner", suffix="_ing")
        match_expr = pl.all_horizontal(
            [
                pl.col("bids_px") == pl.col("bids_px_ing"),
                pl.col("bids_sz") == pl.col("bids_sz_ing"),
                pl.col("asks_px") == pl.col("asks_px_ing"),
                pl.col("asks_sz") == pl.col("asks_sz_ing"),
            ]
        ).alias("_row_match")
        matched_by_line = (
            joined.with_columns(match_expr)
            .group_by("file_line_number")
            .agg(pl.col("_row_match").any().alias("_any_match"))
        )
        mismatched_lines = matched_by_line.filter(~pl.col("_any_match"))

        if missing.is_empty() and mismatched_lines.is_empty():
            return True, ""

        missing_lines = missing.select("file_line_number").head(5).to_series().to_list()
        mismatch_lines = mismatched_lines.select("file_line_number").head(5).to_series().to_list()
        return (
            False,
            "post_ingest_validation_failed: "
            f"missing={missing.height}, mismatched={mismatched_lines.height}, "
            f"missing_lines={missing_lines}, mismatched_lines={mismatch_lines}",
        )

    def _read_bronze_csv(self, path: Path) -> pl.DataFrame:
        """Read a bronze CSV file, handling gzip compression."""
        # Read CSV with float types for price/amount columns (they'll be converted to fixed-point later)
        # Use infer_schema_length to avoid schema inference issues
        read_options = {
            "infer_schema_length": 10000,
            "try_parse_dates": False,
        }
        # Preserve raw CSV line numbers (1-based data rows + header).
        # This lets us map validation failures back to the exact source line.
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
            # Treat empty CSVs as empty DataFrames for idempotent ingestion.
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
        """
        Add exchange, exchange_id, and exchange-local date columns.

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
