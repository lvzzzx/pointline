"""L2 updates ingestion service orchestrating the Bronze → Silver pipeline."""

from __future__ import annotations

import gzip
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import polars as pl

from pointline.config import LAKE_ROOT, normalize_exchange, get_exchange_id
from pointline.dim_symbol import check_coverage
from pointline.io.protocols import (
    BronzeFileMetadata,
    IngestionManifestRepository,
    IngestionResult,
    TableRepository,
)
from pointline.services.base_service import BaseService
from pointline.l2_updates import (
    encode_l2_updates_fixed_point,
    normalize_l2_updates_schema,
    parse_tardis_l2_updates_csv,
    resolve_symbol_ids,
    validate_l2_updates,
)

logger = logging.getLogger(__name__)


class L2UpdatesIngestionService(BaseService):
    """
    Orchestrates l2_updates ingestion from Bronze CSV files to Silver Delta tables.
    """

    def __init__(
        self,
        repo: TableRepository,
        dim_symbol_repo: TableRepository,
        manifest_repo: IngestionManifestRepository,
    ):
        self.repo = repo
        self.dim_symbol_repo = dim_symbol_repo
        self.manifest_repo = manifest_repo

    def validate(self, data: pl.DataFrame) -> pl.DataFrame:
        return validate_l2_updates(data)

    def compute_state(self, valid_data: pl.DataFrame) -> pl.DataFrame:
        return normalize_l2_updates_schema(valid_data)

    def write(self, result: pl.DataFrame) -> None:
        if result.is_empty():
            logger.warning("write: skipping empty DataFrame")
            return

        if hasattr(self.repo, "append"):
            self.repo.append(result)
        else:
            raise NotImplementedError("Repository must support append() for l2_updates")

    def ingest_file(
        self,
        meta: BronzeFileMetadata,
        file_id: int,
        *,
        bronze_root: Path | None = None,
    ) -> IngestionResult:
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
            df = self._read_bronze_csv(bronze_path)

            if df.is_empty():
                logger.warning(f"Empty CSV file: {bronze_path}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=None,
                )

            # 2. Parse CSV
            df = parse_tardis_l2_updates_csv(df)

            # 3. Load dim_symbol for quarantine check
            dim_symbol = self.dim_symbol_repo.read_all()

            # 4. Quarantine check
            exchange_id = self._resolve_exchange_id(meta.exchange)
            is_valid, error_msg = self._check_quarantine(
                meta, dim_symbol, exchange_id, df
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
            df = resolve_symbol_ids(
                df,
                dim_symbol,
                exchange_id,
                meta.symbol,
                ts_col="ts_local_us",
            )

            # 6. Encode fixed-point
            df = encode_l2_updates_fixed_point(df, dim_symbol)

            # 7. Add lineage columns
            df = self._add_lineage(df, file_id)

            # 8. Add exchange, exchange_id and date
            normalized_exchange = normalize_exchange(meta.exchange)
            df = self._add_metadata(df, normalized_exchange, exchange_id)

            # 9. Normalize schema
            df = normalize_l2_updates_schema(df)

            # 10. Validate
            df = self.validate(df)

            if df.is_empty():
                logger.warning(f"No valid rows after validation: {bronze_path}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message="All rows filtered by validation",
                )

            # 11. Append to Delta table
            self.write(df)

            # 12. Compute result stats
            ts_min = df["ts_local_us"].min()
            ts_max = df["ts_local_us"].max()

            logger.info(
                f"Ingested {df.height} l2_updates from {meta.bronze_file_path} "
                f"(ts range: {ts_min} - {ts_max})"
            )

            return IngestionResult(
                row_count=df.height,
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
        read_options = {
            "infer_schema_length": 10000,
            "try_parse_dates": False,
        }
        if path.suffix == ".gz" or str(path).endswith(".csv.gz"):
            with gzip.open(path, "rt", encoding="utf-8") as f:
                return pl.read_csv(f, **read_options)
        else:
            return pl.read_csv(path, **read_options)

    def _resolve_exchange_id(self, exchange: str) -> int:
        return get_exchange_id(exchange)

    def _check_quarantine(
        self,
        meta: BronzeFileMetadata,
        dim_symbol: pl.DataFrame,
        exchange_id: int,
        parsed_df: pl.DataFrame,
    ) -> tuple[bool, str]:
        file_date = meta.date
        day_start = datetime.combine(file_date, datetime.min.time(), tzinfo=timezone.utc)
        day_start_us = int(day_start.timestamp() * 1_000_000)

        day_end = datetime.combine(
            file_date + timedelta(days=1),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        day_end_us = int(day_end.timestamp() * 1_000_000)

        has_coverage = check_coverage(
            dim_symbol,
            exchange_id,
            meta.symbol,
            day_start_us,
            day_end_us,
        )

        if not has_coverage:
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
        file_line_number = pl.int_range(1, df.height + 1, dtype=pl.Int32)
        return df.with_columns([
            pl.lit(file_id, dtype=pl.Int32).alias("file_id"),
            file_line_number.alias("file_line_number"),
            file_line_number.alias("ingest_seq"),
        ])

    def _add_metadata(self, df: pl.DataFrame, exchange: str, exchange_id: int) -> pl.DataFrame:
        result = df.with_columns([
            pl.lit(exchange, dtype=pl.Utf8).alias("exchange"),
            pl.lit(exchange_id, dtype=pl.Int16).alias("exchange_id"),
        ])

        result = result.with_columns([
            pl.from_epoch(pl.col("ts_local_us"), time_unit="us")
            .cast(pl.Date)
            .alias("date"),
        ])

        return result
