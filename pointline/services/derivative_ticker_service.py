"""Derivative ticker ingestion service orchestrating the Bronze → Silver pipeline."""

from __future__ import annotations

import gzip
import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from pointline.config import get_bronze_root, get_exchange_id, normalize_exchange
from pointline.dim_symbol import check_coverage
from pointline.io.protocols import (
    BronzeFileMetadata,
    IngestionManifestRepository,
    IngestionResult,
    TableRepository,
)
from pointline.services.base_service import BaseService
from pointline.tables.derivative_ticker import (
    normalize_derivative_ticker_schema,
    parse_tardis_derivative_ticker_csv,
    resolve_symbol_ids,
    validate_derivative_ticker,
)

logger = logging.getLogger(__name__)


class DerivativeTickerIngestionService(BaseService):
    """Orchestrates derivative_ticker ingestion from Bronze CSV files to Silver Delta tables."""

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
        return validate_derivative_ticker(data)

    def compute_state(self, valid_data: pl.DataFrame) -> pl.DataFrame:
        return normalize_derivative_ticker_schema(valid_data)

    def write(self, result: pl.DataFrame) -> None:
        if result.is_empty():
            logger.warning("write: skipping empty DataFrame")
            return
        if hasattr(self.repo, "append"):
            self.repo.append(result)
        else:
            raise NotImplementedError("Repository must support append() for derivative_ticker")

    def ingest_file(
        self,
        meta: BronzeFileMetadata,
        file_id: int,
        *,
        bronze_root: Path | None = None,
    ) -> IngestionResult:
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
            raw_df = self._read_bronze_csv(bronze_path)
            if raw_df.is_empty():
                logger.warning(f"Empty CSV file: {bronze_path}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=None,
                )

            parsed_df = parse_tardis_derivative_ticker_csv(raw_df)
            dim_symbol = self.dim_symbol_repo.read_all()
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

            resolved_df = resolve_symbol_ids(
                parsed_df,
                dim_symbol,
                exchange_id,
                meta.symbol,
                ts_col="ts_local_us",
            )

            lineage_df = self._add_lineage(resolved_df, file_id)
            normalized_exchange = normalize_exchange(meta.exchange)
            final_df = self._add_metadata(lineage_df, normalized_exchange, exchange_id)
            normalized_df = normalize_derivative_ticker_schema(final_df)
            validated_df = self.validate(normalized_df)

            if validated_df.is_empty():
                logger.warning(f"No valid rows after validation: {bronze_path}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message="All rows filtered by validation",
                )

            self.write(validated_df)

            ts_min = validated_df["ts_local_us"].min()
            ts_max = validated_df["ts_local_us"].max()

            logger.info(
                "Ingested %s derivative_ticker rows from %s (ts range: %s - %s)",
                validated_df.height,
                meta.bronze_file_path,
                ts_min,
                ts_max,
            )

            return IngestionResult(
                row_count=validated_df.height,
                ts_local_min_us=ts_min,
                ts_local_max_us=ts_max,
                error_message=None,
            )
        except Exception as exc:
            error_msg = f"Error ingesting {bronze_path}: {exc}"
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
            "schema_overrides": {
                "exchange": pl.Utf8,
                "symbol": pl.Utf8,
                "timestamp": pl.Int64,
                "local_timestamp": pl.Int64,
                "funding_timestamp": pl.Int64,
                "funding_rate": pl.Float64,
                "predicted_funding_rate": pl.Float64,
                "open_interest": pl.Float64,
                "last_price": pl.Float64,
                "index_price": pl.Float64,
                "mark_price": pl.Float64,
            },
        }
        import inspect

        if "row_index_name" in inspect.signature(pl.read_csv).parameters:
            read_options["row_index_name"] = "file_line_number"
            read_options["row_index_offset"] = 2
        else:
            read_options["row_count_name"] = "file_line_number"
            read_options["row_count_offset"] = 2
        try:
            if path.suffix == ".gz" or str(path).endswith(".csv.gz"):
                with gzip.open(path, "rt", encoding="utf-8") as handle:
                    return pl.read_csv(handle, **read_options)
            return pl.read_csv(path, **read_options)
        except pl.exceptions.NoDataError:
            return pl.DataFrame()

    def _resolve_exchange_id(self, exchange: str) -> int:
        return get_exchange_id(exchange)

    def _check_quarantine(
        self,
        meta: BronzeFileMetadata,
        dim_symbol: pl.DataFrame,
        exchange_id: int,
        parsed_df: pl.DataFrame,
    ) -> tuple[bool, str]:
        from datetime import timedelta

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
                (pl.col("exchange_id") == exchange_id) & (pl.col("exchange_symbol") == meta.symbol)
            )
            if rows.is_empty():
                return False, "missing_symbol"
            return False, "invalid_validity_window"

        return True, ""

    def _add_lineage(self, df: pl.DataFrame, file_id: int) -> pl.DataFrame:
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
        result = df.with_columns(
            [
                pl.lit(exchange, dtype=pl.Utf8).alias("exchange"),
                pl.lit(exchange_id, dtype=pl.Int16).alias("exchange_id"),
            ]
        )
        return result.with_columns(
            [pl.from_epoch(pl.col("ts_local_us"), time_unit="us").cast(pl.Date).alias("date")]
        )
