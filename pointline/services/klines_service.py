"""Binance kline ingestion service orchestrating the Bronze → Silver pipeline."""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path

import polars as pl

from pointline.config import get_bronze_root, normalize_exchange, get_exchange_id
from pointline.dim_symbol import check_coverage
from pointline.io.protocols import BronzeFileMetadata, IngestionManifestRepository, IngestionResult
from pointline.io.vendor.binance import normalize_symbol
from pointline.services.base_service import BaseService
from pointline.tables.klines import (
    encode_fixed_point,
    normalize_klines_schema,
    parse_binance_klines_csv,
    resolve_symbol_ids,
    validate_klines,
    RAW_KLINE_COLUMNS,
)

logger = logging.getLogger(__name__)


class KlinesIngestionService(BaseService):
    """Ingest Binance public klines (1h) from Bronze to Silver."""

    def __init__(
        self,
        repo,
        dim_symbol_repo,
        manifest_repo: IngestionManifestRepository,
    ):
        self.repo = repo
        self.dim_symbol_repo = dim_symbol_repo
        self.manifest_repo = manifest_repo

    def validate(self, data: pl.DataFrame) -> pl.DataFrame:
        return validate_klines(data)

    def compute_state(self, valid_data: pl.DataFrame) -> pl.DataFrame:
        return normalize_klines_schema(valid_data)

    def write(self, result: pl.DataFrame) -> None:
        if result.is_empty():
            logger.warning("write: skipping empty DataFrame")
            return
        if hasattr(self.repo, "append"):
            self.repo.append(result)
        else:
            raise NotImplementedError("Repository must support append() for klines")

    def ingest_file(
        self,
        meta: BronzeFileMetadata,
        file_id: int,
        *,
        bronze_root: Path | None = None,
    ) -> IngestionResult:
        if bronze_root is None:
            bronze_root = get_bronze_root("binance_vision")
        bronze_path = bronze_root / meta.bronze_file_path

        if not bronze_path.exists():
            error_msg = f"Bronze file not found: {bronze_path}"
            logger.error(error_msg)
            return IngestionResult(row_count=0, ts_local_min_us=0, ts_local_max_us=0, error_message=error_msg)

        try:
            raw_df = self._read_bronze_csv(bronze_path)
            if raw_df.is_empty():
                return IngestionResult(row_count=0, ts_local_min_us=0, ts_local_max_us=0)

            parsed_df = parse_binance_klines_csv(raw_df)

            dim_symbol = self.dim_symbol_repo.read_all()
            exchange_id = self._resolve_exchange_id(meta.exchange)
            exchange_symbol = normalize_symbol(meta.exchange, meta.symbol)

            is_valid, error_msg = self._check_quarantine(
                dim_symbol,
                exchange_id,
                exchange_symbol,
                parsed_df,
            )
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
                exchange_symbol,
                ts_col="ts_bucket_start_us",
            )

            encoded_df = encode_fixed_point(resolved_df, dim_symbol)

            lineage_df = self._add_lineage(encoded_df, file_id)
            normalized_exchange = normalize_exchange(meta.exchange)
            final_df = self._add_metadata(lineage_df, normalized_exchange, exchange_id)

            normalized_df = normalize_klines_schema(final_df)
            validated_df = self.validate(normalized_df)

            if validated_df.is_empty():
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message="All rows filtered by validation",
                )

            self.write(validated_df)

            ts_min = validated_df["ts_bucket_start_us"].min()
            ts_max = validated_df["ts_bucket_start_us"].max()
            logger.info(
                f"Ingested {validated_df.height} klines from {meta.bronze_file_path} "
                f"(ts range: {ts_min} - {ts_max})"
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
        read_options: dict = {
            "infer_schema_length": 10000,
            "has_header": False,
            "new_columns": RAW_KLINE_COLUMNS,
        }

        try:
            if path.suffix == ".zip":
                with zipfile.ZipFile(path) as zf:
                    csv_name = next(
                        (name for name in zf.namelist() if name.endswith(".csv")),
                        None,
                    )
                    if csv_name is None:
                        return pl.DataFrame()
                    with zf.open(csv_name) as handle:
                        return pl.read_csv(handle, **read_options)
            if path.suffix == ".gz" or str(path).endswith(".csv.gz"):
                import gzip

                with gzip.open(path, "rt", encoding="utf-8") as handle:
                    return pl.read_csv(handle, **read_options)
            return pl.read_csv(path, **read_options)
        except pl.exceptions.NoDataError:
            return pl.DataFrame()

    def _resolve_exchange_id(self, exchange: str) -> int:
        return get_exchange_id(exchange)

    def _check_quarantine(
        self,
        dim_symbol: pl.DataFrame,
        exchange_id: int,
        exchange_symbol: str,
        parsed_df: pl.DataFrame,
    ) -> tuple[bool, str]:
        if parsed_df.is_empty():
            return True, ""

        ts_min = parsed_df["ts_bucket_start_us"].min()
        ts_max = parsed_df["ts_bucket_end_us"].max()
        has_coverage = check_coverage(
            dim_symbol,
            exchange_id,
            exchange_symbol,
            ts_min,
            ts_max,
        )
        if not has_coverage:
            rows = dim_symbol.filter(
                (pl.col("exchange_id") == exchange_id)
                & (pl.col("exchange_symbol") == exchange_symbol)
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
        ingest_seq = pl.int_range(1, df.height + 1, dtype=pl.Int32)
        return df.with_columns(
            [
                pl.lit(file_id, dtype=pl.Int32).alias("file_id"),
                file_line_number.alias("file_line_number"),
                ingest_seq.alias("ingest_seq"),
            ]
        )

    def _add_metadata(self, df: pl.DataFrame, exchange: str, exchange_id: int) -> pl.DataFrame:
        result = df.with_columns(
            [
                pl.lit(exchange, dtype=pl.Utf8).alias("exchange"),
                pl.lit(exchange_id, dtype=pl.Int16).alias("exchange_id"),
            ]
        )
        result = result.with_columns(
            [
                pl.from_epoch(pl.col("ts_bucket_start_us"), time_unit="us")
                .cast(pl.Date)
                .alias("date"),
            ]
        )
        return result
