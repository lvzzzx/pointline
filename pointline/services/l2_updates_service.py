"""L2 updates ingestion service orchestrating the Bronze → Silver pipeline."""

from __future__ import annotations

import logging
import os
import itertools
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

L2_UPDATES_RAW_COLUMNS = [
    "exchange",
    "symbol",
    "timestamp",
    "local_timestamp",
    "is_snapshot",
    "side",
    "price",
    "amount",
]

DEFAULT_L2_UPDATES_BATCH_SIZE = 200_000
DEFAULT_L2_UPDATES_TARGET_FILE_SIZE_BYTES = 64 * 1024**3


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
            # 1. Load dim_symbol for quarantine check
            dim_symbol = self.dim_symbol_repo.read_all()

            # 2. Quarantine check
            exchange_id = self._resolve_exchange_id(meta.exchange)
            is_valid, error_msg = self._check_quarantine(
                meta, dim_symbol, exchange_id, dim_symbol
            )

            if not is_valid:
                logger.warning(f"File quarantined: {meta.bronze_file_path} - {error_msg}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=error_msg,
                )

            # 3. Stream bronze CSV in batches to avoid loading multi-GB files into memory
            reader = self._read_bronze_csv_batches(bronze_path)

            total_rows = 0
            ts_min: int | None = None
            ts_max: int | None = None
            partition_symbol_id: int | None = None
            partition_date = None
            last_order_key: tuple[int, int, int, int] | None = None

            target_file_size = int(
                os.getenv(
                    "POINTLINE_L2_UPDATES_TARGET_FILE_SIZE_BYTES",
                    str(DEFAULT_L2_UPDATES_TARGET_FILE_SIZE_BYTES),
                )
            )
            if target_file_size <= 0:
                target_file_size = None

            normalized_exchange = normalize_exchange(meta.exchange)

            def _iter_record_batches():
                nonlocal total_rows, ts_min, ts_max, partition_symbol_id, partition_date
                nonlocal last_order_key

                while True:
                    batches = reader.next_batches(1)
                    if not batches:
                        break

                    df = batches[0]
                    if df.is_empty():
                        continue

                    # 4. Parse CSV
                    df = parse_tardis_l2_updates_csv(df)

                    # 5. Resolve symbol IDs (preserve ingest order ties)
                    df = resolve_symbol_ids(
                        df,
                        dim_symbol,
                        exchange_id,
                        meta.symbol,
                        ts_col="ts_local_us",
                        tie_breaker_cols=["file_line_number"],
                    )

                    # 6. Encode fixed-point
                    df = encode_l2_updates_fixed_point(df, dim_symbol)

                    # 7. Add lineage columns
                    df = self._add_lineage(df, file_id)

                    # 8. Add exchange, exchange_id and date
                    df = self._add_metadata(df, normalized_exchange, exchange_id)

                    # 9. Normalize schema
                    df = normalize_l2_updates_schema(df)

                    # 10. Validate
                    df = self.validate(df)

                    if df.is_empty():
                        logger.warning(f"No valid rows after validation: {bronze_path}")
                        continue

                    batch_symbol_min = df["symbol_id"].min()
                    batch_symbol_max = df["symbol_id"].max()
                    if batch_symbol_min != batch_symbol_max:
                        raise ValueError(
                            "l2_updates ingest expects one symbol_id per bronze file"
                        )
                    if partition_symbol_id is None:
                        partition_symbol_id = int(batch_symbol_min)
                    elif int(batch_symbol_min) != partition_symbol_id:
                        raise ValueError(
                            "l2_updates ingest expects a single symbol_id per partition"
                        )

                    batch_date_min = df["date"].min()
                    batch_date_max = df["date"].max()
                    if batch_date_min != batch_date_max:
                        raise ValueError(
                            "l2_updates ingest expects one date per bronze file"
                        )
                    if partition_date is None:
                        partition_date = batch_date_min
                    elif batch_date_min != partition_date:
                        raise ValueError(
                            "l2_updates ingest expects a single date per partition"
                        )

                    first_key = (
                        int(df["ts_local_us"][0]),
                        int(df["ingest_seq"][0]),
                        int(df["file_id"][0]),
                        int(df["file_line_number"][0]),
                    )
                    last_key = (
                        int(df["ts_local_us"][-1]),
                        int(df["ingest_seq"][-1]),
                        int(df["file_id"][-1]),
                        int(df["file_line_number"][-1]),
                    )
                    if last_order_key is not None and first_key < last_order_key:
                        raise ValueError(
                            "l2_updates ingest order violation between batches"
                        )
                    last_order_key = last_key

                    # Update stats
                    batch_min = df["ts_local_us"].min()
                    batch_max = df["ts_local_us"].max()
                    if batch_min is not None:
                        ts_min = batch_min if ts_min is None else min(ts_min, batch_min)
                    if batch_max is not None:
                        ts_max = batch_max if ts_max is None else max(ts_max, batch_max)
                    total_rows += df.height

                    table = df.to_arrow()
                    for record_batch in table.to_batches():
                        yield record_batch

            batch_iter = _iter_record_batches()

            try:
                first_batch = next(batch_iter)
            except StopIteration:
                logger.warning(f"Empty CSV file: {bronze_path}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=None,
                )

            if partition_date is None or partition_symbol_id is None:
                raise ValueError("l2_updates ingest could not resolve partition keys")

            if hasattr(partition_date, "isoformat"):
                partition_date_str = partition_date.isoformat()
            else:
                partition_date_str = str(partition_date)

            if partition_date_str != meta.date.isoformat():
                raise ValueError(
                    "l2_updates ingest date mismatch between metadata and ts_local_us"
                )

            safe_exchange = normalized_exchange.replace("'", "''")
            predicate = (
                f"exchange = '{safe_exchange}' AND date = '{partition_date_str}' "
                f"AND symbol_id = {partition_symbol_id}"
            )

            import pyarrow as pa

            record_batch_reader = pa.RecordBatchReader.from_batches(
                first_batch.schema,
                itertools.chain([first_batch], batch_iter),
            )

            if hasattr(self.repo, "overwrite_partition"):
                self.repo.overwrite_partition(
                    record_batch_reader,
                    predicate=predicate,
                    target_file_size=target_file_size,
                )
            else:
                raise NotImplementedError(
                    "Repository must support overwrite_partition() for l2_updates"
                )

            if total_rows == 0:
                logger.warning(f"Empty CSV file: {bronze_path}")
                return IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=None,
                )

            logger.info(
                f"Ingested {total_rows} l2_updates from {meta.bronze_file_path} "
                f"(ts range: {ts_min} - {ts_max})"
            )

            return IngestionResult(
                row_count=total_rows,
                ts_local_min_us=ts_min or 0,
                ts_local_max_us=ts_max or 0,
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
        return pl.read_csv(path, **read_options)

    def _read_bronze_csv_batches(self, path: Path) -> pl.io.csv.BatchedCsvReader:
        batch_size = int(
            os.getenv("POINTLINE_L2_UPDATES_BATCH_SIZE", str(DEFAULT_L2_UPDATES_BATCH_SIZE))
        )
        read_options = {
            "infer_schema_length": 10000,
            "try_parse_dates": False,
            "low_memory": True,
            "batch_size": batch_size,
            "columns": L2_UPDATES_RAW_COLUMNS,
            "row_index_name": "file_line_number",
            "row_index_offset": 1,
        }
        return pl.read_csv_batched(path, **read_options)

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
        if "file_line_number" in df.columns:
            file_line_number = pl.col("file_line_number").cast(pl.Int32)
        else:
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
