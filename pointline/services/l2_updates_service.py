"""L2 updates ingestion service orchestrating the Bronze → Silver pipeline."""

from __future__ import annotations

import json
import logging
import os
import itertools
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

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
from deltalake import DeltaTable
from deltalake.transaction import AddAction

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

                    # Fail fast if batch is not already in replay order.
                    out_of_order = df.select(
                        (
                            (pl.col("ts_local_us") < pl.col("ts_local_us").shift(1))
                            | (
                                (pl.col("ts_local_us") == pl.col("ts_local_us").shift(1))
                                & (pl.col("ingest_seq") < pl.col("ingest_seq").shift(1))
                            )
                            | (
                                (pl.col("ts_local_us") == pl.col("ts_local_us").shift(1))
                                & (pl.col("ingest_seq") == pl.col("ingest_seq").shift(1))
                                & (pl.col("file_id") < pl.col("file_id").shift(1))
                            )
                            | (
                                (pl.col("ts_local_us") == pl.col("ts_local_us").shift(1))
                                & (pl.col("ingest_seq") == pl.col("ingest_seq").shift(1))
                                & (pl.col("file_id") == pl.col("file_id").shift(1))
                                & (
                                    pl.col("file_line_number")
                                    < pl.col("file_line_number").shift(1)
                                )
                            )
                        )
                        .fill_null(False)
                        .any()
                    ).item()
                    if out_of_order:
                        raise ValueError(
                            "l2_updates ingest order violation within batch"
                        )

                    # Enforce deterministic ordering after joins/validation.
                    # Joins can reorder rows; replay requires strict ordering.
                    df = df.sort([
                        "ts_local_us",
                        "ingest_seq",
                        "file_id",
                        "file_line_number",
                    ])

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

            partition_filters = [
                ("exchange", "=", normalized_exchange),
                ("date", "=", partition_date_str),
                ("symbol_id", "=", str(partition_symbol_id)),
            ]

            safe_exchange = normalized_exchange.replace("'", "''")
            predicate = (
                f"exchange = '{safe_exchange}' AND date = '{partition_date_str}' "
                f"AND symbol_id = {partition_symbol_id}"
            )

            single_file = os.getenv("POINTLINE_L2_UPDATES_SINGLE_FILE", "").lower() in (
                "1",
                "true",
                "yes",
            )

            if single_file:
                partition_cols = set(self.repo.partition_by or [])
                keep_cols = [
                    name
                    for name in first_batch.schema.names
                    if name not in partition_cols
                ]
                schema = first_batch.select(keep_cols).schema

                partition_dir = (
                    Path(self.repo.table_path)
                    / f"exchange={normalized_exchange}"
                    / f"date={partition_date_str}"
                    / f"symbol_id={partition_symbol_id}"
                )
                partition_dir.mkdir(parents=True, exist_ok=True)
                file_name = f"part-00000-{uuid4()}-c000.zstd.parquet"
                file_path = partition_dir / file_name

                stats_min: dict[str, object | None] = {col: None for col in keep_cols}
                stats_max: dict[str, object | None] = {col: None for col in keep_cols}
                stats_nulls: dict[str, int] = {col: 0 for col in keep_cols}

                def _accumulate_stats(batch: pa.RecordBatch) -> None:
                    for col in keep_cols:
                        array = batch.column(batch.schema.get_field_index(col))
                        stats_nulls[col] += array.null_count
                        if len(array) == array.null_count:
                            continue
                        batch_min = pc.min(array).as_py()
                        batch_max = pc.max(array).as_py()
                        if stats_min[col] is None or batch_min < stats_min[col]:
                            stats_min[col] = batch_min
                        if stats_max[col] is None or batch_max > stats_max[col]:
                            stats_max[col] = batch_max

                writer_rows = 0
                with pq.ParquetWriter(
                    file_path,
                    schema,
                    compression="zstd",
                ) as writer:
                    batch = first_batch.select(keep_cols)
                    writer.write_table(pa.Table.from_batches([batch]))
                    _accumulate_stats(batch)
                    writer_rows += batch.num_rows

                    for batch in batch_iter:
                        batch = batch.select(keep_cols)
                        writer.write_table(pa.Table.from_batches([batch]))
                        _accumulate_stats(batch)
                        writer_rows += batch.num_rows

                stats_payload = {
                    "numRecords": writer_rows,
                    "minValues": {
                        k: v for k, v in stats_min.items() if v is not None
                    },
                    "maxValues": {
                        k: v for k, v in stats_max.items() if v is not None
                    },
                    "nullCount": stats_nulls,
                }

                add_action = AddAction(
                    path=str(file_path.relative_to(self.repo.table_path)),
                    size=file_path.stat().st_size,
                    partition_values={
                        "exchange": normalized_exchange,
                        "date": partition_date_str,
                        "symbol_id": str(partition_symbol_id),
                    },
                    modification_time=int(file_path.stat().st_mtime * 1000),
                    data_change=True,
                    stats=json.dumps(stats_payload),
                )

                table = DeltaTable(self.repo.table_path)
                table.delete(predicate=predicate)
                table.create_write_transaction(
                    actions=[add_action],
                    mode="append",
                    schema=table.schema().to_arrow(),
                    partition_by=self.repo.partition_by or [],
                )
                total_rows = writer_rows
            else:
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
