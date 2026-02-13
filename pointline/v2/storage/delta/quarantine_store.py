"""Delta-backed quarantine store for v2 ingestion."""

from __future__ import annotations

from pathlib import Path
from time import time_ns

import polars as pl

from pointline.schemas.control import VALIDATION_LOG
from pointline.v2.storage.contracts import QuarantineStore
from pointline.v2.storage.delta._utils import append_delta, validate_against_spec
from pointline.v2.storage.delta.layout import table_path


def _now_us() -> int:
    return time_ns() // 1_000


class DeltaQuarantineStore(QuarantineStore):
    """Persist quarantined rows as validation_log records."""

    def __init__(
        self,
        *,
        silver_root: Path | None = None,
        validation_log_path: Path | None = None,
    ) -> None:
        if validation_log_path is None and silver_root is None:
            raise ValueError("Provide either silver_root or validation_log_path")

        if validation_log_path is None:
            assert silver_root is not None
            validation_log_path = table_path(silver_root=silver_root, table_name="validation_log")
        self.validation_log_path = validation_log_path

    def append(
        self,
        table_name: str,
        df: pl.DataFrame,
        *,
        reason: str,
        file_id: int,
    ) -> None:
        if df.is_empty():
            return

        rows = df.height
        base_ts = _now_us()
        has_file_seq = "file_seq" in df.columns
        has_ts_event = "ts_event_us" in df.columns
        has_symbol = "symbol" in df.columns
        has_symbol_id = "symbol_id" in df.columns

        log_df = pl.DataFrame(
            {
                "file_id": [file_id] * rows,
                "rule_name": [reason] * rows,
                "severity": ["error"] * rows,
                "logged_at_ts_us": [base_ts + i for i in range(rows)],
                "file_seq": df.get_column("file_seq").to_list() if has_file_seq else [None] * rows,
                "field_name": [None] * rows,
                "field_value": [None] * rows,
                "ts_event_us": (
                    df.get_column("ts_event_us").to_list() if has_ts_event else [None] * rows
                ),
                "symbol": df.get_column("symbol").to_list() if has_symbol else [None] * rows,
                "symbol_id": (
                    df.get_column("symbol_id").to_list() if has_symbol_id else [None] * rows
                ),
                "message": [f"quarantined:{table_name}"] * rows,
            },
            schema=VALIDATION_LOG.to_polars(),
        )

        validate_against_spec(log_df, VALIDATION_LOG)
        append_delta(
            self.validation_log_path,
            df=log_df,
            partition_by=VALIDATION_LOG.partition_by,
        )
