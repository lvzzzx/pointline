"""Function-first v2 ingestion pipeline."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import polars as pl

from pointline.io.protocols import BronzeFileMetadata
from pointline.schemas.registry import get_table_spec
from pointline.schemas.types import (
    INGEST_STATUS_FAILED,
    INGEST_STATUS_QUARANTINED,
    INGEST_STATUS_SUCCESS,
)
from pointline.v2.ingestion.lineage import assign_lineage
from pointline.v2.ingestion.manifest import update_manifest_status
from pointline.v2.ingestion.models import IngestionResult
from pointline.v2.ingestion.normalize import normalize_to_table_spec
from pointline.v2.ingestion.pit import check_pit_coverage
from pointline.v2.ingestion.timezone import derive_trading_date_frame

Parser = Callable[[BronzeFileMetadata], pl.DataFrame]
Writer = Callable[[str, pl.DataFrame], None]

_TABLE_ALIASES: dict[str, str] = {
    "trades": "trades",
    "quotes": "quotes",
    "orderbook_updates": "orderbook_updates",
}


def _resolve_table_name(data_type: str) -> str:
    if data_type not in _TABLE_ALIASES:
        supported = ", ".join(sorted(_TABLE_ALIASES))
        raise ValueError(f"Unsupported v2 data_type '{data_type}'. Supported: {supported}")
    return _TABLE_ALIASES[data_type]


def _result(
    *,
    status: str,
    file_id: int | None,
    row_count: int,
    rows_written: int,
    rows_quarantined: int,
    skipped: bool = False,
    failure_reason: str | None = None,
    error_message: str | None = None,
    trading_date_min: Any | None = None,
    trading_date_max: Any | None = None,
) -> IngestionResult:
    return IngestionResult(
        status=status,
        file_id=file_id,
        row_count=row_count,
        rows_written=rows_written,
        rows_quarantined=rows_quarantined,
        skipped=skipped,
        failure_reason=failure_reason,
        error_message=error_message,
        trading_date_min=trading_date_min,
        trading_date_max=trading_date_max,
    )


def ingest_file(
    meta: BronzeFileMetadata,
    *,
    parser: Parser,
    manifest_repo: Any,
    writer: Writer,
    dim_symbol_df: pl.DataFrame,
    force: bool = False,
    dry_run: bool = False,
) -> IngestionResult:
    """Ingest a single Bronze file through the clean v2 core path."""

    table_name = _resolve_table_name(meta.data_type)
    spec = get_table_spec(table_name)

    if not force and not manifest_repo.filter_pending([meta]):
        return _result(
            status=INGEST_STATUS_SUCCESS,
            file_id=None,
            row_count=0,
            rows_written=0,
            rows_quarantined=0,
            skipped=True,
        )

    file_id = 0 if dry_run else manifest_repo.resolve_file_id(meta)

    try:
        parsed = parser(meta)
    except Exception as exc:  # pragma: no cover - defensive path
        result = _result(
            status=INGEST_STATUS_FAILED,
            file_id=file_id,
            row_count=0,
            rows_written=0,
            rows_quarantined=0,
            failure_reason="parser_error",
            error_message=str(exc),
        )
        if not dry_run:
            update_manifest_status(manifest_repo, meta, file_id, INGEST_STATUS_FAILED, result)
        return result

    if parsed.is_empty():
        result = _result(
            status=INGEST_STATUS_FAILED,
            file_id=file_id,
            row_count=0,
            rows_written=0,
            rows_quarantined=0,
            failure_reason="empty_parse",
            error_message="Parser returned no rows",
        )
        if not dry_run:
            update_manifest_status(manifest_repo, meta, file_id, INGEST_STATUS_FAILED, result)
        return result

    try:
        with_trading_date = derive_trading_date_frame(parsed)
        valid_rows, quarantined_rows, quarantine_reason = check_pit_coverage(
            with_trading_date,
            dim_symbol_df,
        )

        if valid_rows.is_empty():
            result = _result(
                status=INGEST_STATUS_QUARANTINED,
                file_id=file_id,
                row_count=with_trading_date.height,
                rows_written=0,
                rows_quarantined=quarantined_rows.height,
                failure_reason=quarantine_reason,
                error_message="All rows quarantined by PIT coverage",
            )
            if not dry_run:
                update_manifest_status(
                    manifest_repo, meta, file_id, INGEST_STATUS_QUARANTINED, result
                )
            return result

        with_lineage = assign_lineage(valid_rows, file_id=file_id)
        normalized = normalize_to_table_spec(with_lineage, spec)

        if not dry_run:
            writer(table_name, normalized)

        result = _result(
            status=INGEST_STATUS_SUCCESS,
            file_id=file_id,
            row_count=with_trading_date.height,
            rows_written=normalized.height,
            rows_quarantined=quarantined_rows.height,
            trading_date_min=normalized.get_column("trading_date").min(),
            trading_date_max=normalized.get_column("trading_date").max(),
        )
        if not dry_run:
            update_manifest_status(manifest_repo, meta, file_id, INGEST_STATUS_SUCCESS, result)
        return result
    except Exception as exc:  # pragma: no cover - defensive path
        result = _result(
            status=INGEST_STATUS_FAILED,
            file_id=file_id,
            row_count=parsed.height,
            rows_written=0,
            rows_quarantined=0,
            failure_reason="pipeline_error",
            error_message=str(exc),
        )
        if not dry_run:
            update_manifest_status(manifest_repo, meta, file_id, INGEST_STATUS_FAILED, result)
        return result
