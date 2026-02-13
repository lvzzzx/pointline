"""Function-first v2 ingestion pipeline."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import polars as pl

from pointline.ingestion.cn_validation import apply_cn_exchange_validations
from pointline.ingestion.event_validation import apply_event_validations
from pointline.ingestion.lineage import assign_lineage
from pointline.ingestion.manifest import update_manifest_status
from pointline.ingestion.models import IngestionResult
from pointline.ingestion.normalize import normalize_to_table_spec
from pointline.ingestion.pit import check_pit_coverage
from pointline.ingestion.timezone import derive_trading_date_frame
from pointline.protocols import BronzeFileMetadata
from pointline.schemas.registry import get_table_spec
from pointline.schemas.types import (
    INGEST_STATUS_FAILED,
    INGEST_STATUS_QUARANTINED,
    INGEST_STATUS_SUCCESS,
)
from pointline.storage.contracts import EventStore, ManifestStore, QuarantineStore
from pointline.vendors.quant360 import canonicalize_quant360_frame

Parser = Callable[[BronzeFileMetadata], pl.DataFrame]
Writer = Callable[[str, pl.DataFrame], None] | EventStore
QuarantineBatch = tuple[pl.DataFrame, str | None]

_TABLE_ALIASES: dict[str, str] = {
    "trades": "trades",
    "quotes": "quotes",
    "orderbook_updates": "orderbook_updates",
    "incremental_book_L2": "orderbook_updates",
    "incremental_book_l2": "orderbook_updates",
    "cn_order_events": "cn_order_events",
    "order_new": "cn_order_events",
    "l3_orders": "cn_order_events",
    "cn_tick_events": "cn_tick_events",
    "tick_new": "cn_tick_events",
    "l3_ticks": "cn_tick_events",
    "cn_l2_snapshots": "cn_l2_snapshots",
    "L2_new": "cn_l2_snapshots",
    "l2_new": "cn_l2_snapshots",
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


def _combine_reasons(*reasons: str | None) -> str | None:
    ordered = list(dict.fromkeys(reason for reason in reasons if reason))
    if not ordered:
        return None
    return "+".join(ordered)


def _write_rows(writer: Writer, table_name: str, df: pl.DataFrame) -> None:
    if callable(writer):
        writer(table_name, df)
        return
    writer.append(table_name, df)


def _append_quarantine(
    quarantine_store: QuarantineStore | None,
    *,
    dry_run: bool,
    file_id: int | None,
    table_name: str,
    rows: pl.DataFrame,
    reason: str | None,
) -> None:
    if dry_run or quarantine_store is None or rows.is_empty():
        return
    if file_id is None:
        raise ValueError("file_id must be present when writing quarantine rows")
    quarantine_store.append(
        table_name,
        rows,
        reason=reason or "quarantined",
        file_id=file_id,
    )


def _append_quarantine_batches(
    quarantine_store: QuarantineStore | None,
    *,
    dry_run: bool,
    file_id: int | None,
    table_name: str,
    batches: tuple[QuarantineBatch, ...],
) -> None:
    for rows, reason in batches:
        _append_quarantine(
            quarantine_store,
            dry_run=dry_run,
            file_id=file_id,
            table_name=table_name,
            rows=rows,
            reason=reason,
        )


def _concat_rows_like(
    *,
    template: pl.DataFrame,
    rows: tuple[pl.DataFrame, ...],
) -> pl.DataFrame:
    non_empty = [frame for frame in rows if not frame.is_empty()]
    if not non_empty:
        return template.head(0)
    return pl.concat(non_empty, how="vertical")


def ingest_file(
    meta: BronzeFileMetadata,
    *,
    parser: Parser,
    manifest_repo: ManifestStore,
    writer: Writer,
    dim_symbol_df: pl.DataFrame,
    quarantine_store: QuarantineStore | None = None,
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
        canonicalized = (
            canonicalize_quant360_frame(parsed, table_name=table_name)
            if meta.vendor == "quant360"
            else parsed
        )
        with_trading_date = derive_trading_date_frame(canonicalized)

        generic_validated_rows, generic_quarantined_rows, generic_quarantine_reason = (
            apply_event_validations(with_trading_date, table_name=table_name)
        )
        validated_rows, cn_quarantined_rows, cn_quarantine_reason = apply_cn_exchange_validations(
            generic_validated_rows,
            table_name=table_name,
        )
        rule_quarantine_batches: tuple[QuarantineBatch, ...] = (
            (generic_quarantined_rows, generic_quarantine_reason),
            (cn_quarantined_rows, cn_quarantine_reason),
        )
        rule_quarantined_rows = _concat_rows_like(
            template=with_trading_date,
            rows=(generic_quarantined_rows, cn_quarantined_rows),
        )
        rule_quarantine_reason = _combine_reasons(generic_quarantine_reason, cn_quarantine_reason)

        if validated_rows.is_empty():
            _append_quarantine_batches(
                quarantine_store,
                dry_run=dry_run,
                file_id=file_id,
                table_name=table_name,
                batches=rule_quarantine_batches,
            )
            result = _result(
                status=INGEST_STATUS_QUARANTINED,
                file_id=file_id,
                row_count=with_trading_date.height,
                rows_written=0,
                rows_quarantined=rule_quarantined_rows.height,
                failure_reason=rule_quarantine_reason,
                error_message="All rows quarantined by v2 validation rules",
            )
            if not dry_run:
                update_manifest_status(
                    manifest_repo, meta, file_id, INGEST_STATUS_QUARANTINED, result
                )
            return result

        valid_rows, pit_quarantined_rows, pit_quarantine_reason = check_pit_coverage(
            validated_rows,
            dim_symbol_df,
        )
        total_quarantined = rule_quarantined_rows.height + pit_quarantined_rows.height
        quarantine_reason = _combine_reasons(rule_quarantine_reason, pit_quarantine_reason)

        if valid_rows.is_empty():
            _append_quarantine_batches(
                quarantine_store,
                dry_run=dry_run,
                file_id=file_id,
                table_name=table_name,
                batches=(
                    *rule_quarantine_batches,
                    (pit_quarantined_rows, pit_quarantine_reason),
                ),
            )
            result = _result(
                status=INGEST_STATUS_QUARANTINED,
                file_id=file_id,
                row_count=with_trading_date.height,
                rows_written=0,
                rows_quarantined=total_quarantined,
                failure_reason=quarantine_reason,
                error_message="All rows quarantined by v2 validation/PIT coverage",
            )
            if not dry_run:
                update_manifest_status(
                    manifest_repo, meta, file_id, INGEST_STATUS_QUARANTINED, result
                )
            return result

        with_lineage = assign_lineage(valid_rows, file_id=file_id)
        normalized = normalize_to_table_spec(with_lineage, spec)

        _append_quarantine_batches(
            quarantine_store,
            dry_run=dry_run,
            file_id=file_id,
            table_name=table_name,
            batches=(
                *rule_quarantine_batches,
                (pit_quarantined_rows, pit_quarantine_reason),
            ),
        )

        if not dry_run:
            _write_rows(writer, table_name, normalized)

        result = _result(
            status=INGEST_STATUS_SUCCESS,
            file_id=file_id,
            row_count=with_trading_date.height,
            rows_written=normalized.height,
            rows_quarantined=total_quarantined,
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
