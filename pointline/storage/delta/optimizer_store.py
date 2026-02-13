"""Delta-backed partition compaction primitive for v2 storage."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

from deltalake import DeltaTable

from pointline.schemas.registry import get_table_spec
from pointline.storage.contracts import PartitionOptimizer, TableVacuum
from pointline.storage.delta.layout import table_path
from pointline.storage.models import CompactionReport, PartitionCompactionResult, VacuumReport

FilterClause = tuple[str, str, object]
PartitionView = tuple[tuple[str, str], ...]


class DeltaPartitionOptimizer(PartitionOptimizer, TableVacuum):
    """Run explicit partition compaction against a v2 Delta table."""

    def __init__(
        self,
        *,
        silver_root: Path,
        table_paths: Mapping[str, Path] | None = None,
    ) -> None:
        self.silver_root = silver_root
        self.table_paths = dict(table_paths or {})

    def _resolve_path(self, table_name: str) -> Path:
        override = self.table_paths.get(table_name)
        if override is not None:
            return override
        return table_path(silver_root=self.silver_root, table_name=table_name)

    def _render_partition_value(self, value: object) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        return str(value)

    def _normalize_partitions(
        self,
        *,
        partition_keys: tuple[str, ...],
        partitions: list[dict[str, object]],
    ) -> list[tuple[PartitionView, list[FilterClause] | None]]:
        if not partitions:
            return []

        normalized: dict[PartitionView, list[FilterClause] | None] = {}
        expected = set(partition_keys)

        for partition in partitions:
            actual = set(partition)
            if actual != expected:
                raise ValueError(
                    f"Invalid partition keys for table. expected={partition_keys}, "
                    f"got={tuple(sorted(actual))}"
                )

            if not partition_keys:
                normalized[()] = None
                continue

            filters: list[FilterClause] = []
            view_items: list[tuple[str, str]] = []
            for key in partition_keys:
                value = partition[key]
                filters.append((key, "=", value))
                view_items.append((key, self._render_partition_value(value)))
            normalized[tuple(view_items)] = filters

        return sorted(normalized.items(), key=lambda item: item[0])

    def _count_files(
        self, table: DeltaTable, *, partition_filters: list[FilterClause] | None
    ) -> int:
        return len(table.file_uris(partition_filters=partition_filters))

    def _compact_partition(
        self,
        table: DeltaTable,
        *,
        partition_filters: list[FilterClause] | None,
        target_file_size_bytes: int | None,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if target_file_size_bytes is not None:
            kwargs["target_size"] = target_file_size_bytes
        return table.optimize.compact(partition_filters=partition_filters, **kwargs)

    def compact_partitions(
        self,
        *,
        table_name: str,
        partitions: list[dict[str, object]],
        target_file_size_bytes: int | None = None,
        min_small_files: int = 8,
        dry_run: bool = False,
        continue_on_error: bool = True,
    ) -> CompactionReport:
        if min_small_files < 1:
            raise ValueError("min_small_files must be >= 1")

        spec = get_table_spec(table_name)
        table_delta_path = self._resolve_path(table_name)
        if not table_delta_path.exists():
            raise FileNotFoundError(f"Delta table does not exist: {table_delta_path}")

        work_items = self._normalize_partitions(
            partition_keys=spec.partition_by, partitions=partitions
        )

        attempted = 0
        succeeded = 0
        skipped = 0
        failed = 0
        results: list[PartitionCompactionResult] = []

        for partition_view, partition_filters in work_items:
            table_before = DeltaTable(str(table_delta_path))
            before_count = self._count_files(table_before, partition_filters=partition_filters)

            if dry_run:
                skipped += 1
                results.append(
                    PartitionCompactionResult(
                        partition=partition_view,
                        before_file_count=before_count,
                        after_file_count=before_count,
                        rewritten_files=0,
                        added_files=0,
                        skipped_reason="dry_run",
                    )
                )
                continue

            if before_count < min_small_files:
                skipped += 1
                results.append(
                    PartitionCompactionResult(
                        partition=partition_view,
                        before_file_count=before_count,
                        after_file_count=before_count,
                        rewritten_files=0,
                        added_files=0,
                        skipped_reason="below_min_small_files",
                    )
                )
                continue

            attempted += 1
            try:
                metrics = self._compact_partition(
                    table_before,
                    partition_filters=partition_filters,
                    target_file_size_bytes=target_file_size_bytes,
                )
                table_after = DeltaTable(str(table_delta_path))
                after_count = self._count_files(table_after, partition_filters=partition_filters)
                succeeded += 1
                results.append(
                    PartitionCompactionResult(
                        partition=partition_view,
                        before_file_count=before_count,
                        after_file_count=after_count,
                        rewritten_files=int(metrics.get("numFilesRemoved", 0) or 0),
                        added_files=int(metrics.get("numFilesAdded", 0) or 0),
                    )
                )
            except Exception as exc:
                failed += 1
                results.append(
                    PartitionCompactionResult(
                        partition=partition_view,
                        before_file_count=before_count,
                        after_file_count=before_count,
                        rewritten_files=0,
                        added_files=0,
                        error=str(exc),
                    )
                )
                if not continue_on_error:
                    raise

        return CompactionReport(
            table_name=table_name,
            partition_keys=spec.partition_by,
            planned_partitions=len(work_items),
            attempted_partitions=attempted,
            succeeded_partitions=succeeded,
            skipped_partitions=skipped,
            failed_partitions=failed,
            partitions=tuple(results),
        )

    def vacuum_table(
        self,
        *,
        table_name: str,
        retention_hours: int | None = 168,
        dry_run: bool = True,
        enforce_retention_duration: bool = True,
        full: bool = False,
    ) -> VacuumReport:
        if retention_hours is not None and retention_hours < 0:
            raise ValueError("retention_hours must be >= 0 or None")

        # Validate canonical table name early for a clearer failure.
        get_table_spec(table_name)

        table_delta_path = self._resolve_path(table_name)
        if not table_delta_path.exists():
            raise FileNotFoundError(f"Delta table does not exist: {table_delta_path}")

        table = DeltaTable(str(table_delta_path))
        deleted_files = table.vacuum(
            retention_hours=retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=enforce_retention_duration,
            full=full,
        )
        deleted_files_sorted = tuple(sorted(deleted_files))
        return VacuumReport(
            table_name=table_name,
            dry_run=dry_run,
            retention_hours=retention_hours,
            enforce_retention_duration=enforce_retention_duration,
            full=full,
            deleted_count=len(deleted_files_sorted),
            deleted_files=deleted_files_sorted,
        )
