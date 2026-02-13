from __future__ import annotations

from pointline.storage.contracts import PartitionOptimizer, TableVacuum
from pointline.storage.models import (
    CompactionReport,
    PartitionCompactionResult,
    VacuumReport,
)


class _OptimizerImpl:
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
        del table_name
        del partitions
        del target_file_size_bytes
        del min_small_files
        del dry_run
        del continue_on_error
        return CompactionReport(
            table_name="trades",
            partition_keys=("exchange", "trading_date"),
            planned_partitions=0,
            attempted_partitions=0,
            succeeded_partitions=0,
            skipped_partitions=0,
            failed_partitions=0,
            partitions=(),
        )


def test_partition_optimizer_protocol_is_runtime_checkable() -> None:
    assert isinstance(_OptimizerImpl(), PartitionOptimizer)


def test_compaction_report_models_are_stable() -> None:
    result = PartitionCompactionResult(
        partition=(("exchange", "sse"), ("trading_date", "2024-01-02")),
        before_file_count=20,
        after_file_count=3,
        rewritten_files=20,
        added_files=3,
    )
    report = CompactionReport(
        table_name="cn_order_events",
        partition_keys=("exchange", "trading_date"),
        planned_partitions=1,
        attempted_partitions=1,
        succeeded_partitions=1,
        skipped_partitions=0,
        failed_partitions=0,
        partitions=(result,),
    )

    assert report.table_name == "cn_order_events"
    assert report.partitions[0].partition == (
        ("exchange", "sse"),
        ("trading_date", "2024-01-02"),
    )


class _VacuumImpl:
    def vacuum_table(
        self,
        *,
        table_name: str,
        retention_hours: int | None = 168,
        dry_run: bool = True,
        enforce_retention_duration: bool = True,
        full: bool = False,
    ) -> VacuumReport:
        del table_name
        del retention_hours
        del dry_run
        del enforce_retention_duration
        del full
        return VacuumReport(
            table_name="cn_order_events",
            dry_run=True,
            retention_hours=168,
            enforce_retention_duration=True,
            full=False,
            deleted_count=0,
            deleted_files=(),
        )


def test_table_vacuum_protocol_is_runtime_checkable() -> None:
    assert isinstance(_VacuumImpl(), TableVacuum)
