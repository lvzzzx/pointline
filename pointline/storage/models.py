"""Shared v2 storage models."""

from __future__ import annotations

from dataclasses import dataclass

from pointline.protocols import BronzeFileMetadata


@dataclass(frozen=True)
class ManifestIdentity:
    vendor: str
    data_type: str
    bronze_path: str
    file_hash: str

    @classmethod
    def from_meta(cls, meta: BronzeFileMetadata) -> ManifestIdentity:
        return cls(
            vendor=meta.vendor,
            data_type=meta.data_type,
            bronze_path=meta.bronze_file_path,
            file_hash=meta.sha256,
        )

    def as_tuple(self) -> tuple[str, str, str, str]:
        return (self.vendor, self.data_type, self.bronze_path, self.file_hash)


@dataclass(frozen=True)
class PartitionCompactionResult:
    partition: tuple[tuple[str, str], ...]
    before_file_count: int
    after_file_count: int
    rewritten_files: int
    added_files: int
    skipped_reason: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class CompactionReport:
    table_name: str
    partition_keys: tuple[str, ...]
    planned_partitions: int
    attempted_partitions: int
    succeeded_partitions: int
    skipped_partitions: int
    failed_partitions: int
    partitions: tuple[PartitionCompactionResult, ...]


@dataclass(frozen=True)
class VacuumReport:
    table_name: str
    dry_run: bool
    retention_hours: int | None
    enforce_retention_duration: bool
    full: bool
    deleted_count: int
    deleted_files: tuple[str, ...]
