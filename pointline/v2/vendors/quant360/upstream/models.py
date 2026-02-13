"""Data models for Quant360 upstream archive adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from pointline.io.protocols import BronzeFileMetadata
from pointline.v2.vendors.quant360.types import Quant360ArchiveMeta


@dataclass(frozen=True)
class Quant360ArchiveKey:
    source_filename: str
    archive_sha256: str

    def as_string(self) -> str:
        return f"{self.source_filename}:{self.archive_sha256}"


@dataclass(frozen=True)
class Quant360ArchiveJob:
    archive_path: Path
    archive_meta: Quant360ArchiveMeta
    archive_sha256: str

    @property
    def archive_key(self) -> Quant360ArchiveKey:
        return Quant360ArchiveKey(
            source_filename=self.archive_meta.source_filename,
            archive_sha256=self.archive_sha256,
        )


@dataclass(frozen=True)
class Quant360MemberJob:
    archive_job: Quant360ArchiveJob
    member_path: str
    symbol: str

    @property
    def data_type(self) -> str:
        return self.archive_job.archive_meta.stream_type

    @property
    def exchange(self) -> str:
        return self.archive_job.archive_meta.exchange

    @property
    def trading_date(self) -> date:
        return self.archive_job.archive_meta.trading_date


@dataclass(frozen=True)
class Quant360MemberPayload:
    member_job: Quant360MemberJob
    csv_bytes: bytes


@dataclass(frozen=True)
class Quant360PublishedFile:
    bronze_rel_path: str
    output_path: Path
    output_sha256: str
    file_size_bytes: int
    data_type: str
    exchange: str
    symbol: str
    trading_date: date
    already_exists: bool = False

    def to_bronze_file_metadata(self, *, vendor: str = "quant360") -> BronzeFileMetadata:
        return BronzeFileMetadata(
            vendor=vendor,
            data_type=self.data_type,
            bronze_file_path=self.bronze_rel_path,
            file_size_bytes=self.file_size_bytes,
            last_modified_ts=self.output_path.stat().st_mtime_ns // 1_000,
            sha256=self.output_sha256,
            date=self.trading_date,
        )


@dataclass(frozen=True)
class Quant360LedgerRecord:
    archive_key: Quant360ArchiveKey
    status: str
    updated_at_us: int
    failure_reason: str | None = None
    error_message: str | None = None
    member_count: int | None = None
    published_count: int | None = None


@dataclass(frozen=True)
class Quant360UpstreamRunResult:
    processed_archives: int
    total_members: int
    published: int
    skipped: int
    failed: int
    published_files: list[Quant360PublishedFile] = field(default_factory=list)
    failure_records: list[Quant360LedgerRecord] = field(default_factory=list)
