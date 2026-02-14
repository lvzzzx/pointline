"""Data models for Quant360 upstream archive adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from time import time_ns

from pointline.protocols import BronzeFileMetadata
from pointline.vendors.quant360.types import Quant360ArchiveMeta


def _now_us() -> int:
    return time_ns() // 1_000


@dataclass(frozen=True)
class ArchiveKey:
    source_filename: str
    archive_sha256: str

    def __str__(self) -> str:
        return f"{self.source_filename}:{self.archive_sha256}"


@dataclass(frozen=True)
class ArchiveJob:
    archive_path: Path
    archive_meta: Quant360ArchiveMeta
    archive_sha256: str

    @property
    def key(self) -> ArchiveKey:
        return ArchiveKey(
            source_filename=self.archive_meta.source_filename,
            archive_sha256=self.archive_sha256,
        )


@dataclass(frozen=True)
class MemberJob:
    archive_job: ArchiveJob
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
class MemberPayload:
    member_job: MemberJob
    csv_bytes: bytes


@dataclass(frozen=True)
class PublishedFile:
    bronze_rel_path: str
    output_path: Path
    output_sha256: str
    file_size_bytes: int
    data_type: str
    exchange: str
    symbol: str
    trading_date: date
    already_exists: bool = False

    def to_metadata(self, *, vendor: str = "quant360") -> BronzeFileMetadata:
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
class ArchiveState:
    """State of an archive processing attempt."""

    archive_key: ArchiveKey
    status: str  # "success" or "failed"
    member_count: int
    published_count: int
    updated_at_us: int = field(default_factory=_now_us)
    failure_reason: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class RunResult:
    processed_archives: int
    total_members: int
    published: int
    skipped: int
    failed: int
    published_files: list[PublishedFile] = field(default_factory=list)
    failure_states: list[ArchiveState] = field(default_factory=list)
