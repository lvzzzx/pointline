"""IO protocols and data classes for Pointline."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import polars as pl


@runtime_checkable
class TableRepository(Protocol):
    """
    Standard interface for all table repositories.
    Ensures storage-agnostic behavior in the service layer.
    """

    def read_all(self) -> pl.DataFrame:
        """Reads all data from the table as a Polars DataFrame."""
        ...

    def write_full(self, df: pl.DataFrame) -> None:
        """Writes/overwrites the full table with the provided DataFrame."""
        ...

    def merge(self, df: pl.DataFrame, keys: list[str]) -> None:
        """Merges incremental updates into the table based on primary keys."""
        ...


@runtime_checkable
class AppendableTableRepository(TableRepository, Protocol):
    """Optional interface for repositories that support append operations."""

    def append(self, df: pl.DataFrame) -> None:
        """Appends data to the table. Most efficient for immutable event data."""
        ...


@dataclass
class BronzeFileMetadata:
    """Metadata extracted from the source file system before reading contents.

    Required fields:
        - vendor: Vendor identifier
        - data_type: Table type (trades, quotes, l3_orders, klines, etc.)
        - bronze_file_path: Relative path from bronze root
        - file_size_bytes: File size
        - last_modified_ts: Modification timestamp (microseconds)
        - sha256: Content hash for idempotency

    Optional fields (vendor-specific):
        - date: Trading date (if applicable)
        - interval: For klines (1h, 4h, 1d)
        - extra: Vendor-specific metadata
    """

    vendor: str
    data_type: str
    bronze_file_path: str  # The full relative path or key
    file_size_bytes: int
    last_modified_ts: int
    sha256: str
    date: date | None = None
    interval: str | None = None  # For klines: "1h", "4h", "1d", etc.
    extra: dict[str, Any] | None = None


@dataclass
class BronzeLayoutSpec:
    """Vendor-specific bronze layout specification.

    Defines how to discover and extract metadata from bronze files for a given vendor.

    Attributes:
        glob_patterns: List of glob patterns for file discovery (e.g., "exchange=*/type=*/date=*/*.csv.gz")
        required_fields: Set of metadata fields that must be extracted (must include "data_type")
        extract_metadata: Function to parse path/filename and extract metadata fields
        normalize_metadata: Function to combine partial metadata + file stats into BronzeFileMetadata
    """

    glob_patterns: list[str]
    required_fields: set[str]
    extract_metadata: Callable[[Path], dict[str, Any]]
    normalize_metadata: Callable[[dict[str, Any], dict[str, Any]], BronzeFileMetadata]


@dataclass
class BronzeSnapshotManifest:
    """Manifest for v2 API bronze snapshots (manifest + records separation)."""

    schema_version: int
    vendor: str
    dataset: str
    data_type: str
    capture_mode: str
    record_format: str
    complete: bool
    captured_at_us: int
    api_endpoint: str
    request_params: dict[str, Any]
    record_count: int
    records_content_sha256: str
    records_file_sha256: str
    partitions: dict[str, str]
    vendor_effective_ts_us: int | None = None
    expected_record_count: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dict."""
        return {
            "schema_version": self.schema_version,
            "vendor": self.vendor,
            "dataset": self.dataset,
            "data_type": self.data_type,
            "capture_mode": self.capture_mode,
            "record_format": self.record_format,
            "complete": self.complete,
            "captured_at_us": self.captured_at_us,
            "vendor_effective_ts_us": self.vendor_effective_ts_us,
            "api_endpoint": self.api_endpoint,
            "request_params": self.request_params,
            "record_count": self.record_count,
            "expected_record_count": self.expected_record_count,
            "records_content_sha256": self.records_content_sha256,
            "records_file_sha256": self.records_file_sha256,
            "partitions": self.partitions,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> BronzeSnapshotManifest:
        """Deserialize from a dict (e.g. loaded from JSON)."""
        return cls(
            schema_version=d["schema_version"],
            vendor=d["vendor"],
            dataset=d["dataset"],
            data_type=d["data_type"],
            capture_mode=d["capture_mode"],
            record_format=d["record_format"],
            complete=d["complete"],
            captured_at_us=d["captured_at_us"],
            vendor_effective_ts_us=d.get("vendor_effective_ts_us"),
            api_endpoint=d["api_endpoint"],
            request_params=d.get("request_params", {}),
            record_count=d["record_count"],
            expected_record_count=d.get("expected_record_count"),
            records_content_sha256=d["records_content_sha256"],
            records_file_sha256=d["records_file_sha256"],
            partitions=d.get("partitions", {}),
        )

    @classmethod
    def from_file(cls, path: Path) -> BronzeSnapshotManifest:
        """Load manifest from a JSON file."""
        import json

        with open(path, encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


@dataclass
class ApiSnapshotSpec:
    """Vendor dataset contract for API snapshot capture/replay."""

    dataset: str
    data_type: str
    target_table: str
    partition_keys: tuple[str, ...] = ()
    default_glob: str = ""


@dataclass
class ApiCaptureRequest:
    """Capture request passed to vendor plugins."""

    params: dict[str, Any]
    partitions: dict[str, str] | None = None
    captured_at_us: int | None = None


@dataclass
class ApiReplayOptions:
    """Replay options passed to vendor plugins when building table updates."""

    rebuild: bool = False
    effective_ts_us: int | None = None
    partitions: dict[str, str] | None = None
    request: dict[str, Any] | None = None


@runtime_checkable
class BronzeSource(Protocol):
    """Abstraction for a file system scanner."""

    def list_files(self, glob_pattern: str | None = None) -> Iterator[BronzeFileMetadata]:
        """Scans storage using vendor layout spec.

        Args:
            glob_pattern: Optional override pattern. If None, uses vendor's default patterns.

        Returns:
            Iterator of BronzeFileMetadata for discovered files.
        """
        ...


@dataclass
class IngestionResult:
    """Outcomes from the ingestion process."""

    row_count: int
    ts_local_min_us: int
    ts_local_max_us: int
    error_message: str | None = None
    failure_reason: str | None = None
    partial_ingestion: bool = False
    filtered_symbol_count: int = 0
    filtered_row_count: int = 0


@runtime_checkable
class IngestionManifestRepository(Protocol):
    """Abstraction for the state ledger (silver.ingest_manifest)."""

    def resolve_file_id(self, meta: BronzeFileMetadata) -> int:
        """Gets existing ID or mints a new one for a file."""
        ...

    def filter_pending(self, candidates: list[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
        """Returns only files that need processing (efficient batch anti-join)."""
        ...

    def update_status(
        self,
        file_id: int,
        status: str,
        meta: BronzeFileMetadata,
        result: IngestionResult | None = None,
    ) -> None:
        """Records success/failure."""
        ...
