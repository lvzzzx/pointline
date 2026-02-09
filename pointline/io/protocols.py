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
