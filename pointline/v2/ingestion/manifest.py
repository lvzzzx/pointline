"""Manifest helpers for the v2 ingestion pipeline."""

from __future__ import annotations

from typing import Any

from pointline.io.protocols import BronzeFileMetadata
from pointline.v2.ingestion.models import IngestionResult


def build_manifest_identity(meta: BronzeFileMetadata) -> tuple[str, str, str, str]:
    return (meta.vendor, meta.data_type, meta.bronze_file_path, meta.sha256)


def update_manifest_status(
    manifest_repo: Any,
    meta: BronzeFileMetadata,
    file_id: int,
    status: str,
    result: IngestionResult,
) -> None:
    manifest_repo.update_status(file_id, status, meta, result)
