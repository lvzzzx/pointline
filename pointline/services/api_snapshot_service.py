"""Generic API metadata capture/replay service."""

from __future__ import annotations

import gzip
import json
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from pointline.config import get_bronze_root, get_table_path
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.local_source import LocalBronzeSource
from pointline.io.protocols import (
    ApiCaptureRequest,
    ApiReplayOptions,
    BronzeSnapshotManifest,
    IngestionResult,
)
from pointline.io.snapshot_utils import compute_canonical_content_hash, compute_file_hash
from pointline.io.vendors import get_vendor
from pointline.services.dim_asset_stats_service import DimAssetStatsService
from pointline.services.dim_symbol_service import DimSymbolService

logger = logging.getLogger(__name__)


def _sanitize_component(value: str) -> str:
    """Sanitize a string for use in file paths."""
    clean = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return clean or "unknown"


def _extract_partition_value(relative_path: str, key: str) -> str | None:
    """Extract partition value from a path."""
    token = f"{key}="
    for part in Path(relative_path).parts:
        if part.startswith(token):
            value = part.split("=", 1)[1].strip()
            return value or None
    return None


def _sanitize_request_payload(params: dict[str, Any]) -> dict[str, Any]:
    """Sanitize request payload by redacting sensitive keys."""
    redacted_keys = {"api_key", "token", "authorization", "auth"}
    sanitized: dict[str, Any] = {}
    for key, value in params.items():
        if key.lower() in redacted_keys:
            sanitized[key] = "***"
        else:
            sanitized[key] = value
    return sanitized


@dataclass
class ApiCaptureResult:
    """Result of an API capture operation."""

    vendor: str
    dataset: str
    bronze_root: Path
    path: Path
    snapshot_ts_us: int
    row_count: int
    manifest_path: Path | None = None
    records_content_sha256: str | None = None


@dataclass
class ApiReplayFileResult:
    """Result of replaying a single API snapshot file."""

    bronze_file_path: str
    status: str
    row_count: int
    error_message: str | None = None


@dataclass
class ApiReplaySummary:
    """Summary of an API replay operation."""

    vendor: str
    dataset: str
    discovered_files: int
    processed_files: int
    success_count: int
    failed_count: int
    file_results: list[ApiReplayFileResult]


class ApiSnapshotService:
    """Capture API snapshots to bronze and replay via manifest semantics.

    Supports both v1 (per-record envelope) and v2 (manifest + records) formats.
    New captures use v2 format by default.
    """

    SNAPSHOT_SCHEMA_VERSION_V1 = 1
    SNAPSHOT_SCHEMA_VERSION_V2 = 2

    def capture(
        self,
        *,
        vendor: str,
        dataset: str,
        request: ApiCaptureRequest,
        capture_root: str | Path | None = None,
        bronze_format_version: int = 2,
    ) -> ApiCaptureResult:
        plugin = get_vendor(vendor)
        if not plugin.supports_api_snapshots:
            raise ValueError(f"Vendor '{vendor}' does not support API snapshots")

        specs = plugin.get_api_snapshot_specs()
        if dataset not in specs:
            raise ValueError(f"Vendor '{vendor}' does not define API snapshot dataset '{dataset}'")
        spec = specs[dataset]

        snapshot_ts_us = request.captured_at_us or int(time.time() * 1_000_000)
        snapshot_date = request.partitions.get("date") if request.partitions else None
        if not snapshot_date:
            snapshot_date = time.strftime("%Y-%m-%d", time.gmtime(snapshot_ts_us / 1_000_000))

        partitions: dict[str, str] = {}
        if request.partitions:
            partitions.update({k: str(v) for k, v in request.partitions.items() if v is not None})

        for key in spec.partition_keys:
            if key not in partitions:
                raise ValueError(
                    f"Missing required partition '{key}' for {vendor}:{dataset} capture request"
                )

        bronze_root = Path(capture_root).expanduser() if capture_root else get_bronze_root(vendor)
        data_type = spec.data_type
        out_dir = bronze_root / f"type={data_type}"
        for key in spec.partition_keys:
            out_dir = out_dir / f"{key}={_sanitize_component(partitions[key])}"
        out_dir = out_dir / f"date={snapshot_date}" / f"captured_ts={snapshot_ts_us}"
        out_dir.mkdir(parents=True, exist_ok=True)

        records = plugin.capture_api_snapshot(dataset, request)

        if bronze_format_version >= 2:
            return self._write_v2_snapshot(
                vendor=vendor,
                dataset=dataset,
                spec=spec,
                records=records,
                request=request,
                snapshot_ts_us=snapshot_ts_us,
                snapshot_date=snapshot_date,
                partitions=partitions,
                bronze_root=bronze_root,
                out_dir=out_dir,
            )

        # v1 format (backward compat)
        return self._write_v1_snapshot(
            vendor=vendor,
            dataset=dataset,
            records=records,
            request=request,
            snapshot_ts_us=snapshot_ts_us,
            snapshot_date=snapshot_date,
            partitions=partitions,
            bronze_root=bronze_root,
            out_dir=out_dir,
        )

    def _write_v1_snapshot(
        self,
        *,
        vendor: str,
        dataset: str,
        records: list[dict[str, Any]],
        request: ApiCaptureRequest,
        snapshot_ts_us: int,
        snapshot_date: str,
        partitions: dict[str, str],
        bronze_root: Path,
        out_dir: Path,
    ) -> ApiCaptureResult:
        out_path = out_dir / f"{vendor}_{dataset}_{snapshot_ts_us}.jsonl.gz"
        envelope_request = _sanitize_request_payload(request.params)
        with gzip.open(out_path, "wt", encoding="utf-8") as handle:
            for record in records:
                payload = {
                    "schema_version": self.SNAPSHOT_SCHEMA_VERSION_V1,
                    "vendor": vendor,
                    "dataset": dataset,
                    "captured_at_us": snapshot_ts_us,
                    "snapshot_ts_us": snapshot_ts_us,
                    "partitions": {"date": snapshot_date, **partitions},
                    "request": envelope_request,
                    "record": record,
                }
                handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")))
                handle.write("\n")

        return ApiCaptureResult(
            vendor=vendor,
            dataset=dataset,
            bronze_root=bronze_root,
            path=out_path,
            snapshot_ts_us=snapshot_ts_us,
            row_count=len(records),
        )

    def _write_v2_snapshot(
        self,
        *,
        vendor: str,
        dataset: str,
        spec: Any,
        records: list[dict[str, Any]],
        request: ApiCaptureRequest,
        snapshot_ts_us: int,
        snapshot_date: str,
        partitions: dict[str, str],
        bronze_root: Path,
        out_dir: Path,
    ) -> ApiCaptureResult:
        """Write v2 bronze format: _manifest.json + records.jsonl.gz."""
        envelope_request = _sanitize_request_payload(request.params)

        # Compute canonical content hash before compression
        records_content_sha256 = compute_canonical_content_hash(records)

        # Write records file
        records_path = out_dir / "records.jsonl.gz"
        with gzip.open(records_path, "wt", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
                handle.write("\n")

        # Compute file hash of compressed artifact
        records_file_sha256 = compute_file_hash(records_path)

        # Build and write manifest
        manifest = BronzeSnapshotManifest(
            schema_version=self.SNAPSHOT_SCHEMA_VERSION_V2,
            vendor=vendor,
            dataset=dataset,
            data_type=spec.data_type,
            capture_mode="full_snapshot",
            record_format="jsonl.gz",
            complete=True,
            captured_at_us=snapshot_ts_us,
            vendor_effective_ts_us=None,
            api_endpoint=dataset,
            request_params=envelope_request,
            record_count=len(records),
            expected_record_count=None,
            records_content_sha256=records_content_sha256,
            records_file_sha256=records_file_sha256,
            partitions={"date": snapshot_date, **partitions},
        )

        manifest_path = out_dir / "_manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest.to_dict(), f, indent=2, sort_keys=True)

        return ApiCaptureResult(
            vendor=vendor,
            dataset=dataset,
            bronze_root=bronze_root,
            path=out_dir,
            snapshot_ts_us=snapshot_ts_us,
            row_count=len(records),
            manifest_path=manifest_path,
            records_content_sha256=records_content_sha256,
        )

    def replay(
        self,
        *,
        vendor: str,
        dataset: str,
        bronze_root: str | Path | None = None,
        glob_pattern: str | None = None,
        exchange: str | None = None,
        manifest_path: str | Path | None = None,
        table_path: str | Path | None = None,
        force: bool = False,
        rebuild: bool = False,
        effective_ts_us: int | None = None,
    ) -> ApiReplaySummary:
        plugin = get_vendor(vendor)
        if not plugin.supports_api_snapshots:
            raise ValueError(f"Vendor '{vendor}' does not support API snapshots")

        specs = plugin.get_api_snapshot_specs()
        if dataset not in specs:
            raise ValueError(f"Vendor '{vendor}' does not define API snapshot dataset '{dataset}'")
        spec = specs[dataset]

        resolved_root = Path(bronze_root).expanduser() if bronze_root else get_bronze_root(vendor)
        if not resolved_root.exists():
            raise FileNotFoundError(f"metadata root not found: {resolved_root}")

        default_glob = spec.default_glob or f"type={spec.data_type}/**/*.jsonl.gz"
        source = LocalBronzeSource(resolved_root, vendor=vendor, compute_checksums=True)
        files = [
            f
            for f in source.list_files(glob_pattern or default_glob)
            if f.data_type == spec.data_type
        ]

        if exchange:
            exchange_norm = exchange.lower()
            files = [
                f
                for f in files
                if (_extract_partition_value(f.bronze_file_path, "exchange") or "").lower()
                == exchange_norm
            ]
        files = sorted(
            files,
            key=lambda meta: (meta.date.isoformat() if meta.date else "", meta.bronze_file_path),
        )
        discovered = len(files)

        manifest_repo = DeltaManifestRepository(
            Path(manifest_path or get_table_path("ingest_manifest"))
        )
        if not force:
            files = manifest_repo.filter_pending(files)

        success_count = 0
        failed_count = 0
        results: list[ApiReplayFileResult] = []
        processed = len(files)

        for meta in files:
            file_id = manifest_repo.resolve_file_id(meta)
            try:
                full_path = resolved_root / meta.bronze_file_path
                records, request_payload, partitions, v2_manifest = self._load_snapshot_records(
                    full_path
                )

                # v2 completeness gate
                if v2_manifest is not None and not v2_manifest.complete:
                    logger.warning("Skipping incomplete v2 snapshot: %s", meta.bronze_file_path)
                    manifest_repo.update_status(
                        file_id,
                        "skipped_incomplete",
                        meta,
                        IngestionResult(
                            row_count=0,
                            ts_local_min_us=0,
                            ts_local_max_us=0,
                            error_message="Incomplete snapshot (complete=false)",
                        ),
                    )
                    results.append(
                        ApiReplayFileResult(
                            bronze_file_path=meta.bronze_file_path,
                            status="skipped_incomplete",
                            row_count=0,
                            error_message="Incomplete snapshot",
                        )
                    )
                    continue

                options = ApiReplayOptions(
                    rebuild=rebuild,
                    effective_ts_us=effective_ts_us,
                    partitions=partitions,
                    request=request_payload,
                )
                updates = plugin.build_updates_from_snapshot(dataset, records, options)
                ingestion_result = self._apply_updates(
                    target_table=spec.target_table,
                    updates=updates,
                    table_path=Path(table_path).expanduser() if table_path else None,
                    rebuild=rebuild,
                )
                manifest_repo.update_status(file_id, "success", meta, ingestion_result)
                success_count += 1
                results.append(
                    ApiReplayFileResult(
                        bronze_file_path=meta.bronze_file_path,
                        status="success",
                        row_count=ingestion_result.row_count,
                    )
                )
            except Exception as exc:
                failed_count += 1
                manifest_repo.update_status(
                    file_id,
                    "failed",
                    meta,
                    IngestionResult(
                        row_count=0,
                        ts_local_min_us=0,
                        ts_local_max_us=0,
                        error_message=str(exc),
                    ),
                )
                results.append(
                    ApiReplayFileResult(
                        bronze_file_path=meta.bronze_file_path,
                        status="failed",
                        row_count=0,
                        error_message=str(exc),
                    )
                )

        return ApiReplaySummary(
            vendor=vendor,
            dataset=dataset,
            discovered_files=discovered,
            processed_files=processed,
            success_count=success_count,
            failed_count=failed_count,
            file_results=results,
        )

    def _load_snapshot_records(
        self, path: Path
    ) -> tuple[
        list[dict[str, Any]], dict[str, Any] | None, dict[str, str], BronzeSnapshotManifest | None
    ]:
        """Load records from a snapshot path, detecting v1 vs v2 format.

        Returns:
            (records, request_payload, partitions, v2_manifest_or_none)
        """
        # v2 detection: path is a directory containing _manifest.json,
        # or path's parent directory contains _manifest.json
        manifest_file = None
        if path.is_dir() and (path / "_manifest.json").exists():
            manifest_file = path / "_manifest.json"
        elif path.is_file() and (path.parent / "_manifest.json").exists():
            manifest_file = path.parent / "_manifest.json"

        if manifest_file is not None:
            return self._load_v2_snapshot(manifest_file)

        # v1 path: existing envelope parsing
        return (*self._load_v1_snapshot_records(path), None)

    def _load_v1_snapshot_records(
        self, path: Path
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None, dict[str, str]]:
        """Load v1 format (per-record envelopes)."""
        opener = gzip.open if path.suffix == ".gz" else open
        records: list[dict[str, Any]] = []
        request_payload: dict[str, Any] | None = None
        partitions: dict[str, str] = {}

        with opener(path, "rt", encoding="utf-8") as handle:  # type: ignore[arg-type]
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                payload = json.loads(raw)
                if isinstance(payload, dict) and "record" in payload:
                    record = payload.get("record")
                    if not isinstance(record, dict):
                        raise ValueError(f"Snapshot record payload must be object: {path}")
                    records.append(record)
                    if request_payload is None and isinstance(payload.get("request"), dict):
                        request_payload = payload["request"]
                    if not partitions and isinstance(payload.get("partitions"), dict):
                        partitions = {
                            str(k): str(v)
                            for k, v in payload["partitions"].items()
                            if isinstance(k, str) and v is not None
                        }
                elif isinstance(payload, dict):
                    # Clean break allows old raw line shape to be replayed as-is.
                    records.append(payload)
                else:
                    raise ValueError(f"Snapshot line is not a JSON object: {path}")

        if not partitions:
            date_value = _extract_partition_value(str(path), "date")
            exchange = _extract_partition_value(str(path), "exchange")
            if date_value:
                partitions["date"] = date_value
            if exchange:
                partitions["exchange"] = exchange
        return records, request_payload, partitions

    def _load_v2_snapshot(
        self, manifest_file: Path
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None, dict[str, str], BronzeSnapshotManifest]:
        """Load v2 format (_manifest.json + records file)."""
        manifest = BronzeSnapshotManifest.from_file(manifest_file)
        snapshot_dir = manifest_file.parent

        # Determine records file
        record_format = manifest.record_format
        if record_format == "jsonl.gz":
            records_path = snapshot_dir / "records.jsonl.gz"
        elif record_format == "parquet":
            records_path = snapshot_dir / "records.parquet"
        else:
            raise ValueError(f"Unsupported v2 record_format: {record_format}")

        if not records_path.exists():
            raise FileNotFoundError(f"v2 records file not found: {records_path}")

        # Load records
        records: list[dict[str, Any]] = []
        if record_format == "jsonl.gz":
            with gzip.open(records_path, "rt", encoding="utf-8") as handle:
                for line in handle:
                    raw = line.strip()
                    if raw:
                        records.append(json.loads(raw))
        elif record_format == "parquet":
            df = pl.read_parquet(records_path)
            records = df.to_dicts()

        return records, manifest.request_params, manifest.partitions, manifest

    def _apply_updates(
        self,
        *,
        target_table: str,
        updates: pl.DataFrame,
        table_path: Path | None,
        rebuild: bool,
    ) -> IngestionResult:
        if updates.is_empty():
            return IngestionResult(
                row_count=0, ts_local_min_us=0, ts_local_max_us=0, error_message=None
            )

        if target_table == "dim_symbol":
            repo = BaseDeltaRepository(table_path or get_table_path("dim_symbol"))
            service = DimSymbolService(repo)
            if rebuild:
                service.rebuild(updates)
            else:
                service.update(updates)
            min_ts = (
                int(updates["valid_from_ts"].min()) if "valid_from_ts" in updates.columns else 0
            )
            max_ts = (
                int(updates["valid_from_ts"].max()) if "valid_from_ts" in updates.columns else 0
            )
            return IngestionResult(
                row_count=updates.height,
                ts_local_min_us=min_ts,
                ts_local_max_us=max_ts,
                error_message=None,
            )

        if target_table == "dim_asset_stats":
            repo = BaseDeltaRepository(table_path or get_table_path("dim_asset_stats"))
            service = DimAssetStatsService(repo)
            service.update(updates)
            min_ts = (
                int(updates["updated_at_ts"].min()) if "updated_at_ts" in updates.columns else 0
            )
            max_ts = (
                int(updates["updated_at_ts"].max()) if "updated_at_ts" in updates.columns else 0
            )
            return IngestionResult(
                row_count=updates.height,
                ts_local_min_us=min_ts,
                ts_local_max_us=max_ts,
                error_message=None,
            )

        raise ValueError(f"Unsupported snapshot target table: {target_table}")
