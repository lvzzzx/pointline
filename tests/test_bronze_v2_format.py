"""Tests for v2 bronze format: manifest, canonical hashing, format detection."""

from __future__ import annotations

import gzip
import json

from pointline.io.protocols import BronzeSnapshotManifest
from pointline.io.snapshot_utils import compute_canonical_content_hash, compute_file_hash


# ---------------------------------------------------------------------------
# Manifest serialization round-trip
# ---------------------------------------------------------------------------
class TestManifestSerialization:
    def test_round_trip(self):
        manifest = BronzeSnapshotManifest(
            schema_version=2,
            vendor="tushare",
            dataset="dim_symbol",
            data_type="dim_symbol_metadata",
            capture_mode="full_snapshot",
            record_format="jsonl.gz",
            complete=True,
            captured_at_us=1714521600000000,
            vendor_effective_ts_us=None,
            api_endpoint="stock_basic",
            request_params={"exchange": "SZSE"},
            record_count=2847,
            expected_record_count=None,
            records_content_sha256="d4e5f6" * 10 + "d4e5",
            records_file_sha256="a1b2c3" * 10 + "a1b2",
            partitions={"exchange": "szse", "date": "2024-05-01"},
        )

        d = manifest.to_dict()
        restored = BronzeSnapshotManifest.from_dict(d)

        assert restored.schema_version == 2
        assert restored.vendor == "tushare"
        assert restored.dataset == "dim_symbol"
        assert restored.data_type == "dim_symbol_metadata"
        assert restored.capture_mode == "full_snapshot"
        assert restored.record_format == "jsonl.gz"
        assert restored.complete is True
        assert restored.captured_at_us == 1714521600000000
        assert restored.vendor_effective_ts_us is None
        assert restored.api_endpoint == "stock_basic"
        assert restored.request_params == {"exchange": "SZSE"}
        assert restored.record_count == 2847
        assert restored.expected_record_count is None
        assert restored.records_content_sha256 == manifest.records_content_sha256
        assert restored.records_file_sha256 == manifest.records_file_sha256
        assert restored.partitions == {"exchange": "szse", "date": "2024-05-01"}

    def test_from_file(self, tmp_path):
        manifest = BronzeSnapshotManifest(
            schema_version=2,
            vendor="coingecko",
            dataset="dim_asset_stats",
            data_type="dim_asset_stats_metadata",
            capture_mode="full_snapshot",
            record_format="jsonl.gz",
            complete=True,
            captured_at_us=1000,
            api_endpoint="coins/markets",
            request_params={"vs_currency": "usd"},
            record_count=100,
            records_content_sha256="abc123",
            records_file_sha256="def456",
            partitions={"date": "2024-05-01"},
        )

        path = tmp_path / "_manifest.json"
        with open(path, "w") as f:
            json.dump(manifest.to_dict(), f)

        loaded = BronzeSnapshotManifest.from_file(path)
        assert loaded.vendor == "coingecko"
        assert loaded.complete is True
        assert loaded.record_count == 100

    def test_incomplete_manifest(self):
        manifest = BronzeSnapshotManifest(
            schema_version=2,
            vendor="tushare",
            dataset="dim_symbol",
            data_type="dim_symbol_metadata",
            capture_mode="full_snapshot",
            record_format="jsonl.gz",
            complete=False,
            captured_at_us=1000,
            api_endpoint="stock_basic",
            request_params={},
            record_count=5,
            records_content_sha256="abc",
            records_file_sha256="def",
            partitions={},
        )

        d = manifest.to_dict()
        assert d["complete"] is False
        restored = BronzeSnapshotManifest.from_dict(d)
        assert restored.complete is False


# ---------------------------------------------------------------------------
# Canonical hash determinism
# ---------------------------------------------------------------------------
class TestCanonicalHashing:
    def test_deterministic_across_order(self):
        """Same records in different order produce the same hash."""
        records = [
            {"symbol": "ETHUSDT", "tick_size": 0.1},
            {"symbol": "BTCUSDT", "tick_size": 0.01},
        ]
        h1 = compute_canonical_content_hash(records)
        h2 = compute_canonical_content_hash(list(reversed(records)))
        assert h1 == h2

    def test_deterministic_with_natural_key(self):
        """Sorting by natural key columns is deterministic."""
        records = [
            {"exchange_symbol": "ETHUSDT", "tick_size": 0.1, "exchange_id": 1},
            {"exchange_symbol": "BTCUSDT", "tick_size": 0.01, "exchange_id": 1},
        ]
        h1 = compute_canonical_content_hash(
            records, natural_key_cols=["exchange_id", "exchange_symbol"]
        )
        h2 = compute_canonical_content_hash(
            list(reversed(records)), natural_key_cols=["exchange_id", "exchange_symbol"]
        )
        assert h1 == h2

    def test_empty_records(self):
        """Empty records produce a deterministic hash."""
        h = compute_canonical_content_hash([])
        assert len(h) == 64  # SHA-256 hex digest

    def test_different_content_different_hash(self):
        """Changed values produce different hashes."""
        records1 = [{"symbol": "BTCUSDT", "tick_size": 0.01}]
        records2 = [{"symbol": "BTCUSDT", "tick_size": 0.001}]
        assert compute_canonical_content_hash(records1) != compute_canonical_content_hash(records2)

    def test_file_hash(self, tmp_path):
        """File hash is SHA-256 of raw bytes."""
        path = tmp_path / "test.bin"
        path.write_bytes(b"hello world")
        h = compute_file_hash(path)
        assert len(h) == 64
        # Verify against known SHA-256
        import hashlib

        expected = hashlib.sha256(b"hello world").hexdigest()
        assert h == expected


# ---------------------------------------------------------------------------
# v1 vs v2 format detection
# ---------------------------------------------------------------------------
class TestFormatDetection:
    def test_v2_detected_by_manifest(self, tmp_path):
        """Directory with _manifest.json is detected as v2."""
        from pointline.services.api_snapshot_service import ApiSnapshotService

        snapshot_dir = tmp_path / "captured_ts=1000"
        snapshot_dir.mkdir(parents=True)

        manifest = BronzeSnapshotManifest(
            schema_version=2,
            vendor="tushare",
            dataset="dim_symbol",
            data_type="dim_symbol_metadata",
            capture_mode="full_snapshot",
            record_format="jsonl.gz",
            complete=True,
            captured_at_us=1000,
            api_endpoint="stock_basic",
            request_params={},
            record_count=1,
            records_content_sha256="abc",
            records_file_sha256="def",
            partitions={"exchange": "szse"},
        )
        with open(snapshot_dir / "_manifest.json", "w") as f:
            json.dump(manifest.to_dict(), f)

        with gzip.open(snapshot_dir / "records.jsonl.gz", "wt") as f:
            f.write(json.dumps({"ts_code": "000001.SZ"}) + "\n")

        service = ApiSnapshotService()
        records, request_params, partitions, v2_manifest = service._load_snapshot_records(
            snapshot_dir
        )
        assert v2_manifest is not None
        assert v2_manifest.schema_version == 2
        assert records == [{"ts_code": "000001.SZ"}]
        assert partitions == {"exchange": "szse"}

    def test_v1_detected_without_manifest(self, tmp_path):
        """File without _manifest.json in parent is detected as v1."""
        from pointline.services.api_snapshot_service import ApiSnapshotService

        envelope_path = tmp_path / "test.jsonl.gz"
        with gzip.open(envelope_path, "wt") as f:
            f.write(
                json.dumps(
                    {
                        "schema_version": 1,
                        "record": {"id": 1},
                        "request": {"foo": "bar"},
                        "partitions": {"exchange": "binance"},
                    }
                )
                + "\n"
            )

        service = ApiSnapshotService()
        records, request_payload, partitions, v2_manifest = service._load_snapshot_records(
            envelope_path
        )
        assert v2_manifest is None
        assert records == [{"id": 1}]
        assert request_payload == {"foo": "bar"}

    def test_v2_file_in_manifest_dir_detected(self, tmp_path):
        """A records file in a directory with _manifest.json is detected as v2."""
        from pointline.services.api_snapshot_service import ApiSnapshotService

        snapshot_dir = tmp_path / "snapshot"
        snapshot_dir.mkdir()

        manifest = BronzeSnapshotManifest(
            schema_version=2,
            vendor="test",
            dataset="test",
            data_type="test",
            capture_mode="full_snapshot",
            record_format="jsonl.gz",
            complete=True,
            captured_at_us=1000,
            api_endpoint="test",
            request_params={},
            record_count=1,
            records_content_sha256="abc",
            records_file_sha256="def",
            partitions={},
        )
        with open(snapshot_dir / "_manifest.json", "w") as f:
            json.dump(manifest.to_dict(), f)

        records_path = snapshot_dir / "records.jsonl.gz"
        with gzip.open(records_path, "wt") as f:
            f.write(json.dumps({"data": "value"}) + "\n")

        service = ApiSnapshotService()
        # Pass the records file path (not the directory) — should still detect v2
        records, _, _, v2_manifest = service._load_snapshot_records(records_path)
        assert v2_manifest is not None
        assert records == [{"data": "value"}]


# ---------------------------------------------------------------------------
# Completeness gate rejection
# ---------------------------------------------------------------------------
class TestCompletenessGate:
    def test_incomplete_v2_snapshot_skipped(self, tmp_path):
        """v2 snapshot with complete=false returns manifest with complete=false."""
        from pointline.services.api_snapshot_service import ApiSnapshotService

        snapshot_dir = tmp_path / "snapshot"
        snapshot_dir.mkdir()

        manifest = BronzeSnapshotManifest(
            schema_version=2,
            vendor="tushare",
            dataset="dim_symbol",
            data_type="dim_symbol_metadata",
            capture_mode="full_snapshot",
            record_format="jsonl.gz",
            complete=False,
            captured_at_us=1000,
            api_endpoint="stock_basic",
            request_params={},
            record_count=3,
            records_content_sha256="abc",
            records_file_sha256="def",
            partitions={"exchange": "szse"},
        )
        with open(snapshot_dir / "_manifest.json", "w") as f:
            json.dump(manifest.to_dict(), f)

        with gzip.open(snapshot_dir / "records.jsonl.gz", "wt") as f:
            f.write(json.dumps({"ts_code": "000001.SZ"}) + "\n")

        service = ApiSnapshotService()
        records, _, _, v2_manifest = service._load_snapshot_records(snapshot_dir)
        assert v2_manifest is not None
        assert v2_manifest.complete is False
        # The service should skip this — verified by the replay loop
