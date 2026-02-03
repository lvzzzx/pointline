import contextlib
import hashlib
import logging
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path

from pointline.io.protocols import BronzeFileMetadata

logger = logging.getLogger(__name__)


class LocalBronzeSource:
    """
    Implementation of BronzeSource for local filesystem.
    Expects Hive-style partitioning under a vendor root (e.g., bronze/tardis).
    """

    # Required Hive partition keys that must be present in bronze file paths
    REQUIRED_PARTITIONS = {"exchange", "type", "date", "symbol"}

    def __init__(
        self,
        root_path: Path,
        vendor: str | None = None,
        strict_validation: bool = True,
        compute_checksums: bool = True,
    ):
        """
        Initialize LocalBronzeSource.

        Args:
            root_path: Root path to scan for bronze files
            vendor: Vendor name (auto-detected if None)
            strict_validation: If True, raise error on missing required partitions.
                              If False, use default values (for backward compatibility).
            compute_checksums: If True, compute SHA256 for each file (slower but needed
                             for manifest creation). If False, use empty string (faster
                             for discovery-only operations).
        """
        self.root_path = root_path
        self.strict_validation = strict_validation
        self.compute_checksums = compute_checksums

        if vendor:
            # Explicit vendor parameter takes precedence
            self.vendor = vendor
        else:
            # Auto-detect vendor via plugin system
            self.vendor = self._detect_vendor()

    def _detect_vendor(self) -> str | None:
        """Auto-detect vendor using plugin system.

        Returns:
            Vendor name if detected, None otherwise
        """
        from pointline.io.vendors.registry import detect_vendor

        return detect_vendor(self.root_path)

    def list_files(self, glob_pattern: str) -> Iterator[BronzeFileMetadata]:
        """
        Scans storage for files matching the pattern within root_path.
        Extracts metadata from Hive partitions (exchange=X/date=Y) if present.

        Raises:
            ValueError: If strict_validation=True and required partitions are missing
        """
        for p in self.root_path.glob(glob_pattern):
            if not p.is_file():
                continue

            stat = p.stat()
            rel_path = p.relative_to(self.root_path)

            # Extract metadata from path parts
            # Example: .../exchange=binance/type=quotes/date=2024-05-01/symbol=BTCUSDT/...
            meta = self._extract_metadata(p)
            vendor = self.vendor or (rel_path.parts[0] if rel_path.parts else "unknown")

            # Validate required partitions are present
            if self.strict_validation:
                missing = self.REQUIRED_PARTITIONS - meta.keys()
                if missing:
                    raise ValueError(
                        f"Missing required Hive partitions: {missing} in path '{rel_path}'. "
                        f"Expected partitions: {self.REQUIRED_PARTITIONS}. "
                        f"Found partitions: {set(meta.keys())}. "
                        f"Set strict_validation=False to use default values instead."
                    )

            sha256 = self._compute_sha256(p) if self.compute_checksums else ""
            yield BronzeFileMetadata(
                vendor=vendor,
                exchange=meta.get("exchange", "unknown"),
                data_type=meta.get("type", "unknown"),
                symbol=meta.get("symbol", "unknown"),
                date=meta.get("date", date(1970, 1, 1)),
                bronze_file_path=str(rel_path),
                file_size_bytes=stat.st_size,
                last_modified_ts=int(stat.st_mtime * 1_000_000),  # microseconds
                sha256=sha256,
                interval=meta.get("interval"),  # Extract interval (for klines: "1h", "4h", etc.)
            )

    def _extract_metadata(self, path: Path) -> dict:
        """Helper to parse Hive-style path components."""
        meta = {}
        for part in path.parts:
            if "=" in part:
                key, val = part.split("=", 1)
                if key == "date":
                    with contextlib.suppress(ValueError):
                        meta[key] = datetime.strptime(val, "%Y-%m-%d").date()
                else:
                    meta[key] = val
        return meta

    def _compute_sha256(self, path: Path, chunk_size: int = 1024 * 1024) -> str:
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(chunk_size), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
