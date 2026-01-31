import contextlib
import hashlib
import logging
import subprocess
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path

from pointline.io.protocols import BronzeFileMetadata

logger = logging.getLogger(__name__)


class LocalBronzeSource:
    """
    Implementation of BronzeSource for local filesystem.
    Expects Hive-style partitioning under a vendor root (e.g., bronze/tardis).

    Supports automatic prehooks for vendor-specific archive reorganization.
    """

    # Required Hive partition keys that must be present in bronze file paths
    REQUIRED_PARTITIONS = {"exchange", "type", "date", "symbol"}

    def __init__(
        self,
        root_path: Path,
        vendor: str | None = None,
        strict_validation: bool = True,
        enable_prehooks: bool = True,
        compute_checksums: bool = True,
    ):
        """
        Initialize LocalBronzeSource.

        Args:
            root_path: Root path to scan for bronze files
            vendor: Vendor name (inferred from root_path if None)
            strict_validation: If True, raise error on missing required partitions.
                              If False, use default values (for backward compatibility).
            enable_prehooks: If True, run vendor-specific prehooks before file discovery
                           (e.g., archive reorganization for quant360)
            compute_checksums: If True, compute SHA256 for each file (slower but needed
                             for manifest creation). If False, use empty string (faster
                             for discovery-only operations).
        """
        self.root_path = root_path
        self.strict_validation = strict_validation
        self.enable_prehooks = enable_prehooks
        self.compute_checksums = compute_checksums
        if vendor:
            self.vendor = vendor
        elif root_path.name != "bronze":
            self.vendor = root_path.name
        else:
            self.vendor = None

    def list_files(self, glob_pattern: str) -> Iterator[BronzeFileMetadata]:
        """
        Scans storage for files matching the pattern within root_path.
        Extracts metadata from Hive partitions (exchange=X/date=Y) if present.

        Runs vendor-specific prehooks if enabled (e.g., archive reorganization).

        Raises:
            ValueError: If strict_validation=True and required partitions are missing
        """
        # Prehook: Auto-reorganize archives if needed
        if self.enable_prehooks:
            self._run_prehooks()

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

    def _run_prehooks(self) -> None:
        """
        Run vendor-specific prehooks before file discovery.

        Currently supports:
        - quant360: Auto-reorganize .7z archives into Hive partitions
        """
        detected_vendor = self._detect_vendor()
        if not detected_vendor:
            return  # No prehook needed

        if detected_vendor == "quant360":
            self._prehook_quant360_reorganize()
        # Add other vendors here as needed

    def _detect_vendor(self) -> str | None:
        """
        Auto-detect vendor from directory structure or archive patterns.

        Returns:
            Vendor name if detected, None otherwise
        """
        # Explicit vendor parameter takes precedence
        if self.vendor and self.vendor != "bronze":
            return self.vendor

        # Detect from directory name
        if self.root_path.name in ["quant360", "data.quant360.com"]:
            return "quant360"

        # Detect from archive patterns
        if list(self.root_path.glob("*_new_STK_*.7z")):
            logger.info("Detected quant360 archives (pattern: *_new_STK_*.7z)")
            return "quant360"

        return None

    def _prehook_quant360_reorganize(self) -> None:
        """
        Prehook: Reorganize quant360 .7z archives if present.

        Checks for .7z archives and reorganizes them into Hive partitions.
        Skips if already reorganized (idempotent).
        """
        # Check if archives exist
        archives = list(self.root_path.glob("*.7z"))
        if not archives:
            # Check one level up (in case root_path is already bronze/quant360)
            parent_archives = list(self.root_path.parent.glob("*.7z"))
            if not parent_archives:
                return  # No archives to reorganize

            # Use parent directory as source
            source_dir = self.root_path.parent
            archives = parent_archives
        else:
            source_dir = self.root_path

        # Check if already reorganized (has Hive partitions)
        hive_partitions = list(self.root_path.glob("exchange=*/type=*/date=*/symbol=*/*.csv.gz"))
        if hive_partitions and not archives:
            # Already reorganized and no new archives
            return

        logger.info(
            f"Detected {len(archives)} quant360 archive(s) - running reorganization prehook"
        )

        # Find reorganization script
        script_path = self._find_reorganization_script()
        if not script_path:
            logger.warning(
                "Quant360 archives detected but reorganization script not found. "
                "Skipping prehook. Run 'pointline bronze reorganize' manually."
            )
            return

        # Determine bronze root (parent of vendor directory)
        if self.root_path.name in ["quant360", "data.quant360.com"]:
            bronze_root = self.root_path.parent
        else:
            bronze_root = self.root_path

        # Run reorganization script
        logger.info(f"Running: {script_path} {source_dir} {bronze_root}")
        try:
            result = subprocess.run(
                [str(script_path), str(source_dir), str(bronze_root)],
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                logger.info("Reorganization prehook completed successfully")
            else:
                logger.warning(
                    f"Reorganization prehook failed (exit code {result.returncode}): "
                    f"{result.stderr}"
                )
        except Exception as e:
            logger.warning(f"Reorganization prehook error: {e}")

    def _find_reorganization_script(self) -> Path | None:
        """Find the quant360 reorganization script."""
        candidates = [
            # Relative to package root (installed)
            Path(__file__).parent.parent.parent / "scripts" / "reorganize_quant360.sh",
            # Current working directory (development)
            Path.cwd() / "scripts" / "reorganize_quant360.sh",
        ]

        for path in candidates:
            if path.exists():
                return path
        return None
