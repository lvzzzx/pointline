import hashlib
import logging
from collections.abc import Iterator
from pathlib import Path

from pointline.io.protocols import BronzeFileMetadata, BronzeLayoutSpec

logger = logging.getLogger(__name__)


class LocalBronzeSource:
    """Implementation of BronzeSource for local filesystem.

    Uses vendor-specific layout specs to discover and extract metadata from bronze files.
    No hardcoded partition assumptions - each vendor defines its own layout.
    """

    def __init__(
        self,
        root_path: Path,
        vendor: str | None = None,
        compute_checksums: bool = True,
    ):
        """Initialize LocalBronzeSource.

        Args:
            root_path: Root path to scan for bronze files
            vendor: Vendor name (auto-detected if None)
            compute_checksums: If True, compute SHA256 for each file (slower but needed
                             for manifest creation). If False, use empty string (faster
                             for discovery-only operations).
        """
        self.root_path = root_path
        self.compute_checksums = compute_checksums

        if vendor:
            # Explicit vendor parameter takes precedence
            self.vendor = vendor
        else:
            # Auto-detect vendor via plugin system
            self.vendor = self._detect_vendor()

        # Get vendor plugin and layout spec
        from pointline.io.vendors import get_vendor

        plugin = get_vendor(self.vendor)
        self.layout_spec: BronzeLayoutSpec = plugin.get_bronze_layout_spec()

    def _detect_vendor(self) -> str | None:
        """Auto-detect vendor using plugin system.

        Returns:
            Vendor name if detected, None otherwise
        """
        from pointline.io.vendors.registry import detect_vendor

        return detect_vendor(self.root_path)

    def list_files(self, glob_pattern: str | None = None) -> Iterator[BronzeFileMetadata]:
        """Scan storage using vendor layout spec.

        Args:
            glob_pattern: Optional override pattern. If None, uses vendor's default patterns.

        Returns:
            Iterator of BronzeFileMetadata for discovered files.

        Raises:
            ValueError: If vendor layout spec fails to extract data_type (required field)
        """
        patterns = [glob_pattern] if glob_pattern else self.layout_spec.glob_patterns

        for pattern in patterns:
            for p in self.root_path.glob(pattern):
                if not p.is_file():
                    continue

                # Compute file stats
                stat = p.stat()
                rel_path = p.relative_to(self.root_path)
                sha256 = self._compute_sha256(p) if self.compute_checksums else ""

                file_stats = {
                    "rel_path": rel_path,
                    "size": stat.st_size,
                    "mtime_us": int(stat.st_mtime * 1_000_000),
                    "sha256": sha256,
                }

                # Delegate metadata extraction to vendor spec
                partial_meta = self.layout_spec.extract_metadata(p)

                # Enforce data_type is required
                if "data_type" not in partial_meta:
                    raise ValueError(
                        f"Vendor '{self.vendor}' layout spec failed to extract data_type "
                        f"from path: {rel_path}"
                    )

                # Normalize to BronzeFileMetadata
                yield self.layout_spec.normalize_metadata(partial_meta, file_stats)

    def _compute_sha256(self, path: Path, chunk_size: int = 1024 * 1024) -> str:
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(chunk_size), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
