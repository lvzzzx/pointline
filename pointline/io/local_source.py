import hashlib
from pathlib import Path
from typing import Iterator
from datetime import datetime, date

from pointline.io.protocols import BronzeSource, BronzeFileMetadata

class LocalBronzeSource:
    """
    Implementation of BronzeSource for local filesystem.
    Expects Hive-style partitioning under a vendor root (e.g., bronze/tardis).
    """
    
    def __init__(self, root_path: Path, vendor: str | None = None):
        self.root_path = root_path
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
            
            sha256 = self._compute_sha256(p)
            yield BronzeFileMetadata(
                vendor=vendor,
                exchange=meta.get("exchange", "unknown"),
                data_type=meta.get("type", "unknown"),
                symbol=meta.get("symbol", "unknown"),
                date=meta.get("date", date(1970, 1, 1)),
                bronze_file_path=str(rel_path),
                file_size_bytes=stat.st_size,
                last_modified_ts=int(stat.st_mtime * 1_000_000), # microseconds
                sha256=sha256,
            )

    def _extract_metadata(self, path: Path) -> dict:
        """Helper to parse Hive-style path components."""
        meta = {}
        for part in path.parts:
            if "=" in part:
                key, val = part.split("=", 1)
                if key == "date":
                    try:
                        meta[key] = datetime.strptime(val, "%Y-%m-%d").date()
                    except ValueError:
                        pass
                else:
                    meta[key] = val
        return meta

    def _compute_sha256(self, path: Path, chunk_size: int = 1024 * 1024) -> str:
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(chunk_size), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
