import os
from pathlib import Path
from typing import Iterator
from datetime import datetime, date

from pointline.io.protocols import BronzeSource, BronzeFileMetadata

class LocalBronzeSource:
    """
    Implementation of BronzeSource for local filesystem.
    Expects Hive-style partitioning or standard Tardis directory structure.
    """
    
    def __init__(self, root_path: Path):
        self.root_path = root_path

    def list_files(self, glob_pattern: str) -> Iterator[BronzeFileMetadata]:
        """
        Scans storage for files matching the pattern within root_path.
        Extracts metadata from Hive partitions (exchange=X/date=Y) if present.
        """
        for p in self.root_path.glob(glob_pattern):
            if not p.is_file():
                continue
                
            stat = p.stat()
            
            # Extract metadata from path parts
            # Example: .../exchange=binance/type=quotes/date=2024-05-01/symbol=BTCUSDT/...
            meta = self._extract_metadata(p)
            
            yield BronzeFileMetadata(
                exchange=meta.get("exchange", "unknown"),
                data_type=meta.get("type", "unknown"),
                symbol=meta.get("symbol", "unknown"),
                date=meta.get("date", date(1970, 1, 1)),
                bronze_file_path=str(p.relative_to(self.root_path)),
                file_size_bytes=stat.st_size,
                last_modified_ts=int(stat.st_mtime * 1_000_000) # microseconds
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
