"""Shared utilities for Quant360 upstream adapter."""

from __future__ import annotations

import hashlib
from pathlib import Path


def file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Compute SHA256 hash of a file."""
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()
