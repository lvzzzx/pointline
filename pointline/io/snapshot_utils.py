"""Canonical hashing utilities for API bronze snapshots."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


def compute_canonical_content_hash(
    records: list[dict],
    natural_key_cols: list[str] | None = None,
) -> str:
    """Compute SHA-256 of canonical uncompressed payload for logical dedup.

    1. Sort records by natural key columns in deterministic order.
    2. Serialize each record with sorted keys, UTF-8 encoded, newline-separated.
    3. SHA-256 of the byte stream.

    Args:
        records: List of record dicts.
        natural_key_cols: Columns to sort by. If None, sorts by all keys of the first record.

    Returns:
        Hex-encoded SHA-256 hash string.
    """
    if not records:
        return hashlib.sha256(b"").hexdigest()

    if natural_key_cols:
        sorted_records = sorted(
            records, key=lambda r: tuple(r.get(k, "") for k in natural_key_cols)
        )
    else:
        # Sort by all keys for determinism
        sorted_records = sorted(records, key=lambda r: json.dumps(r, sort_keys=True))

    hasher = hashlib.sha256()
    for record in sorted_records:
        line = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
        hasher.update(line)
        hasher.update(b"\n")
    return hasher.hexdigest()


def compute_file_hash(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Compute SHA-256 of raw file bytes for artifact integrity.

    Args:
        path: Path to the file.
        chunk_size: Read chunk size.

    Returns:
        Hex-encoded SHA-256 hash string.
    """
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()
