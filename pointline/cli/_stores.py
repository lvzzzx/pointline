"""Construct Delta Lake stores from a silver root path."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def build_stores(silver_root: Path) -> dict[str, Any]:
    """Build all Delta stores needed for ingestion.

    Returns a dict with keys: manifest, event, dimension, quarantine, optimizer.
    """
    from pointline.storage.delta import (
        DeltaDimensionStore,
        DeltaEventStore,
        DeltaManifestStore,
        DeltaPartitionOptimizer,
        DeltaQuarantineStore,
    )

    return {
        "manifest": DeltaManifestStore(table_path=silver_root / "ingest_manifest"),
        "event": DeltaEventStore(silver_root=silver_root),
        "dimension": DeltaDimensionStore(silver_root=silver_root),
        "quarantine": DeltaQuarantineStore(silver_root=silver_root),
        "optimizer": DeltaPartitionOptimizer(silver_root=silver_root),
    }
