"""Minimal v2 research API (discovery, querying, metadata, spine)."""

from pointline.v2.research.discovery import discover_symbols
from pointline.v2.research.metadata import load_symbol_meta
from pointline.v2.research.query import load_events
from pointline.v2.research.spine import (
    ClockSpineConfig,
    DollarSpineConfig,
    TradesSpineConfig,
    VolumeSpineConfig,
    align_to_spine,
    build_spine,
)

__all__ = [
    "ClockSpineConfig",
    "DollarSpineConfig",
    "TradesSpineConfig",
    "VolumeSpineConfig",
    "align_to_spine",
    "build_spine",
    "discover_symbols",
    "load_symbol_meta",
    "load_events",
]
