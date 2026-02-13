"""Minimal v2 research API (discovery + event loading)."""

from pointline.v2.research.discovery import discover_symbols
from pointline.v2.research.metadata import load_symbol_meta
from pointline.v2.research.query import load_events

__all__ = [
    "discover_symbols",
    "load_symbol_meta",
    "load_events",
]
