"""Minimal v2 research API (discovery, querying, metadata, spine)."""

from pointline.research.cn_trading_phases import TradingPhase, add_phase_column, filter_by_phase
from pointline.research.discovery import discover_symbols
from pointline.research.metadata import load_symbol_meta
from pointline.research.primitives import decode_scaled_columns, join_symbol_meta
from pointline.research.query import load_events
from pointline.research.spine import (
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
    "TradingPhase",
    "VolumeSpineConfig",
    "add_phase_column",
    "align_to_spine",
    "build_spine",
    "decode_scaled_columns",
    "discover_symbols",
    "filter_by_phase",
    "join_symbol_meta",
    "load_symbol_meta",
    "load_events",
]
