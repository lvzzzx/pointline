"""Spine builder plugins for resampling strategies.

This module provides extensible resampling methods for feature engineering:
- Clock spine: Fixed time intervals
- Trades spine: Event-driven (every trade)
- Volume spine: Volume bars (every N contracts)
- Dollar spine: Dollar bars (every $N notional)
- Tick spine: Tick bars (every N price changes)
- Imbalance spine: Imbalance bars (buy/sell imbalance threshold)
- Quote event spine: Quote-event bars (BBO updates)
- Time weighted spine: Time-weighted bars (TWAP/VWAP)

Usage:
    from pointline.research.spines import get_builder, ClockSpineConfig

    builder = get_builder("clock")
    spine = builder.build_spine(
        symbol_id=12345,
        start_ts_us=...,
        end_ts_us=...,
        config=ClockSpineConfig(step_ms=1000)
    )
"""

# Base abstractions
# Import all builders to trigger auto-registration
from . import clock, dollar, trades, volume  # noqa: F401
from .base import SpineBuilder, SpineBuilderConfig
from .cache import CacheEntry, SpineCache

# Export builder configs and builders
from .clock import ClockSpineBuilder, ClockSpineConfig, generate_bar_end_timestamps
from .dollar import DollarBarConfig

# Registry functions
from .registry import (
    detect_builder,
    get_builder,
    get_builder_by_config,
    get_builder_info,
    list_builders,
    register_builder,
)
from .trades import TradesSpineConfig
from .volume import VolumeBarConfig

__all__ = [
    # Base
    "SpineBuilder",
    "SpineBuilderConfig",
    # Cache
    "SpineCache",
    "CacheEntry",
    # Registry
    "register_builder",
    "get_builder",
    "get_builder_by_config",
    "detect_builder",
    "list_builders",
    "get_builder_info",
    # Builders
    "ClockSpineBuilder",
    # Configs
    "ClockSpineConfig",
    "generate_bar_end_timestamps",
    "TradesSpineConfig",
    "VolumeBarConfig",
    "DollarBarConfig",
]
