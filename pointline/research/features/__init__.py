"""Feature engineering utilities for PIT-correct research."""

from pointline.research.features.core import EventSpineConfig, build_event_spine, pit_align

# Spine builder configs (for explicit builder configuration)
from pointline.research.spines import (
    ClockSpineConfig,
    DollarBarConfig,
    TradesSpineConfig,
    VolumeBarConfig,
)

__all__ = [
    # Core API
    "EventSpineConfig",
    "build_event_spine",
    "pit_align",
    # Spine builder configs
    "ClockSpineConfig",
    "TradesSpineConfig",
    "VolumeBarConfig",
    "DollarBarConfig",
]
