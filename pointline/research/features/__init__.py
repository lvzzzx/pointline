"""Feature engineering utilities for PIT-correct research."""

from pointline.research.features.core import EventSpineConfig, build_event_spine, pit_align
from pointline.research.features.runner import (
    FeatureRunConfig,
    build_cross_venue_mid_frame,
    build_feature_frame,
)

# Spine builder configs (for explicit builder configuration)
from pointline.research.features.spines import (
    ClockSpineConfig,
    DollarBarConfig,
    TradesSpineConfig,
    VolumeBarConfig,
)

__all__ = [
    # Core API
    "EventSpineConfig",
    "FeatureRunConfig",
    "build_cross_venue_mid_frame",
    "build_event_spine",
    "build_feature_frame",
    "pit_align",
    # Spine builder configs
    "ClockSpineConfig",
    "TradesSpineConfig",
    "VolumeBarConfig",
    "DollarBarConfig",
]
