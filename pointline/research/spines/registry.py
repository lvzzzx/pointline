"""Global registry for spine builders with auto-discovery.

The registry pattern follows the vendor plugin architecture:
- Builders self-register on module import
- Runtime dispatch via get_builder()
- Auto-detection via detect_builder() for backward compatibility
"""

from typing import Any

from .base import SpineBuilder

# Global registry: name -> builder
_SPINE_REGISTRY: dict[str, SpineBuilder] = {}

# Priority order for auto-detection (first match wins)
_DETECTION_PRIORITY = [
    "clock",
    "trades",
    "volume",
    "dollar",
    "tick",
    "imbalance",
    "quote_event",
    "time_weighted",
]


def register_builder(builder: SpineBuilder) -> None:
    """Register a spine builder plugin.

    Args:
        builder: SpineBuilder instance to register

    Raises:
        ValueError: If builder name conflicts with existing registration
    """
    if builder.name in _SPINE_REGISTRY:
        existing = _SPINE_REGISTRY[builder.name]
        if not isinstance(builder, type(existing)):
            raise ValueError(
                f"Spine builder name conflict: '{builder.name}' already registered "
                f"by {type(existing).__name__}"
            )
    _SPINE_REGISTRY[builder.name] = builder


def get_builder(name: str) -> SpineBuilder:
    """Lookup spine builder by name.

    Args:
        name: Builder name (e.g., 'volume', 'dollar')

    Returns:
        SpineBuilder instance

    Raises:
        KeyError: If builder not found
    """
    if name not in _SPINE_REGISTRY:
        available = ", ".join(sorted(_SPINE_REGISTRY.keys()))
        raise KeyError(f"Unknown spine builder: '{name}'. Available builders: {available}")
    return _SPINE_REGISTRY[name]


def detect_builder(mode: str) -> str:
    """Auto-detect builder name from mode string.

    Uses priority-based matching: checks each builder in priority order,
    returns the first match. This ensures backward compatibility with
    legacy mode strings like "clock", "time", "fixed_time".

    Args:
        mode: Mode string (e.g., "clock", "trades", "volume")

    Returns:
        Builder name

    Raises:
        ValueError: If no builder recognizes the mode
    """
    mode_lower = mode.lower()

    # Try priority-ordered builders first
    for builder_name in _DETECTION_PRIORITY:
        if builder_name in _SPINE_REGISTRY:
            builder = _SPINE_REGISTRY[builder_name]
            if builder.can_handle(mode_lower):
                return builder_name

    # Fallback: try all builders (for custom plugins not in priority list)
    for builder_name, builder in _SPINE_REGISTRY.items():
        if builder.can_handle(mode_lower):
            return builder_name

    available = ", ".join(sorted(_SPINE_REGISTRY.keys()))
    raise ValueError(f"No spine builder can handle mode '{mode}'. Available builders: {available}")


def list_builders() -> list[str]:
    """List all registered builder names.

    Returns:
        Sorted list of builder names
    """
    return sorted(_SPINE_REGISTRY.keys())


def get_builder_info() -> dict[str, dict[str, Any]]:
    """Get metadata for all registered builders.

    Returns:
        Dict mapping builder name to metadata:
        - display_name: Human-readable name
        - supports_single_symbol: bool
        - supports_multi_symbol: bool
    """
    return {
        name: {
            "display_name": builder.display_name,
            "supports_single_symbol": builder.supports_single_symbol,
            "supports_multi_symbol": builder.supports_multi_symbol,
        }
        for name, builder in _SPINE_REGISTRY.items()
    }


def get_builder_by_config(config: Any) -> SpineBuilder:
    """Get builder by config type.

    Args:
        config: Builder-specific config instance

    Returns:
        SpineBuilder instance

    Raises:
        ValueError: If no builder matches the config type
    """
    config_type_name = type(config).__name__

    # Direct mappings
    mappings = {
        "ClockSpineConfig": "clock",
        "TradesSpineConfig": "trades",
        "VolumeBarConfig": "volume",
        "DollarBarConfig": "dollar",
    }

    builder_name = mappings.get(config_type_name)
    if builder_name:
        return get_builder(builder_name)

    # Fallback: try to infer from class name
    for name in [
        "clock",
        "trades",
        "volume",
        "dollar",
        "tick",
        "imbalance",
        "quote_event",
        "time_weighted",
    ]:
        if name in config_type_name.lower():
            return get_builder(name)

    raise ValueError(
        f"Could not determine builder for config type '{config_type_name}'. "
        f"Available builders: {', '.join(list_builders())}"
    )
