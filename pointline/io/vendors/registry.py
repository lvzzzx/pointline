"""Vendor plugin registry.

This module provides a centralized registry for vendor plugins and maintains
backward compatibility with the old parser registry API.
"""

from __future__ import annotations

from typing import Callable

import polars as pl

from pointline.io.vendors.base import VendorPlugin

# Global vendor registry
_VENDOR_REGISTRY: dict[str, VendorPlugin] = {}


def register_vendor(plugin: VendorPlugin) -> None:
    """Register a vendor plugin.

    Args:
        plugin: Vendor plugin instance

    Raises:
        ValueError: If vendor with same name already registered
    """
    if plugin.name in _VENDOR_REGISTRY:
        raise ValueError(f"Vendor '{plugin.name}' is already registered")
    _VENDOR_REGISTRY[plugin.name] = plugin


def get_vendor(name: str) -> VendorPlugin:
    """Get a registered vendor plugin.

    Args:
        name: Vendor name (e.g., "tardis", "binance_vision")

    Returns:
        Vendor plugin instance

    Raises:
        KeyError: If vendor not registered
    """
    if name not in _VENDOR_REGISTRY:
        available = list_vendors()
        raise KeyError(
            f"Vendor '{name}' not registered. Available vendors: {available}"
        )
    return _VENDOR_REGISTRY[name]


def list_vendors() -> list[str]:
    """List all registered vendor names.

    Returns:
        Sorted list of vendor names
    """
    return sorted(_VENDOR_REGISTRY.keys())


def is_vendor_registered(name: str) -> bool:
    """Check if a vendor is registered.

    Args:
        name: Vendor name

    Returns:
        True if vendor is registered
    """
    return name in _VENDOR_REGISTRY


# ============================================================================
# Parser Registry API (Backward Compatibility)
# ============================================================================
# These functions provide backward compatibility with the old parser registry API.
# They delegate to vendor plugins behind the scenes.


def get_parser(vendor: str, data_type: str) -> Callable[[pl.DataFrame], pl.DataFrame]:
    """Get parser for vendor and data type.

    This is a compatibility wrapper around the vendor plugin system.

    Args:
        vendor: Vendor name (e.g., "tardis", "binance_vision")
        data_type: Data type (e.g., "trades", "quotes", "klines")

    Returns:
        Parser function

    Raises:
        KeyError: If vendor not registered or parser not found
    """
    vendor_plugin = get_vendor(vendor)

    if not vendor_plugin.supports_parsers:
        raise KeyError(
            f"Vendor '{vendor}' does not provide parsers. "
            f"Available parser vendors: {[v for v in list_vendors() if get_vendor(v).supports_parsers]}"
        )

    parsers = vendor_plugin.get_parsers()
    if data_type not in parsers:
        available = list(parsers.keys())
        raise KeyError(
            f"No parser registered for vendor={vendor}, data_type={data_type}. "
            f"Available for {vendor}: {available}"
        )

    return parsers[data_type]


def list_supported_combinations() -> list[tuple[str, str]]:
    """List all supported (vendor, data_type) combinations.

    Returns:
        Sorted list of (vendor, data_type) tuples
    """
    combinations = []
    for vendor_name in list_vendors():
        vendor = get_vendor(vendor_name)
        if vendor.supports_parsers:
            parsers = vendor.get_parsers()
            for data_type in parsers.keys():
                combinations.append((vendor_name, data_type))
    return sorted(combinations)


def is_parser_registered(vendor: str, data_type: str) -> bool:
    """Check if parser is registered for vendor and data type.

    Args:
        vendor: Vendor name
        data_type: Data type

    Returns:
        True if parser exists
    """
    try:
        get_parser(vendor, data_type)
        return True
    except KeyError:
        return False


# Legacy decorator for backward compatibility
# Note: This is now redundant - parsers should be registered within vendor plugins
def register_parser(vendor: str, data_type: str):
    """Decorator to register a parser function (legacy compatibility).

    NOTE: This decorator is deprecated. Parsers should be registered
    automatically when vendor plugins are imported.

    This decorator is kept for backward compatibility with existing code.
    """

    def decorator(func: Callable[[pl.DataFrame], pl.DataFrame]):
        # In the new architecture, parsers are registered via vendor plugins.
        # This decorator is a no-op but preserves the syntax for migration.
        return func

    return decorator
