"""Execution engine for context/risk plugins."""

from __future__ import annotations

from typing import Any

import polars as pl

from .config import ContextSpec
from .registry import ContextRegistry


def apply_context_plugins(
    frame: pl.LazyFrame,
    context_specs: list[dict[str, Any]] | None,
    *,
    research_mode: str,
) -> pl.LazyFrame:
    """Apply context plugins in request order.

    Args:
        frame: Bar-level feature frame
        context_specs: List of context plugin specs from request
        research_mode: HFT/MFT/LFT mode gate for plugin allowlist

    Returns:
        LazyFrame with context/risk columns appended
    """
    if not context_specs:
        return frame

    result = frame
    for payload in context_specs:
        spec = ContextSpec.from_dict(payload)
        plugin_name = spec.plugin

        if not ContextRegistry.exists(plugin_name):
            raise ValueError(f"Context plugin not registered: {plugin_name}")

        ContextRegistry.validate_for_mode(plugin_name, research_mode)
        ContextRegistry.validate_params(plugin_name, spec.params)
        available_columns = set(result.collect_schema().names())
        ContextRegistry.validate_required_columns(plugin_name, available_columns, spec.params)

        meta = ContextRegistry.get(plugin_name)
        normalized_params = ContextRegistry.normalize_params(plugin_name, spec.params)
        resolved = ContextSpec(
            name=spec.name,
            plugin=spec.plugin,
            required_columns=spec.required_columns or meta.required_columns,
            params=normalized_params,
            mode_allowlist=spec.mode_allowlist or meta.mode_allowlist,
            pit_policy=spec.pit_policy or meta.pit_policy,
            determinism_policy=spec.determinism_policy or meta.determinism_policy,
            version=spec.version or meta.version,
            impl_ref=spec.impl_ref or meta.impl_ref,
        )
        result = meta.apply_context(result, resolved)

    return result
