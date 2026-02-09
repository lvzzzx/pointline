"""Registry and validation for context/risk plugins."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from .config import ContextSpec

ContextCallable = Callable[[pl.LazyFrame, ContextSpec], pl.LazyFrame]


@dataclass(frozen=True)
class ContextMetadata:
    """Metadata contract for context plugin implementations."""

    name: str
    required_columns: list[str]
    mode_allowlist: list[str]
    pit_policy: dict[str, Any]
    determinism_policy: dict[str, Any]
    version: str
    impl_ref: str
    apply_context: ContextCallable
    required_params: dict[str, str] = field(default_factory=dict)
    optional_params: dict[str, str] = field(default_factory=dict)
    default_params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.mode_allowlist:
            raise ValueError(f"{self.name}: mode_allowlist cannot be empty")

        overlap = set(self.required_params).intersection(self.optional_params)
        if overlap:
            raise ValueError(f"{self.name}: params overlap in required/optional: {sorted(overlap)}")

        allowed = set(self.required_params) | set(self.optional_params)
        unknown_defaults = sorted(set(self.default_params) - allowed)
        if unknown_defaults:
            raise ValueError(
                f"{self.name}: defaults provided for unknown params: {unknown_defaults}"
            )


class ContextRegistry:
    """Global registry for context plugin metadata and callables."""

    _registry: dict[str, ContextMetadata] = {}

    @classmethod
    def register_context(
        cls,
        *,
        name: str,
        required_columns: list[str],
        mode_allowlist: list[str],
        pit_policy: dict[str, Any] | None = None,
        determinism_policy: dict[str, Any] | None = None,
        required_params: dict[str, str] | None = None,
        optional_params: dict[str, str] | None = None,
        default_params: dict[str, Any] | None = None,
    ):
        """Register a context plugin callable.

        Callable signature:
            func(frame: pl.LazyFrame, spec: ContextSpec) -> pl.LazyFrame
        """

        def decorator(func: ContextCallable):
            cls._registry[name] = ContextMetadata(
                name=name,
                required_columns=list(required_columns),
                mode_allowlist=list(mode_allowlist),
                pit_policy=pit_policy or {"feature_direction": "backward_only"},
                determinism_policy=determinism_policy
                or {"required_sort": ["exchange_id", "symbol_id", "ts_local_us"]},
                version="2.0",
                impl_ref=f"{func.__module__}.{func.__name__}",
                apply_context=func,
                required_params=required_params or {},
                optional_params=optional_params or {},
                default_params=default_params or {},
            )
            return func

        return decorator

    @classmethod
    def exists(cls, name: str) -> bool:
        return name in cls._registry

    @classmethod
    def get(cls, name: str) -> ContextMetadata:
        if name not in cls._registry:
            raise ValueError(f"Context plugin not registered: {name}")
        return cls._registry[name]

    @classmethod
    def validate_for_mode(cls, name: str, research_mode: str) -> None:
        meta = cls.get(name)
        if research_mode not in meta.mode_allowlist:
            raise ValueError(
                f"Context plugin {name} not allowed in {research_mode}. "
                f"Allowed modes: {meta.mode_allowlist}"
            )

    @classmethod
    def normalize_params(cls, name: str, params: dict[str, Any] | None) -> dict[str, Any]:
        meta = cls.get(name)
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise ValueError(f"Context params for {name} must be an object")

        merged = dict(meta.default_params)
        merged.update(params)
        return merged

    @classmethod
    def validate_params(cls, name: str, params: dict[str, Any] | None) -> None:
        meta = cls.get(name)
        merged = cls.normalize_params(name, params)

        missing = sorted(key for key in meta.required_params if key not in merged)
        if missing:
            raise ValueError(f"Context plugin {name} missing required params: {missing}")

        allowed = set(meta.required_params) | set(meta.optional_params)
        unknown = sorted(set(merged) - allowed)
        if unknown:
            raise ValueError(f"Context plugin {name} has unknown params: {unknown}")

        for key, value in merged.items():
            type_name = meta.required_params.get(key) or meta.optional_params.get(key)
            if type_name is None:
                continue
            if not _matches_type(type_name, value):
                raise ValueError(
                    f"Context plugin {name} param {key} expected {type_name}, "
                    f"got {type(value).__name__}"
                )

    @classmethod
    def validate_required_columns(
        cls,
        name: str,
        available_columns: set[str],
        params: dict[str, Any] | None,
    ) -> None:
        meta = cls.get(name)
        merged = cls.normalize_params(name, params)

        missing = sorted(col for col in meta.required_columns if col not in available_columns)
        if missing:
            raise ValueError(f"Context plugin {name} missing required columns: {missing}")

        param_types = dict(meta.required_params)
        param_types.update(meta.optional_params)
        for key, type_name in param_types.items():
            if type_name != "column_name":
                continue
            if key not in merged:
                continue
            col_name = merged[key]
            if col_name not in available_columns:
                raise ValueError(
                    f"Context plugin {name} references missing column via {key}: {col_name}"
                )

    @classmethod
    def list_plugins(cls) -> list[str]:
        return sorted(cls._registry.keys())


def _matches_type(type_name: str, value: Any) -> bool:
    match type_name:
        case "string":
            return isinstance(value, str)
        case "column_name":
            return isinstance(value, str)
        case "number":
            return isinstance(value, int | float) and not isinstance(value, bool)
        case "integer":
            return isinstance(value, int) and not isinstance(value, bool)
        case "boolean":
            return isinstance(value, bool)
        case _:
            raise ValueError(f"Unsupported param type in context metadata: {type_name}")
