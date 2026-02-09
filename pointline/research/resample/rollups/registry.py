"""Registry for user-defined Pattern B rollup methods."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import polars as pl

FeatureRollupCallable = Callable[[str, dict[str, Any]], pl.Expr]


@dataclass(frozen=True)
class FeatureRollupMetadata:
    """Metadata contract for custom feature rollup methods."""

    name: str
    required_params: dict[str, str]
    required_columns: list[str]
    mode_allowlist: list[str]
    semantic_allowlist: list[str]
    pit_policy: dict[str, Any]
    determinism_policy: dict[str, Any]
    version: str = "2.0"
    impl_ref: str = ""
    optional_params: dict[str, str] = field(default_factory=dict)
    default_params: dict[str, Any] = field(default_factory=dict)
    compute_rollup: FeatureRollupCallable | None = None

    def __post_init__(self):
        if self.compute_rollup is None:
            raise ValueError(f"{self.name}: compute_rollup callable is required")
        if not self.mode_allowlist:
            raise ValueError(f"{self.name}: mode_allowlist cannot be empty")
        if not self.semantic_allowlist:
            raise ValueError(f"{self.name}: semantic_allowlist cannot be empty")

        overlap = set(self.required_params).intersection(self.optional_params)
        if overlap:
            raise ValueError(
                f"{self.name}: params cannot be both required and optional: {sorted(overlap)}"
            )

        allowed = set(self.required_params) | set(self.optional_params)
        unknown_defaults = sorted(set(self.default_params) - allowed)
        if unknown_defaults:
            raise ValueError(
                f"{self.name}: defaults provided for unknown params: {unknown_defaults}"
            )


class FeatureRollupRegistry:
    """Registry-backed governance for custom feature rollups."""

    _registry: dict[str, FeatureRollupMetadata] = {}

    @classmethod
    def register_feature_rollup(
        cls,
        *,
        name: str,
        required_params: dict[str, str] | None = None,
        required_columns: list[str] | None = None,
        mode_allowlist: list[str],
        semantic_allowlist: list[str],
        pit_policy: dict[str, Any] | None = None,
        determinism_policy: dict[str, Any] | None = None,
        optional_params: dict[str, str] | None = None,
        default_params: dict[str, Any] | None = None,
    ):
        """Register a custom rollup callable.

        Callable signature:
            func(feature_col: str, params: dict[str, Any]) -> pl.Expr
        """

        def decorator(func: FeatureRollupCallable):
            meta = FeatureRollupMetadata(
                name=name,
                required_params=required_params or {},
                required_columns=required_columns or [],
                mode_allowlist=mode_allowlist,
                semantic_allowlist=semantic_allowlist,
                pit_policy=pit_policy or {"feature_direction": "backward_only"},
                determinism_policy=determinism_policy
                or {"required_sort": ["exchange_id", "symbol_id", "ts_local_us"]},
                version="2.0",
                impl_ref=f"{func.__module__}.{func.__name__}",
                optional_params=optional_params or {},
                default_params=default_params or {},
                compute_rollup=func,
            )
            cls._registry[name] = meta
            return func

        return decorator

    @classmethod
    def exists(cls, name: str) -> bool:
        return name in cls._registry

    @classmethod
    def get(cls, name: str) -> FeatureRollupMetadata:
        if name not in cls._registry:
            raise ValueError(f"Feature rollup not registered: {name}")
        return cls._registry[name]

    @classmethod
    def validate_for_mode(cls, name: str, research_mode: str) -> None:
        meta = cls.get(name)
        if research_mode not in meta.mode_allowlist:
            raise ValueError(
                f"Feature rollup {name} not allowed in {research_mode}. "
                f"Allowed modes: {meta.mode_allowlist}"
            )

    @classmethod
    def validate_semantic(cls, name: str, semantic_type: str | None) -> None:
        if semantic_type is None:
            return
        meta = cls.get(name)
        allow = set(meta.semantic_allowlist)
        if "any" in allow:
            return
        if semantic_type not in allow:
            raise ValueError(
                f"Feature rollup {name} not allowed for semantic type {semantic_type}. "
                f"Allowed semantics: {meta.semantic_allowlist}"
            )

    @classmethod
    def normalize_params(cls, name: str, params: dict[str, Any] | None) -> dict[str, Any]:
        meta = cls.get(name)
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise ValueError(f"Feature rollup params for {name} must be an object")

        merged = dict(meta.default_params)
        merged.update(params)
        return merged

    @classmethod
    def validate_params(cls, name: str, params: dict[str, Any] | None) -> None:
        meta = cls.get(name)
        merged = cls.normalize_params(name, params)

        missing = sorted(key for key in meta.required_params if key not in merged)
        if missing:
            raise ValueError(f"Feature rollup {name} missing required params: {missing}")

        allowed = set(meta.required_params) | set(meta.optional_params)
        unknown = sorted(set(merged) - allowed)
        if unknown:
            raise ValueError(f"Feature rollup {name} has unknown params: {unknown}")

        for key, value in merged.items():
            type_name = meta.required_params.get(key) or meta.optional_params.get(key)
            if type_name is None:
                continue
            if not _matches_type(type_name, value):
                raise ValueError(
                    f"Feature rollup {name} param {key} expected {type_name}, "
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
            raise ValueError(f"Feature rollup {name} missing required columns: {missing}")

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
                    f"Feature rollup {name} references missing column via {key}: {col_name}"
                )

    @classmethod
    def build_expr(cls, name: str, feature_col: str, params: dict[str, Any] | None) -> pl.Expr:
        cls.validate_params(name, params)
        normalized = cls.normalize_params(name, params)
        meta = cls.get(name)
        return meta.compute_rollup(feature_col, normalized)


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
            raise ValueError(f"Unsupported param type in rollup metadata: {type_name}")
