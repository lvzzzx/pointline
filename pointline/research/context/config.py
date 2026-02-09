"""Typed configuration models for context/risk plugins."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ContextSpec:
    """Normalized context plugin request spec."""

    name: str
    plugin: str
    required_columns: list[str]
    params: dict[str, Any]
    mode_allowlist: list[str]
    pit_policy: dict[str, Any]
    determinism_policy: dict[str, Any]
    version: str
    impl_ref: str = ""

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ContextSpec:
        """Create a typed spec from request payload."""
        return cls(
            name=str(payload["name"]),
            plugin=str(payload["plugin"]),
            required_columns=list(payload.get("required_columns", [])),
            params=dict(payload.get("params", {})),
            mode_allowlist=list(payload.get("mode_allowlist", [])),
            pit_policy=dict(payload.get("pit_policy", {})),
            determinism_policy=dict(payload.get("determinism_policy", {})),
            version=str(payload.get("version", "")),
            impl_ref=str(payload.get("impl_ref", "")),
        )
