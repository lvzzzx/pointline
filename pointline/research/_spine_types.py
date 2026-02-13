"""Typed config models for v2 research spine builders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias

BuilderName: TypeAlias = Literal["clock", "trades", "volume", "dollar"]


@dataclass(frozen=True)
class ClockSpineConfig:
    step_us: int
    max_rows: int = 5_000_000


@dataclass(frozen=True)
class TradesSpineConfig:
    max_rows: int = 5_000_000


@dataclass(frozen=True)
class VolumeSpineConfig:
    volume_threshold_scaled: int
    max_rows: int = 5_000_000


@dataclass(frozen=True)
class DollarSpineConfig:
    dollar_threshold_scaled: int
    max_rows: int = 5_000_000


SpineConfig: TypeAlias = (
    ClockSpineConfig | TradesSpineConfig | VolumeSpineConfig | DollarSpineConfig
)
