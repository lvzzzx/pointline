"""Typed metadata primitives for Quant360 adapter parsing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Quant360ArchiveMeta:
    source_filename: str
    stream_type: str
    market: str
    exchange: str
    trading_date: date
    canonical_data_type: str
