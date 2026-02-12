"""V2 ingestion result models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class IngestionResult:
    status: str
    row_count: int
    rows_written: int
    rows_quarantined: int
    file_id: int | None = None
    skipped: bool = False
    failure_reason: str | None = None
    error_message: str | None = None
    trading_date_min: date | None = None
    trading_date_max: date | None = None
