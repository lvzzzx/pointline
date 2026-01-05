"""Researcher-facing L2 replay helpers backed by the Rust engine."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterator

from pointline.config import get_table_path

try:
    import l2_replay as _rust_l2_replay
except Exception as exc:  # pragma: no cover - optional import
    raise ImportError(
        "l2_replay: Rust extension not available. Build the PyO3 module to use replay APIs."
    ) from exc


def _normalize_date(value: date | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    return value


def _checkpoint_path() -> str | None:
    path = Path(get_table_path("l2_state_checkpoint"))
    if Path(path, "_delta_log").exists():
        return str(path)
    return None


def snapshot_at(
    *,
    exchange_id: int,
    symbol_id: int,
    ts_local_us: int,
    exchange: str | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
) -> dict[str, object]:
    """Return full-depth bids/asks at a point-in-time using ts_local_us."""
    return _rust_l2_replay.snapshot_at(
        updates_path=str(get_table_path("l2_updates")),
        checkpoint_path=_checkpoint_path(),
        exchange=exchange,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        ts_local_us=ts_local_us,
        start_date=_normalize_date(start_date),
        end_date=_normalize_date(end_date),
    )


def replay_between(
    *,
    exchange_id: int,
    symbol_id: int,
    start_ts_local_us: int,
    end_ts_local_us: int,
    every_us: int | None = None,
    every_updates: int | None = None,
    exchange: str | None = None,
) -> Iterator[dict[str, object]]:
    """Yield snapshots between timestamps on a cadence."""
    snapshots = _rust_l2_replay.replay_between(
        updates_path=str(get_table_path("l2_updates")),
        checkpoint_path=_checkpoint_path(),
        exchange=exchange,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        start_ts_local_us=start_ts_local_us,
        end_ts_local_us=end_ts_local_us,
        every_us=every_us,
        every_updates=every_updates,
    )
    return iter(snapshots)


__all__ = ["snapshot_at", "replay_between"]
