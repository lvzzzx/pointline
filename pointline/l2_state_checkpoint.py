"""Build utilities for gold.l2_state_checkpoint."""

from __future__ import annotations

from datetime import date
from pathlib import Path


def build_state_checkpoints_delta(
    *,
    updates_path: Path | str,
    output_path: Path | str,
    exchange: str | None,
    exchange_id: int | None,
    symbol_id: int,
    start_date: date | str,
    end_date: date | str,
    checkpoint_every_us: int | None = None,
    checkpoint_every_updates: int | None = None,
    validate_monotonic: bool = False,
    assume_sorted: bool = False,
) -> int:
    """Build state checkpoints via the Rust replay engine and write to Delta."""
    try:
        import l2_replay as _l2_replay
    except Exception as exc:  # pragma: no cover - optional import
        raise ImportError(
            "l2_replay Rust extension not available; build the PyO3 module to use this path."
        ) from exc

    start_str = start_date.isoformat() if isinstance(start_date, date) else str(start_date)
    end_str = end_date.isoformat() if isinstance(end_date, date) else str(end_date)

    return _l2_replay.build_state_checkpoints(
        updates_path=str(updates_path),
        output_path=str(output_path),
        exchange=exchange,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        start_date=start_str,
        end_date=end_str,
        checkpoint_every_us=checkpoint_every_us,
        checkpoint_every_updates=checkpoint_every_updates,
        validate_monotonic=validate_monotonic,
        assume_sorted=assume_sorted,
    )
