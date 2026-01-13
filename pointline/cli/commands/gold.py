"""Gold table commands."""

from __future__ import annotations

import argparse

from pointline.cli.utils import parse_symbol_id_single
from pointline.config import get_table_path
from pointline.tables.l2_state_checkpoint import build_state_checkpoints_delta
from pointline.registry import resolve_symbol


def cmd_l2_state_checkpoint_build(args: argparse.Namespace) -> int:
    try:
        symbol_id = parse_symbol_id_single(args.symbol_id)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 2

    if symbol_id is None:
        print("Error: symbol_id is required for l2-state-checkpoint")
        return 2

    exchange, exchange_id, _ = resolve_symbol(symbol_id)

    rows_written = build_state_checkpoints_delta(
        updates_path=get_table_path("l2_updates"),
        output_path=get_table_path("l2_state_checkpoint"),
        exchange=exchange,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        start_date=args.start_date,
        end_date=args.end_date,
        checkpoint_every_us=args.checkpoint_every_us,
        checkpoint_every_updates=args.checkpoint_every_updates,
        validate_monotonic=args.validate_monotonic,
        assume_sorted=args.assume_sorted,
    )
    print(f"l2_state_checkpoint: wrote {rows_written} row(s)")
    return 0
