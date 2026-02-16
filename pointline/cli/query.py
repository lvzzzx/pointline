"""``pointline query`` — load events from Silver tables."""

from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("query", help="Query event data from a Silver table")
    p.add_argument("table", help="Event table name (e.g. trades, quotes)")
    p.add_argument("--exchange", required=True, help="Exchange code")
    p.add_argument("--symbol", required=True, help="Symbol")
    p.add_argument(
        "--start", required=True, help="Start time (ISO datetime, date, or microsecond timestamp)"
    )
    p.add_argument(
        "--end", required=True, help="End time (ISO datetime, date, or microsecond timestamp)"
    )
    p.add_argument("--silver-root", default=None, help="Silver data root directory")
    p.add_argument("--columns", default=None, help="Comma-separated column names to select")
    p.add_argument(
        "--format",
        choices=["table", "csv", "json", "parquet"],
        default="table",
        help="Output format (default: table)",
    )
    p.add_argument("--output", default=None, help="Output file path (default: stdout)")
    p.add_argument("--limit", type=int, default=None, help="Limit number of output rows")
    p.set_defaults(handler=_handle)


def _handle(args: argparse.Namespace) -> int:
    from pointline.cli._config import resolve_root, resolve_silver_root
    from pointline.cli._output import write_output
    from pointline.research.query import load_events

    root = resolve_root(getattr(args, "root", None))
    silver_root = resolve_silver_root(args.silver_root, root=root)
    columns = [c.strip() for c in args.columns.split(",")] if args.columns else None

    try:
        df = load_events(
            silver_root=silver_root,
            table=args.table,
            exchange=args.exchange,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            columns=columns,
        )
    except (KeyError, ValueError) as exc:
        print(f"error: {exc}")
        return 1

    if df.is_empty():
        print("No rows found.")
        return 0

    write_output(df, fmt=args.format, output=args.output, limit=args.limit)
    return 0
