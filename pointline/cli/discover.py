"""``pointline discover`` — symbol discovery from dim_symbol."""

from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("discover", help="Discover symbols from dim_symbol")
    p.add_argument("--exchange", required=True, help="Exchange code")
    p.add_argument("--silver-root", default=None, help="Silver data root directory")
    p.add_argument("--query", default=None, dest="q", help="Search query for symbol/name filtering")
    p.add_argument(
        "--as-of", default=None, help="Point-in-time filter (ISO datetime or microsecond timestamp)"
    )
    p.add_argument("--limit", type=int, default=50, help="Max results (default: 50)")
    p.set_defaults(handler=_handle)


def _handle(args: argparse.Namespace) -> int:
    from pointline.cli._config import resolve_root, resolve_silver_root
    from pointline.cli._output import write_output
    from pointline.research.discovery import discover_symbols

    root = resolve_root(getattr(args, "root", None))
    silver_root = resolve_silver_root(args.silver_root, root=root)

    try:
        df = discover_symbols(
            silver_root=silver_root,
            exchange=args.exchange,
            q=args.q,
            as_of=args.as_of,
            limit=args.limit,
        )
    except (KeyError, ValueError) as exc:
        print(f"error: {exc}")
        return 1

    if df.is_empty():
        print("No symbols found.")
        return 0

    write_output(df, fmt="table")
    return 0
