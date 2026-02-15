"""Pointline CLI — unified operational interface for the data lake.

Entry point: ``pointline`` console script via ``main()``.
"""

from __future__ import annotations

import argparse
import sys


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pointline",
        description="Pointline data lake CLI",
    )
    parser.add_argument(
        "--root",
        default=None,
        help="Lake root directory (derives bronze/ and silver/). Env: POINTLINE_ROOT",
    )
    sub = parser.add_subparsers(dest="command")

    # Lazy-import each command module to avoid pulling in Polars/Delta for schema-only use.
    from pointline.cli import (
        compact,
        dim_symbol,
        discover,
        ingest,
        manifest,
        query,
        schema,
        vacuum,
    )

    schema.register(sub)
    ingest.register(sub)
    manifest.register(sub)
    dim_symbol.register(sub)
    compact.register(sub)
    vacuum.register(sub)
    query.register(sub)
    discover.register(sub)

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.  Returns exit code (0=ok, 1=user error, 2=data error)."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 1

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 1

    try:
        return handler(args) or 0
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 1
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1


def cli() -> None:  # pragma: no cover
    """Console-script wrapper that calls ``sys.exit``."""
    sys.exit(main())
