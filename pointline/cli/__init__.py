"""Pointline CLI package."""

from __future__ import annotations

from collections.abc import Sequence

from pointline.cli.parser import build_parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the Pointline CLI.

    Args:
        argv: Command-line arguments. If None, uses sys.argv.

    Returns:
        Exit code (0 for success, 2 if no command specified).
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 2
    return args.func(args)
