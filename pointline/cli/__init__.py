"""Pointline CLI package."""

from __future__ import annotations

from typing import Sequence

from pointline.cli.parser import build_parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 2
    return args.func(args)
