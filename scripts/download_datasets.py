#!/usr/bin/env python3
"""Download Tardis datasets into the repo's Bronze layout."""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import Callable

from tardis_dev import datasets


def _build_get_filename(template: str) -> Callable[[str, str, datetime, str, str], str]:
    def _get_filename(exchange: str, data_type: str, date: datetime, symbol: str, fmt: str) -> str:
        date_str = date.strftime("%Y-%m-%d")
        path = template.format(
            exchange=exchange,
            data_type=data_type,
            date=date_str,
            symbol=symbol,
            format=fmt,
        )
        if not path.endswith(".gz"):
            path = f"{path}.gz"
        return path

    return _get_filename


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Tardis datasets")
    parser.add_argument("download", nargs="?")
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--data-types", required=True, help="Comma-separated list")
    parser.add_argument("--symbols", required=True, help="Comma-separated list")
    parser.add_argument("--from-date", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD (non-inclusive)")
    parser.add_argument("--format", default="csv", help="Dataset format (default: csv)")
    parser.add_argument(
        "--download-dir",
        default="./data/lake",
        help="Root directory for downloads (default: ./data/lake)",
    )
    parser.add_argument(
        "--filename-template",
        required=True,
        help="Template with {exchange},{data_type},{date},{symbol},{format}",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("TARDIS_API_KEY", ""),
        help="Tardis API key (falls back to TARDIS_API_KEY)",
    )
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--http-proxy", default=None)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    download_dir = Path(args.download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    data_types = [item.strip() for item in args.data_types.split(",") if item.strip()]
    symbols = [item.strip() for item in args.symbols.split(",") if item.strip()]

    if not data_types or not symbols:
        raise SystemExit("data-types and symbols must be non-empty")

    get_filename = _build_get_filename(args.filename_template)

    datasets.download(
        exchange=args.exchange,
        data_types=data_types,
        symbols=symbols,
        from_date=args.from_date,
        to_date=args.to_date,
        format=args.format,
        api_key=args.api_key,
        download_dir=str(download_dir),
        get_filename=get_filename,
        concurrency=args.concurrency,
        http_proxy=args.http_proxy,
    )


if __name__ == "__main__":
    main()
