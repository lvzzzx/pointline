"""Download commands."""

from __future__ import annotations

import argparse

from pointline.io.vendors.tardis import download_tardis_datasets


def cmd_download(args: argparse.Namespace) -> int:
    """Download Tardis datasets into the Bronze layer."""
    data_types = [item.strip() for item in args.data_types.split(",") if item.strip()]
    symbols = [item.strip() for item in args.symbols.split(",") if item.strip()]

    if not data_types or not symbols:
        print("Error: data-types and symbols must be non-empty")
        return 1

    try:
        download_tardis_datasets(
            exchange=args.exchange,
            data_types=data_types,
            symbols=symbols,
            from_date=args.from_date,
            to_date=args.to_date,
            format=args.format,
            api_key=args.api_key,
            download_dir=args.download_dir,
            filename_template=args.filename_template,
            concurrency=args.concurrency,
            http_proxy=args.http_proxy,
        )
        print("Download complete.")
        return 0
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}")
        return 2
