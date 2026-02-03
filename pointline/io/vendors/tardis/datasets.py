from __future__ import annotations

import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from pointline.config import get_bronze_root

try:
    from tardis_dev import datasets as tardis_datasets
except ImportError as _import_error:
    tardis_datasets = None
    _TARDIS_IMPORT_ERROR = _import_error


def download_tardis_datasets(
    *,
    exchange: str,
    data_types: list[str],
    symbols: list[str],
    from_date: str,
    to_date: str,
    filename_template: str,
    download_dir: str | Path | None = None,
    format: str = "csv",
    api_key: str | None = None,
    concurrency: int = 5,
    http_proxy: str | None = None,
) -> None:
    """Download datasets from Tardis using the official SDK."""
    if tardis_datasets is None:
        raise ImportError(
            "tardis-dev package is required. "
            "Install with: pip install tardis-dev\n"
            f"Original error: {_TARDIS_IMPORT_ERROR}"
        )

    api_key = api_key or os.getenv("TARDIS_API_KEY", "")
    if not api_key:
        raise ValueError("Tardis API key is required.")

    if download_dir is None:
        download_dir = get_bronze_root("tardis")
    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    get_filename = _build_get_filename(filename_template)

    tardis_datasets.download(
        exchange=exchange,
        data_types=data_types,
        symbols=symbols,
        from_date=from_date,
        to_date=to_date,
        format=format,
        api_key=api_key,
        download_dir=str(download_dir),
        get_filename=get_filename,
        concurrency=concurrency,
        http_proxy=http_proxy,
    )


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
