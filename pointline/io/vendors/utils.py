"""Shared utilities for vendor plugins."""

from __future__ import annotations

import gzip
import io
import logging
import zipfile
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def read_csv_with_lineage(
    path: Path,
    *,
    has_header: bool = True,
    columns: list[str] | None = None,
    **read_options,
) -> pl.DataFrame:
    """Standard CSV reader with lineage tracking and compression handling.

    Handles:
    - Gzip compression (.gz, .csv.gz)
    - ZIP archives (.zip - single CSV only; warns if multiple)
    - 7z archives (.7z - single CSV only; warns if multiple)
    - Headerless CSVs (has_header=False with explicit columns)
    - Row numbering (file_line_number column)
    """
    if not has_header and columns is None:
        raise ValueError("Headerless CSV requires explicit column names")

    options = {
        "infer_schema_length": 10000,
        "try_parse_dates": False,
        **read_options,
    }

    if not has_header:
        options["has_header"] = False
        options["new_columns"] = columns

    # For headerless CSVs, add row index after reading to avoid name conflicts.
    add_row_index_after = not has_header

    if not add_row_index_after:
        import inspect

        if "row_index_name" in inspect.signature(pl.read_csv).parameters:
            options["row_index_name"] = "file_line_number"
            options["row_index_offset"] = 2  # Skip header row
        else:
            options["row_count_name"] = "file_line_number"
            options["row_count_offset"] = 2

    try:
        if path.suffix == ".zip":
            df = _read_zip_csv(path, options)
        elif path.suffix == ".7z":
            df = _read_7z_csv(path, options)
        elif path.suffix == ".gz" or str(path).endswith(".csv.gz"):
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                df = pl.read_csv(handle, **options)
        else:
            df = pl.read_csv(path, **options)

        if add_row_index_after and not df.is_empty():
            df = df.with_row_index(name="file_line_number", offset=1)

        return df

    except pl.exceptions.NoDataError:
        return pl.DataFrame()


def _read_zip_csv(path: Path, options: dict) -> pl.DataFrame:
    with zipfile.ZipFile(path) as zf:
        csv_files = [name for name in zf.namelist() if name.endswith(".csv")]
        if not csv_files:
            logger.warning("No CSV file found in ZIP archive: %s", path)
            return pl.DataFrame()
        if len(csv_files) > 1:
            logger.warning(
                "ZIP contains %s CSVs, using first only: %s",
                len(csv_files),
                path,
            )
        with zf.open(csv_files[0]) as handle:
            return pl.read_csv(handle, **options)


def _read_7z_csv(path: Path, options: dict) -> pl.DataFrame:
    try:
        import py7zr
    except ImportError as exc:
        raise ImportError("py7zr is required to read .7z archives") from exc

    with py7zr.SevenZipFile(path, mode="r") as archive:
        names = [name for name in archive.getnames() if name.endswith(".csv")]
        if not names:
            logger.warning("No CSV file found in 7z archive: %s", path)
            return pl.DataFrame()
        if len(names) > 1:
            logger.warning(
                "7z contains %s CSVs, using first only: %s",
                len(names),
                path,
            )
        data = archive.read([names[0]])
        payload = data.get(names[0])
        if payload is None:
            return pl.DataFrame()
        if isinstance(payload, io.BytesIO):
            payload.seek(0)
            return pl.read_csv(payload, **options)
        # py7zr may return raw bytes for small files
        return pl.read_csv(io.BytesIO(payload), **options)
