"""Filename and member-path parsing for Quant360 archive inputs."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from pointline.v2.vendors.quant360.types import Quant360ArchiveMeta

_ARCHIVE_RE = re.compile(
    r"^(?P<stream_type>order_new|tick_new|L2_new|L1_new)_(?P<market>STK|ConFI)_(?P<exchange>SZ|SH)_(?P<date>\d{8})\.7z$"
)

_EXCHANGE_MAP: dict[str, str] = {
    "SZ": "szse",
    "SH": "sse",
}

_STREAM_TO_CANONICAL: dict[str, str] = {
    "order_new": "cn_order_events",
    "tick_new": "cn_tick_events",
    "L2_new": "cn_l2_snapshots",
}


def parse_archive_filename(name: str) -> Quant360ArchiveMeta:
    match = _ARCHIVE_RE.match(name)
    if match is None:
        raise ValueError(
            "Invalid Quant360 archive filename. Expected "
            "'<type>_<market>_<exchange>_<YYYYMMDD>.7z'."
        )

    stream_type = match.group("stream_type")
    try:
        canonical_data_type = _STREAM_TO_CANONICAL[stream_type]
    except KeyError as exc:
        raise ValueError(f"Unsupported Quant360 stream type: {stream_type}") from exc

    date_str = match.group("date")
    return Quant360ArchiveMeta(
        source_filename=name,
        stream_type=stream_type,
        market=match.group("market"),
        exchange=_EXCHANGE_MAP[match.group("exchange")],
        trading_date=datetime.strptime(date_str, "%Y%m%d").date(),
        canonical_data_type=canonical_data_type,
    )


def parse_symbol_from_member_path(path: str) -> str:
    member = Path(path)
    if member.suffix.lower() != ".csv":
        raise ValueError(f"Quant360 member path must point to CSV: {path}")
    symbol = member.stem.strip()
    if not symbol:
        raise ValueError(f"Cannot derive symbol from member path: {path}")
    return symbol
