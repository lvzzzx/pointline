"""Timestamp parsing for Quant360 local exchange timestamps."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def parse_quant360_timestamp(value: str | int) -> int:
    ts = str(value).strip()
    if len(ts) != 17 or not ts.isdigit():
        raise ValueError(
            f"Invalid Quant360 timestamp format: {value!r}. Expected YYYYMMDDHHMMSSmmm."
        )

    dt_local = datetime(
        int(ts[0:4]),
        int(ts[4:6]),
        int(ts[6:8]),
        int(ts[8:10]),
        int(ts[10:12]),
        int(ts[12:14]),
        int(ts[14:17]) * 1000,
        tzinfo=_SHANGHAI_TZ,
    )
    return int(dt_local.astimezone(timezone.utc).timestamp() * 1_000_000)
