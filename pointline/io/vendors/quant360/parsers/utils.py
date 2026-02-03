"""Quant360 utility functions.

Shared utilities for parsing Quant360 data files.
"""


def parse_quant360_timestamp(timestamp_str: str | int) -> int:
    """Parse Quant360 timestamp format to microseconds UTC.

    Quant360 format: YYYYMMDDHHMMSSmmm (17 digits, milliseconds precision)
    Timezone: Asia/Shanghai (UTC+8, no DST)

    Example: 20240930091500000 → 2024-09-30 09:15:00.000 CST → 2024-09-30 01:15:00.000 UTC

    Args:
        timestamp_str: Timestamp string or integer in Asia/Shanghai timezone

    Returns:
        Timestamp in microseconds since epoch (UTC, int64)

    Raises:
        ValueError: If timestamp format is invalid
    """
    ts_str = str(timestamp_str).strip()

    if len(ts_str) != 17:
        raise ValueError(
            f"Invalid Quant360 timestamp format: {ts_str}. Expected YYYYMMDDHHMMSSmmm (17 digits)"
        )

    # Parse components
    year = int(ts_str[0:4])
    month = int(ts_str[4:6])
    day = int(ts_str[6:8])
    hour = int(ts_str[8:10])
    minute = int(ts_str[10:12])
    second = int(ts_str[12:14])
    millisecond = int(ts_str[14:17])

    # Create datetime in Asia/Shanghai timezone, then convert to UTC
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo

    dt_shanghai = datetime(
        year, month, day, hour, minute, second, millisecond * 1000, tzinfo=ZoneInfo("Asia/Shanghai")
    )
    dt_utc = dt_shanghai.astimezone(timezone.utc)
    return int(dt_utc.timestamp() * 1_000_000)
