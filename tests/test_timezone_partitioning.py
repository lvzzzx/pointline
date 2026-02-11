"""Tests for exchange-local date partitioning with timezone handling."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from pointline.config import get_exchange_timezone


class TestExchangeTimezoneRegistry:
    """Test timezone registry configuration."""

    def test_crypto_exchanges_use_utc(self):
        """Verify crypto exchanges default to UTC timezone."""
        assert get_exchange_timezone("binance-futures") == "UTC"
        assert get_exchange_timezone("coinbase") == "UTC"
        assert get_exchange_timezone("okx") == "UTC"

    def test_chinese_exchanges_use_cst(self):
        """Verify Chinese exchanges use Asia/Shanghai timezone."""
        assert get_exchange_timezone("szse") == "Asia/Shanghai"
        assert get_exchange_timezone("sse") == "Asia/Shanghai"

    def test_unknown_exchange_raises_in_strict_mode(self):
        """Verify unknown exchanges raise ValueError in strict mode (default)."""
        with pytest.raises(ValueError, match="not found in dim_exchange"):
            get_exchange_timezone("unknown-exchange")

    def test_unknown_exchange_defaults_to_utc_in_non_strict_mode(self):
        """Verify unknown exchanges default to UTC in non-strict mode."""
        with pytest.warns(UserWarning, match="not found in dim_exchange"):
            result = get_exchange_timezone("unknown-exchange", strict=False)
        assert result == "UTC"


class TestExchangeLocalDatePartitioning:
    """Test date partition derivation with exchange-local timezones."""

    def test_szse_cst_boundary_early_morning(self):
        """
        Verify early CST trading times map to correct date partition.

        For SZSE trading day 2024-09-30:
        - 2024-09-30 00:30 CST = 2024-09-29 16:30 UTC
        - Should map to date=2024-09-30 (CST date), not 2024-09-29
        """
        # Create timestamp: 2024-09-30 00:30:00 CST
        ts_cst = datetime(2024, 9, 30, 0, 30, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        ts_us = int(ts_cst.timestamp() * 1_000_000)

        df = pl.DataFrame({"ts_local_us": [ts_us]})

        # Convert to CST date (simulating _add_metadata logic)
        exchange_tz = get_exchange_timezone("szse")
        result = df.with_columns(
            [
                pl.from_epoch(pl.col("ts_local_us"), time_unit="us")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone(exchange_tz)
                .dt.date()
                .alias("date"),
            ]
        )

        # Both should map to CST date 2024-09-30
        assert result["date"][0] == date(2024, 9, 30)

    def test_szse_cst_boundary_late_night(self):
        """
        Verify late CST trading times map to correct date partition.

        For SZSE trading day 2024-09-30:
        - 2024-09-30 23:30 CST = 2024-09-30 15:30 UTC
        - Should map to date=2024-09-30
        """
        # Create timestamp: 2024-09-30 23:30:00 CST
        ts_cst = datetime(2024, 9, 30, 23, 30, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        ts_us = int(ts_cst.timestamp() * 1_000_000)

        df = pl.DataFrame({"ts_local_us": [ts_us]})

        # Convert to CST date
        exchange_tz = get_exchange_timezone("szse")
        result = df.with_columns(
            [
                pl.from_epoch(pl.col("ts_local_us"), time_unit="us")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone(exchange_tz)
                .dt.date()
                .alias("date"),
            ]
        )

        assert result["date"][0] == date(2024, 9, 30)

    def test_szse_full_trading_day_single_partition(self):
        """
        Verify an entire CST trading day maps to exactly one partition.

        SZSE trading hours: 09:30-15:00 CST
        All events within 2024-09-30 00:00-23:59 CST should map to date=2024-09-30.
        """
        # Create timestamps spanning entire CST day
        timestamps_cst = [
            datetime(2024, 9, 30, 0, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai")),  # Midnight
            datetime(2024, 9, 30, 9, 30, 0, tzinfo=ZoneInfo("Asia/Shanghai")),  # Market open
            datetime(2024, 9, 30, 12, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai")),  # Noon
            datetime(2024, 9, 30, 15, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai")),  # Market close
            datetime(2024, 9, 30, 23, 59, 59, tzinfo=ZoneInfo("Asia/Shanghai")),  # End of day
        ]
        ts_us_list = [int(ts.timestamp() * 1_000_000) for ts in timestamps_cst]

        df = pl.DataFrame({"ts_local_us": ts_us_list})

        # Convert to CST date
        exchange_tz = get_exchange_timezone("szse")
        result = df.with_columns(
            [
                pl.from_epoch(pl.col("ts_local_us"), time_unit="us")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone(exchange_tz)
                .dt.date()
                .alias("date"),
            ]
        )

        # All timestamps should map to same date partition
        assert result["date"].unique().to_list() == [date(2024, 9, 30)]

    def test_crypto_utc_partitioning_unchanged(self):
        """
        Verify crypto exchanges continue to use UTC date partitioning.

        This ensures backward compatibility for crypto data.
        """
        # Create UTC timestamp: 2024-09-30 00:30:00 UTC
        ts_utc = datetime(2024, 9, 30, 0, 30, 0, tzinfo=ZoneInfo("UTC"))
        ts_us = int(ts_utc.timestamp() * 1_000_000)

        df = pl.DataFrame({"ts_local_us": [ts_us]})

        # Convert to UTC date (binance-futures uses UTC)
        exchange_tz = get_exchange_timezone("binance-futures")
        result = df.with_columns(
            [
                pl.from_epoch(pl.col("ts_local_us"), time_unit="us")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone(exchange_tz)
                .dt.date()
                .alias("date"),
            ]
        )

        assert result["date"][0] == date(2024, 9, 30)

    def test_utc_vs_cst_same_instant_different_dates(self):
        """
        Verify same UTC instant produces different partition dates for different exchanges.

        Example: 2024-09-29 16:30 UTC
        - For binance-futures (UTC): date=2024-09-29
        - For SZSE (CST): date=2024-09-30 (next day at 00:30 CST)
        """
        # UTC: 2024-09-29 16:30:00
        ts_utc = datetime(2024, 9, 29, 16, 30, 0, tzinfo=ZoneInfo("UTC"))
        ts_us = int(ts_utc.timestamp() * 1_000_000)

        df = pl.DataFrame({"ts_local_us": [ts_us]})

        # Binance (UTC timezone)
        binance_tz = get_exchange_timezone("binance-futures")
        binance_result = df.with_columns(
            [
                pl.from_epoch(pl.col("ts_local_us"), time_unit="us")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone(binance_tz)
                .dt.date()
                .alias("date"),
            ]
        )
        assert binance_result["date"][0] == date(2024, 9, 29)

        # SZSE (CST timezone, UTC+8)
        szse_tz = get_exchange_timezone("szse")
        szse_result = df.with_columns(
            [
                pl.from_epoch(pl.col("ts_local_us"), time_unit="us")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone(szse_tz)
                .dt.date()
                .alias("date"),
            ]
        )
        # 16:30 UTC = 00:30+1 CST (next day)
        assert szse_result["date"][0] == date(2024, 9, 30)
