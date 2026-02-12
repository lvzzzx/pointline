"""Tests for per-asset-class fixed-point encoding profiles."""

import polars as pl
import pytest

from pointline.encoding import (
    PROFILES,
    decode_amount,
    decode_price,
    encode_amount,
    encode_price,
    get_profile,
    get_profile_by_asset_class,
)


class TestProfileLookup:
    """Test profile resolution."""

    def test_crypto_exchange_resolves_to_crypto_profile(self):
        """Crypto exchanges should resolve to the crypto profile."""
        for exchange in ["binance", "binance-futures", "deribit", "bybit"]:
            profile = get_profile(exchange)
            assert profile == PROFILES["crypto"], f"Failed for {exchange}"

    def test_cn_equity_exchange_resolves_to_cn_equity_profile(self):
        """Chinese equity exchanges should resolve to cn-equity profile."""
        for exchange in ["szse", "sse"]:
            profile = get_profile(exchange)
            assert profile == PROFILES["cn-equity"], f"Failed for {exchange}"

    def test_unknown_exchange_raises(self):
        """Unknown exchange should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown exchange"):
            get_profile("nonexistent-exchange")

    def test_asset_class_lookup_direct(self):
        """Direct asset class lookup should work."""
        assert get_profile_by_asset_class("crypto-spot") == PROFILES["crypto"]
        assert get_profile_by_asset_class("crypto-derivatives") == PROFILES["crypto"]
        assert get_profile_by_asset_class("stocks-cn") == PROFILES["cn-equity"]

    def test_unknown_asset_class_raises(self):
        """Unknown asset class should raise ValueError."""
        with pytest.raises(ValueError, match="No encoding profile"):
            get_profile_by_asset_class("forex")

    def test_profile_is_frozen(self):
        """ScalarProfile should be immutable."""
        profile = PROFILES["crypto"]
        with pytest.raises(AttributeError):
            profile.price = 1e-6  # type: ignore


class TestEncodeDecodeRoundtrip:
    """Test encode/decode roundtrip for different profiles."""

    def test_roundtrip_crypto_price(self):
        """Encode then decode should recover original price for crypto."""
        profile = PROFILES["crypto"]
        df = pl.DataFrame({"price_px": [50000.0, 0.001, 99999.99]})

        encoded = df.select(encode_price("price_px", profile).alias("px_int"))
        decoded = encoded.select(decode_price("px_int", profile).alias("price_px"))

        for orig, result in zip(
            df["price_px"].to_list(), decoded["price_px"].to_list(), strict=True
        ):
            assert result == pytest.approx(orig, rel=1e-9)

    def test_roundtrip_cn_equity_price(self):
        """Encode then decode should recover original price for CN equity."""
        profile = PROFILES["cn-equity"]
        df = pl.DataFrame({"price_px": [10.50, 100.01, 0.01]})

        encoded = df.select(encode_price("price_px", profile).alias("px_int"))
        decoded = encoded.select(decode_price("px_int", profile).alias("price_px"))

        for orig, result in zip(
            df["price_px"].to_list(), decoded["price_px"].to_list(), strict=True
        ):
            assert result == pytest.approx(orig, rel=1e-9)

    def test_roundtrip_crypto_amount(self):
        """Encode then decode should recover original amount for crypto."""
        profile = PROFILES["crypto"]
        df = pl.DataFrame({"qty": [0.1, 0.00001, 1000.0]})

        encoded = df.select(encode_amount("qty", profile).alias("qty_int"))
        decoded = encoded.select(decode_amount("qty_int", profile).alias("qty"))

        for orig, result in zip(df["qty"].to_list(), decoded["qty"].to_list(), strict=True):
            assert result == pytest.approx(orig, rel=1e-9)

    def test_roundtrip_cn_equity_amount(self):
        """CN equity amounts are integers (1 share = 1 unit)."""
        profile = PROFILES["cn-equity"]
        df = pl.DataFrame({"qty": [100.0, 200.0, 1.0]})

        encoded = df.select(encode_amount("qty", profile).alias("qty_int"))
        decoded = encoded.select(decode_amount("qty_int", profile).alias("qty"))

        for orig, result in zip(df["qty"].to_list(), decoded["qty"].to_list(), strict=True):
            assert result == pytest.approx(orig, rel=1e-9)

    def test_int64_range_crypto_price(self):
        """Verify crypto prices stay within Int64 range."""
        profile = PROFILES["crypto"]
        # Max safe price: 2^63 * 1e-9 ~ 9.2 billion (well beyond any price)
        max_price = 1_000_000.0  # 1M USD
        df = pl.DataFrame({"price_px": [max_price]})
        encoded = df.select(encode_price("price_px", profile).alias("px_int"))
        assert encoded["px_int"][0] == 1_000_000_000_000_000  # Fits in Int64

    def test_int64_range_cn_equity_price(self):
        """Verify CN equity prices stay within Int64 range."""
        profile = PROFILES["cn-equity"]
        max_price = 10_000.0  # 10000 CNY (high-priced stock)
        df = pl.DataFrame({"price_px": [max_price]})
        encoded = df.select(encode_price("price_px", profile).alias("px_int"))
        assert encoded["px_int"][0] == 100_000_000  # Fits easily in Int64


class TestScalarProfileValues:
    """Verify scalar profile constant values are correct."""

    def test_crypto_profile_values(self):
        profile = PROFILES["crypto"]
        assert profile.price == 1e-9
        assert profile.amount == 1e-9
        assert profile.rate == 1e-12
        assert profile.quote_vol == 1e-6

    def test_cn_equity_profile_values(self):
        profile = PROFILES["cn-equity"]
        assert profile.price == 1e-4
        assert profile.amount == 1.0
        assert profile.rate == 1e-8
        assert profile.quote_vol == 1e-4
