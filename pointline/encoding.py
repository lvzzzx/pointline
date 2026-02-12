"""Per-asset-class fixed-point encoding scalars.

The encoding scalar is a property of the asset class, not the instrument.
This decouples encoding from mutable exchange metadata (tick_size, lot_size)
and eliminates the need for dim_symbol joins on encode/decode paths.

IMPORTANT: Once data is written with a profile's scalars, those scalars must
NEVER change — all historical data depends on them.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class ScalarProfile:
    """Fixed-point encoding scalars for an asset class.

    Each field defines what integer value 1 represents in the encoded domain:
      px_int = round(price / price_scalar)
      price  = px_int * price_scalar

    Attributes:
        price: Scalar for prices (value of px_int=1)
        amount: Scalar for quantities (value of qty_int=1)
        rate: Scalar for rates — funding, interest (value of rate_int=1)
        quote_vol: Scalar for quote volumes (value of quote_vol_int=1)
    """

    price: float
    amount: float
    rate: float
    quote_vol: float


# Temporary scalar columns used by decode paths.
PROFILE_PRICE_COL = "__pl_profile_price"
PROFILE_AMOUNT_COL = "__pl_profile_amount"
PROFILE_RATE_COL = "__pl_profile_rate"
PROFILE_QUOTE_VOL_COL = "__pl_profile_quote_vol"
PROFILE_SCALAR_COLS: tuple[str, ...] = (
    PROFILE_PRICE_COL,
    PROFILE_AMOUNT_COL,
    PROFILE_RATE_COL,
    PROFILE_QUOTE_VOL_COL,
)


# ---------------------------------------------------------------------------
# Profile definitions — immutable after first data is written
# ---------------------------------------------------------------------------

PROFILES: dict[str, ScalarProfile] = {
    # Crypto: sub-cent tokens, fractional BTC quantities
    "crypto": ScalarProfile(
        price=1e-9,  # nano-dollar, covers 0.000000001 to 9.2e9
        amount=1e-9,  # nano-unit, covers 0.000000001 BTC
        rate=1e-12,  # pico-unit, funding rates ~1e-4 to 1e-6
        quote_vol=1e-6,  # micro-dollar, up to $9.2T
    ),
    # Chinese equities: 0.01 CNY tick, integer shares
    "cn-equity": ScalarProfile(
        price=1e-4,  # sub-fen (0.0001 CNY), covers up to 922T
        amount=1.0,  # 1 share — quantities are always integers
        rate=1e-8,  # for dividend yields, interest rates
        quote_vol=1e-4,  # 0.0001 CNY resolution for turnover
    ),
}

# Leaf-level asset_class → profile key
_ASSET_CLASS_TO_PROFILE: dict[str, str] = {
    "crypto-spot": "crypto",
    "crypto-derivatives": "crypto",
    "stocks-cn": "cn-equity",
}


def get_profile(exchange: str) -> ScalarProfile:
    """Resolve encoding profile for an exchange.

    Looks up the exchange's asset_class via config, then maps to a profile.

    Args:
        exchange: Exchange name (e.g., "binance-futures", "szse")

    Returns:
        ScalarProfile for the exchange's asset class

    Raises:
        ValueError: If exchange or its asset class has no profile
    """
    from pointline.config import get_exchange_metadata

    meta = get_exchange_metadata(exchange)
    if meta is None:
        raise ValueError(f"Unknown exchange: {exchange!r}")

    asset_class = meta.get("asset_class", "unknown")
    return get_profile_by_asset_class(asset_class)


def get_profile_by_asset_class(asset_class: str) -> ScalarProfile:
    """Resolve encoding profile for an asset class.

    Args:
        asset_class: Leaf-level asset class (e.g., "crypto-spot", "stocks-cn")

    Returns:
        ScalarProfile for the asset class

    Raises:
        ValueError: If asset class has no profile
    """
    profile_key = _ASSET_CLASS_TO_PROFILE.get(asset_class)
    if profile_key is None:
        raise ValueError(
            f"No encoding profile for asset class {asset_class!r}. "
            f"Known: {sorted(_ASSET_CLASS_TO_PROFILE.keys())}"
        )
    return PROFILES[profile_key]


# ---------------------------------------------------------------------------
# Polars expression helpers — pure functions, no DataFrame joins
# ---------------------------------------------------------------------------


def encode_price(col: str, profile: ScalarProfile) -> pl.Expr:
    """Encode a float price column to fixed-point Int64."""
    return (pl.col(col) / profile.price).round().cast(pl.Int64)


def decode_price(col: str, profile: ScalarProfile) -> pl.Expr:
    """Decode a fixed-point Int64 price column to Float64."""
    return (pl.col(col) * profile.price).cast(pl.Float64)


def encode_amount(col: str, profile: ScalarProfile) -> pl.Expr:
    """Encode a float quantity column to fixed-point Int64."""
    return (pl.col(col) / profile.amount).round().cast(pl.Int64)


def decode_amount(col: str, profile: ScalarProfile) -> pl.Expr:
    """Decode a fixed-point Int64 quantity column to Float64."""
    return (pl.col(col) * profile.amount).cast(pl.Float64)


def encode_rate(col: str, profile: ScalarProfile) -> pl.Expr:
    """Encode a float rate column to fixed-point Int64."""
    return (pl.col(col) / profile.rate).round().cast(pl.Int64)


def decode_rate(col: str, profile: ScalarProfile) -> pl.Expr:
    """Decode a fixed-point Int64 rate column to Float64."""
    return (pl.col(col) * profile.rate).cast(pl.Float64)


def encode_quote_vol(col: str, profile: ScalarProfile) -> pl.Expr:
    """Encode a float quote volume column to fixed-point Int64."""
    return (pl.col(col) / profile.quote_vol).round().cast(pl.Int64)


def decode_quote_vol(col: str, profile: ScalarProfile) -> pl.Expr:
    """Decode a fixed-point Int64 quote volume column to Float64."""
    return (pl.col(col) * profile.quote_vol).cast(pl.Float64)


def encode_nullable_price(col: str, profile: ScalarProfile) -> pl.Expr:
    """Encode a nullable float price column to fixed-point Int64."""
    return (
        pl.when(pl.col(col).is_not_null())
        .then((pl.col(col) / profile.price).round().cast(pl.Int64))
        .otherwise(None)
    )


def decode_nullable_price(col: str, profile: ScalarProfile) -> pl.Expr:
    """Decode a nullable fixed-point Int64 price column to Float64."""
    return (
        pl.when(pl.col(col).is_not_null())
        .then((pl.col(col) * profile.price).cast(pl.Float64))
        .otherwise(None)
    )


def decode_nullable_amount(col: str, profile: ScalarProfile) -> pl.Expr:
    """Decode a nullable fixed-point Int64 quantity column to Float64."""
    return (
        pl.when(pl.col(col).is_not_null())
        .then((pl.col(col) * profile.amount).cast(pl.Float64))
        .otherwise(None)
    )


def with_profile_scalars(df: pl.DataFrame, *, exchange_col: str = "exchange") -> pl.DataFrame:
    """Attach per-row scalar columns resolved from the exchange column.

    This enables mixed-exchange decode in a single DataFrame.
    """
    if exchange_col not in df.columns:
        raise ValueError(f"decode_fixed_point: no '{exchange_col}' column")

    has_null_exchange = df.select(pl.col(exchange_col).is_null().any()).item()
    if has_null_exchange:
        raise ValueError(f"decode_fixed_point: '{exchange_col}' contains null values")

    if df.is_empty():
        return df.with_columns(
            [
                pl.lit(None, dtype=pl.Float64).alias(PROFILE_PRICE_COL),
                pl.lit(None, dtype=pl.Float64).alias(PROFILE_AMOUNT_COL),
                pl.lit(None, dtype=pl.Float64).alias(PROFILE_RATE_COL),
                pl.lit(None, dtype=pl.Float64).alias(PROFILE_QUOTE_VOL_COL),
            ]
        )

    exchanges = df.get_column(exchange_col).unique().to_list()
    profiles = {exchange: get_profile(exchange) for exchange in exchanges}

    profile_df = pl.DataFrame(
        {
            exchange_col: exchanges,
            PROFILE_PRICE_COL: [profiles[exchange].price for exchange in exchanges],
            PROFILE_AMOUNT_COL: [profiles[exchange].amount for exchange in exchanges],
            PROFILE_RATE_COL: [profiles[exchange].rate for exchange in exchanges],
            PROFILE_QUOTE_VOL_COL: [profiles[exchange].quote_vol for exchange in exchanges],
        }
    )
    return df.join(profile_df, on=exchange_col, how="left")


def with_profile_scalars_lazy(lf: pl.LazyFrame, *, exchange_col: str = "exchange") -> pl.LazyFrame:
    """Attach per-row scalar columns lazily from exchange values.

    Unlike the eager variant, this resolves profile values via expressions so
    decode can remain lazy end-to-end.
    """
    schema = lf.collect_schema()
    if exchange_col not in schema:
        raise ValueError(f"decode_fixed_point: no '{exchange_col}' column")

    def _price(exchange: str | None) -> float:
        if exchange is None:
            raise ValueError(f"decode_fixed_point: '{exchange_col}' contains null values")
        return get_profile(exchange).price

    def _amount(exchange: str | None) -> float:
        if exchange is None:
            raise ValueError(f"decode_fixed_point: '{exchange_col}' contains null values")
        return get_profile(exchange).amount

    def _rate(exchange: str | None) -> float:
        if exchange is None:
            raise ValueError(f"decode_fixed_point: '{exchange_col}' contains null values")
        return get_profile(exchange).rate

    def _quote_vol(exchange: str | None) -> float:
        if exchange is None:
            raise ValueError(f"decode_fixed_point: '{exchange_col}' contains null values")
        return get_profile(exchange).quote_vol

    return lf.with_columns(
        [
            pl.col(exchange_col)
            .map_elements(_price, return_dtype=pl.Float64)
            .alias(PROFILE_PRICE_COL),
            pl.col(exchange_col)
            .map_elements(_amount, return_dtype=pl.Float64)
            .alias(PROFILE_AMOUNT_COL),
            pl.col(exchange_col)
            .map_elements(_rate, return_dtype=pl.Float64)
            .alias(PROFILE_RATE_COL),
            pl.col(exchange_col)
            .map_elements(_quote_vol, return_dtype=pl.Float64)
            .alias(PROFILE_QUOTE_VOL_COL),
        ]
    )
