"""Feature family builders for MFT research workflows."""

from __future__ import annotations

import polars as pl

from pointline.tables.trades import SIDE_BUY, SIDE_SELL


def _top_of_book_exprs() -> tuple[pl.Expr, pl.Expr, pl.Expr, pl.Expr]:
    bid_px = pl.col("bids_px_int").list.get(0)
    ask_px = pl.col("asks_px_int").list.get(0)
    bid_sz = pl.col("bids_sz_int").list.get(0)
    ask_sz = pl.col("asks_sz_int").list.get(0)
    return bid_px, ask_px, bid_sz, ask_sz


def add_microstructure_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Add top-of-book and depth imbalance features."""
    bid_px, ask_px, bid_sz, ask_sz = _top_of_book_exprs()

    top_bid = bid_px.alias("ms_bid_px_int")
    top_ask = ask_px.alias("ms_ask_px_int")
    top_bid_sz = bid_sz.alias("ms_bid_sz_int")
    top_ask_sz = ask_sz.alias("ms_ask_sz_int")

    spread = (ask_px - bid_px).alias("ms_spread_int")
    mid = ((ask_px + bid_px) / 2.0).alias("ms_mid_px")

    denom = bid_sz + ask_sz
    microprice = (
        pl.when(denom > 0)
        .then((ask_px * bid_sz + bid_px * ask_sz) / denom)
        .otherwise(None)
        .alias("ms_microprice")
    )

    def _imbalance(k: int) -> pl.Expr:
        bid_sum = pl.col("bids_sz_int").list.slice(0, k).list.sum()
        ask_sum = pl.col("asks_sz_int").list.slice(0, k).list.sum()
        denom_k = bid_sum + ask_sum
        return (
            pl.when(denom_k > 0)
            .then((bid_sum - ask_sum) / denom_k)
            .otherwise(None)
            .alias(f"ms_imbalance_{k}")
        )

    return lf.with_columns(
        [
            top_bid,
            top_ask,
            top_bid_sz,
            top_ask_sz,
            spread,
            mid,
            microprice,
            _imbalance(1),
            _imbalance(5),
            _imbalance(10),
            _imbalance(25),
        ]
    )


def add_trade_flow_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Add last-trade and signed flow proxies."""
    signed_qty = (
        pl.when(pl.col("side") == SIDE_BUY)
        .then(pl.col("qty_int"))
        .when(pl.col("side") == SIDE_SELL)
        .then(-pl.col("qty_int"))
        .otherwise(0)
        .alias("of_signed_qty_int")
    )

    trade_sign = (
        pl.when(pl.col("side") == SIDE_BUY)
        .then(1)
        .when(pl.col("side") == SIDE_SELL)
        .then(-1)
        .otherwise(0)
        .alias("of_trade_sign")
    )

    return lf.with_columns(
        [
            signed_qty,
            trade_sign,
            pl.col("qty_int").alias("of_last_qty_int"),
            pl.col("px_int").alias("of_last_px_int"),
        ]
    )


def add_flow_rolling_features(lf: pl.LazyFrame, *, window_rows: int = 50) -> pl.LazyFrame:
    """Add rolling flow imbalance and trade intensity proxies."""
    if window_rows <= 1:
        raise ValueError("window_rows must be > 1")

    lf = lf.sort(["exchange_id", "symbol_id", "ts_local_us"])

    signed_qty = pl.col("of_signed_qty_int")
    abs_qty = pl.col("qty_int").abs()
    trade_count = pl.when(pl.col("qty_int").is_not_null()).then(1).otherwise(0)

    signed_sum = (
        signed_qty.rolling_sum(window_size=window_rows)
        .over(["exchange_id", "symbol_id"])
        .alias(f"of_signed_qty_sum_{window_rows}")
    )
    abs_sum = (
        abs_qty.rolling_sum(window_size=window_rows)
        .over(["exchange_id", "symbol_id"])
        .alias(f"of_abs_qty_sum_{window_rows}")
    )
    count_sum = (
        trade_count.rolling_sum(window_size=window_rows)
        .over(["exchange_id", "symbol_id"])
        .alias(f"of_trade_count_{window_rows}")
    )

    imbalance = (
        pl.when(pl.col(f"of_abs_qty_sum_{window_rows}") > 0)
        .then(pl.col(f"of_signed_qty_sum_{window_rows}") / pl.col(f"of_abs_qty_sum_{window_rows}"))
        .otherwise(None)
        .alias(f"of_flow_imbalance_{window_rows}")
    )

    return lf.with_columns([signed_sum, abs_sum, count_sum]).with_columns([imbalance])


def add_funding_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Add perp funding/basis features."""
    basis = (pl.col("mark_px") - pl.col("index_px")).alias("fr_basis")
    basis_bps = (
        pl.when(pl.col("index_px") != 0)
        .then(10_000 * (pl.col("mark_px") - pl.col("index_px")) / pl.col("index_px"))
        .otherwise(None)
        .alias("fr_basis_bps")
    )
    funding_delta = pl.col("funding_rate").diff().alias("fr_funding_delta")
    oi_delta = pl.col("open_interest").diff().alias("fr_oi_delta")

    return lf.with_columns([basis, basis_bps, funding_delta, oi_delta])


def add_book_shape_features(lf: pl.LazyFrame, *, depth: int = 10) -> pl.LazyFrame:
    """Add book slope and depth-weighted price proxies."""
    if depth <= 1:
        raise ValueError("depth must be > 1")

    bid_px = pl.col("bids_px_int").list.slice(0, depth)
    ask_px = pl.col("asks_px_int").list.slice(0, depth)
    bid_sz = pl.col("bids_sz_int").list.slice(0, depth)
    ask_sz = pl.col("asks_sz_int").list.slice(0, depth)

    bid_depth = bid_sz.list.sum().alias(f"bs_bid_depth_{depth}")
    ask_depth = ask_sz.list.sum().alias(f"bs_ask_depth_{depth}")

    bid_px_w = (bid_px * bid_sz).list.sum()
    ask_px_w = (ask_px * ask_sz).list.sum()

    bid_vwap = (pl.when(bid_depth > 0).then(bid_px_w / bid_depth).otherwise(None)).alias(
        f"bs_bid_vwap_{depth}"
    )
    ask_vwap = (pl.when(ask_depth > 0).then(ask_px_w / ask_depth).otherwise(None)).alias(
        f"bs_ask_vwap_{depth}"
    )

    slope = (
        pl.when((bid_depth + ask_depth) > 0)
        .then((ask_vwap - bid_vwap) / (bid_depth + ask_depth))
        .otherwise(None)
        .alias(f"bs_slope_{depth}")
    )

    return lf.with_columns([bid_depth, ask_depth, bid_vwap, ask_vwap, slope])


def add_execution_cost_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Add simple execution cost proxies from top-of-book data."""
    bid_px, ask_px, bid_sz, ask_sz = _top_of_book_exprs()
    spread = (ask_px - bid_px).alias("ex_spread_int")
    half_spread = ((ask_px - bid_px) / 2.0).alias("ex_half_spread_int")

    depth_denom = bid_sz + ask_sz
    depth_pressure = (pl.when(depth_denom > 0).then(bid_sz / depth_denom).otherwise(None)).alias(
        "ex_depth_pressure"
    )

    return lf.with_columns([spread, half_spread, depth_pressure])


def add_spread_dynamics_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Add short-horizon dynamics of spread and microprice."""
    bid_px, ask_px, _, _ = _top_of_book_exprs()
    spread = (ask_px - bid_px).alias("sd_spread_int")
    mid = ((ask_px + bid_px) / 2.0).alias("sd_mid_px")

    mid_chg = pl.col("sd_mid_px").diff().alias("sd_mid_chg")
    spread_chg = pl.col("sd_spread_int").diff().alias("sd_spread_chg")

    return (
        lf.with_columns([spread, mid])
        .with_columns([mid_chg, spread_chg])
        .with_columns(
            [
                pl.col("sd_mid_chg").abs().alias("sd_mid_abs_chg"),
                pl.col("sd_spread_chg").abs().alias("sd_spread_abs_chg"),
            ]
        )
    )


def add_liquidity_shock_features(lf: pl.LazyFrame, *, window_rows: int = 50) -> pl.LazyFrame:
    """Add rolling depth shock features based on top-of-book depth."""
    if window_rows <= 1:
        raise ValueError("window_rows must be > 1")

    _, _, bid_sz, ask_sz = _top_of_book_exprs()
    depth_expr = bid_sz + ask_sz
    depth = depth_expr.alias("ls_top_depth")
    depth_mean = (
        depth_expr.rolling_mean(window_size=window_rows)
        .over(["exchange_id", "symbol_id"])
        .alias(f"ls_depth_mean_{window_rows}")
    )
    depth_std = (
        depth_expr.rolling_std(window_size=window_rows)
        .over(["exchange_id", "symbol_id"])
        .alias(f"ls_depth_std_{window_rows}")
    )
    depth_z = (
        pl.when(pl.col(f"ls_depth_std_{window_rows}") > 0)
        .then(
            (pl.col("ls_top_depth") - pl.col(f"ls_depth_mean_{window_rows}"))
            / pl.col(f"ls_depth_std_{window_rows}")
        )
        .otherwise(None)
        .alias(f"ls_depth_z_{window_rows}")
    )

    return lf.with_columns([depth, depth_mean, depth_std]).with_columns([depth_z])


def add_basis_momentum_features(lf: pl.LazyFrame, *, window_rows: int = 100) -> pl.LazyFrame:
    """Add rolling basis momentum and z-score features."""
    if window_rows <= 1:
        raise ValueError("window_rows must be > 1")

    basis_expr = pl.col("mark_px") - pl.col("index_px")
    basis = basis_expr.alias("bm_basis")
    basis_mean = (
        basis_expr.rolling_mean(window_size=window_rows)
        .over(["exchange_id", "symbol_id"])
        .alias(f"bm_basis_mean_{window_rows}")
    )
    basis_std = (
        basis_expr.rolling_std(window_size=window_rows)
        .over(["exchange_id", "symbol_id"])
        .alias(f"bm_basis_std_{window_rows}")
    )
    basis_z = (
        pl.when(pl.col(f"bm_basis_std_{window_rows}") > 0)
        .then(
            (pl.col("bm_basis") - pl.col(f"bm_basis_mean_{window_rows}"))
            / pl.col(f"bm_basis_std_{window_rows}")
        )
        .otherwise(None)
        .alias(f"bm_basis_z_{window_rows}")
    )
    basis_mom = basis_expr.diff().over(["exchange_id", "symbol_id"]).alias("bm_basis_mom")

    return lf.with_columns([basis, basis_mean, basis_std, basis_mom]).with_columns([basis_z])


def add_trade_burst_features(lf: pl.LazyFrame, *, window_rows: int = 100) -> pl.LazyFrame:
    """Add inter-arrival timing and burstiness proxies."""
    if window_rows <= 1:
        raise ValueError("window_rows must be > 1")

    lf = lf.sort(["exchange_id", "symbol_id", "ts_local_us"])
    dt_expr = pl.col("ts_local_us").diff().over(["exchange_id", "symbol_id"])
    dt_us = dt_expr.alias("tb_dt_us")
    dt_mean = (
        dt_expr.rolling_mean(window_size=window_rows)
        .over(["exchange_id", "symbol_id"])
        .alias(f"tb_dt_mean_{window_rows}")
    )
    dt_std = (
        dt_expr.rolling_std(window_size=window_rows)
        .over(["exchange_id", "symbol_id"])
        .alias(f"tb_dt_std_{window_rows}")
    )
    burst_score = (
        pl.when(pl.col(f"tb_dt_mean_{window_rows}") > 0)
        .then(pl.col("tb_dt_us") / pl.col(f"tb_dt_mean_{window_rows}"))
        .otherwise(None)
        .alias(f"tb_burst_score_{window_rows}")
    )

    return lf.with_columns([dt_us, dt_mean, dt_std]).with_columns([burst_score])


def add_cross_venue_features(
    lf: pl.LazyFrame,
    *,
    spot_mid_col: str = "spot_mid_px",
    perp_mid_col: str = "perp_mid_px",
) -> pl.LazyFrame:
    """Add cross-venue dislocation features.

    Requires pre-joined columns for spot and perp mid prices.
    """
    basis = (pl.col(perp_mid_col) - pl.col(spot_mid_col)).alias("cv_basis")
    basis_bps = (
        pl.when(pl.col(spot_mid_col) != 0)
        .then(10_000 * (pl.col(perp_mid_col) - pl.col(spot_mid_col)) / pl.col(spot_mid_col))
        .otherwise(None)
        .alias("cv_basis_bps")
    )
    return lf.with_columns([basis, basis_bps])


def add_regime_features(lf: pl.LazyFrame, *, window_rows: int = 30) -> pl.LazyFrame:
    """Add short-horizon return and volatility proxies."""
    if window_rows <= 1:
        raise ValueError("window_rows must be > 1")

    lf = lf.sort(["exchange_id", "symbol_id", "ts_local_us"])
    ret = pl.col("ms_mid_px").pct_change().alias("rg_ret_1")
    vol = (
        pl.col("rg_ret_1")
        .rolling_std(window_size=window_rows)
        .over(["exchange_id", "symbol_id"])
        .alias(f"rg_vol_{window_rows}")
    )
    return lf.with_columns([ret]).with_columns([vol])
