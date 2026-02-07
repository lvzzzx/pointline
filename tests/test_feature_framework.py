import polars as pl

from pointline.research.features import core as feature_core
from pointline.research.features import families


def test_build_clock_spine_row_count():
    lf = feature_core.build_clock_spine(
        symbol_id=[1, 2],
        exchange_id=[10, 10],
        start_ts_us=0,
        end_ts_us=2_000_000,
        step_ms=1000,
        max_rows=10,
    )
    df = lf.collect()
    assert df.height == 6
    assert set(df.columns) == {"symbol_id", "exchange_id", "ts_local_us"}


def test_microstructure_features():
    df = pl.DataFrame(
        {
            "exchange_id": [1],
            "symbol_id": [100],
            "ts_local_us": [1],
            "bids_px_int": [[100, 99, 98]],
            "asks_px_int": [[101, 102, 103]],
            "bids_sz_int": [[5, 4, 3]],
            "asks_sz_int": [[6, 5, 4]],
        }
    )
    lf = families.add_microstructure_features(df.lazy())
    out = lf.collect()
    assert out["ms_bid_px_int"][0] == 100
    assert out["ms_ask_px_int"][0] == 101
    assert out["ms_spread_int"][0] == 1
    assert out["ms_imbalance_1"][0] == (5 - 6) / (5 + 6)


def test_trade_flow_features():
    df = pl.DataFrame(
        {
            "exchange_id": [1, 1],
            "symbol_id": [100, 100],
            "ts_local_us": [1, 2],
            "side": [0, 1],
            "px_int": [100, 101],
            "qty_int": [2, 3],
        }
    )
    lf = families.add_trade_flow_features(df.lazy())
    out = lf.collect()
    assert out["of_trade_sign"].to_list() == [1, -1]
    assert out["of_signed_qty_int"].to_list() == [2, -3]


def test_flow_rolling_features():
    df = pl.DataFrame(
        {
            "exchange_id": [1, 1, 1],
            "symbol_id": [100, 100, 100],
            "ts_local_us": [1, 2, 3],
            "side": [0, 1, 0],
            "px_int": [100, 101, 102],
            "qty_int": [2, 3, 4],
        }
    ).with_columns(
        [
            pl.when(pl.col("side") == 0)
            .then(pl.col("qty_int"))
            .when(pl.col("side") == 1)
            .then(-pl.col("qty_int"))
            .otherwise(0)
            .alias("of_signed_qty_int")
        ]
    )
    lf = families.add_flow_rolling_features(df.lazy(), window_rows=2)
    out = lf.collect()
    assert out["of_trade_count_2"].to_list()[-1] == 2


def test_book_shape_features():
    df = pl.DataFrame(
        {
            "exchange_id": [1],
            "symbol_id": [100],
            "ts_local_us": [1],
            "bids_px_int": [[100, 99]],
            "asks_px_int": [[101, 102]],
            "bids_sz_int": [[5, 5]],
            "asks_sz_int": [[6, 6]],
        }
    )
    lf = families.add_book_shape_features(df.lazy(), depth=2)
    out = lf.collect()
    assert out["bs_bid_depth_2"][0] == 10
    assert out["bs_ask_depth_2"][0] == 12


def test_execution_cost_features():
    df = pl.DataFrame(
        {
            "exchange_id": [1],
            "symbol_id": [100],
            "ts_local_us": [1],
            "bids_px_int": [[100]],
            "asks_px_int": [[101]],
            "bids_sz_int": [[5]],
            "asks_sz_int": [[5]],
        }
    )
    lf = families.add_execution_cost_features(df.lazy())
    out = lf.collect()
    assert out["ex_spread_int"][0] == 1


def test_spread_dynamics_features():
    df = pl.DataFrame(
        {
            "exchange_id": [1, 1],
            "symbol_id": [100, 100],
            "ts_local_us": [1, 2],
            "bids_px_int": [[100], [101]],
            "asks_px_int": [[102], [103]],
            "bids_sz_int": [[5], [5]],
            "asks_sz_int": [[5], [5]],
        }
    )
    lf = families.add_spread_dynamics_features(df.lazy())
    out = lf.collect()
    assert out["sd_spread_int"].to_list() == [2, 2]
    assert out["sd_mid_chg"][1] == 1


def test_liquidity_shock_features():
    df = pl.DataFrame(
        {
            "exchange_id": [1, 1, 1],
            "symbol_id": [100, 100, 100],
            "ts_local_us": [1, 2, 3],
            "bids_px_int": [[100], [100], [100]],
            "asks_px_int": [[101], [101], [101]],
            "bids_sz_int": [[5], [5], [5]],
            "asks_sz_int": [[5], [5], [5]],
        }
    )
    lf = families.add_liquidity_shock_features(df.lazy(), window_rows=2)
    out = lf.collect()
    assert "ls_depth_z_2" in out.columns


def test_basis_momentum_features():
    df = pl.DataFrame(
        {
            "exchange_id": [1, 1, 1],
            "symbol_id": [100, 100, 100],
            "ts_local_us": [1, 2, 3],
            "mark_px": [100.0, 101.0, 102.0],
            "index_px": [99.0, 100.0, 100.0],
        }
    )
    lf = families.add_basis_momentum_features(df.lazy(), window_rows=2)
    out = lf.collect()
    assert "bm_basis_z_2" in out.columns


def test_trade_burst_features():
    df = pl.DataFrame(
        {
            "exchange_id": [1, 1, 1],
            "symbol_id": [100, 100, 100],
            "ts_local_us": [1, 3, 6],
        }
    )
    lf = families.add_trade_burst_features(df.lazy(), window_rows=2)
    out = lf.collect()
    assert "tb_burst_score_2" in out.columns


def test_cross_venue_features():
    df = pl.DataFrame(
        {
            "exchange_id": [1],
            "symbol_id": [100],
            "ts_local_us": [1],
            "spot_mid_px": [100.0],
            "perp_mid_px": [101.0],
        }
    )
    lf = families.add_cross_venue_features(df.lazy())
    out = lf.collect()
    assert out["cv_basis"][0] == 1.0
