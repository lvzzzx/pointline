"""Feature engineering orchestrator."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

import polars as pl

from pointline.research import core as research_core
from pointline.research.features import core as feature_core
from pointline.research.features import families
from pointline.research.features.spines import ClockSpineConfig
from pointline.types import TimestampInput


def _default_spine_config() -> feature_core.EventSpineConfig:
    """Default spine config: 1-second clock intervals."""
    return feature_core.EventSpineConfig(builder_config=ClockSpineConfig(step_ms=1000))


@dataclass(frozen=True)
class FeatureRunConfig:
    """Configuration for feature generation."""

    spine: feature_core.EventSpineConfig = field(default_factory=_default_spine_config)
    include_microstructure: bool = True
    include_trade_flow: bool = True
    include_flow_rolling: bool = True
    include_funding: bool = True
    include_book_shape: bool = True
    include_execution_cost: bool = True
    include_spread_dynamics: bool = True
    include_liquidity_shock: bool = True
    include_basis_momentum: bool = True
    include_trade_burst: bool = True
    include_cross_venue: bool = False
    include_regime: bool = True
    flow_window_rows: int = 50
    book_shape_depth: int = 10
    liquidity_window_rows: int = 50
    basis_window_rows: int = 100
    trade_burst_window_rows: int = 100
    cross_venue_spot_mid_col: str = "spot_mid_px"
    cross_venue_perp_mid_col: str = "perp_mid_px"
    cross_venue_spot_symbol_id: int | None = None
    cross_venue_perp_symbol_id: int | None = None
    cross_venue_step_ms: int = 1000
    regime_window_rows: int = 30


def _scan_table(
    table: str,
    *,
    symbol_id: int | Iterable[int],
    start_ts_us: TimestampInput,
    end_ts_us: TimestampInput,
    columns: list[str],
) -> pl.LazyFrame:
    return research_core.scan_table(
        table,
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        columns=columns,
    )


def load_feature_inputs(
    *,
    symbol_id: int | Iterable[int],
    start_ts_us: TimestampInput,
    end_ts_us: TimestampInput,
    include_quotes: bool = False,
) -> dict[str, pl.LazyFrame]:
    """Load the canonical inputs for feature computation."""
    tables: dict[str, pl.LazyFrame] = {}

    tables["book_snapshot_25"] = _scan_table(
        "book_snapshot_25",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        columns=[
            "ts_local_us",
            "exchange_id",
            "symbol_id",
            "bids_px_int",
            "asks_px_int",
            "bids_sz_int",
            "asks_sz_int",
            "file_id",
            "file_line_number",
        ],
    )

    tables["trades"] = _scan_table(
        "trades",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        columns=[
            "ts_local_us",
            "exchange_id",
            "symbol_id",
            "side",
            "px_int",
            "qty_int",
            "file_id",
            "file_line_number",
        ],
    )

    tables["derivative_ticker"] = _scan_table(
        "derivative_ticker",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        columns=[
            "ts_local_us",
            "exchange_id",
            "symbol_id",
            "mark_px",
            "index_px",
            "last_px",
            "funding_rate",
            "open_interest",
            "file_id",
            "file_line_number",
        ],
    )

    if include_quotes:
        tables["quotes"] = _scan_table(
            "quotes",
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            columns=[
                "ts_local_us",
                "exchange_id",
                "symbol_id",
                "bid_px_int",
                "ask_px_int",
                "bid_sz_int",
                "ask_sz_int",
                "file_id",
                "file_line_number",
            ],
        )

    return tables


def build_cross_venue_mid_frame(
    *,
    spot_symbol_id: int,
    perp_symbol_id: int,
    start_ts_us: TimestampInput,
    end_ts_us: TimestampInput,
    step_ms: int = 1000,
    spot_mid_col: str = "spot_mid_px",
    perp_mid_col: str = "perp_mid_px",
) -> pl.LazyFrame:
    """Build a PIT-aligned frame with spot/perp mid prices on a shared clock spine."""
    spine = feature_core.build_clock_spine(
        symbol_id=perp_symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        step_ms=step_ms,
    )

    def _mid_frame(symbol_id: int, mid_col: str) -> pl.LazyFrame:
        lf = _scan_table(
            "book_snapshot_25",
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            columns=[
                "ts_local_us",
                "exchange_id",
                "symbol_id",
                "bids_px_int",
                "asks_px_int",
                "file_id",
                "file_line_number",
            ],
        )
        bid_px = pl.col("bids_px_int").list.get(0)
        ask_px = pl.col("asks_px_int").list.get(0)
        mid = ((ask_px + bid_px) / 2.0).alias(mid_col)
        return lf.with_columns([mid]).select(
            ["ts_local_us", "file_id", "file_line_number", mid_col]
        )

    spot_mid = _mid_frame(spot_symbol_id, spot_mid_col).sort(
        ["ts_local_us", "file_id", "file_line_number"]
    )
    perp_mid = _mid_frame(perp_symbol_id, perp_mid_col).sort(
        ["ts_local_us", "file_id", "file_line_number"]
    )

    aligned = spine.sort(["ts_local_us"]).join_asof(
        spot_mid,
        on="ts_local_us",
        strategy="backward",
        suffix="_spot",
    )
    aligned = aligned.join_asof(
        perp_mid,
        on="ts_local_us",
        strategy="backward",
        suffix="_perp",
    )
    return aligned.select(["ts_local_us", spot_mid_col, perp_mid_col])


def build_feature_frame(
    *,
    symbol_id: int | Iterable[int],
    start_ts_us: TimestampInput,
    end_ts_us: TimestampInput,
    config: FeatureRunConfig | None = None,
) -> pl.LazyFrame:
    """Build a PIT-aligned feature frame for the requested symbols."""
    if config is None:
        config = FeatureRunConfig()

    spine = feature_core.build_event_spine(
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        config=config.spine,
    )

    tables = load_feature_inputs(
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
    )

    aligned = feature_core.pit_align(spine, tables)

    if config.include_microstructure:
        aligned = families.add_microstructure_features(aligned)
    if config.include_trade_flow:
        aligned = families.add_trade_flow_features(aligned)
    if config.include_flow_rolling:
        aligned = families.add_flow_rolling_features(aligned, window_rows=config.flow_window_rows)
    if config.include_funding:
        aligned = families.add_funding_features(aligned)
    if config.include_book_shape:
        aligned = families.add_book_shape_features(aligned, depth=config.book_shape_depth)
    if config.include_execution_cost:
        aligned = families.add_execution_cost_features(aligned)
    if config.include_spread_dynamics:
        aligned = families.add_spread_dynamics_features(aligned)
    if config.include_liquidity_shock:
        aligned = families.add_liquidity_shock_features(
            aligned, window_rows=config.liquidity_window_rows
        )
    if config.include_basis_momentum:
        aligned = families.add_basis_momentum_features(
            aligned, window_rows=config.basis_window_rows
        )
    if config.include_trade_burst:
        aligned = families.add_trade_burst_features(
            aligned, window_rows=config.trade_burst_window_rows
        )
    if config.include_cross_venue:
        if config.cross_venue_spot_symbol_id is None or config.cross_venue_perp_symbol_id is None:
            raise ValueError(
                "cross_venue_spot_symbol_id and cross_venue_perp_symbol_id are required "
                "when include_cross_venue=True."
            )
        cross_mid = build_cross_venue_mid_frame(
            spot_symbol_id=config.cross_venue_spot_symbol_id,
            perp_symbol_id=config.cross_venue_perp_symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            step_ms=config.cross_venue_step_ms,
            spot_mid_col=config.cross_venue_spot_mid_col,
            perp_mid_col=config.cross_venue_perp_mid_col,
        )
        aligned = aligned.join(
            cross_mid,
            on="ts_local_us",
            how="left",
        )
        aligned = families.add_cross_venue_features(
            aligned,
            spot_mid_col=config.cross_venue_spot_mid_col,
            perp_mid_col=config.cross_venue_perp_mid_col,
        )
    if config.include_regime:
        aligned = families.add_regime_features(aligned, window_rows=config.regime_window_rows)

    return aligned
