"""Demo: Resampling Methods for Feature Engineering

This example demonstrates how to use different resampling methods
(spine builders) for feature engineering in Pointline.

Run: python examples/resampling_methods_demo.py
"""

from pointline.registry import find_symbol
from pointline.research.features import (
    ClockSpineConfig,
    DollarBarConfig,
    EventSpineConfig,
    FeatureRunConfig,
    TradesSpineConfig,
    VolumeBarConfig,
    build_event_spine,
    build_feature_frame,
)


def demo_clock_spine():
    """Demo 1: Clock spine (fixed time intervals)."""
    print("\n=== Demo 1: Clock Spine (1-second intervals) ===")

    # Find BTCUSDT symbol_id
    symbols = find_symbol("BTCUSDT", exchange="binance-futures")
    if symbols.is_empty():
        print("BTCUSDT not found, skipping demo")
        return

    symbol_id = int(symbols["symbol_id"][0])

    # Build clock spine: 1-second intervals
    config = EventSpineConfig(builder_config=ClockSpineConfig(step_ms=1000))

    spine = build_event_spine(
        symbol_id=symbol_id,
        start_ts_us="2024-05-01T00:00:00Z",
        end_ts_us="2024-05-01T00:05:00Z",
        config=config,
    )

    df = spine.collect()
    print(f"Generated {df.height} clock spine points (5 minutes × 60 = ~300)")
    print(df.head(3))


def demo_trades_spine():
    """Demo 2: Trades spine (event-driven)."""
    print("\n=== Demo 2: Trades Spine (every trade) ===")

    symbols = find_symbol("BTCUSDT", exchange="binance-futures")
    if symbols.is_empty():
        print("BTCUSDT not found, skipping demo")
        return

    symbol_id = int(symbols["symbol_id"][0])

    # Build trades spine
    config = EventSpineConfig(builder_config=TradesSpineConfig())

    spine = build_event_spine(
        symbol_id=symbol_id,
        start_ts_us="2024-05-01T00:00:00Z",
        end_ts_us="2024-05-01T00:05:00Z",
        config=config,
    )

    df = spine.collect()
    print(f"Generated {df.height} trades spine points (1 per trade)")
    print(df.head(3))


def demo_volume_bars():
    """Demo 3: Volume bars (activity-normalized)."""
    print("\n=== Demo 3: Volume Bars (every 1000 contracts) ===")

    symbols = find_symbol("BTCUSDT", exchange="binance-futures")
    if symbols.is_empty():
        print("BTCUSDT not found, skipping demo")
        return

    symbol_id = int(symbols["symbol_id"][0])

    # Build volume bar spine: sample every 1000 contracts
    config = EventSpineConfig(
        builder_config=VolumeBarConfig(
            volume_threshold=1000.0,
            use_absolute_volume=True,
        )
    )

    spine = build_event_spine(
        symbol_id=symbol_id,
        start_ts_us="2024-05-01T00:00:00Z",
        end_ts_us="2024-05-01T00:05:00Z",
        config=config,
    )

    df = spine.collect()
    print(f"Generated {df.height} volume bar spine points")
    print(df.head(3))


def demo_dollar_bars():
    """Demo 4: Dollar bars (economic-significance normalized)."""
    print("\n=== Demo 4: Dollar Bars (every $100k notional) ===")

    symbols = find_symbol("BTCUSDT", exchange="binance-futures")
    if symbols.is_empty():
        print("BTCUSDT not found, skipping demo")
        return

    symbol_id = int(symbols["symbol_id"][0])

    # Build dollar bar spine: sample every $100k notional
    config = EventSpineConfig(builder_config=DollarBarConfig(dollar_threshold=100_000.0))

    spine = build_event_spine(
        symbol_id=symbol_id,
        start_ts_us="2024-05-01T00:00:00Z",
        end_ts_us="2024-05-01T00:05:00Z",
        config=config,
    )

    df = spine.collect()
    print(f"Generated {df.height} dollar bar spine points")
    print(df.head(3))


def demo_features_with_volume_bars():
    """Demo 5: Full feature pipeline with volume bars."""
    print("\n=== Demo 5: Features with Volume Bars ===")

    symbols = find_symbol("BTCUSDT", exchange="binance-futures")
    if symbols.is_empty():
        print("BTCUSDT not found, skipping demo")
        return

    symbol_id = int(symbols["symbol_id"][0])

    # Build features with volume bar resampling
    config = FeatureRunConfig(
        spine=EventSpineConfig(builder_config=VolumeBarConfig(volume_threshold=1000.0)),
        include_microstructure=True,  # Spread, depth, imbalance
        include_trade_flow=True,  # Trade flow features
    )

    lf = build_feature_frame(
        symbol_id=symbol_id,
        start_ts_us="2024-05-01T00:00:00Z",
        end_ts_us="2024-05-01T00:05:00Z",
        config=config,
    )

    df = lf.collect()
    print(f"Generated {df.height} rows × {len(df.columns)} features")
    print(f"Feature columns: {df.columns[:10]}")  # Show first 10 columns


def demo_comparison():
    """Demo 6: Compare different resampling methods."""
    print("\n=== Demo 6: Comparison of Resampling Methods ===")

    symbols = find_symbol("BTCUSDT", exchange="binance-futures")
    if symbols.is_empty():
        print("BTCUSDT not found, skipping demo")
        return

    symbol_id = int(symbols["symbol_id"][0])
    start = "2024-05-01T00:00:00Z"
    end = "2024-05-01T00:05:00Z"

    methods = [
        ("Clock (1s)", EventSpineConfig(builder_config=ClockSpineConfig(step_ms=1000))),
        ("Trades", EventSpineConfig(builder_config=TradesSpineConfig())),
        (
            "Volume (1000)",
            EventSpineConfig(builder_config=VolumeBarConfig(volume_threshold=1000.0)),
        ),
        (
            "Dollar ($100k)",
            EventSpineConfig(builder_config=DollarBarConfig(dollar_threshold=100_000.0)),
        ),
    ]

    print(f"Symbol: BTCUSDT, Period: {start} to {end}")
    print("-" * 60)

    for name, config in methods:
        spine = build_event_spine(
            symbol_id=symbol_id,
            start_ts_us=start,
            end_ts_us=end,
            config=config,
        )
        df = spine.collect()
        print(f"{name:20s} → {df.height:6d} spine points")


if __name__ == "__main__":
    print("Pointline Resampling Methods Demo")
    print("=" * 60)

    # Run all demos
    demo_clock_spine()
    demo_trades_spine()
    demo_volume_bars()
    demo_dollar_bars()
    demo_features_with_volume_bars()
    demo_comparison()

    print("\n" + "=" * 60)
    print("Demo complete! See docs/guides/resampling-methods.md for details.")
