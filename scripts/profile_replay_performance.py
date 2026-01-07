#!/usr/bin/env python3
"""Profile replay_between performance to identify bottlenecks."""

import time
import cProfile
import pstats
from datetime import datetime, timedelta
from io import StringIO
from pointline import l2_replay
from pointline.config import get_table_path
import polars as pl

def profile_replay():
    """Profile the replay function step by step."""
    print("=" * 80)
    print("REPLAY PERFORMANCE PROFILING")
    print("=" * 80)
    print()
    
    # Setup
    exchange_id = 2
    symbol_id = 1738699174
    start_ts = int(datetime(2024, 4, 27, 0, 0, 0).timestamp() * 1_000_000)
    end_ts = start_ts + int(timedelta(hours=1).total_seconds() * 1_000_000)
    
    print(f"Replay parameters:")
    print(f"  Exchange ID: {exchange_id}")
    print(f"  Symbol ID: {symbol_id}")
    print(f"  Start: {datetime.fromtimestamp(start_ts / 1_000_000)}")
    print(f"  End: {datetime.fromtimestamp(end_ts / 1_000_000)}")
    print(f"  Duration: 1 hour")
    print(f"  Snapshot interval: 10 minutes (600_000_000 us)")
    print()
    
    # Step 1: Check data size
    print("=" * 80)
    print("STEP 1: DATA SIZE ANALYSIS")
    print("=" * 80)
    print()
    
    updates_path = get_table_path("l2_updates")
    start_time = time.time()
    
    try:
        # Read metadata about the data
        df = pl.read_delta(str(updates_path))
        
        # Filter to our date range
        start_date = datetime.fromtimestamp(start_ts / 1_000_000).date()
        filtered = df.filter(
            (pl.col("exchange") == "binance-futures") &
            (pl.col("symbol_id") == symbol_id) &
            (pl.col("date") == start_date)
        )
        
        row_count = filtered.height
        size_time = time.time() - start_time
        
        print(f"Total rows in date range: {row_count:,}")
        print(f"Time to scan metadata: {size_time:.2f}s")
        print()
        
        if row_count > 0:
            # Estimate data size
            sample = filtered.head(1000)
            avg_row_size = sample.estimated_size() / 1000 if sample.height > 0 else 0
            total_size_mb = (row_count * avg_row_size) / (1024 * 1024)
            print(f"Estimated data size: {total_size_mb:.2f} MB")
            print()
        
    except Exception as e:
        print(f"Error reading data: {e}")
        print()
    
    # Step 2: Checkpoint lookup
    print("=" * 80)
    print("STEP 2: CHECKPOINT LOOKUP")
    print("=" * 80)
    print()
    
    checkpoint_path = get_table_path("l2_state_checkpoint")
    start_time = time.time()
    
    try:
        checkpoint_df = pl.read_delta(str(checkpoint_path))
        checkpoint_time = time.time() - start_time
        
        # Check if checkpoint exists
        checkpoint_filtered = checkpoint_df.filter(
            (pl.col("exchange") == "binance-futures") &
            (pl.col("symbol_id") == symbol_id) &
            (pl.col("ts_local_us") <= start_ts)
        ).sort("ts_local_us", descending=True).head(1)
        
        if checkpoint_filtered.height > 0:
            cp_ts = checkpoint_filtered["ts_local_us"][0]
            cp_time = datetime.fromtimestamp(cp_ts / 1_000_000)
            print(f"✓ Checkpoint found: {cp_time}")
            print(f"  Timestamp: {cp_ts}")
        else:
            print("✗ No checkpoint found before start time")
            print("  → Will replay from beginning of day")
        
        print(f"Time to lookup checkpoint: {checkpoint_time:.2f}s")
        print()
        
    except Exception as e:
        print(f"Error reading checkpoint: {e}")
        print()
    
    # Step 3: Profile actual replay
    print("=" * 80)
    print("STEP 3: REPLAY EXECUTION (PROFILED)")
    print("=" * 80)
    print()
    
    # Use cProfile
    profiler = cProfile.Profile()
    profiler.enable()
    
    replay_start = time.time()
    
    try:
        df = l2_replay.replay_between(
            exchange_id=exchange_id,
            symbol_id=symbol_id,
            start_ts_local_us=start_ts,
            end_ts_local_us=end_ts,
            every_us=600_000_000,  # 10 minutes
            exchange="binance-futures",
        )
        
        replay_time = time.time() - replay_start
        profiler.disable()
        
        print(f"✓ Replay completed")
        print(f"  Snapshots generated: {df.height}")
        print(f"  Total time: {replay_time:.2f}s")
        print(f"  Time per snapshot: {replay_time / df.height:.2f}s" if df.height > 0 else "N/A")
        print()
        
        # Show snapshot details
        if df.height > 0:
            print("Snapshot timestamps:")
            for i, row in enumerate(df.head(5).iter_rows(named=True), 1):
                ts = row['ts_local_us']
                dt = datetime.fromtimestamp(ts / 1_000_000)
                bid_levels = len(row['bids'])
                ask_levels = len(row['asks'])
                print(f"  {i}. {dt} - {bid_levels} bids, {ask_levels} asks")
            if df.height > 5:
                print(f"  ... and {df.height - 5} more")
            print()
        
    except Exception as e:
        profiler.disable()
        replay_time = time.time() - replay_start
        print(f"✗ Replay failed after {replay_time:.2f}s")
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
        print()
    
    # Step 4: Profile analysis
    print("=" * 80)
    print("STEP 4: PROFILING RESULTS")
    print("=" * 80)
    print()
    
    s = StringIO()
    ps = pstats.Stats(profiler, stream=s)
    ps.sort_stats('cumulative')
    ps.print_stats(30)  # Top 30 functions
    
    print(s.getvalue())
    
    # Step 5: Detailed timing breakdown
    print("=" * 80)
    print("STEP 5: TIMING BREAKDOWN")
    print("=" * 80)
    print()
    
    # Analyze profile stats
    stats_dict = {}
    for func_name, (cc, nc, tt, ct, callers) in ps.stats.items():
        stats_dict[func_name] = {
            'cumulative': ct,
            'total': tt,
            'calls': nc,
        }
    
    # Find slowest functions
    slowest = sorted(stats_dict.items(), key=lambda x: x[1]['cumulative'], reverse=True)[:10]
    
    print("Top 10 slowest functions (by cumulative time):")
    print(f"{'Function':<60} {'Cumulative':<15} {'Total':<15} {'Calls':<10}")
    print("-" * 100)
    for func_name, stats in slowest:
        func_short = func_name[0] if isinstance(func_name, tuple) else str(func_name)
        if len(func_short) > 60:
            func_short = func_short[:57] + "..."
        print(f"{func_short:<60} {stats['cumulative']:>14.3f}s {stats['total']:>14.3f}s {stats['calls']:>9,}")
    print()
    
    # Step 6: Recommendations
    print("=" * 80)
    print("STEP 6: BOTTLENECK ANALYSIS & RECOMMENDATIONS")
    print("=" * 80)
    print()
    
    # Check for common bottlenecks
    bottleneck_found = False
    
    # Check for DataFrame operations
    df_ops = [f for f in stats_dict.keys() if 'datafusion' in str(f).lower() or 'arrow' in str(f).lower()]
    if df_ops:
        print("⚠️  Potential bottleneck: DataFrame/Arrow operations")
        print("   Functions:")
        for f in df_ops[:5]:
            print(f"     - {f}")
        print("   Recommendation:")
        print("     • Check if predicate pushdown is working")
        print("     • Verify partition pruning is effective")
        print("     • Consider using checkpoint to skip early data")
        print()
        bottleneck_found = True
    
    # Check for I/O operations
    io_ops = [f for f in stats_dict.keys() if 'read' in str(f).lower() or 'parquet' in str(f).lower()]
    if io_ops:
        print("⚠️  Potential bottleneck: I/O operations")
        print("   Functions:")
        for f in io_ops[:5]:
            print(f"     - {f}")
        print("   Recommendation:")
        print("     • Data might be on slow storage")
        print("     • Consider using faster storage (SSD)")
        print("     • Check if data is compressed (affects read speed)")
        print()
        bottleneck_found = True
    
    # Check for Python-Rust boundary
    py_rust = [f for f in stats_dict.keys() if 'pyo3' in str(f).lower() or 'pyobject' in str(f).lower()]
    if py_rust:
        print("⚠️  Potential bottleneck: Python-Rust boundary")
        print("   Functions:")
        for f in py_rust[:5]:
            print(f"     - {f}")
        print("   Recommendation:")
        print("     • Minimize data conversion between Python and Rust")
        print("     • Consider batching snapshots")
        print()
        bottleneck_found = True
    
    # Check for order book operations
    book_ops = [f for f in stats_dict.keys() if 'orderbook' in str(f).lower() or 'apply' in str(f).lower()]
    if book_ops:
        print("ℹ️  Order book operations detected")
        print("   This is expected - each update must be applied")
        print("   If this is slow, check:")
        print("     • Number of updates being processed")
        print("     • Order book size (number of levels)")
        print()
    
    if not bottleneck_found:
        print("✓ No obvious bottlenecks detected in profiling")
        print("  Check the detailed profile above for specific functions")
        print()
    
    # General recommendations
    print("GENERAL RECOMMENDATIONS:")
    print()
    print("1. Use checkpoints:")
    print("   • Build l2_state_checkpoint table first")
    print("   • Replay will start from checkpoint, not beginning")
    print("   • Can skip millions of updates")
    print()
    print("2. Optimize data layout:")
    print("   • Ensure Delta table is optimized (OPTIMIZE command)")
    print("   • Check partition pruning is working")
    print("   • Verify predicate pushdown is effective")
    print()
    print("3. Reduce time range:")
    print("   • Replay smaller time windows if possible")
    print("   • Use checkpoints to jump to specific times")
    print()
    print("4. Check data size:")
    print(f"   • 1 hour of data = {row_count:,} rows")
    if row_count > 1_000_000:
        print("   • Large dataset - consider using checkpoints")
    print()

if __name__ == "__main__":
    profile_replay()
