#!/usr/bin/env python3
"""Simple diagnostic for replay performance bottlenecks."""

import time
from datetime import datetime, timedelta
from pointline.config import get_table_path
import polars as pl

def diagnose():
    """Diagnose replay performance issues."""
    print("=" * 80)
    print("REPLAY PERFORMANCE DIAGNOSIS")
    print("=" * 80)
    print()
    
    exchange_id = 2
    symbol_id = 1738699174
    start_ts = int(datetime(2024, 4, 27, 0, 0, 0).timestamp() * 1_000_000)
    end_ts = start_ts + int(timedelta(hours=1).total_seconds() * 1_000_000)
    start_date = datetime.fromtimestamp(start_ts / 1_000_000).date()
    
    print(f"Replay parameters:")
    print(f"  Date: {start_date}")
    print(f"  Duration: 1 hour")
    print(f"  Symbol: BTCUSDT (ID: {symbol_id})")
    print()
    
    # 1. Check data size
    print("=" * 80)
    print("1. DATA SIZE CHECK")
    print("=" * 80)
    print()
    
    updates_path = get_table_path("l2_updates")
    
    try:
        # Use lazy evaluation to avoid loading all data
        df = pl.scan_delta(str(updates_path))
        
        # Count rows with filters
        start_time = time.time()
        row_count = df.filter(
            (pl.col("exchange") == "binance-futures") &
            (pl.col("symbol_id") == symbol_id) &
            (pl.col("date") == start_date) &
            (pl.col("ts_local_us") >= start_ts) &
            (pl.col("ts_local_us") < end_ts)
        ).select(pl.len()).collect().item()
        count_time = time.time() - start_time
        
        print(f"✓ Rows in time range: {row_count:,}")
        print(f"  Count query time: {count_time:.2f}s")
        print()
        
        if row_count > 10_000_000:
            print("⚠️  WARNING: Very large dataset (>10M rows)")
            print("   This will be slow to replay")
            print()
        
        # Check if checkpoint exists
        checkpoint_path = get_table_path("l2_state_checkpoint")
        try:
            cp_df = pl.scan_delta(str(checkpoint_path))
            cp_count = cp_df.filter(
                (pl.col("exchange") == "binance-futures") &
                (pl.col("symbol_id") == symbol_id) &
                (pl.col("ts_local_us") < start_ts)
            ).select(pl.len()).collect().item()
            
            if cp_count > 0:
                print(f"✓ Checkpoints available: {cp_count}")
                print("  → Replay should use checkpoint (faster)")
            else:
                print("✗ No checkpoints found")
                print("  → Replay will start from beginning (SLOW)")
                print("  → Recommendation: Build checkpoints first")
        except Exception as e:
            print(f"✗ Error checking checkpoints: {e}")
        
        print()
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        print()
    
    # 2. Check partition structure
    print("=" * 80)
    print("2. PARTITION STRUCTURE CHECK")
    print("=" * 80)
    print()
    
    try:
        # Check if table is partitioned correctly
        from deltalake import DeltaTable
        dt = DeltaTable(str(updates_path))
        schema = dt.schema()
        partition_cols = dt.metadata().partition_columns if hasattr(dt.metadata(), 'partition_columns') else []
        
        print(f"Partition columns: {partition_cols}")
        print()
        
        # Check partition files
        files = dt.files()
        print(f"Total files in table: {len(files)}")
        
        # Count files in our partition
        date_str = start_date.isoformat()
        partition_files = [f for f in files if f'exchange=binance-futures' in f and f'date={date_str}' in f]
        print(f"Files in partition (exchange=binance-futures/date={date_str}): {len(partition_files)}")
        print()
        
        if len(partition_files) > 100:
            print("⚠️  WARNING: Many small files")
            print("   Recommendation: Run OPTIMIZE to consolidate files")
            print()
        
    except Exception as e:
        print(f"Could not check partition structure: {e}")
        print()
    
    # 3. Estimate replay time
    print("=" * 80)
    print("3. PERFORMANCE ESTIMATE")
    print("=" * 80)
    print()
    
    if 'row_count' in locals():
        # Rough estimate: 100k rows/second is reasonable
        estimated_time = row_count / 100_000
        print(f"Estimated replay time (rough): {estimated_time:.1f}s")
        print(f"  Assumption: ~100k rows/second processing rate")
        print()
        
        if estimated_time > 60:
            print("⚠️  WARNING: Estimated time > 1 minute")
            print("   This is expected for large datasets")
            print("   Use checkpoints to speed up!")
            print()
    
    # 4. Recommendations
    print("=" * 80)
    print("4. RECOMMENDATIONS")
    print("=" * 80)
    print()
    
    print("To speed up replay:")
    print()
    print("A. USE CHECKPOINTS (Most Important):")
    print("   pointline l2-state-checkpoint build \\")
    print("     --exchange binance-futures \\")
    print("     --symbol-id 1738699174 \\")
    print("     --start-date 2024-04-27 \\")
    print("     --end-date 2024-04-27 \\")
    print("     --checkpoint-every-us 600_000_000")
    print()
    print("   This will:")
    print("     • Create checkpoints every 10 minutes")
    print("     • Allow replay to start from checkpoint")
    print("     • Skip millions of updates")
    print()
    
    print("B. OPTIMIZE DELTA TABLE:")
    print("   from deltalake import DeltaTable")
    print("   dt = DeltaTable(str(updates_path))")
    print("   dt.optimize()")
    print()
    print("   This will:")
    print("     • Consolidate small files")
    print("     • Improve read performance")
    print()
    
    print("C. REDUCE TIME RANGE:")
    print("   • Replay smaller windows (e.g., 10 minutes)")
    print("   • Use checkpoints to jump to specific times")
    print()
    
    print("D. CHECK STORAGE:")
    print("   • Ensure data is on fast storage (SSD)")
    print("   • Check if network storage is slow")
    print()
    
    # 5. Quick test
    print("=" * 80)
    print("5. QUICK TEST (10 seconds of data)")
    print("=" * 80)
    print()
    
    test_end_ts = start_ts + int(timedelta(seconds=10).total_seconds() * 1_000_000)
    
    try:
        from pointline import l2_replay
        
        print("Testing replay of 10 seconds...")
        test_start = time.time()
        
        df = l2_replay.replay_between(
            exchange_id=exchange_id,
            symbol_id=symbol_id,
            start_ts_local_us=start_ts,
            end_ts_local_us=test_end_ts,
            every_us=5_000_000,  # 5 seconds
            exchange="binance-futures",
        )
        
        test_time = time.time() - test_start
        
        print(f"✓ Test completed in {test_time:.2f}s")
        print(f"  Snapshots: {df.height}")
        
        if test_time > 0:
            # Extrapolate
            updates_per_sec = row_count / (end_ts - start_ts) * 1_000_000 if 'row_count' in locals() else 0
            test_updates = updates_per_sec * 10
            rate = test_updates / test_time if test_time > 0 else 0
            
            print(f"  Estimated processing rate: {rate:,.0f} updates/second")
            
            if rate > 0:
                full_time = row_count / rate if 'row_count' in locals() else 0
                print(f"  Estimated full replay time: {full_time:.1f}s ({full_time/60:.1f} minutes)")
        
        print()
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        print()

if __name__ == "__main__":
    diagnose()
