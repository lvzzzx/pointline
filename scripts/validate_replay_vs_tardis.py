#!/usr/bin/env python3
"""
Validation script: Compare replayed L2 snapshots against Tardis book_snapshot_25 source of truth.

This script ensures that the l2_replay engine produces bit-exact matches with the
authoritative snapshots provided by Tardis (book_snapshot_25).

Usage:
    python scripts/validate_replay_vs_tardis.py --exchange binance-futures --symbol BTCUSDT --date 2024-04-27
"""

import argparse
import logging
import sys
from datetime import datetime
from typing import List, Tuple

import polars as pl
from pointline import l2_replay
from pointline.config import get_table_path, get_exchange_id

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Validate L2 replay against Tardis snapshots")
    parser.add_argument("--exchange", required=True, help="Exchange name (e.g., binance-futures)")
    parser.add_argument("--symbol", required=True, help="Symbol name (e.g., BTCUSDT)")
    parser.add_argument("--date", required=True, help="Date to validate (YYYY-MM-DD)")
    parser.add_argument("--samples", type=int, default=20, help="Number of random snapshots to check")
    parser.add_argument("--tolerate-missing", action="store_true", help="Don't fail if no snapshots found")
    return parser.parse_args()

def load_reference_snapshots(
    exchange: str,
    symbol: str,
    date_str: str,
    limit: int
) -> pl.DataFrame:
    """Load random sample of book_snapshot_25 from Silver layer."""
    logger.info(f"Loading reference snapshots for {exchange} {symbol} on {date_str}...")
    
    # We need symbol_id first to query efficiently
    try:
        dim_symbol = pl.read_delta(str(get_table_path("dim_symbol")))
        exchange_id = get_exchange_id(exchange)
        
        # Simple lookup (assuming symbol doesn't change ID mid-day for this check)
        # In a real rigorous check, we'd handle time-varying IDs, but usually stable per day.
        symbol_row = dim_symbol.filter(
            (pl.col("exchange_id") == exchange_id) & 
            (pl.col("exchange_symbol") == symbol)
        ).head(1)
        
        if symbol_row.is_empty():
            raise ValueError(f"Symbol {symbol} not found in dim_symbol for exchange {exchange}")
            
        symbol_id = symbol_row["symbol_id"][0]
        logger.info(f"Resolved symbol_id: {symbol_id}")
        
    except Exception as e:
        logger.error(f"Failed to resolve symbol: {e}")
        sys.exit(1)

    # Load snapshots
    snap_path = get_table_path("book_snapshot_25")
    
    # We use scan_delta for efficiency
    try:
        lf = pl.scan_delta(str(snap_path))
        
        # Filter
        start_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        
        filtered = lf.filter(
            (pl.col("exchange") == exchange) &
            (pl.col("date") == start_date) &
            (pl.col("symbol_id") == symbol_id)
        )
        
        # Get count
        count = filtered.select(pl.len()).collect().item()
        logger.info(f"Found {count} reference snapshots available.")
        
        if count == 0:
            return pl.DataFrame()

        # Sample
        # Since we can't easily random sample lazily without a full scan, 
        # we'll grab a larger chunk and sample in memory if it fits, 
        # or just take 'limit' spaced out rows.
        
        # Since we can't easily random sample lazily without a full scan,
        # we'll collect the filtered data and sample in memory.
        # book_snapshot_25 is usually manageable for one day/symbol.
        
        df = filtered.collect()
        
        if df.height > limit:
            # Random sample
            df = df.sample(limit, with_replacement=False, seed=42)
            
        return df.sort("ts_local_us")
        
    except Exception as e:
        logger.error(f"Failed to load snapshots: {e}")
        sys.exit(1)

def compare_snapshot(
    replayed_bids: List[Tuple[int, int]],
    replayed_asks: List[Tuple[int, int]],
    ref_bids_px: List[int],
    ref_bids_sz: List[int],
    ref_asks_px: List[int],
    ref_asks_sz: List[int],
    ts_local_us: int
) -> bool:
    """
    Compare top 25 levels.
    Returns True if match, False otherwise.
    """
    # 1. Compare Bids
    # Reference lists might have nulls for missing levels or be shorter than 25
    # Replayed bids are (price, size) tuples
    
    # Construct structured list from reference
    ref_bids = []
    for px, sz in zip(ref_bids_px, ref_bids_sz):
        if px is not None and sz is not None:
            ref_bids.append((px, sz))
    
    # Truncate replayed to matched length or 25
    cmp_len = min(len(ref_bids), 25)
    replayed_top = replayed_bids[:cmp_len]
    ref_top = ref_bids[:cmp_len]
    
    # Warn if replayed book has fewer levels than reference
    if len(replayed_bids) < len(ref_bids):
        logger.warning(f"Replayed book has {len(replayed_bids)} bid levels, reference has {len(ref_bids)}")
    
    if replayed_top != ref_top:
        logger.error(f"Mismatch at {ts_local_us} (BIDS)")
        logger.error(f"  Ref len: {len(ref_top)}, Replay len: {len(replayed_top)}")
        for i, (ref, rep) in enumerate(zip(ref_top, replayed_top)):
            if ref != rep:
                logger.error(f"  Level {i}: Ref={ref} vs Rep={rep}")
                break
        return False

    # 2. Compare Asks
    ref_asks = []
    for px, sz in zip(ref_asks_px, ref_asks_sz):
        if px is not None and sz is not None:
            ref_asks.append((px, sz))
            
    cmp_len = min(len(ref_asks), 25)
    replayed_top = replayed_asks[:cmp_len]
    ref_top = ref_asks[:cmp_len]
    
    # Warn if replayed book has fewer levels than reference
    if len(replayed_asks) < len(ref_asks):
        logger.warning(f"Replayed book has {len(replayed_asks)} ask levels, reference has {len(ref_asks)}")
    
    if replayed_top != ref_top:
        logger.error(f"Mismatch at {ts_local_us} (ASKS)")
        logger.error(f"  Ref len: {len(ref_top)}, Replay len: {len(replayed_top)}")
        for i, (ref, rep) in enumerate(zip(ref_top, replayed_top)):
            if ref != rep:
                logger.error(f"  Level {i}: Ref={ref} vs Rep={rep}")
                break
        return False
        
    return True

def main():
    args = parse_args()
    
    # 1. Load Reference Data
    ref_df = load_reference_snapshots(args.exchange, args.symbol, args.date, args.samples)
    
    if ref_df.is_empty():
        if args.tolerate_missing:
            logger.warning("No reference data found. Skipping validation.")
            sys.exit(0)
        else:
            logger.error("No reference data found.")
            sys.exit(1)
            
    symbol_id = ref_df["symbol_id"][0]
    exchange_id = ref_df["exchange_id"][0]
    
    logger.info(f"Validating {ref_df.height} snapshots for symbol_id={symbol_id}...")
    
    # 2. Run Validation
    success_count = 0
    failure_count = 0
    
    for row in ref_df.iter_rows(named=True):
        ts = row["ts_local_us"]
        # Use symbol_id from each row in case it changes during the day (SCD Type 2)
        row_symbol_id = row["symbol_id"]
        
        # 3. Replay
        # We use snapshot_at to get the state at exactly 'ts'
        try:
            # Returns dict: {'bids': [{'price_int': ..., 'size_int': ...}], ...}
            # Note: snapshot_at returns a dict, replay_between returns a DataFrame.
            # We are using snapshot_at here for point-checks.
            result = l2_replay.snapshot_at(
                exchange=args.exchange,
                exchange_id=exchange_id,
                symbol_id=row_symbol_id,
                ts_local_us=ts,
                start_date=args.date,
                end_date=args.date,
            )
            
            # Validate timestamp matches
            if result['ts_local_us'] != ts:
                logger.error(f"Timestamp mismatch at {ts}: requested {ts}, got {result['ts_local_us']}")
                failure_count += 1
                continue
            
            # Convert result dict to list of tuples for comparison
            # Structure: result['bids'] is a list of dicts like {'price_int': 100, 'size_int': 1}
            rep_bids = [(x['price_int'], x['size_int']) for x in result['bids']]
            rep_asks = [(x['price_int'], x['size_int']) for x in result['asks']]
            
            # Compare
            match = compare_snapshot(
                rep_bids, rep_asks,
                row["bids_px"], row["bids_sz"],
                row["asks_px"], row["asks_sz"],
                ts
            )
            
            if match:
                success_count += 1
                if success_count % 10 == 0:
                    logger.info(f"Verified {success_count} snapshots...")
            else:
                failure_count += 1
                # Fail fast on first error to avoid spam
                # logger.error("Stopping on first failure.")
                # break
                
        except Exception as e:
            logger.error(f"Replay failed at {ts}: {e}")
            failure_count += 1
    
    print("=" * 40)
    print(f"Validation Complete: {args.date}")
    print(f"Success: {success_count}")
    print(f"Failed:  {failure_count}")
    print("=" * 40)
    
    if failure_count > 0:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
