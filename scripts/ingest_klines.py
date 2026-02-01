#!/usr/bin/env python3
"""Helper script to ingest klines using updated code."""

import sys
from pathlib import Path

from pointline.cli import main

# Configure ingestion
BRONZE_ROOT = Path.home() / "data/lake/bronze/binance_vision"
GLOB_PATTERN = "**/*.zip"  # All kline files
DATA_TYPE = "klines"

sys.argv = [
    "pointline",
    "bronze",
    "ingest",
    "--bronze-root",
    str(BRONZE_ROOT),
    "--glob",
    GLOB_PATTERN,
    "--data-type",
    DATA_TYPE,
]

if __name__ == "__main__":
    print(f"Ingesting klines from: {BRONZE_ROOT}")
    print(f"Pattern: {GLOB_PATTERN}")
    print(f"Data type: {DATA_TYPE}")
    print()
    main()
