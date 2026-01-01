#!/usr/bin/env python3
"""Delta Lake smoke test for dim_symbol SCD2 behavior.

Runs a tiny update sequence, writes to a Delta table, and validates results.
"""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import polars as pl

from src.dim_symbol import DEFAULT_VALID_UNTIL_TS_US
from src.io.base_repository import BaseDeltaRepository
from src.services.dim_symbol_service import DimSymbolService


def _base_updates(valid_from_ts: int, tick_size: float = 0.5) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange_id": [1],
            "exchange_symbol": ["BTC-PERPETUAL"],
            "base_asset": ["BTC"],
            "quote_asset": ["USD"],
            "asset_type": [1],
            "tick_size": [tick_size],
            "lot_size": [1.0],
            "price_increment": [tick_size],
            "amount_increment": [0.1],
            "contract_size": [1.0],
            "valid_from_ts": [valid_from_ts],
        }
    )


def _run_smoke(lake_root: Path) -> None:
    table_path = lake_root / "silver" / "dim_symbol"
    table_path.parent.mkdir(parents=True, exist_ok=True)

    repo = BaseDeltaRepository(table_path)
    service = DimSymbolService(repo)

    # Initial insert
    service.update(_base_updates(100, tick_size=0.5))
    after_first = repo.read_all()

    assert after_first.height == 1
    assert after_first.filter(pl.col("is_current")).height == 1
    assert after_first.select("valid_until_ts").item() == DEFAULT_VALID_UNTIL_TS_US

    # Change tick size to force an SCD2 update
    service.update(_base_updates(200, tick_size=1.0))
    after_second = repo.read_all()

    assert after_second.height == 2

    current = after_second.filter(pl.col("is_current"))
    history = after_second.filter(pl.col("is_current") == False)  # noqa: E712

    assert current.height == 1
    assert history.height == 1
    assert history.select("valid_until_ts").item() == 200
    assert current.select("tick_size").item() == 1.0

    print("smoke_dim_symbol: PASS")


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test for dim_symbol Delta Lake IO")
    parser.add_argument(
        "--lake-root",
        type=Path,
        default=None,
        help="Optional root directory for the lake (will be created if missing).",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep a temporary lake directory for inspection (ignored if --lake-root is set).",
    )

    args = parser.parse_args()

    if args.lake_root is not None:
        lake_root = args.lake_root.resolve()
        lake_root.mkdir(parents=True, exist_ok=True)
        _run_smoke(lake_root)
        print(f"lake_root: {lake_root}")
        return

    if args.keep:
        lake_root = Path(tempfile.mkdtemp(prefix="dim-symbol-smoke-")).resolve()
        _run_smoke(lake_root)
        print(f"lake_root: {lake_root}")
        return

    with tempfile.TemporaryDirectory(prefix="dim-symbol-smoke-") as tmp:
        lake_root = Path(tmp).resolve()
        _run_smoke(lake_root)
        print(f"lake_root (temp, deleted): {lake_root}")


if __name__ == "__main__":
    main()
