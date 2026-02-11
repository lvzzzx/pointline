"""CLI commands for dim_exchange management."""

from __future__ import annotations

import argparse


def cmd_exchange_init(args: argparse.Namespace) -> None:
    """Bootstrap dim_exchange from seed data."""
    from pointline.config import get_table_path, invalidate_exchange_cache
    from pointline.tables.dim_exchange import bootstrap_from_config

    table_path = get_table_path("dim_exchange")
    force = getattr(args, "force", False)

    if table_path.exists() and not force:
        print(f"dim_exchange already exists at {table_path}")
        print("Use --force to overwrite.")
        return

    df = bootstrap_from_config()
    table_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_delta(str(table_path), mode="overwrite")
    invalidate_exchange_cache()
    print(f"Bootstrapped dim_exchange with {len(df)} exchanges at {table_path}")


def cmd_exchange_list(args: argparse.Namespace) -> None:
    """List all exchanges from dim_exchange."""
    from pointline.config import _ensure_dim_exchange

    dim_ex = _ensure_dim_exchange()

    rows = sorted(dim_ex.values(), key=lambda r: r["exchange_id"])

    print(f"{'exchange':<25} {'id':>4}  {'asset_class':<22} {'tz':<16} {'active'}")
    print("-" * 80)
    for row in rows:
        active = "yes" if row.get("is_active", True) else "no"
        print(
            f"{row['exchange']:<25} {row['exchange_id']:>4}  "
            f"{row.get('asset_class', 'unknown'):<22} "
            f"{row.get('timezone', 'UTC'):<16} {active}"
        )
    print(f"\nTotal: {len(rows)} exchanges")
