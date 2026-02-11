"""Config helpers for the Pointline CLI."""

from __future__ import annotations

import os
from argparse import Namespace

from pointline.config import (
    CONFIG_PATH,
    DEFAULT_LAKE_ROOT,
    LAKE_ROOT,
    get_config_lake_root,
    set_config_lake_root,
)


def cmd_config_show(args: Namespace) -> int:
    """Show resolved configuration values."""
    env_value = os.getenv("LAKE_ROOT")
    config_value = get_config_lake_root()

    print("Pointline config")
    print(f"  config_path: {CONFIG_PATH}")
    print(f"  resolved_lake_root: {LAKE_ROOT}")
    print(f"  env_LAKE_ROOT: {env_value or '(unset)'}")
    print(f"  file_lake_root: {config_value or '(unset)'}")
    print(f"  default_lake_root: {DEFAULT_LAKE_ROOT}")
    return 0


def cmd_config_set(args: Namespace) -> int:
    """Set configuration values."""
    if args.lake_root is None:
        raise ValueError("config set: --lake-root is required")

    resolved = set_config_lake_root(args.lake_root)
    print(f"Updated lake_root in {CONFIG_PATH} -> {resolved}")
    if os.getenv("LAKE_ROOT"):
        print("Note: LAKE_ROOT env var is set and will override config in new sessions.")
    return 0
