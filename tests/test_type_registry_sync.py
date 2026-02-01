"""Tests to ensure type definitions stay in sync with runtime registries.

This test ensures that when new tables are added to TABLE_PATHS in config.py,
the TableName Literal in types.py is also updated.
"""

from typing import get_args

from pointline.config import TABLE_PATHS
from pointline.types import TableName


def test_table_name_literal_matches_table_paths():
    """Ensure TableName Literal includes all registered tables.

    This is a CI guard to prevent drift between the static TableName type
    and the runtime TABLE_PATHS registry.
    """
    literal_tables = set(get_args(TableName))
    registered_tables = set(TABLE_PATHS.keys())

    missing_in_literal = registered_tables - literal_tables
    extra_in_literal = literal_tables - registered_tables

    assert not missing_in_literal, (
        f"TableName Literal is missing tables registered in TABLE_PATHS: "
        f"{sorted(missing_in_literal)}\n"
        f"Update pointline/types.py to include these tables."
    )

    assert not extra_in_literal, (
        f"TableName Literal includes tables not in TABLE_PATHS: "
        f"{sorted(extra_in_literal)}\n"
        f"Remove these from pointline/types.py or add to TABLE_PATHS."
    )
