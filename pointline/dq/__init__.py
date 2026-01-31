"""Data quality utilities."""

from pointline.dq.registry import TableDQConfig, get_dq_config, list_dq_tables
from pointline.dq.runner import run_dq_for_all_tables, run_dq_for_table

__all__ = [
    "TableDQConfig",
    "get_dq_config",
    "list_dq_tables",
    "run_dq_for_all_tables",
    "run_dq_for_table",
]
