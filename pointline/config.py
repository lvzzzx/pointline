import os
from pathlib import Path

# Base Paths
LAKE_ROOT = Path(os.getenv("LAKE_ROOT", "./data/lake"))

# Table Registry (Table Name -> Relative Path from LAKE_ROOT)
TABLE_PATHS = {
    "dim_symbol": "silver/dim_symbol",
    "ingest_manifest": "silver/ingest_manifest",
}

# Storage Settings
STORAGE_OPTIONS = {
    "compression": "zstd",
}

def get_table_path(table_name: str) -> Path:
    """
    Resolves the absolute path for a given table name.
    
    Args:
        table_name: The name of the table to resolve.
        
    Returns:
        Path: The absolute path to the table.
        
    Raises:
        KeyError: If the table name is not registered in TABLE_PATHS.
    """
    if table_name not in TABLE_PATHS:
        raise KeyError(f"Table '{table_name}' not found in TABLE_PATHS registry.")
    
    return LAKE_ROOT / TABLE_PATHS[table_name]
