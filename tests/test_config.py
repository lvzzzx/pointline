import pytest
import os
from pathlib import Path

def test_config_attributes_exist():
    import src.config as config
    
    assert hasattr(config, "LAKE_ROOT")
    assert hasattr(config, "TABLE_PATHS")
    assert hasattr(config, "STORAGE_OPTIONS")

def test_lake_root_type():
    import src.config as config
    assert isinstance(config.LAKE_ROOT, (str, Path))

def test_table_paths_structure():
    import src.config as config
    assert isinstance(config.TABLE_PATHS, dict)
    # Check if existing tables are registered (even if empty for now, config should support it)

def test_storage_options_structure():
    import src.config as config
    assert isinstance(config.STORAGE_OPTIONS, dict)
    # Expecting Delta Lake / Storage specific options
    assert "compression" in config.STORAGE_OPTIONS
    assert config.STORAGE_OPTIONS["compression"] == "zstd"

def test_get_table_path_resolution(monkeypatch):
    import src.config as config
    
    # Mock LAKE_ROOT for deterministic testing
    monkeypatch.setattr(config, "LAKE_ROOT", Path("/tmp/lake"))
    monkeypatch.setitem(config.TABLE_PATHS, "test_table", "silver/test_table")
    
    expected = "/tmp/lake/silver/test_table"
    assert str(config.get_table_path("test_table")) == expected

def test_get_table_path_missing_table():
    import src.config as config
    with pytest.raises(KeyError):
        config.get_table_path("non_existent_table")
