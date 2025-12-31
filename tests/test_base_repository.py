import pytest
import polars as pl
from pathlib import Path
from src.io.protocols import TableRepository

def test_base_delta_repository_implements_protocol():
    from src.io.base_repository import BaseDeltaRepository
    assert isinstance(BaseDeltaRepository("/tmp/test"), TableRepository)

def test_base_delta_repository_read_write(tmp_path):
    from src.io.base_repository import BaseDeltaRepository
    
    table_path = tmp_path / "test_delta_table"
    repo = BaseDeltaRepository(table_path)
    
    df = pl.DataFrame({
        "a": [1, 2, 3],
        "b": ["x", "y", "z"]
    })
    
    # Write
    repo.write_full(df)
    
    # Read
    read_df = repo.read_all()
    
    assert read_df.equals(df)

def test_base_delta_repository_append(tmp_path):
    from src.io.base_repository import BaseDeltaRepository
    
    table_path = tmp_path / "test_append_table"
    repo = BaseDeltaRepository(table_path)
    
    df1 = pl.DataFrame({"a": [1]})
    df2 = pl.DataFrame({"a": [2]})
    
    repo.write_full(df1)
    repo.append(df2)
    
    read_df = repo.read_all().sort("a")
    assert read_df.height == 2
    assert read_df["a"].to_list() == [1, 2]

def test_base_delta_repository_merge_interface(tmp_path):
    from src.io.base_repository import BaseDeltaRepository
    
    table_path = tmp_path / "test_merge_table"
    repo = BaseDeltaRepository(table_path)
    
    # For now, merge might just be a placeholder or partial implementation
    # But let's check it doesn't crash if called (or if we decide it's abstract)
    df = pl.DataFrame({"id": [1], "val": ["a"]})
    repo.write_full(df)
    
    # Incremental update
    updates = pl.DataFrame({"id": [1], "val": ["b"]})
    # This might fail if merge is not implemented yet, which is fine for Red phase
    repo.merge(updates, keys=["id"])
    
    read_df = repo.read_all()
    assert read_df.filter(pl.col("id") == 1)["val"][0] == "b"
