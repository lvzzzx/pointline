import pytest
import polars as pl
from typing import runtime_checkable, Protocol

def test_table_repository_protocol_definition():
    # This will fail because src.io.protocols doesn't exist yet
    from src.io.protocols import TableRepository
    
    class MockRepo:
        def read_all(self) -> pl.DataFrame:
            return pl.DataFrame()
        def write_full(self, df: pl.DataFrame) -> None:
            pass
        def merge(self, df: pl.DataFrame, keys: list[str]) -> None:
            pass
            
    assert isinstance(MockRepo(), TableRepository)

def test_table_repository_protocol_enforcement():
    from src.io.protocols import TableRepository
    
    class IncompleteRepo:
        def read_all(self) -> pl.DataFrame:
            return pl.DataFrame()
        # Missing write_full and merge
            
    # If the protocol is runtime_checkable, this should be False
    assert not isinstance(IncompleteRepo(), TableRepository)
