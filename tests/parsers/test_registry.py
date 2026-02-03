"""Tests for parser registry."""

import polars as pl
import pytest

from pointline.io.parsers.registry import (
    _PARSER_REGISTRY,
    get_parser,
    is_parser_registered,
    list_supported_combinations,
    register_parser,
)


@pytest.fixture(autouse=True)
def clear_registry():
    """Clear registry before each test."""
    _PARSER_REGISTRY.clear()
    yield
    _PARSER_REGISTRY.clear()


def test_register_parser_basic():
    """Test basic parser registration."""

    @register_parser(vendor="test", data_type="foo")
    def parse_test_foo(df: pl.DataFrame) -> pl.DataFrame:
        return df

    assert is_parser_registered("test", "foo")
    assert ("test", "foo") in list_supported_combinations()


def test_register_parser_case_insensitive():
    """Test that registration keys are case-insensitive."""

    @register_parser(vendor="TEST", data_type="FOO")
    def parse_test_foo(df: pl.DataFrame) -> pl.DataFrame:
        return df

    assert is_parser_registered("test", "foo")
    assert is_parser_registered("TEST", "FOO")
    assert is_parser_registered("Test", "Foo")


def test_register_parser_duplicate_error():
    """Test that duplicate registration raises ValueError."""

    @register_parser(vendor="test", data_type="foo")
    def parse_test_foo1(df: pl.DataFrame) -> pl.DataFrame:
        return df

    with pytest.raises(ValueError, match="Parser already registered"):

        @register_parser(vendor="test", data_type="foo")
        def parse_test_foo2(df: pl.DataFrame) -> pl.DataFrame:
            return df


def test_get_parser_success():
    """Test successful parser retrieval."""

    @register_parser(vendor="test", data_type="foo")
    def parse_test_foo(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(pl.lit(42).alias("result"))

    parser = get_parser("test", "foo")
    assert parser is not None
    assert callable(parser)

    # Test that parser works
    df = pl.DataFrame({"a": [1, 2, 3]})
    result = parser(df)
    assert "result" in result.columns
    assert result["result"].to_list() == [42, 42, 42]


def test_get_parser_not_found():
    """Test that get_parser raises KeyError for unknown parser."""

    @register_parser(vendor="test", data_type="foo")
    def parse_test_foo(df: pl.DataFrame) -> pl.DataFrame:
        return df

    with pytest.raises(KeyError, match="No parser registered"):
        get_parser("test", "bar")

    with pytest.raises(KeyError, match="No parser registered"):
        get_parser("unknown", "foo")


def test_list_supported_combinations():
    """Test listing all registered parsers."""

    @register_parser(vendor="vendor1", data_type="type1")
    def parse1(df: pl.DataFrame) -> pl.DataFrame:
        return df

    @register_parser(vendor="vendor1", data_type="type2")
    def parse2(df: pl.DataFrame) -> pl.DataFrame:
        return df

    @register_parser(vendor="vendor2", data_type="type1")
    def parse3(df: pl.DataFrame) -> pl.DataFrame:
        return df

    combos = list_supported_combinations()
    assert len(combos) == 3
    assert ("vendor1", "type1") in combos
    assert ("vendor1", "type2") in combos
    assert ("vendor2", "type1") in combos


def test_is_parser_registered():
    """Test checking if parser is registered."""

    assert not is_parser_registered("test", "foo")

    @register_parser(vendor="test", data_type="foo")
    def parse_test_foo(df: pl.DataFrame) -> pl.DataFrame:
        return df

    assert is_parser_registered("test", "foo")
    assert not is_parser_registered("test", "bar")
