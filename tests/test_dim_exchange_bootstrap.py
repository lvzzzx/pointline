"""Tests for dim_exchange auto-bootstrap and CLI commands."""

from __future__ import annotations

import pytest

from pointline.config import (
    _SEED_EXCHANGE_MAP,
    _ensure_dim_exchange,
    get_exchange_id,
    get_exchange_name,
    get_exchange_timezone,
    invalidate_exchange_cache,
)


@pytest.fixture()
def tmp_lake(tmp_path, monkeypatch):
    """Set up a temporary lake root and clear caches."""
    monkeypatch.setattr("pointline.config.LAKE_ROOT", tmp_path)
    # Clear any cached dim_exchange state
    invalidate_exchange_cache()
    monkeypatch.setattr("pointline.config._dim_exchange_bootstrapped", False)
    yield tmp_path
    # Clean up cache after test
    invalidate_exchange_cache()
    monkeypatch.setattr("pointline.config._dim_exchange_bootstrapped", False)


def test_auto_bootstrap_creates_table(tmp_lake):
    """_ensure_dim_exchange should auto-create dim_exchange if missing."""
    table_path = tmp_lake / "silver" / "dim_exchange"
    assert not table_path.exists()

    result = _ensure_dim_exchange()

    assert table_path.exists()
    assert len(result) == len(_SEED_EXCHANGE_MAP)
    assert "binance-futures" in result
    assert result["binance-futures"]["exchange_id"] == 2


def test_auto_bootstrap_idempotent(tmp_lake):
    """Multiple calls to _ensure_dim_exchange should return same data."""
    result1 = _ensure_dim_exchange()
    result2 = _ensure_dim_exchange()

    assert result1.keys() == result2.keys()
    for name in result1:
        assert result1[name]["exchange_id"] == result2[name]["exchange_id"]


def test_timezone_lookup_after_bootstrap(tmp_lake):
    """get_exchange_timezone should work after auto-bootstrap."""
    assert get_exchange_timezone("binance-futures") == "UTC"
    assert get_exchange_timezone("szse") == "Asia/Shanghai"
    assert get_exchange_timezone("sse") == "Asia/Shanghai"


def test_exchange_id_lookup_after_bootstrap(tmp_lake):
    """get_exchange_id should work after auto-bootstrap."""
    assert get_exchange_id("binance-futures") == 2
    assert get_exchange_id("szse") == 30


def test_exchange_name_lookup_after_bootstrap(tmp_lake):
    """get_exchange_name should work after auto-bootstrap."""
    assert get_exchange_name(2) == "binance-futures"
    assert get_exchange_name(30) == "szse"


def test_cli_exchange_init(tmp_lake, capsys):
    """pointline exchange init should bootstrap dim_exchange."""
    import argparse

    from pointline.cli.commands.exchange import cmd_exchange_init

    args = argparse.Namespace(force=False)
    cmd_exchange_init(args)

    captured = capsys.readouterr()
    assert "Bootstrapped dim_exchange" in captured.out
    assert "exchanges" in captured.out


def test_cli_exchange_init_force(tmp_lake, capsys):
    """pointline exchange init --force should overwrite existing table."""
    import argparse

    from pointline.cli.commands.exchange import cmd_exchange_init

    # First init
    args = argparse.Namespace(force=False)
    cmd_exchange_init(args)

    # Second init without force should skip
    cmd_exchange_init(args)
    captured = capsys.readouterr()
    assert "already exists" in captured.out

    # With force should overwrite
    args = argparse.Namespace(force=True)
    cmd_exchange_init(args)
    captured = capsys.readouterr()
    assert "Bootstrapped dim_exchange" in captured.out


def test_cli_exchange_list(tmp_lake, capsys):
    """pointline exchange list should show all exchanges."""
    import argparse

    from pointline.cli.commands.exchange import cmd_exchange_list

    args = argparse.Namespace()
    cmd_exchange_list(args)

    captured = capsys.readouterr()
    assert "binance-futures" in captured.out
    assert "szse" in captured.out
    assert "Total:" in captured.out
