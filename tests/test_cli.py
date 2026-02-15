"""Tests for the pointline CLI entry point."""

from __future__ import annotations

import json

import pytest

from pointline.cli import main

# ---------------------------------------------------------------------------
# schema commands (no stores/fixtures needed)
# ---------------------------------------------------------------------------


class TestSchemaList:
    def test_returns_zero(self):
        assert main(["schema", "list"]) == 0

    def test_output(self, capsys):
        main(["schema", "list"])
        out = capsys.readouterr().out
        assert "trades" in out
        assert "dim_symbol" in out
        assert "event" in out
        assert "dimension" in out


class TestSchemaShow:
    def test_existing_table(self, capsys):
        assert main(["schema", "show", "trades"]) == 0
        out = capsys.readouterr().out
        assert "trades" in out
        assert "ts_event_us" in out
        assert "price" in out

    def test_nonexistent_table(self, capsys):
        assert main(["schema", "show", "nonexistent_table"]) == 1
        out = capsys.readouterr().out
        assert "Unknown" in out

    def test_json_format(self, capsys):
        assert main(["schema", "show", "trades", "--format", "json"]) == 0
        data = json.loads(capsys.readouterr().out)
        assert data["name"] == "trades"
        assert data["kind"] == "event"
        assert isinstance(data["columns"], list)
        assert any(c["name"] == "price" for c in data["columns"])

    def test_all_tables_showable(self):
        """Every registered table can be shown without error."""
        from pointline.schemas.registry import list_table_specs

        for name in list_table_specs():
            assert main(["schema", "show", name]) == 0


# ---------------------------------------------------------------------------
# _config module
# ---------------------------------------------------------------------------


class TestConfig:
    def test_resolve_silver_root_from_arg(self):
        from pointline.cli._config import resolve_silver_root

        p = resolve_silver_root("/tmp/test")
        assert str(p) == "/tmp/test"

    def test_resolve_silver_root_from_env(self, monkeypatch):
        from pointline.cli._config import resolve_silver_root

        monkeypatch.setenv("POINTLINE_SILVER_ROOT", "/env/silver")
        p = resolve_silver_root(None)
        assert str(p) == "/env/silver"

    def test_resolve_silver_root_from_root(self, monkeypatch):
        from pathlib import Path

        from pointline.cli._config import resolve_silver_root

        monkeypatch.delenv("POINTLINE_SILVER_ROOT", raising=False)
        p = resolve_silver_root(None, root=Path("/lake"))
        assert str(p) == "/lake/silver"

    def test_resolve_silver_root_missing_raises(self, monkeypatch):
        from pointline.cli._config import resolve_silver_root

        monkeypatch.delenv("POINTLINE_SILVER_ROOT", raising=False)
        with pytest.raises(SystemExit):
            resolve_silver_root(None)

    def test_resolve_bronze_root_from_root_with_vendor(self, monkeypatch):
        from pathlib import Path

        from pointline.cli._config import resolve_bronze_root

        monkeypatch.delenv("POINTLINE_BRONZE_ROOT", raising=False)
        p = resolve_bronze_root(None, root=Path("/lake"), vendor="tardis")
        assert str(p) == "/lake/bronze/tardis"

    def test_resolve_bronze_root_from_root_without_vendor(self, monkeypatch):
        from pathlib import Path

        from pointline.cli._config import resolve_bronze_root

        monkeypatch.delenv("POINTLINE_BRONZE_ROOT", raising=False)
        p = resolve_bronze_root(None, root=Path("/lake"))
        assert str(p) == "/lake/bronze"

    def test_resolve_root_from_env(self, monkeypatch):
        from pointline.cli._config import resolve_root

        monkeypatch.setenv("POINTLINE_ROOT", "/data/lake")
        p = resolve_root(None)
        assert str(p) == "/data/lake"

    def test_resolve_root_none_when_unset(self, monkeypatch):
        from pointline.cli._config import resolve_root

        monkeypatch.delenv("POINTLINE_ROOT", raising=False)
        assert resolve_root(None) is None

    def test_resolve_tushare_token_from_env(self, monkeypatch):
        from pointline.cli._config import resolve_tushare_token

        monkeypatch.setenv("TUSHARE_TOKEN", "tok123")
        assert resolve_tushare_token(None) == "tok123"


# ---------------------------------------------------------------------------
# _output module
# ---------------------------------------------------------------------------


class TestOutput:
    def test_print_table_basic(self, capsys):
        from pointline.cli._output import print_table

        rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "yy"}]
        print_table(rows)
        out = capsys.readouterr().out
        assert "a" in out
        assert "b" in out
        assert "1" in out
        assert "yy" in out

    def test_print_table_empty(self, capsys):
        from pointline.cli._output import print_table

        print_table([])
        assert capsys.readouterr().out == ""

    def test_write_output_csv(self, capsys):
        import polars as pl

        from pointline.cli._output import write_output

        df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
        write_output(df, fmt="csv")
        out = capsys.readouterr().out
        assert "x,y" in out
        assert "1,a" in out

    def test_write_output_json(self, capsys):
        import polars as pl

        from pointline.cli._output import write_output

        df = pl.DataFrame({"x": [1]})
        write_output(df, fmt="json")
        data = json.loads(capsys.readouterr().out)
        assert data == [{"x": 1}]

    def test_write_output_parquet(self, tmp_path):
        import polars as pl

        from pointline.cli._output import write_output

        df = pl.DataFrame({"x": [1, 2, 3]})
        out_path = tmp_path / "out.parquet"
        write_output(df, fmt="parquet", output=out_path)
        result = pl.read_parquet(out_path)
        assert result.height == 3

    def test_write_output_parquet_requires_output(self):
        import polars as pl

        from pointline.cli._output import write_output

        df = pl.DataFrame({"x": [1]})
        with pytest.raises(SystemExit):
            write_output(df, fmt="parquet", output=None)

    def test_write_output_limit(self, capsys):
        import polars as pl

        from pointline.cli._output import write_output

        df = pl.DataFrame({"x": list(range(100))})
        write_output(df, fmt="csv", limit=5)
        out = capsys.readouterr().out
        lines = out.strip().split("\n")
        assert len(lines) == 6  # header + 5 data rows


# ---------------------------------------------------------------------------
# _stores module
# ---------------------------------------------------------------------------


class TestBuildStores:
    def test_build_stores_returns_all_keys(self, tmp_path):
        from pointline.cli._stores import build_stores

        stores = build_stores(tmp_path)
        assert set(stores.keys()) == {"manifest", "event", "dimension", "quarantine", "optimizer"}


# ---------------------------------------------------------------------------
# no-arg / help behavior
# ---------------------------------------------------------------------------


class TestNoArgs:
    def test_no_command_returns_1(self):
        assert main([]) == 1

    def test_schema_no_subcommand_returns_1(self):
        assert main(["schema"]) == 1


class TestRootFlag:
    def test_root_derives_silver(self, tmp_path, capsys):
        """--root derives silver_root = root/silver for manifest."""
        result = main(["--root", str(tmp_path), "manifest", "list"])
        assert result == 0
        assert "empty" in capsys.readouterr().out

    def test_root_derives_bronze_for_diff(self, tmp_path, capsys):
        """--root derives bronze_root = root/bronze/{vendor} for manifest diff."""
        bronze = tmp_path / "bronze" / "tardis"
        bronze.mkdir(parents=True)
        result = main(["--root", str(tmp_path), "manifest", "diff", "--vendor", "tardis"])
        assert result == 0
        assert "No files found" in capsys.readouterr().out

    def test_root_env_var(self, tmp_path, monkeypatch, capsys):
        """POINTLINE_ROOT env var works the same as --root."""
        monkeypatch.setenv("POINTLINE_ROOT", str(tmp_path))
        result = main(["dim-symbol", "validate"])
        assert result == 0
        assert "empty" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# ingest (requires --silver-root and file; test error paths)
# ---------------------------------------------------------------------------


class TestIngestErrors:
    def test_missing_file_returns_1(self, tmp_path, capsys):
        result = main(
            [
                "ingest",
                "/nonexistent/file.csv",
                "--vendor",
                "tardis",
                "--data-type",
                "trades",
                "--silver-root",
                str(tmp_path),
            ]
        )
        assert result == 1
        assert "not found" in capsys.readouterr().out

    def test_quant360_missing_exchange_symbol(self, tmp_path):
        # Create a dummy file
        dummy = tmp_path / "test.csv"
        dummy.write_text("a,b\n1,2\n")
        result = main(
            [
                "ingest",
                str(dummy),
                "--vendor",
                "quant360",
                "--data-type",
                "cn_order_events",
                "--silver-root",
                str(tmp_path),
            ]
        )
        assert result == 1  # SystemExit caught by main()


# ---------------------------------------------------------------------------
# dim-symbol (error paths without real data)
# ---------------------------------------------------------------------------


class TestDimSymbolValidate:
    def test_empty_dim_symbol(self, tmp_path, capsys):
        result = main(["dim-symbol", "validate", "--silver-root", str(tmp_path)])
        assert result == 0
        assert "empty" in capsys.readouterr().out


class TestDimSymbolShow:
    def test_empty_dim_symbol(self, tmp_path, capsys):
        result = main(["dim-symbol", "show", "--silver-root", str(tmp_path)])
        assert result == 0
        assert "empty" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# query (error paths)
# ---------------------------------------------------------------------------


class TestQueryErrors:
    def test_unknown_table(self, tmp_path, capsys):
        result = main(
            [
                "query",
                "nonexistent",
                "--exchange",
                "deribit",
                "--symbol",
                "BTC-PERPETUAL",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-02",
                "--silver-root",
                str(tmp_path),
            ]
        )
        assert result == 1


class TestQueryEmpty:
    def test_no_data(self, tmp_path, capsys):
        result = main(
            [
                "query",
                "trades",
                "--exchange",
                "deribit",
                "--symbol",
                "BTC-PERPETUAL",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-02",
                "--silver-root",
                str(tmp_path),
            ]
        )
        assert result == 0
        assert "No rows" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# discover (error paths)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# manifest commands
# ---------------------------------------------------------------------------


class TestManifestEmpty:
    def test_list_empty(self, tmp_path, capsys):
        result = main(["manifest", "list", "--silver-root", str(tmp_path)])
        assert result == 0
        assert "empty" in capsys.readouterr().out

    def test_show_empty(self, tmp_path, capsys):
        result = main(["manifest", "show", "1", "--silver-root", str(tmp_path)])
        assert result == 0
        assert "empty" in capsys.readouterr().out.lower()

    def test_summary_empty(self, tmp_path, capsys):
        result = main(["manifest", "summary", "--silver-root", str(tmp_path)])
        assert result == 0
        assert "empty" in capsys.readouterr().out.lower()


class TestManifestDiff:
    def test_diff_no_bronze_files(self, tmp_path, capsys):
        bronze = tmp_path / "bronze"
        bronze.mkdir()
        result = main(
            [
                "manifest",
                "diff",
                "--vendor",
                "tardis",
                "--bronze-root",
                str(bronze),
                "--silver-root",
                str(tmp_path),
            ]
        )
        assert result == 0
        assert "No files found" in capsys.readouterr().out

    def test_diff_all_missing(self, tmp_path, capsys):
        """Bronze files with no manifest at all → everything shows as missing."""
        bronze = tmp_path / "bronze"
        (bronze / "exchange=x" / "type=trades" / "date=2024-01-01" / "symbol=A").mkdir(parents=True)
        f = bronze / "exchange=x" / "type=trades" / "date=2024-01-01" / "symbol=A" / "a.csv.gz"
        f.write_bytes(b"data")
        result = main(
            [
                "manifest",
                "diff",
                "--vendor",
                "tardis",
                "--bronze-root",
                str(bronze),
                "--silver-root",
                str(tmp_path),
            ]
        )
        assert result == 0
        out = capsys.readouterr().out
        assert "1 files not in manifest" in out
        assert "a.csv.gz" in out

    def test_diff_nonexistent_bronze_root(self, tmp_path, capsys):
        result = main(
            [
                "manifest",
                "diff",
                "--vendor",
                "tardis",
                "--bronze-root",
                "/nonexistent/path",
                "--silver-root",
                str(tmp_path),
            ]
        )
        assert result == 1
        assert "does not exist" in capsys.readouterr().out

    def test_diff_exchange_filter_narrows_scan(self, tmp_path, capsys):
        bronze = tmp_path / "bronze"
        (bronze / "exchange=a").mkdir(parents=True)
        # No files under exchange=a
        result = main(
            [
                "manifest",
                "diff",
                "--vendor",
                "test",
                "--bronze-root",
                str(bronze),
                "--silver-root",
                str(tmp_path),
                "--exchange",
                "a",
            ]
        )
        assert result == 0
        assert "No files found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# discover (error paths)
# ---------------------------------------------------------------------------


class TestDiscoverEmpty:
    def test_no_data(self, tmp_path, capsys):
        result = main(
            [
                "discover",
                "--exchange",
                "deribit",
                "--silver-root",
                str(tmp_path),
            ]
        )
        assert result == 0
        assert "No symbols" in capsys.readouterr().out
