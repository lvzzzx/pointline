import polars as pl

from pointline.tables.derivative_ticker import (
    DERIVATIVE_TICKER_SCHEMA,
    normalize_derivative_ticker_schema,
    parse_tardis_derivative_ticker_csv,
    validate_derivative_ticker,
)


def _sample_raw_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "symbol": ["BTCUSDT"],
            "timestamp": [1714521600000000],
            "local_timestamp": [1714521600037264],
            "funding_timestamp": [1714550400000000],
            "funding_rate": ["0.0001"],
            "predicted_funding_rate": [""],
            "open_interest": ["123.456"],
            "last_price": ["60645"],
            "index_price": ["60674.63680851"],
            "mark_price": ["60651.1"],
        }
    )


def test_parse_tardis_derivative_ticker_csv() -> None:
    parsed = parse_tardis_derivative_ticker_csv(_sample_raw_df())
    assert set(parsed.columns) == {
        "ts_local_us",
        "ts_exch_us",
        "funding_ts_us",
        "funding_rate",
        "predicted_funding_rate",
        "open_interest",
        "last_px",
        "index_px",
        "mark_px",
    }
    assert parsed["ts_local_us"][0] == 1714521600037264
    assert parsed["index_px"][0] == 60674.63680851


def test_normalize_derivative_ticker_schema_fills_optional() -> None:
    parsed = parse_tardis_derivative_ticker_csv(_sample_raw_df())
    df = parsed.with_columns(
        [
            pl.lit("binance-futures").alias("exchange"),
            pl.lit(2, dtype=pl.Int16).alias("exchange_id"),
            pl.lit(100, dtype=pl.Int64).alias("symbol_id"),
            pl.lit(1, dtype=pl.Int32).alias("ingest_seq"),
            pl.lit(1, dtype=pl.Int32).alias("file_id"),
            pl.lit(1, dtype=pl.Int32).alias("file_line_number"),
            pl.lit("2024-05-01").str.strptime(pl.Date, "%Y-%m-%d").alias("date"),
        ]
    )
    normalized = normalize_derivative_ticker_schema(df)
    assert list(normalized.schema.keys()) == list(DERIVATIVE_TICKER_SCHEMA.keys())


def test_validate_derivative_ticker_accepts_valid() -> None:
    parsed = parse_tardis_derivative_ticker_csv(_sample_raw_df())
    df = parsed.with_columns(
        [
            pl.lit("binance-futures").alias("exchange"),
            pl.lit(2, dtype=pl.Int16).alias("exchange_id"),
            pl.lit(100, dtype=pl.Int64).alias("symbol_id"),
        ]
    )
    validated = validate_derivative_ticker(df)
    assert validated.height == 1
