import polars as pl


class DataQualityWarning(RuntimeWarning):
    """Warning raised when data quality issues are detected during validation.

    This warning class is used instead of UserWarning to ensure data quality
    issues are not easily suppressed by default warning filters. Data quality
    problems in ETL pipelines should be loud and visible.
    """

    pass


_EXCHANGE_MAP_DF: pl.DataFrame | None = None


def _get_exchange_map_df() -> pl.DataFrame:
    """Lazily build exchange mapping DataFrame from dim_exchange."""
    global _EXCHANGE_MAP_DF
    if _EXCHANGE_MAP_DF is not None:
        return _EXCHANGE_MAP_DF

    from pointline.config import _ensure_dim_exchange

    dim_ex = _ensure_dim_exchange()
    _EXCHANGE_MAP_DF = pl.DataFrame(
        {
            "exchange_norm": list(dim_ex.keys()),
            "expected_exchange_id": [row["exchange_id"] for row in dim_ex.values()],
        }
    ).with_columns(pl.col("expected_exchange_id").cast(pl.Int16))
    return _EXCHANGE_MAP_DF


def with_expected_exchange_id(df: pl.DataFrame) -> pl.DataFrame:
    """Attach expected_exchange_id using a join on normalized exchange."""
    return df.with_columns(
        pl.col("exchange")
        .cast(pl.Utf8)
        .str.to_lowercase()
        .str.strip_chars()
        .alias("_exchange_norm")
    ).join(_get_exchange_map_df(), left_on="_exchange_norm", right_on="exchange_norm", how="left")
