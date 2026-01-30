import polars as pl

from pointline.config import EXCHANGE_MAP


class DataQualityWarning(RuntimeWarning):
    """Warning raised when data quality issues are detected during validation.

    This warning class is used instead of UserWarning to ensure data quality
    issues are not easily suppressed by default warning filters. Data quality
    problems in ETL pipelines should be loud and visible.
    """

    pass


_EXCHANGE_MAP_DF = pl.DataFrame(
    {
        "exchange_norm": list(EXCHANGE_MAP.keys()),
        "expected_exchange_id": list(EXCHANGE_MAP.values()),
    }
).with_columns(pl.col("expected_exchange_id").cast(pl.Int16))


def with_expected_exchange_id(df: pl.DataFrame) -> pl.DataFrame:
    """Attach expected_exchange_id using a join on normalized exchange."""
    return df.with_columns(
        pl.col("exchange")
        .cast(pl.Utf8)
        .str.to_lowercase()
        .str.strip_chars()
        .alias("_exchange_norm")
    ).join(_EXCHANGE_MAP_DF, left_on="_exchange_norm", right_on="exchange_norm", how="left")
