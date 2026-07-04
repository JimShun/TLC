"""
Silver layer – fail-fast data quality checks.

Each check raises ``DataQualityError`` immediately when the assertion fails
(fail-fast semantics).  Callers should run all checks against the silver
DataFrame **before** persisting it or passing it to downstream gold logic.

Design principles
-----------------
* Every public function takes a ``DataFrame`` (and optional threshold
  override) and either returns ``None`` or raises ``DataQualityError``.
* Functions are pure – no side-effects other than logging.
* The :func:`run_all_checks` convenience function runs every check in the
  recommended order and is the main entry point used by the pipeline.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.pipeline_config import (
    DQ_MIN_ROW_COUNT,
    DQ_MAX_NULL_RATE,
    DQ_CRITICAL_COLUMNS,
    DQ_MIN_TRIP_DISTANCE,
    DQ_MAX_TRIP_DISTANCE,
    DQ_MIN_FARE_AMOUNT,
    DQ_MAX_FARE_AMOUNT,
    TIMEBOX_START,
    TIMEBOX_END,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------

class DataQualityError(RuntimeError):
    """Raised when a fail-fast data quality check fails."""


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def check_min_row_count(
    df: DataFrame,
    min_count: int = DQ_MIN_ROW_COUNT,
) -> None:
    """
    Fail if the DataFrame has fewer than *min_count* rows.

    This guards against accidentally writing an empty or truncated silver
    table to the lakehouse.
    """
    actual = df.count()
    logger.info("DQ row-count check: %d rows (min required: %d)", actual, min_count)
    if actual < min_count:
        raise DataQualityError(
            f"Row count {actual:,} is below minimum threshold {min_count:,}. "
            "The silver table would be dangerously small."
        )


def check_null_rates(
    df: DataFrame,
    columns: Optional[list] = None,
    max_null_rate: float = DQ_MAX_NULL_RATE,
) -> None:
    """
    Fail if any column in *columns* has a null rate above *max_null_rate*.

    Parameters
    ----------
    df:
        DataFrame to inspect.
    columns:
        Columns to check.  Defaults to ``DQ_CRITICAL_COLUMNS``.
    max_null_rate:
        Maximum acceptable fraction of nulls (0.0 – 1.0).
    """
    columns = columns or DQ_CRITICAL_COLUMNS
    total = df.count()
    if total == 0:
        raise DataQualityError("DataFrame is empty – cannot compute null rates.")

    null_counts = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]
    ).collect()[0].asDict()

    failed = []
    for col, null_count in null_counts.items():
        rate = null_count / total
        logger.info("DQ null-rate check: %s → %.4f (max allowed: %.4f)", col, rate, max_null_rate)
        if rate > max_null_rate:
            failed.append(f"  {col}: {rate:.4f} > {max_null_rate:.4f}")

    if failed:
        raise DataQualityError(
            "Null-rate threshold exceeded for the following columns:\n"
            + "\n".join(failed)
        )


def check_numeric_ranges(
    df: DataFrame,
    min_distance: float = DQ_MIN_TRIP_DISTANCE,
    max_distance: float = DQ_MAX_TRIP_DISTANCE,
    min_fare: float = DQ_MIN_FARE_AMOUNT,
    max_fare: float = DQ_MAX_FARE_AMOUNT,
) -> None:
    """
    Fail if any rows fall outside the expected numeric ranges for key fields.

    After the silver cleaning step these out-of-range rows should have been
    removed.  This check acts as a safety net.
    """
    violations = df.filter(
        (F.col("trip_distance") < min_distance)
        | (F.col("trip_distance") > max_distance)
        | (F.col("fare_amount") < min_fare)
        | (F.col("fare_amount") > max_fare)
    ).count()

    logger.info("DQ numeric-range check: %d violation(s) found", violations)
    if violations > 0:
        raise DataQualityError(
            f"Found {violations:,} row(s) with trip_distance or fare_amount "
            "outside the valid range after silver cleaning."
        )


def check_timebox(
    df: DataFrame,
    start: str = TIMEBOX_START,
    end: str = TIMEBOX_END,
) -> None:
    """
    Fail if any row's pickup datetime falls outside [start, end].

    Ensures the timebox filter was applied correctly.
    """
    from pyspark.sql.types import TimestampType

    out_of_window = df.filter(
        (F.col("tpep_pickup_datetime") < F.lit(start).cast(TimestampType()))
        | (F.col("tpep_pickup_datetime") >= F.date_add(F.lit(end).cast("date"), 1).cast(TimestampType()))
    ).count()

    logger.info(
        "DQ timebox check [%s, %s]: %d out-of-window row(s)", start, end, out_of_window
    )
    if out_of_window > 0:
        raise DataQualityError(
            f"Found {out_of_window:,} row(s) with pickup datetime outside the "
            f"allowed window [{start}, {end}]."
        )


def check_no_negative_durations(df: DataFrame) -> None:
    """
    Fail if any trip has a non-positive duration (dropoff ≤ pickup).

    Silver cleaning should have removed these; this check is a safety net.
    """
    bad = df.filter(
        F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")
    ).count()

    logger.info("DQ duration check: %d negative/zero-duration row(s)", bad)
    if bad > 0:
        raise DataQualityError(
            f"Found {bad:,} row(s) where dropoff datetime ≤ pickup datetime."
        )


def check_location_ids(df: DataFrame) -> None:
    """
    Fail if PULocationID or DOLocationID contains values outside the valid
    NYC TLC zone range (1–265).
    """
    bad = df.filter(
        ~F.col("PULocationID").between(1, 265)
        | ~F.col("DOLocationID").between(1, 265)
    ).count()

    logger.info("DQ location-ID check: %d invalid location(s)", bad)
    if bad > 0:
        raise DataQualityError(
            f"Found {bad:,} row(s) with PULocationID or DOLocationID outside [1, 265]."
        )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_all_checks(
    df: DataFrame,
    min_row_count: int = DQ_MIN_ROW_COUNT,
    max_null_rate: float = DQ_MAX_NULL_RATE,
) -> None:
    """
    Run **all** quality checks in order.  Raises ``DataQualityError`` on the
    first failure encountered (fail-fast).

    Parameters
    ----------
    df:
        Silver DataFrame to validate.
    min_row_count:
        Override for the minimum row count threshold.
    max_null_rate:
        Override for the maximum null-rate threshold.
    """
    logger.info("=== Running data quality checks ===")
    check_min_row_count(df, min_count=min_row_count)
    check_null_rates(df, max_null_rate=max_null_rate)
    check_numeric_ranges(df)
    check_timebox(df)
    check_no_negative_durations(df)
    check_location_ids(df)
    logger.info("=== All data quality checks passed ===")
