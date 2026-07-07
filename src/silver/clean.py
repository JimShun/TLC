"""
Silver layer – cleaning and standardisation of TLC yellow taxi data.

Responsibilities
----------------
* **Timebox**: keep only trips whose pickup datetime falls within Jan 2024.
* **Schema normalisation**: cast columns to their correct Spark types.
* **Business-rule filters**: drop rows that violate domain constraints
  (negative fares, impossible distances, invalid passenger counts, etc.).
* **Derived columns**: ``trip_duration_minutes``, ``fare_per_mile``.
* **Deduplication** (optional but enabled by default).

All cleaning steps are pure functions that accept and return DataFrames so
that each step can be unit-tested in isolation without I/O.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from config.pipeline_config import (
    BRONZE_PATH,
    SILVER_PATH,
    TIMEBOX_START,
    TIMEBOX_END,
    DQ_MIN_TRIP_DISTANCE,
    DQ_MAX_TRIP_DISTANCE,
    DQ_MIN_FARE_AMOUNT,
    DQ_MAX_FARE_AMOUNT,
    DQ_VALID_PASSENGER_COUNTS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Individual cleaning transformations
# ---------------------------------------------------------------------------

def cast_schema(df: DataFrame) -> DataFrame:
    """
    Cast all columns to their canonical types for yellow taxi trip records.

    Silently coerces unparseable values to ``null`` (Spark default).
    """
    return (
        df
        .withColumn("VendorID", F.col("VendorID").cast(IntegerType()))
        .withColumn(
            "tpep_pickup_datetime",
            F.col("tpep_pickup_datetime").cast(TimestampType()),
        )
        .withColumn(
            "tpep_dropoff_datetime",
            F.col("tpep_dropoff_datetime").cast(TimestampType()),
        )
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("RatecodeID", F.col("RatecodeID").cast(IntegerType()))
        .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast(StringType()))
        .withColumn("PULocationID", F.col("PULocationID").cast(LongType()))
        .withColumn("DOLocationID", F.col("DOLocationID").cast(LongType()))
        .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("extra", F.col("extra").cast(DoubleType()))
        .withColumn("mta_tax", F.col("mta_tax").cast(DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType()))
        .withColumn(
            "improvement_surcharge",
            F.col("improvement_surcharge").cast(DoubleType()),
        )
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn(
            "congestion_surcharge",
            F.col("congestion_surcharge").cast(DoubleType()),
        )
    )


def apply_timebox(
    df: DataFrame,
    start: str = TIMEBOX_START,
    end: str = TIMEBOX_END,
) -> DataFrame:
    """
    Retain only trips whose pickup datetime falls within [start, end] inclusive.

    Parameters
    ----------
    df:
        Input DataFrame (must have ``tpep_pickup_datetime`` as TimestampType).
    start:
        ISO date string (``YYYY-MM-DD``) for the window start.
    end:
        ISO date string (``YYYY-MM-DD``) for the window end (inclusive).
    """
    return df.filter(
        (F.col("tpep_pickup_datetime") >= F.lit(start).cast(TimestampType()))
        & (F.col("tpep_pickup_datetime") < F.date_add(F.lit(end).cast("date"), 1).cast(TimestampType()))
    )


def drop_nulls_in_critical_columns(df: DataFrame) -> DataFrame:
    """Drop rows where any critical column is null."""
    critical = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "fare_amount",
    ]
    condition = F.lit(True)
    for col in critical:
        condition = condition & F.col(col).isNotNull()
    return df.filter(condition)


def filter_business_rules(df: DataFrame) -> DataFrame:
    """
    Apply domain-knowledge business rules to remove invalid trips:

    * ``trip_distance`` must be within [DQ_MIN_TRIP_DISTANCE, DQ_MAX_TRIP_DISTANCE].
    * ``fare_amount`` must be within [DQ_MIN_FARE_AMOUNT, DQ_MAX_FARE_AMOUNT].
    * ``passenger_count`` must be in DQ_VALID_PASSENGER_COUNTS (1–6).
    * Dropoff must be after pickup.
    * ``total_amount`` must be non-negative.
    """
    return df.filter(
        (F.col("trip_distance").between(DQ_MIN_TRIP_DISTANCE, DQ_MAX_TRIP_DISTANCE))
        & (F.col("fare_amount").between(DQ_MIN_FARE_AMOUNT, DQ_MAX_FARE_AMOUNT))
        & (F.col("passenger_count").isin(DQ_VALID_PASSENGER_COUNTS))
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        & (F.col("total_amount") >= 0)
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Add derived / convenience columns:

    * ``trip_duration_minutes``: duration of trip in fractional minutes.
    * ``fare_per_mile``: fare amount per mile (null-safe; 0 distance → null).
    * ``pickup_hour``: hour-of-day integer for the pickup time.
    * ``pickup_date``: date portion of the pickup timestamp.
    """
    return (
        df
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            ).cast(DoubleType()) / 60.0,
        )
        .withColumn(
            "fare_per_mile",
            F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance"))
             .otherwise(F.lit(None).cast(DoubleType())),
        )
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove exact duplicates across all original columns (ignores derived cols).
    """
    return df.dropDuplicates()


# ---------------------------------------------------------------------------
# Orchestrated cleaning pipeline
# ---------------------------------------------------------------------------

def clean(df: DataFrame) -> DataFrame:
    """
    Run all silver cleaning steps in order and return the cleaned DataFrame.

    Steps (in order):
    1. Cast schema
    2. Apply timebox (Jan 2024)
    3. Drop nulls in critical columns
    4. Apply business-rule filters
    5. Add derived columns
    6. Deduplicate
    """
    logger.info("Silver cleaning: cast_schema")
    df = cast_schema(df)

    logger.info("Silver cleaning: apply_timebox")
    df = apply_timebox(df)

    logger.info("Silver cleaning: drop_nulls_in_critical_columns")
    df = drop_nulls_in_critical_columns(df)

    logger.info("Silver cleaning: filter_business_rules")
    df = filter_business_rules(df)

    logger.info("Silver cleaning: add_derived_columns")
    df = add_derived_columns(df)

    logger.info("Silver cleaning: deduplicate")
    df = deduplicate(df)

    return df


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def write_silver(
    df: DataFrame,
    output_path: Optional[str] = None,
) -> None:
    """Persist the silver DataFrame to a Delta table."""
    output_path = output_path or SILVER_PATH
    logger.info("Writing silver layer to %s", output_path)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("pickup_date")
        .save(output_path)
    )
    logger.info("Silver layer written successfully.")


def read_silver(spark: SparkSession, path: Optional[str] = None) -> DataFrame:
    """Read the silver Delta table and return a DataFrame."""
    path = path or SILVER_PATH
    logger.info("Reading silver Delta table from %s", path)
    return spark.read.format("delta").load(path)
