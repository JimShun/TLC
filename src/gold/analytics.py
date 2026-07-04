"""
Gold layer – analytics aggregations for TLC Jan 2024.

Each function produces a single analytics DataFrame that can be written to
the lakehouse or consumed directly by a BI tool / notebook.

Available outputs
-----------------
* :func:`daily_trip_summary`   – trips, distance, fares per day
* :func:`hourly_demand`        – trips per hour-of-day across the month
* :func:`payment_type_breakdown` – trip and revenue split by payment type
* :func:`top_pickup_zones`     – most popular pickup locations
* :func:`top_dropoff_zones`    – most popular dropoff locations
* :func:`fare_stats`           – fare amount statistics (mean, p50, p95)
* :func:`trip_distance_buckets` – distribution of trip distances
* :func:`compute_all`          – run & save every output in one call
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.pipeline_config import GOLD_PATH, SILVER_PATH

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Payment type legend (TLC data dictionary)
# ---------------------------------------------------------------------------
PAYMENT_LABELS = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}


# ---------------------------------------------------------------------------
# Analytics aggregations
# ---------------------------------------------------------------------------

def daily_trip_summary(df: DataFrame) -> DataFrame:
    """
    Daily aggregate: trip count, total distance, total fare, average fare,
    and average trip duration.

    Partitioned by ``pickup_date``.
    """
    return (
        df.groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("trip_distance"), 2).alias("total_distance_miles"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare_amount"),
            F.round(F.avg("fare_amount"), 4).alias("avg_fare_amount"),
            F.round(F.avg("trip_duration_minutes"), 4).alias("avg_trip_duration_min"),
            F.round(F.avg("trip_distance"), 4).alias("avg_trip_distance_miles"),
        )
        .orderBy("pickup_date")
    )


def hourly_demand(df: DataFrame) -> DataFrame:
    """
    Hourly demand across the full month: trips per hour of day (0–23).

    Useful for identifying peak demand periods.
    """
    return (
        df.groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 4).alias("avg_fare_amount"),
        )
        .orderBy("pickup_hour")
    )


def payment_type_breakdown(df: DataFrame) -> DataFrame:
    """
    Split of trips and revenue by payment type.

    Includes a human-readable ``payment_label`` column derived from the
    TLC data dictionary.
    """
    payment_map = F.create_map(
        *[item for pair in PAYMENT_LABELS.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )

    return (
        df.groupBy("payment_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("tip_amount"), 4).alias("avg_tip_amount"),
        )
        .withColumn("payment_label", payment_map[F.col("payment_type")])
        .orderBy("payment_type")
    )


def top_pickup_zones(df: DataFrame, top_n: int = 20) -> DataFrame:
    """
    Top *top_n* pickup zones by trip count.

    Returns ``PULocationID``, ``trip_count``, ``pct_of_total`` and a rank
    column.  ``row_number()`` is used so that exactly *top_n* rows are
    returned even when multiple zones share the same trip count.
    """
    total = df.count()
    agg = (
        df.groupBy("PULocationID")
        .agg(F.count("*").alias("trip_count"))
    )
    window = Window.orderBy(F.desc("trip_count"))
    return (
        agg
        .withColumn("rank", F.row_number().over(window))
        .withColumn("pct_of_total", F.round(F.col("trip_count") / total * 100, 4))
        .filter(F.col("rank") <= top_n)
        .orderBy("rank")
    )


def top_dropoff_zones(df: DataFrame, top_n: int = 20) -> DataFrame:
    """
    Top *top_n* dropoff zones by trip count.

    ``row_number()`` ensures at most *top_n* rows even in case of tied counts.
    """
    total = df.count()
    agg = (
        df.groupBy("DOLocationID")
        .agg(F.count("*").alias("trip_count"))
    )
    window = Window.orderBy(F.desc("trip_count"))
    return (
        agg
        .withColumn("rank", F.row_number().over(window))
        .withColumn("pct_of_total", F.round(F.col("trip_count") / total * 100, 4))
        .filter(F.col("rank") <= top_n)
        .orderBy("rank")
    )


def fare_stats(df: DataFrame) -> DataFrame:
    """
    Overall fare_amount statistics: mean, stddev, min, max, p50, p95.
    """
    return df.select(
        F.round(F.mean("fare_amount"), 4).alias("mean_fare"),
        F.round(F.stddev("fare_amount"), 4).alias("stddev_fare"),
        F.round(F.min("fare_amount"), 4).alias("min_fare"),
        F.round(F.max("fare_amount"), 4).alias("max_fare"),
        F.round(F.percentile_approx("fare_amount", 0.50), 4).alias("p50_fare"),
        F.round(F.percentile_approx("fare_amount", 0.95), 4).alias("p95_fare"),
    )


def trip_distance_buckets(df: DataFrame) -> DataFrame:
    """
    Distribution of trips across distance buckets (miles):
    [0–1), [1–3), [3–5), [5–10), [10–20), [20+).
    """
    bucket_col = (
        F.when(F.col("trip_distance") < 1, "0-1 mi")
        .when(F.col("trip_distance") < 3, "1-3 mi")
        .when(F.col("trip_distance") < 5, "3-5 mi")
        .when(F.col("trip_distance") < 10, "5-10 mi")
        .when(F.col("trip_distance") < 20, "10-20 mi")
        .otherwise("20+ mi")
    )
    return (
        df.withColumn("distance_bucket", bucket_col)
        .groupBy("distance_bucket")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 4).alias("avg_fare"),
        )
        .orderBy("distance_bucket")
    )


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def _write_gold(df: DataFrame, name: str, base_path: str) -> None:
    """Write a single gold output as a Delta table."""
    path = f"{base_path}/{name}"
    logger.info("Writing gold output '%s' → %s", name, path)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    logger.info("  Written: %s", path)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def compute_all(
    df: DataFrame,
    output_base_path: Optional[str] = None,
) -> dict:
    """
    Compute every analytics output and write each one to its own Delta table
    under *output_base_path*.

    Returns
    -------
    dict
        Mapping of output name → DataFrame (lazy; already written to disk).
    """
    base = output_base_path or GOLD_PATH

    outputs = {
        "daily_trip_summary": daily_trip_summary(df),
        "hourly_demand": hourly_demand(df),
        "payment_type_breakdown": payment_type_breakdown(df),
        "top_pickup_zones": top_pickup_zones(df),
        "top_dropoff_zones": top_dropoff_zones(df),
        "fare_stats": fare_stats(df),
        "trip_distance_buckets": trip_distance_buckets(df),
    }

    for name, result_df in outputs.items():
        _write_gold(result_df, name, base)

    logger.info("All gold outputs written to %s", base)
    return outputs
