"""
Unit tests for src.gold.analytics – analytics aggregation functions.
"""

from __future__ import annotations

import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.gold.analytics import (
    daily_trip_summary,
    hourly_demand,
    payment_type_breakdown,
    top_pickup_zones,
    top_dropoff_zones,
    fare_stats,
    trip_distance_buckets,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(s: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(s)


SILVER_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("trip_duration_minutes", DoubleType(), True),
    StructField("fare_per_mile", DoubleType(), True),
    StructField("pickup_hour", IntegerType(), True),
    StructField("pickup_date", DateType(), True),
])


def _row(
    pickup_date: datetime.date = datetime.date(2024, 1, 15),
    pickup_hour: int = 10,
    trip_distance: float = 3.5,
    fare_amount: float = 15.0,
    payment_type: int = 1,
    pu_id: int = 100,
    do_id: int = 200,
    trip_duration_minutes: float = 20.0,
    tip_amount: float = 2.0,
    total_amount: float = 18.0,
) -> dict:
    pickup_dt = datetime.datetime(
        pickup_date.year, pickup_date.month, pickup_date.day, pickup_hour, 0, 0
    )
    return dict(
        VendorID=1,
        tpep_pickup_datetime=pickup_dt,
        tpep_dropoff_datetime=pickup_dt + datetime.timedelta(minutes=trip_duration_minutes),
        passenger_count=1,
        trip_distance=trip_distance,
        RatecodeID=1,
        store_and_fwd_flag="N",
        PULocationID=pu_id,
        DOLocationID=do_id,
        payment_type=payment_type,
        fare_amount=fare_amount,
        extra=0.5,
        mta_tax=0.5,
        tip_amount=tip_amount,
        tolls_amount=0.0,
        improvement_surcharge=0.3,
        total_amount=total_amount,
        congestion_surcharge=2.5,
        trip_duration_minutes=trip_duration_minutes,
        fare_per_mile=fare_amount / trip_distance if trip_distance > 0 else None,
        pickup_hour=pickup_hour,
        pickup_date=pickup_date,
    )


# ---------------------------------------------------------------------------
# daily_trip_summary
# ---------------------------------------------------------------------------

class TestDailyTripSummary:
    def test_returns_one_row_per_day(self, spark: SparkSession):
        rows = [
            _row(pickup_date=datetime.date(2024, 1, 1)),
            _row(pickup_date=datetime.date(2024, 1, 1)),
            _row(pickup_date=datetime.date(2024, 1, 2)),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = daily_trip_summary(df)
        assert result.count() == 2

    def test_trip_count_correct(self, spark: SparkSession):
        rows = [_row(pickup_date=datetime.date(2024, 1, 5)) for _ in range(7)]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = daily_trip_summary(df)
        row = result.collect()[0]
        assert row["trip_count"] == 7

    def test_contains_expected_columns(self, spark: SparkSession):
        df = spark.createDataFrame([_row()], schema=SILVER_SCHEMA)
        result = daily_trip_summary(df)
        for col in (
            "pickup_date",
            "trip_count",
            "total_distance_miles",
            "total_fare_amount",
            "avg_fare_amount",
            "avg_trip_duration_min",
        ):
            assert col in result.columns


# ---------------------------------------------------------------------------
# hourly_demand
# ---------------------------------------------------------------------------

class TestHourlyDemand:
    def test_one_row_per_hour(self, spark: SparkSession):
        rows = [_row(pickup_hour=h) for h in [8, 9, 10]]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = hourly_demand(df)
        assert result.count() == 3

    def test_aggregates_multiple_trips_same_hour(self, spark: SparkSession):
        rows = [_row(pickup_hour=8) for _ in range(5)]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = hourly_demand(df)
        row = result.collect()[0]
        assert row["trip_count"] == 5


# ---------------------------------------------------------------------------
# payment_type_breakdown
# ---------------------------------------------------------------------------

class TestPaymentTypeBreakdown:
    def test_groups_by_payment_type(self, spark: SparkSession):
        rows = [
            _row(payment_type=1),
            _row(payment_type=1),
            _row(payment_type=2),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = payment_type_breakdown(df)
        assert result.count() == 2

    def test_has_payment_label_column(self, spark: SparkSession):
        df = spark.createDataFrame([_row(payment_type=1)], schema=SILVER_SCHEMA)
        result = payment_type_breakdown(df)
        assert "payment_label" in result.columns

    def test_credit_card_label(self, spark: SparkSession):
        df = spark.createDataFrame([_row(payment_type=1)], schema=SILVER_SCHEMA)
        row = payment_type_breakdown(df).collect()[0]
        assert row["payment_label"] == "Credit card"


# ---------------------------------------------------------------------------
# top_pickup_zones
# ---------------------------------------------------------------------------

class TestTopPickupZones:
    def test_returns_at_most_top_n(self, spark: SparkSession):
        rows = [_row(pu_id=i) for i in range(1, 31)]  # 30 distinct zones
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = top_pickup_zones(df, top_n=10)
        assert result.count() <= 10

    def test_contains_rank_column(self, spark: SparkSession):
        df = spark.createDataFrame([_row()], schema=SILVER_SCHEMA)
        result = top_pickup_zones(df, top_n=5)
        assert "rank" in result.columns

    def test_most_common_zone_ranked_first(self, spark: SparkSession):
        rows = (
            [_row(pu_id=42)] * 5    # zone 42 appears 5 times → rank 1
            + [_row(pu_id=99)] * 2  # zone 99 appears 2 times → rank 2
        )
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = top_pickup_zones(df, top_n=2)
        first = result.orderBy("rank").collect()[0]
        assert first["PULocationID"] == 42
        assert first["rank"] == 1


# ---------------------------------------------------------------------------
# top_dropoff_zones
# ---------------------------------------------------------------------------

class TestTopDropoffZones:
    def test_returns_at_most_top_n(self, spark: SparkSession):
        rows = [_row(do_id=i) for i in range(1, 31)]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = top_dropoff_zones(df, top_n=10)
        assert result.count() <= 10


# ---------------------------------------------------------------------------
# fare_stats
# ---------------------------------------------------------------------------

class TestFareStats:
    def test_returns_single_row(self, spark: SparkSession):
        rows = [_row(fare_amount=float(i)) for i in range(1, 11)]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = fare_stats(df)
        assert result.count() == 1

    def test_contains_expected_columns(self, spark: SparkSession):
        df = spark.createDataFrame([_row()], schema=SILVER_SCHEMA)
        result = fare_stats(df)
        for col in ("mean_fare", "stddev_fare", "min_fare", "max_fare", "p50_fare", "p95_fare"):
            assert col in result.columns

    def test_min_max_correct(self, spark: SparkSession):
        rows = [_row(fare_amount=float(i)) for i in [5, 10, 15]]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        row = fare_stats(df).collect()[0]
        assert row["min_fare"] == 5.0
        assert row["max_fare"] == 15.0


# ---------------------------------------------------------------------------
# trip_distance_buckets
# ---------------------------------------------------------------------------

class TestTripDistanceBuckets:
    def test_returns_expected_buckets(self, spark: SparkSession):
        rows = [
            _row(trip_distance=0.5),
            _row(trip_distance=2.0),
            _row(trip_distance=4.0),
            _row(trip_distance=7.0),
            _row(trip_distance=15.0),
            _row(trip_distance=25.0),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result = trip_distance_buckets(df)
        assert result.count() == 6   # one row per distinct bucket

    def test_has_trip_count_column(self, spark: SparkSession):
        df = spark.createDataFrame([_row(trip_distance=1.5)], schema=SILVER_SCHEMA)
        result = trip_distance_buckets(df)
        assert "trip_count" in result.columns
        assert "distance_bucket" in result.columns
