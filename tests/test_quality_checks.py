"""
Unit tests for src.silver.quality_checks – fail-fast DQ checks.
"""

from __future__ import annotations

import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DateType,
)

from src.silver.quality_checks import (
    DataQualityError,
    check_min_row_count,
    check_null_rates,
    check_numeric_ranges,
    check_timebox,
    check_no_negative_durations,
    check_location_ids,
    run_all_checks,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(s: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(s)


# Silver schema (post-clean, includes derived columns)
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


def _valid_row(
    pickup: str = "2024-01-15 10:00:00",
    dropoff: str = "2024-01-15 10:20:00",
    distance: float = 3.5,
    fare: float = 15.0,
    pu_id: int = 100,
    do_id: int = 200,
) -> dict:
    return dict(
        VendorID=1,
        tpep_pickup_datetime=_ts(pickup),
        tpep_dropoff_datetime=_ts(dropoff),
        passenger_count=1,
        trip_distance=distance,
        RatecodeID=1,
        store_and_fwd_flag="N",
        PULocationID=pu_id,
        DOLocationID=do_id,
        payment_type=1,
        fare_amount=fare,
        extra=0.5,
        mta_tax=0.5,
        tip_amount=2.0,
        tolls_amount=0.0,
        improvement_surcharge=0.3,
        total_amount=fare + 3.3,
        congestion_surcharge=2.5,
        trip_duration_minutes=20.0,
        fare_per_mile=fare / distance,
        pickup_hour=10,
        pickup_date=datetime.date(2024, 1, 15),
    )


# ---------------------------------------------------------------------------
# check_min_row_count
# ---------------------------------------------------------------------------

class TestCheckMinRowCount:
    def test_passes_when_above_threshold(self, spark: SparkSession):
        rows = [_valid_row() for _ in range(5)]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        check_min_row_count(df, min_count=3)   # should not raise

    def test_fails_when_below_threshold(self, spark: SparkSession):
        df = spark.createDataFrame([_valid_row()], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError, match="Row count"):
            check_min_row_count(df, min_count=100)

    def test_fails_on_empty_dataframe(self, spark: SparkSession):
        df = spark.createDataFrame([], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError):
            check_min_row_count(df, min_count=1)


# ---------------------------------------------------------------------------
# check_null_rates
# ---------------------------------------------------------------------------

class TestCheckNullRates:
    def test_passes_with_no_nulls(self, spark: SparkSession):
        df = spark.createDataFrame([_valid_row()], schema=SILVER_SCHEMA)
        check_null_rates(df, columns=["fare_amount", "trip_distance"], max_null_rate=0.05)

    def test_fails_when_null_rate_too_high(self, spark: SparkSession):
        row_with_null = _valid_row()
        row_with_null["fare_amount"] = None
        df = spark.createDataFrame([row_with_null], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError, match="fare_amount"):
            check_null_rates(df, columns=["fare_amount"], max_null_rate=0.05)

    def test_fails_on_empty_dataframe(self, spark: SparkSession):
        df = spark.createDataFrame([], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError, match="empty"):
            check_null_rates(df, columns=["fare_amount"], max_null_rate=0.05)


# ---------------------------------------------------------------------------
# check_numeric_ranges
# ---------------------------------------------------------------------------

class TestCheckNumericRanges:
    def test_passes_valid_rows(self, spark: SparkSession):
        df = spark.createDataFrame([_valid_row()], schema=SILVER_SCHEMA)
        check_numeric_ranges(df)   # should not raise

    def test_fails_negative_fare(self, spark: SparkSession):
        row = _valid_row(fare=-5.0)
        df = spark.createDataFrame([row], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError, match="valid range"):
            check_numeric_ranges(df)

    def test_fails_excessive_distance(self, spark: SparkSession):
        row = _valid_row(distance=999.0)
        df = spark.createDataFrame([row], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError, match="valid range"):
            check_numeric_ranges(df)


# ---------------------------------------------------------------------------
# check_timebox
# ---------------------------------------------------------------------------

class TestCheckTimebox:
    def test_passes_row_in_jan_2024(self, spark: SparkSession):
        df = spark.createDataFrame([_valid_row()], schema=SILVER_SCHEMA)
        check_timebox(df)   # should not raise

    def test_fails_row_before_jan_2024(self, spark: SparkSession):
        row = _valid_row(pickup="2023-12-31 23:00:00", dropoff="2023-12-31 23:30:00")
        df = spark.createDataFrame([row], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError, match="outside the allowed window"):
            check_timebox(df)

    def test_fails_row_after_jan_2024(self, spark: SparkSession):
        row = _valid_row(pickup="2024-02-01 00:00:00", dropoff="2024-02-01 00:20:00")
        df = spark.createDataFrame([row], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError, match="outside the allowed window"):
            check_timebox(df)


# ---------------------------------------------------------------------------
# check_no_negative_durations
# ---------------------------------------------------------------------------

class TestCheckNoNegativeDurations:
    def test_passes_positive_duration(self, spark: SparkSession):
        df = spark.createDataFrame([_valid_row()], schema=SILVER_SCHEMA)
        check_no_negative_durations(df)   # should not raise

    def test_fails_zero_duration(self, spark: SparkSession):
        row = _valid_row(pickup="2024-01-15 10:00:00", dropoff="2024-01-15 10:00:00")
        df = spark.createDataFrame([row], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError, match="dropoff"):
            check_no_negative_durations(df)

    def test_fails_negative_duration(self, spark: SparkSession):
        row = _valid_row(pickup="2024-01-15 11:00:00", dropoff="2024-01-15 10:00:00")
        df = spark.createDataFrame([row], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError, match="dropoff"):
            check_no_negative_durations(df)


# ---------------------------------------------------------------------------
# check_location_ids
# ---------------------------------------------------------------------------

class TestCheckLocationIds:
    def test_passes_valid_location_ids(self, spark: SparkSession):
        df = spark.createDataFrame([_valid_row(pu_id=1, do_id=265)], schema=SILVER_SCHEMA)
        check_location_ids(df)   # should not raise

    def test_fails_pu_location_zero(self, spark: SparkSession):
        df = spark.createDataFrame([_valid_row(pu_id=0)], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError, match="PULocationID"):
            check_location_ids(df)

    def test_fails_do_location_too_large(self, spark: SparkSession):
        df = spark.createDataFrame([_valid_row(do_id=999)], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError, match="DOLocationID"):
            check_location_ids(df)


# ---------------------------------------------------------------------------
# run_all_checks (integration)
# ---------------------------------------------------------------------------

class TestRunAllChecks:
    def test_passes_on_clean_data(self, spark: SparkSession):
        rows = [_valid_row() for _ in range(10)]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        run_all_checks(df, min_row_count=1, max_null_rate=0.05)   # should not raise

    def test_fails_fast_on_first_violation(self, spark: SparkSession):
        # An empty DataFrame should fail at the very first check (row count)
        df = spark.createDataFrame([], schema=SILVER_SCHEMA)
        with pytest.raises(DataQualityError):
            run_all_checks(df, min_row_count=1)
