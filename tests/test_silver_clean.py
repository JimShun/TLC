"""
Unit tests for src.silver.clean – silver cleaning transformations.

Each test targets a single transformation function and verifies it with a
small, hand-crafted DataFrame so the suite stays fast.
"""

from __future__ import annotations

import datetime

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.silver.clean import (
    apply_timebox,
    cast_schema,
    drop_nulls_in_critical_columns,
    filter_business_rules,
    add_derived_columns,
    deduplicate,
    clean,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(dt_str: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(dt_str)


# Minimal schema that mirrors the yellow taxi parquet columns used by clean()
YELLOW_SCHEMA = StructType([
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
])


def _make_valid_row(
    pickup: str = "2024-01-15 10:00:00",
    dropoff: str = "2024-01-15 10:20:00",
    trip_distance: float = 3.5,
    fare_amount: float = 15.0,
    passenger_count: int = 1,
    total_amount: float = 18.0,
    pu_id: int = 100,
    do_id: int = 200,
) -> dict:
    return dict(
        VendorID=1,
        tpep_pickup_datetime=_ts(pickup),
        tpep_dropoff_datetime=_ts(dropoff),
        passenger_count=passenger_count,
        trip_distance=trip_distance,
        RatecodeID=1,
        store_and_fwd_flag="N",
        PULocationID=pu_id,
        DOLocationID=do_id,
        payment_type=1,
        fare_amount=fare_amount,
        extra=0.5,
        mta_tax=0.5,
        tip_amount=2.0,
        tolls_amount=0.0,
        improvement_surcharge=0.3,
        total_amount=total_amount,
        congestion_surcharge=2.5,
    )


# ---------------------------------------------------------------------------
# Tests: apply_timebox
# ---------------------------------------------------------------------------

class TestApplyTimebox:
    def test_keeps_rows_in_window(self, spark: SparkSession):
        rows = [_make_valid_row(pickup="2024-01-01 00:00:00")]
        df = spark.createDataFrame(rows, schema=YELLOW_SCHEMA)
        result = apply_timebox(df)
        assert result.count() == 1

    def test_removes_rows_before_window(self, spark: SparkSession):
        rows = [_make_valid_row(pickup="2023-12-31 23:59:59")]
        df = spark.createDataFrame(rows, schema=YELLOW_SCHEMA)
        result = apply_timebox(df)
        assert result.count() == 0

    def test_removes_rows_after_window(self, spark: SparkSession):
        rows = [_make_valid_row(pickup="2024-02-01 00:00:00")]
        df = spark.createDataFrame(rows, schema=YELLOW_SCHEMA)
        result = apply_timebox(df)
        assert result.count() == 0

    def test_keeps_last_day_of_window(self, spark: SparkSession):
        rows = [_make_valid_row(pickup="2024-01-31 23:59:59")]
        df = spark.createDataFrame(rows, schema=YELLOW_SCHEMA)
        result = apply_timebox(df)
        assert result.count() == 1

    def test_mixed_rows(self, spark: SparkSession):
        rows = [
            _make_valid_row(pickup="2024-01-15 12:00:00"),
            _make_valid_row(pickup="2023-12-01 12:00:00"),
            _make_valid_row(pickup="2024-02-15 12:00:00"),
        ]
        df = spark.createDataFrame(rows, schema=YELLOW_SCHEMA)
        result = apply_timebox(df)
        assert result.count() == 1


# ---------------------------------------------------------------------------
# Tests: drop_nulls_in_critical_columns
# ---------------------------------------------------------------------------

class TestDropNullsInCriticalColumns:
    def test_removes_null_fare(self, spark: SparkSession):
        row = _make_valid_row()
        row["fare_amount"] = None
        df = spark.createDataFrame([row], schema=YELLOW_SCHEMA)
        assert drop_nulls_in_critical_columns(df).count() == 0

    def test_removes_null_pickup_datetime(self, spark: SparkSession):
        row = _make_valid_row()
        row["tpep_pickup_datetime"] = None
        df = spark.createDataFrame([row], schema=YELLOW_SCHEMA)
        assert drop_nulls_in_critical_columns(df).count() == 0

    def test_keeps_complete_row(self, spark: SparkSession):
        df = spark.createDataFrame([_make_valid_row()], schema=YELLOW_SCHEMA)
        assert drop_nulls_in_critical_columns(df).count() == 1


# ---------------------------------------------------------------------------
# Tests: filter_business_rules
# ---------------------------------------------------------------------------

class TestFilterBusinessRules:
    def test_removes_negative_fare(self, spark: SparkSession):
        row = _make_valid_row(fare_amount=-5.0, total_amount=-5.0)
        df = spark.createDataFrame([row], schema=YELLOW_SCHEMA)
        assert filter_business_rules(df).count() == 0

    def test_removes_zero_passenger_count(self, spark: SparkSession):
        row = _make_valid_row(passenger_count=0)
        df = spark.createDataFrame([row], schema=YELLOW_SCHEMA)
        assert filter_business_rules(df).count() == 0

    def test_removes_invalid_passenger_count(self, spark: SparkSession):
        row = _make_valid_row(passenger_count=10)
        df = spark.createDataFrame([row], schema=YELLOW_SCHEMA)
        assert filter_business_rules(df).count() == 0

    def test_removes_excessive_distance(self, spark: SparkSession):
        row = _make_valid_row(trip_distance=600.0)
        df = spark.createDataFrame([row], schema=YELLOW_SCHEMA)
        assert filter_business_rules(df).count() == 0

    def test_removes_dropoff_before_pickup(self, spark: SparkSession):
        row = _make_valid_row(
            pickup="2024-01-15 10:00:00",
            dropoff="2024-01-15 09:00:00",
        )
        df = spark.createDataFrame([row], schema=YELLOW_SCHEMA)
        assert filter_business_rules(df).count() == 0

    def test_removes_negative_total_amount(self, spark: SparkSession):
        row = _make_valid_row(total_amount=-1.0)
        df = spark.createDataFrame([row], schema=YELLOW_SCHEMA)
        assert filter_business_rules(df).count() == 0

    def test_keeps_valid_row(self, spark: SparkSession):
        df = spark.createDataFrame([_make_valid_row()], schema=YELLOW_SCHEMA)
        assert filter_business_rules(df).count() == 1


# ---------------------------------------------------------------------------
# Tests: add_derived_columns
# ---------------------------------------------------------------------------

class TestAddDerivedColumns:
    def test_adds_expected_columns(self, spark: SparkSession):
        df = spark.createDataFrame([_make_valid_row()], schema=YELLOW_SCHEMA)
        result = add_derived_columns(df)
        for col in ("trip_duration_minutes", "fare_per_mile", "pickup_hour", "pickup_date"):
            assert col in result.columns

    def test_trip_duration_minutes_correct(self, spark: SparkSession):
        # 20 minutes trip
        df = spark.createDataFrame(
            [_make_valid_row(pickup="2024-01-15 10:00:00", dropoff="2024-01-15 10:20:00")],
            schema=YELLOW_SCHEMA,
        )
        result = add_derived_columns(df)
        duration = result.collect()[0]["trip_duration_minutes"]
        assert abs(duration - 20.0) < 0.01

    def test_fare_per_mile_null_for_zero_distance(self, spark: SparkSession):
        row = _make_valid_row(trip_distance=0.0)
        df = spark.createDataFrame([row], schema=YELLOW_SCHEMA)
        result = add_derived_columns(df)
        assert result.collect()[0]["fare_per_mile"] is None

    def test_pickup_hour_extracted(self, spark: SparkSession):
        df = spark.createDataFrame(
            [_make_valid_row(pickup="2024-01-15 14:30:00")],
            schema=YELLOW_SCHEMA,
        )
        result = add_derived_columns(df)
        assert result.collect()[0]["pickup_hour"] == 14


# ---------------------------------------------------------------------------
# Tests: deduplicate
# ---------------------------------------------------------------------------

class TestDeduplicate:
    def test_removes_exact_duplicates(self, spark: SparkSession):
        row = _make_valid_row()
        df = spark.createDataFrame([row, row], schema=YELLOW_SCHEMA)
        result = deduplicate(df)
        assert result.count() == 1

    def test_keeps_distinct_rows(self, spark: SparkSession):
        rows = [
            _make_valid_row(fare_amount=10.0),
            _make_valid_row(fare_amount=20.0),
        ]
        df = spark.createDataFrame(rows, schema=YELLOW_SCHEMA)
        result = deduplicate(df)
        assert result.count() == 2


# ---------------------------------------------------------------------------
# Tests: clean (full pipeline)
# ---------------------------------------------------------------------------

class TestClean:
    def test_valid_row_passes_through(self, spark: SparkSession):
        df = spark.createDataFrame([_make_valid_row()], schema=YELLOW_SCHEMA)
        result = clean(df)
        assert result.count() == 1

    def test_out_of_timebox_removed(self, spark: SparkSession):
        row = _make_valid_row(pickup="2023-11-01 10:00:00", dropoff="2023-11-01 10:20:00")
        df = spark.createDataFrame([row], schema=YELLOW_SCHEMA)
        result = clean(df)
        assert result.count() == 0

    def test_derived_columns_added(self, spark: SparkSession):
        df = spark.createDataFrame([_make_valid_row()], schema=YELLOW_SCHEMA)
        result = clean(df)
        for col in ("trip_duration_minutes", "fare_per_mile", "pickup_hour", "pickup_date"):
            assert col in result.columns
