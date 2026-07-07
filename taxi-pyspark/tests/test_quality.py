import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

from src.quality import assert_not_null, assert_accepted_values, assert_range


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test-quality").getOrCreate()
    yield s
    s.stop()


def test_staging_not_null_pass(spark):
    # staging-like schema uses pickup_ts/dropoff_ts/trip_distance/payment_type
    df = spark.createDataFrame([
        Row(pickup_ts="2024-01-01 10:00:00", dropoff_ts="2024-01-01 10:20:00", trip_distance=2.5, payment_type=1),
        Row(pickup_ts="2024-01-01 11:00:00", dropoff_ts="2024-01-01 11:15:00", trip_distance=1.2, payment_type=2),
    ])
    assert_not_null(df, ["pickup_ts", "dropoff_ts", "trip_distance"], "stg_yellow_trips")


def test_staging_not_null_fail(spark):
    df = spark.createDataFrame([
        Row(pickup_ts="2024-01-01 10:00:00", dropoff_ts="2024-01-01 10:20:00", trip_distance=2.5, payment_type=1),
        Row(pickup_ts=None, dropoff_ts="2024-01-01 11:15:00", trip_distance=1.2, payment_type=2),
    ])
    with pytest.raises(ValueError):
        assert_not_null(df, ["pickup_ts", "dropoff_ts", "trip_distance"], "stg_yellow_trips")


def test_staging_accepted_values_fail_payment_type(spark):
    # staging payment_type should be in accepted domain
    df = spark.createDataFrame([
        Row(payment_type=1),
        Row(payment_type=9),
    ])
    with pytest.raises(ValueError):
        assert_accepted_values(df, "payment_type", [1, 2, 3, 4, 5, 6], "stg_yellow_trips")


def test_staging_distance_range_fail(spark):
    # equivalent to dbt staging distance test
    df = spark.createDataFrame([
        Row(trip_distance=10.0),
        Row(trip_distance=999.0),
    ])
    with pytest.raises(ValueError):
        assert_range(df, "trip_distance", 0.0, 100.0, "stg_yellow_trips")


def test_staging_duration_range_fail(spark):
    # staging does not store trip_duration_min -> compute from pickup/dropoff
    df = spark.createDataFrame([
        Row(pickup_ts="2024-01-01 10:00:00", dropoff_ts="2024-01-01 10:20:00"),
        Row(pickup_ts="2024-01-01 12:00:00", dropoff_ts="2024-01-01 04:00:00"),  # negative duration
    ]).withColumn(
        "trip_duration_min",
        (F.col("dropoff_ts").cast("timestamp").cast("long") - F.col("pickup_ts").cast("timestamp").cast("long")) / 60.0,
    )

    with pytest.raises(ValueError):
        assert_range(df, "trip_duration_min", 0.0, 300.0, "stg_yellow_trips")