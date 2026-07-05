import pytest
from pyspark.sql import SparkSession, Row

from src.quality import assert_not_null, assert_accepted_values, assert_range


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test-quality").getOrCreate()
    yield s
    s.stop()


def test_not_null_pass(spark):
    df = spark.createDataFrame([Row(a=1), Row(a=2)])
    assert_not_null(df, ["a"], "t")


def test_not_null_fail(spark):
    df = spark.createDataFrame([Row(a=1), Row(a=None)])
    with pytest.raises(ValueError):
        assert_not_null(df, ["a"], "t")


def test_accepted_values_fail(spark):
    df = spark.createDataFrame([Row(p=1), Row(p=9)])
    with pytest.raises(ValueError):
        assert_accepted_values(df, "p", [1, 2], "t")


def test_range_fail(spark):
    df = spark.createDataFrame([Row(x=10.0), Row(x=999.0)])
    with pytest.raises(ValueError):
        assert_range(df, "x", 0.0, 100.0, "t")