"""
Shared fixtures for the TLC pipeline test suite.

Uses a module-scoped SparkSession so that each test module shares a single
session rather than starting/stopping PySpark on every file.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Lightweight local SparkSession for unit tests (no Delta I/O).

    Module-scoped so each test module gets an isolated session, preventing
    any accidental cross-module SparkSession state leakage.
    """
    session = (
        SparkSession.builder
        .appName("tlc-pipeline-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
