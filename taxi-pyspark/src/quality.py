from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_not_null(df: DataFrame, cols: list[str], name: str):
    for c in cols:
        n = df.filter(F.col(c).isNull()).count()
        if n > 0:
            raise ValueError(f"[DQ FAIL] {name}.{c} has {n} null rows")


def assert_accepted_values(df: DataFrame, col: str, accepted: list[int], name: str):
    n = df.filter(~F.col(col).isin(accepted)).count()
    if n > 0:
        raise ValueError(f"[DQ FAIL] {name}.{col} has {n} rows outside accepted values {accepted}")


def assert_range(df: DataFrame, col: str, min_v: float, max_v: float, name: str):
    n = df.filter((F.col(col) < min_v) | (F.col(col) > max_v)).count()
    if n > 0:
        raise ValueError(f"[DQ FAIL] {name}.{col} has {n} rows outside [{min_v}, {max_v}]")