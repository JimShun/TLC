from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def daily_metrics(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg(F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance"))), 4).alias("avg_fare_per_mile"),
        )
        .orderBy("pickup_date")
    )


def card_vs_cash_share(trips: DataFrame) -> DataFrame:
    base = trips.withColumn("pickup_month", F.date_format("pickup_date", "yyyy-MM"))
    totals = base.groupBy("pickup_month").agg(F.count("*").alias("month_trip_count"))

    return (
        base.filter(F.col("payment_type").isin([1, 2]))
        .withColumn("payment_label", F.when(F.col("payment_type") == 1, F.lit("card")).otherwise(F.lit("cash")))
        .groupBy("pickup_month", "payment_type", "payment_label")
        .agg(F.count("*").alias("trip_count"))
        .join(totals, on="pickup_month", how="left")
        .withColumn("share_pct", F.round(F.col("trip_count") / F.col("month_trip_count") * 100, 2))
        .orderBy("pickup_month", "payment_type")
    )