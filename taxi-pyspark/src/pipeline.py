from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import Config
from quality import assert_not_null, assert_accepted_values, assert_range
from analytics import daily_metrics, card_vs_cash_share


def ensure_dirs(cfg: Config):
    for p in [cfg.data_dir, cfg.raw_dir, cfg.silver_dir, cfg.output_dir]:
        Path(p).mkdir(parents=True, exist_ok=True)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("taxi-pyspark-lite")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main():
    cfg = Config()
    ensure_dirs(cfg)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Load raw
        trips_raw = spark.read.parquet(cfg.yellow_jan_url)
        zones_raw = spark.read.option("header", True).csv(cfg.zone_lookup_url)

        # Persist raw copies
        trips_raw.write.mode("overwrite").parquet(str(cfg.raw_dir / "yellow_2024_01"))
        zones_raw.write.mode("overwrite").parquet(str(cfg.raw_dir / "taxi_zone_lookup"))

        # Silver transform
        trips = (
            trips_raw
            .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
            .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
            .withColumn("pickup_date", F.to_date("pickup_ts"))
            .withColumn("pickup_hour", F.hour("pickup_ts"))
            .withColumn("trip_duration_min", (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0)
            .select(
                "VendorID", "pickup_ts", "dropoff_ts", "pickup_date", "pickup_hour",
                "PULocationID", "DOLocationID", "trip_distance", "fare_amount",
                "tip_amount", "total_amount", "payment_type", "trip_duration_min"
            )
            .filter(F.col("fare_amount") > 0)
            .filter(F.col("trip_distance") >= 0)
            .filter(F.col("dropoff_ts") >= F.col("pickup_ts"))
            .filter((F.col("trip_duration_min") >= cfg.min_duration) & (F.col("trip_duration_min") <= cfg.max_duration))
            .dropDuplicates(["VendorID", "pickup_ts", "dropoff_ts", "PULocationID", "DOLocationID", "fare_amount"])
        )

        zones = (
            zones_raw
            .withColumn("LocationID", F.col("LocationID").cast("int"))
            .withColumnRenamed("Borough", "borough")
            .withColumnRenamed("Zone", "zone")
            .select("LocationID", "borough", "zone")
        )

        # Validate location IDs (drop invalid for lite demo)
        valid_ids = zones.select(F.col("LocationID").alias("valid_loc")).distinct()
        trips = trips.join(valid_ids, trips.PULocationID == valid_ids.valid_loc, "inner").drop("valid_loc")
        trips = trips.join(valid_ids, trips.DOLocationID == valid_ids.valid_loc, "inner").drop("valid_loc")

        # DQ checks
        assert_not_null(trips, ["pickup_ts", "dropoff_ts", "PULocationID", "DOLocationID"], "silver_trips")
        assert_accepted_values(trips, "payment_type", [1, 2, 3, 4, 5, 6], "silver_trips")
        assert_range(trips, "trip_duration_min", cfg.min_duration, cfg.max_duration, "silver_trips")
        assert_range(trips, "trip_distance", 0.0, 100.0, "silver_trips")

        # Write silver
        trips.write.mode("overwrite").partitionBy("pickup_date").parquet(str(cfg.silver_dir / "trips"))
        zones.write.mode("overwrite").parquet(str(cfg.silver_dir / "zones"))

        # Outputs (2 metrics only for lite scope)
        out_daily = daily_metrics(trips)
        out_pay = card_vs_cash_share(trips)

        out_daily.write.mode("overwrite").parquet(str(cfg.output_dir / "daily_metrics"))
        out_pay.write.mode("overwrite").parquet(str(cfg.output_dir / "card_vs_cash_share"))

        # Optional CSV for quick viewing
        out_daily.coalesce(1).write.mode("overwrite").option("header", True).csv(str(cfg.output_dir / "csv_daily_metrics"))
        out_pay.coalesce(1).write.mode("overwrite").option("header", True).csv(str(cfg.output_dir / "csv_card_vs_cash_share"))

        print("[SUCCESS] Lite PySpark pipeline complete.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()