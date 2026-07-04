"""
Pipeline configuration for the TLC Jan 2024 lakehouse pipeline.
All paths, timebox bounds, and quality thresholds live here.
"""

from datetime import date

# ---------------------------------------------------------------------------
# Timebox – January 2024 only
# ---------------------------------------------------------------------------
TIMEBOX_START: str = "2024-01-01"
TIMEBOX_END: str = "2024-01-31"
TIMEBOX_START_DATE: date = date(2024, 1, 1)
TIMEBOX_END_DATE: date = date(2024, 1, 31)

# ---------------------------------------------------------------------------
# Data source
# ---------------------------------------------------------------------------
TLC_YELLOW_URL: str = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
)

# ---------------------------------------------------------------------------
# Lakehouse paths  (relative; override with absolute paths in production)
# ---------------------------------------------------------------------------
BRONZE_PATH: str = "data/bronze/yellow_tripdata_2024_01"
SILVER_PATH: str = "data/silver/yellow_tripdata_2024_01"
GOLD_PATH: str = "data/gold/yellow_tripdata_2024_01"

# ---------------------------------------------------------------------------
# Data quality thresholds (fail-fast)
# ---------------------------------------------------------------------------
DQ_MIN_ROW_COUNT: int = 1_000_000          # Jan 2024 typically ~2.9 M rows
DQ_MAX_NULL_RATE: float = 0.05             # ≤ 5 % nulls in critical columns
DQ_CRITICAL_COLUMNS: list = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
]
DQ_MIN_TRIP_DISTANCE: float = 0.0
DQ_MAX_TRIP_DISTANCE: float = 500.0        # miles; anything above is suspect
DQ_MIN_FARE_AMOUNT: float = 0.0
DQ_MAX_FARE_AMOUNT: float = 10_000.0
DQ_VALID_PASSENGER_COUNTS: list = list(range(1, 7))   # 1–6

# ---------------------------------------------------------------------------
# Spark settings
# ---------------------------------------------------------------------------
SPARK_APP_NAME: str = "TLC-Jan2024-Pipeline"
SPARK_MASTER: str = "local[*]"
DELTA_LOG_RETENTION_DAYS: int = 7
