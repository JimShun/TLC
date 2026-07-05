from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    yellow_jan_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    zone_lookup_url: str = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    data_dir: Path = Path("data")
    raw_dir: Path = Path("data/raw")
    silver_dir: Path = Path("data/silver")
    output_dir: Path = Path("data/output")

    min_duration: float = 0.0
    max_duration: float = 300.0