"""
Bronze layer – raw ingestion of TLC yellow taxi parquet for January 2024.

Responsibilities
----------------
* Download the monthly parquet from the TLC public dataset URL (if not
  already present locally).
* Write the raw bytes as-is into the bronze Delta table, preserving full
  fidelity of the source data.
* Add ingestion metadata columns (``_ingested_at``, ``_source_url``).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config.pipeline_config import (
    TLC_YELLOW_URL,
    BRONZE_PATH,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def download_raw(
    url: str = TLC_YELLOW_URL,
    dest_path: str = "/tmp/tlc_yellow_2024_01_raw.parquet",
) -> str:
    """
    Download the raw parquet file to *dest_path* if it does not already exist.

    Returns
    -------
    str
        Local file-system path to the downloaded parquet.
    """
    dest = Path(dest_path)
    if dest.exists():
        logger.info("Raw file already present at %s – skipping download.", dest)
        return str(dest)

    logger.info("Downloading TLC yellow taxi Jan 2024 from %s …", url)
    dest.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):  # 1 MiB
                fh.write(chunk)

    size_mb = dest.stat().st_size / (1 << 20)
    logger.info("Downloaded %.1f MiB → %s", size_mb, dest)
    return str(dest)


def load_bronze(
    spark: SparkSession,
    raw_parquet_path: str,
    source_url: str = TLC_YELLOW_URL,
    output_path: Optional[str] = None,
) -> DataFrame:
    """
    Read the raw parquet, attach ingestion metadata, and persist to the
    bronze Delta table.

    Parameters
    ----------
    spark:
        Active SparkSession.
    raw_parquet_path:
        Local (or HDFS / S3) path to the raw parquet file.
    source_url:
        Original download URL – stored as lineage metadata.
    output_path:
        Delta table path for the bronze layer.  Defaults to
        ``config.BRONZE_PATH``.

    Returns
    -------
    DataFrame
        Bronze DataFrame (with metadata columns attached).
    """
    output_path = output_path or BRONZE_PATH

    logger.info("Reading raw parquet from %s", raw_parquet_path)
    df = spark.read.parquet(raw_parquet_path)

    # Add ingestion metadata
    df = df.withColumn("_ingested_at", F.current_timestamp()) \
           .withColumn("_source_url", F.lit(source_url))

    logger.info(
        "Writing %d columns to bronze Delta table at %s",
        len(df.columns),
        output_path,
    )
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )

    logger.info("Bronze layer written successfully.")
    return df


def read_bronze(spark: SparkSession, path: Optional[str] = None) -> DataFrame:
    """Read the bronze Delta table and return a DataFrame."""
    path = path or BRONZE_PATH
    logger.info("Reading bronze Delta table from %s", path)
    return spark.read.format("delta").load(path)
