"""
TLC Jan 2024 Lakehouse Pipeline – main entry point.

Pipeline stages
---------------
1. **Bronze** – Download raw parquet from the TLC public dataset and persist
   it as-is to a Delta table with ingestion metadata.
2. **Silver** – Clean and standardise the data: cast types, apply the Jan 2024
   timebox, remove invalid rows, add derived columns, deduplicate.
3. **Silver DQ** – Run fail-fast data quality checks against the cleaned
   DataFrame before writing it.
4. **Gold** – Compute analytics aggregations and write each output to its own
   Delta table.

Usage
-----
Run the full pipeline end-to-end::

    python pipeline.py

Skip the download step if the raw parquet is already present::

    python pipeline.py --skip-download

Point to a pre-existing raw parquet file::

    python pipeline.py --raw-parquet /path/to/yellow_tripdata_2024-01.parquet

Adjust lakehouse paths::

    python pipeline.py --bronze-path /data/bronze --silver-path /data/silver \\
                       --gold-path /data/gold
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from pyspark.sql import SparkSession

from config.pipeline_config import (
    BRONZE_PATH,
    SILVER_PATH,
    GOLD_PATH,
    SPARK_APP_NAME,
    SPARK_MASTER,
    TLC_YELLOW_URL,
    DQ_MIN_ROW_COUNT,
    DQ_MAX_NULL_RATE,
)
from src.bronze.ingest import download_raw, load_bronze, read_bronze
from src.silver.clean import clean, write_silver
from src.silver.quality_checks import run_all_checks
from src.gold.analytics import compute_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SparkSession factory
# ---------------------------------------------------------------------------

def build_spark(app_name: str = SPARK_APP_NAME, master: str = SPARK_MASTER) -> SparkSession:
    """Create (or retrieve) a SparkSession with Delta Lake support."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def run_bronze(
    spark: SparkSession,
    raw_parquet_path: Optional[str] = None,
    skip_download: bool = False,
    bronze_path: str = BRONZE_PATH,
) -> None:
    """
    Stage 1: Bronze ingestion.

    Parameters
    ----------
    spark:
        Active SparkSession.
    raw_parquet_path:
        Path to an already-downloaded raw parquet.  If ``None`` and
        ``skip_download`` is ``False`` the file is downloaded first.
    skip_download:
        When ``True`` the download is skipped; the pipeline looks for
        ``raw_parquet_path`` on disk.
    bronze_path:
        Destination Delta table path.
    """
    logger.info("=== Stage 1: Bronze Ingestion ===")

    if not skip_download:
        raw_parquet_path = download_raw(dest_path=raw_parquet_path or "/tmp/tlc_yellow_2024_01_raw.parquet")
    elif raw_parquet_path is None:
        raw_parquet_path = "/tmp/tlc_yellow_2024_01_raw.parquet"

    load_bronze(spark, raw_parquet_path, output_path=bronze_path)
    logger.info("Bronze stage complete.")


def run_silver(
    spark: SparkSession,
    bronze_path: str = BRONZE_PATH,
    silver_path: str = SILVER_PATH,
    min_row_count: int = DQ_MIN_ROW_COUNT,
    max_null_rate: float = DQ_MAX_NULL_RATE,
) -> None:
    """
    Stage 2 & 3: Silver cleaning + fail-fast data quality checks.

    Parameters
    ----------
    spark:
        Active SparkSession.
    bronze_path:
        Source bronze Delta table.
    silver_path:
        Destination silver Delta table.
    min_row_count:
        DQ threshold override.
    max_null_rate:
        DQ threshold override.
    """
    logger.info("=== Stage 2: Silver Cleaning ===")
    bronze_df = read_bronze(spark, path=bronze_path)
    silver_df = clean(bronze_df)

    logger.info("=== Stage 3: Data Quality Checks ===")
    run_all_checks(silver_df, min_row_count=min_row_count, max_null_rate=max_null_rate)

    logger.info("=== Stage 2 (write): Persisting Silver Layer ===")
    write_silver(silver_df, output_path=silver_path)
    logger.info("Silver stage complete.")


def run_gold(
    spark: SparkSession,
    silver_path: str = SILVER_PATH,
    gold_path: str = GOLD_PATH,
) -> None:
    """
    Stage 4: Gold analytics outputs.

    Parameters
    ----------
    spark:
        Active SparkSession.
    silver_path:
        Source silver Delta table.
    gold_path:
        Base path for all gold output Delta tables.
    """
    logger.info("=== Stage 4: Gold Analytics ===")
    from src.silver.clean import read_silver
    silver_df = read_silver(spark, path=silver_path)
    compute_all(silver_df, output_base_path=gold_path)
    logger.info("Gold stage complete.")


# ---------------------------------------------------------------------------
# End-to-end orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(
    skip_download: bool = False,
    raw_parquet_path: Optional[str] = None,
    bronze_path: str = BRONZE_PATH,
    silver_path: str = SILVER_PATH,
    gold_path: str = GOLD_PATH,
    min_row_count: int = DQ_MIN_ROW_COUNT,
    max_null_rate: float = DQ_MAX_NULL_RATE,
) -> None:
    """
    Run the full TLC Jan 2024 lakehouse pipeline end-to-end.

    Raises
    ------
    DataQualityError
        If any fail-fast data quality check fails during the silver stage.
    """
    spark = build_spark()
    try:
        run_bronze(
            spark,
            raw_parquet_path=raw_parquet_path,
            skip_download=skip_download,
            bronze_path=bronze_path,
        )
        run_silver(
            spark,
            bronze_path=bronze_path,
            silver_path=silver_path,
            min_row_count=min_row_count,
            max_null_rate=max_null_rate,
        )
        run_gold(
            spark,
            silver_path=silver_path,
            gold_path=gold_path,
        )
        logger.info("=== Pipeline completed successfully ===")
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="TLC Jan 2024 PySpark Lakehouse Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading the raw parquet (assumes it already exists).",
    )
    parser.add_argument(
        "--raw-parquet",
        default=None,
        metavar="PATH",
        help="Path to the raw parquet file (downloaded here if not present).",
    )
    parser.add_argument("--bronze-path", default=BRONZE_PATH, metavar="PATH")
    parser.add_argument("--silver-path", default=SILVER_PATH, metavar="PATH")
    parser.add_argument("--gold-path", default=GOLD_PATH, metavar="PATH")
    parser.add_argument(
        "--min-row-count",
        type=int,
        default=DQ_MIN_ROW_COUNT,
        help="Minimum acceptable silver row count (DQ fail-fast).",
    )
    parser.add_argument(
        "--max-null-rate",
        type=float,
        default=DQ_MAX_NULL_RATE,
        help="Maximum acceptable null rate in critical columns (DQ fail-fast).",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    try:
        run_pipeline(
            skip_download=args.skip_download,
            raw_parquet_path=args.raw_parquet,
            bronze_path=args.bronze_path,
            silver_path=args.silver_path,
            gold_path=args.gold_path,
            min_row_count=args.min_row_count,
            max_null_rate=args.max_null_rate,
        )
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        sys.exit(1)
