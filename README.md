# TLC Jan 2024 PySpark Lakehouse Pipeline

End-to-end PySpark pipeline for NYC Taxi & Limousine Commission (TLC) Yellow
Taxi trip data, **timeboxed to January 2024**.  
Implements the medallion lakehouse architecture (Bronze → Silver → Gold) with
fail-fast data quality checks and seven analytics outputs.

---

## Architecture

```
TLC public parquet (Jan 2024)
        │
        ▼
┌───────────────────────────────────────────────────────┐
│  BRONZE  – raw ingest, no changes, + metadata columns │
│  data/bronze/yellow_tripdata_2024_01  (Delta)         │
└───────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────┐
│  SILVER  – clean + validate                           │
│  • Cast schema                                        │
│  • Timebox to 2024-01-01 – 2024-01-31                 │
│  • Drop nulls in critical columns                     │
│  • Business-rule filters (distance, fare, pax, …)    │
│  • Derived columns (duration, fare/mile, hour, date)  │
│  • Deduplication                                      │
│  • Fail-fast DQ checks (6 checks)                     │
│  data/silver/yellow_tripdata_2024_01  (Delta, by date)│
└───────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────┐
│  GOLD  – analytics (7 outputs, each a Delta table)    │
│  • daily_trip_summary                                 │
│  • hourly_demand                                      │
│  • payment_type_breakdown                             │
│  • top_pickup_zones   (top 20)                        │
│  • top_dropoff_zones  (top 20)                        │
│  • fare_stats                                         │
│  • trip_distance_buckets                              │
│  data/gold/yellow_tripdata_2024_01/<output>/          │
└───────────────────────────────────────────────────────┘
```

---

## Project layout

```
TLC/
├── config/
│   └── pipeline_config.py   # timebox bounds, paths, DQ thresholds
├── src/
│   ├── bronze/
│   │   └── ingest.py        # download + load raw parquet → Delta
│   ├── silver/
│   │   ├── clean.py         # all cleaning transformations
│   │   └── quality_checks.py# fail-fast DQ checks + DataQualityError
│   └── gold/
│       └── analytics.py     # 7 analytics aggregations
├── pipeline.py              # end-to-end orchestrator + CLI
├── tests/
│   ├── conftest.py          # shared SparkSession fixture
│   ├── test_silver_clean.py # 27 unit tests for silver cleaning
│   ├── test_quality_checks.py # 17 unit tests for DQ checks
│   └── test_analytics.py   # 17 unit tests for gold analytics
└── requirements.txt
```

---

## Quick start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

> Requires Java 8 or 11 in `PATH` for PySpark.

### 2. Run the full pipeline

```bash
python pipeline.py
```

The pipeline will:
1. Download `yellow_tripdata_2024-01.parquet` from the TLC public dataset
   (~300 MB) to `/tmp/`.
2. Write the bronze Delta table to `data/bronze/`.
3. Clean the data and run all DQ checks; write silver Delta table (partitioned
   by `pickup_date`) to `data/silver/`.
4. Compute seven gold outputs to `data/gold/`.

### 3. CLI options

```
usage: pipeline.py [--skip-download] [--raw-parquet PATH]
                   [--bronze-path PATH] [--silver-path PATH]
                   [--gold-path PATH]
                   [--min-row-count N] [--max-null-rate F]

  --skip-download      Skip downloading (assumes file exists)
  --raw-parquet PATH   Path to already-downloaded parquet
  --bronze-path PATH   Bronze Delta table path  (default: data/bronze/…)
  --silver-path PATH   Silver Delta table path  (default: data/silver/…)
  --gold-path   PATH   Gold output base path    (default: data/gold/…)
  --min-row-count N    DQ minimum row count     (default: 1 000 000)
  --max-null-rate F    DQ max null rate 0–1     (default: 0.05)
```

---

## Data quality checks (fail-fast)

All checks run against the **silver** DataFrame before it is written.
A single failure raises `DataQualityError` and aborts the pipeline.

| Check | What it verifies |
|-------|-----------------|
| `check_min_row_count` | At least 1 000 000 rows after cleaning |
| `check_null_rates` | ≤ 5 % nulls in 6 critical columns |
| `check_numeric_ranges` | `trip_distance` ∈ [0, 500], `fare_amount` ∈ [0, 10 000] |
| `check_timebox` | All pickup datetimes within Jan 2024 |
| `check_no_negative_durations` | Dropoff always after pickup |
| `check_location_ids` | `PULocationID` / `DOLocationID` ∈ [1, 265] |

---

## Running the tests

```bash
python -m pytest tests/ -v
```

61 unit tests covering every cleaning step, every DQ check, and every gold
aggregation function. Tests use an in-process `local[2]` SparkSession and
require no network access or external storage.

---

## Configuration

All pipeline constants are centralised in `config/pipeline_config.py`:

| Constant | Default | Purpose |
|----------|---------|---------|
| `TIMEBOX_START` | `2024-01-01` | Inclusive pickup window start |
| `TIMEBOX_END` | `2024-01-31` | Inclusive pickup window end |
| `DQ_MIN_ROW_COUNT` | `1_000_000` | Fail if silver has fewer rows |
| `DQ_MAX_NULL_RATE` | `0.05` | Max null fraction in critical cols |
| `BRONZE_PATH` | `data/bronze/…` | Delta table output path |
| `SILVER_PATH` | `data/silver/…` | Delta table output path |
| `GOLD_PATH` | `data/gold/…` | Gold outputs base path |
