# Taxi Lakehouse - dbt 

## Scope
Implementation uses **dbt**.  


## Implemented
- Raw ingestion via external files (TLC Jan-Mar 2024 + zone lookup)
- Staging models
- Silver cleaned trips with:
  - timestamp standardization
  - pickup_date, pickup_hour
  - invalid row filtering
  - deduplication
  - zone FK validation
- Gold star schema:
  - `fact_trip` (incremental)
  - `dim_zone`
  - `dim_date`
- Analytics outputs:
  - daily metrics
  - top-10 O-D pairs per month
  - tip-rate by borough/hour
  - card vs cash share by month
- Data quality tests:
  - not_null, accepted_values, relationships
  - custom range tests for duration/distance

## Setup

### 1) Create venv and install
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install dbt-duckdb==1.9.2
```

### 2) Configure profile
Copy `dbt_taxi/profiles_template.yml` to `~/.dbt/profiles.yml`.

### 3) Run
```bash
cd dbt_taxi
dbt deps
dbt run
dbt test
```

## Incremental approach
- Add April source and process only new partition/month.
- `fact_trip` is incremental with `unique_key=trip_sk`.
- Current filter loads records with `pickup_date >= max(pickup_date)` in existing fact.
- For late-arriving updates, evolve to merge strategy with ingestion watermark and deterministic business key.

## Future
- Add source freshness checks and exposures.
- Add quarantine model for invalid zone IDs instead of dropping.
- Add orchestration and CI pipeline for `dbt run` + `dbt test`.