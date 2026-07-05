# taxi-pyspark

Lightweight PySpark demo pipeline for NYC Yellow Taxi Jan 2024.

## Purpose
This repo is to demonstrate practical PySpark capability.

## Delivered
- Raw ingestion (yellow trips Jan 2024 + zone lookup)
- Silver cleaning:
  - timestamp parsing
  - pickup_date, pickup_hour
  - invalid row filtering
  - de-duplication
  - zone ID validation
- Data quality checks:
  - non-null
  - accepted payment_type
  - duration and distance ranges
- Analytics outputs:
  - daily metrics
  - card vs cash share

## Setup
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run
```bash
python src/pipeline.py
```

## Test
```bash
pytest -q
```

## Outputs
- `data/silver/trips/`
- `data/silver/zones/`
- `data/output/daily_metrics/`
- `data/output/card_vs_cash_share/`

## Incremental note (1 month only)
To add April without incremental load:
- process April file only
- write/overwrite April partition only in silver/gold outputs
- keep de-dup key stable for idempotency