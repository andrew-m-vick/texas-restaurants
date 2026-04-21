# Houston Restaurant Industry Analytics

ETL pipeline + dashboard joining Texas mixed beverage gross receipts (Houston-filtered) with City of Houston food service inspections and violations. Medallion architecture (bronze/silver/gold) in PostgreSQL, orchestrated by Airflow, surfaced through a Flask + Chart.js + Leaflet dashboard.

## Quickstart

```bash
# 1. Postgres
docker compose up -d

# 2. Python env
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in dataset IDs

# 3. Initial ingest (bronze -> silver -> gold)
python -m pipeline.bronze.mixed_beverage
python -m pipeline.bronze.inspections
python -m pipeline.silver.clean_mixed_beverage
python -m pipeline.silver.clean_inspections
python -m pipeline.silver.match_establishments
python -m pipeline.gold.aggregates

# 4. Dashboard
python run.py
```

## Architecture

- **Bronze** — raw API dumps, schema-on-read
- **Silver** — cleaned, typed, deduped; fuzzy-matched `establishments` joins mixed beverage ↔ inspections on name + address
- **Gold** — pre-aggregated revenue trends, inspection distributions, correlation metrics, neighborhood rollups

## Airflow DAGs

| DAG | Schedule |
| --- | --- |
| `ingest_mixed_beverage` | monthly |
| `ingest_inspections` | weekly |
| `build_silver_layer` | after ingest |
| `build_gold_layer` | after silver |

## Dashboard tabs

Overview · Revenue Trends · Inspection Analysis · Correlation · Map · Pipeline Status
