# Austin Restaurant Industry Analytics

End-to-end data pipeline and analytics dashboard for the Austin restaurant and bar scene. Joins two public datasets that nobody joins — **Texas mixed beverage gross receipts** (filtered to Austin) and the **City of Austin food establishment inspection scores** — to surface questions only an industry insider would ask:

- Does a clean kitchen correlate with higher bar revenue?
- Which ZIPs are booming, and which are sliding?
- Which establishments keep failing inspections *and* keep selling?

Built around a medallion (bronze / silver / gold) warehouse, orchestrated by Airflow, and served through a Flask + Chart.js + Leaflet dashboard.

---

## Stack

| Layer | Tools |
| --- | --- |
| Ingestion | Python, `requests`, Socrata Open Data APIs |
| Storage | PostgreSQL 16 (schemas: `bronze`, `silver`, `gold`, `ops`) |
| Transform | `pandas`, `rapidfuzz`, SQL |
| Geocoding | `pgeocode` (offline US postal-code centroids) |
| Orchestration | Apache Airflow |
| Serving | Flask, SQLAlchemy |
| Frontend | Chart.js, Leaflet, vanilla JS |
| Infra | Docker Compose (Postgres) |

## Data sources

- [Texas Open Data Portal — Mixed Beverage Gross Receipts](https://data.texas.gov/dataset/Mixed-Beverage-Gross-Receipts/naix-2893) (statewide, monthly, filtered to `location_city = 'AUSTIN'`)
- [City of Austin — Food Establishment Inspection Scores](https://data.austintexas.gov/Health-and-Community-Services/Food-Establishment-Inspection-Scores/ecmv-9xxi) (rolling 3-year window)

## Architecture

```
          ┌─────────────────────────────┐         ┌────────────────────────────┐
          │ data.texas.gov              │         │ data.austintexas.gov       │
          │ Mixed Beverage Receipts     │         │ Food Inspection Scores     │
          └──────────────┬──────────────┘         └──────────────┬─────────────┘
                         │                                       │
                         ▼                                       ▼
              ┌─────────────────────┐                 ┌──────────────────────┐
              │ ingest_mixed_bev    │                 │ ingest_inspections   │
              │ (Airflow, monthly)  │                 │ (Airflow, weekly)    │
              └──────────┬──────────┘                 └──────────┬───────────┘
                         └──────────────┬────────────────────────┘
                                        ▼
                            ┌────────────────────────┐
                            │  BRONZE (raw JSONB)    │
                            └───────────┬────────────┘
                                        ▼
                            ┌────────────────────────┐
                            │  SILVER (cleaned +     │
                            │  fuzzy-matched)        │
                            │  establishments table  │
                            └───────────┬────────────┘
                                        ▼
                            ┌────────────────────────┐
                            │  GOLD (aggregates +    │
                            │  ZIP centroids)        │
                            └───────────┬────────────┘
                                        ▼
                            ┌────────────────────────┐
                            │  Flask + Chart.js      │
                            │  + Leaflet dashboard   │
                            └────────────────────────┘
```

### Why medallion?

- **Bronze** is append-on-truncate raw. If a field name changes upstream, nothing downstream breaks — I re-ingest, inspect the JSONB, and patch silver.
- **Silver** is where the engineering lives: typing, deduping, and the **fuzzy join** across the two datasets.
- **Gold** is pre-computed aggregates shaped for specific dashboard queries. The dashboard never reads silver directly.

### The fuzzy match (the interview-worthy bit)

The two sources don't share keys. A single establishment shows up in mixed-beverage receipts as `"MAIKO L.P. / MAIKO JAPANESE RESTAURANT"` and in inspections as `"Maiko Sushi Lounge"`. Matching them is where the value is created.

Approach in [`pipeline/silver/match_establishments.py`](pipeline/silver/match_establishments.py):

1. **Normalize** names (strip business suffixes like LLC/INC, punctuation, casing) and addresses (abbreviate street/directional suffixes) into comparable keys. See [`pipeline/silver/keys.py`](pipeline/silver/keys.py).
2. **Collapse inspections to distinct facilities** (one row per `facility_id`) — Austin publishes repeated inspections per facility.
3. **Block by ZIP** — restaurants in different ZIPs are never the same establishment, dropping candidate pairs from O(n·m) to O(Σ nᵢ·mᵢ) over ZIP buckets.
4. Within each block, score `(name, address)` pairs with `rapidfuzz.token_set_ratio` under a weighted composite (`0.6·name + 0.4·address`).
5. **Greedy assignment with removal**: for each mixed-beverage row pick the best inspection candidate above threshold 78, then remove that candidate so it can't be double-matched.
6. Unmatched rows are still written — with `match_method = 'mb_only'` or `'inspection_only'` — so reports don't silently drop data. Match provenance (`match_score`, `match_method`) is persisted so confidence filters are trivial at the gold layer.

## Dashboard

Six tabs:

| Tab | What's there |
| --- | --- |
| Overview | Headline KPIs: establishments tracked, avg inspection score, total receipts, top/bottom ZIPs |
| Revenue | Monthly time series, revenue by ZIP |
| Inspections | Score distribution (A/B/C/D), repeat low-score establishments |
| Correlation | Inspection score vs. avg monthly receipts scatter with regression line + Pearson *r* |
| Map | Leaflet ZIP-level circle map (centroids from pgeocode), toggleable between revenue intensity and inspection score |
| Pipeline | Live OPS tab: row counts by layer, recent DAG runs with status |

## Quickstart

```bash
# 1. Postgres (auto-applies SQL schemas on first boot via /docker-entrypoint-initdb.d)
docker compose up -d

# 2. Python env
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# 3. Run the full pipeline once
python -m pipeline.bronze.mixed_beverage
python -m pipeline.bronze.inspections
python -m pipeline.silver.clean_mixed_beverage
python -m pipeline.silver.clean_inspections
python -m pipeline.silver.match_establishments
python -m pipeline.gold.aggregates

# 4. Serve the dashboard
python run.py   # http://localhost:5000
```

### Running with Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow standalone
```

| DAG | Schedule | Triggers |
| --- | --- | --- |
| `ingest_mixed_beverage` | `@monthly` | → `build_silver_layer` |
| `ingest_inspections` | `@weekly` | → `build_silver_layer` |
| `build_silver_layer` | manual/trigger | → `build_gold_layer` |
| `build_gold_layer` | manual/trigger | — |

## Project layout

```
texas-restaurants/
├── sql/                  # Schema definitions (applied by docker-compose on first boot)
├── pipeline/
│   ├── bronze/           # Raw ingest scripts
│   ├── silver/           # Clean + fuzzy match
│   ├── gold/             # Analytics aggregates
│   ├── config.py, db.py, ops.py, socrata.py
├── airflow/dags/         # DAG definitions
├── app/                  # Flask dashboard
│   ├── routes.py
│   ├── templates/
│   └── static/{css,js}/
├── docker-compose.yml
├── requirements.txt
└── run.py
```

## License

MIT.
