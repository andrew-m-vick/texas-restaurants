# Texas Restaurant Industry Analytics

End-to-end data pipeline and analytics dashboard joining two public Texas datasets that nobody joins: **state mixed beverage gross receipts** (who sells liquor, and how much) and **city food service inspections** (how clean those kitchens actually are). Covers **Austin** (live Socrata API) and **Dallas** (historical Socrata data, 2016–2024).

Built around a medallion (bronze → silver → gold) warehouse, orchestrated by Airflow, served through a Flask + Chart.js + Leaflet dashboard.

---

## Headline finding

**Inspection scores do not meaningfully predict bar revenue in Texas.**

Across **2,781 matched establishments** (1,159 in Austin + 1,622 in Dallas):

- **Pearson *r* = 0.053** (raw), **0.027** (on log-transformed revenue)
- Regression slope: +$718 per inspection-score point, which is a rounding error against the $30k–$1M/month revenue range
- Pattern holds in both cities independently

**Why this matters.** Restaurants that score poorly on health inspections are not punished by their customers in revenue terms. Top earners span the full score range from 65 to 100. The industry myth that "customers reward cleanliness" isn't visible at this level of data — at best the signal exists somewhere below what either dataset captures.

This is a genuine null result, and I think that's more interesting than a contrived correlation. Null results are honest; they also tell you something about the limits of the data (inspection scores are a compliance signal, not a quality signal visible to diners).

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

- [Texas Open Data Portal — Mixed Beverage Gross Receipts](https://data.texas.gov/dataset/Mixed-Beverage-Gross-Receipts/naix-2893) — statewide, monthly, filtered to target cities
- [City of Austin — Food Establishment Inspection Scores](https://data.austintexas.gov/Health-and-Community-Services/Food-Establishment-Inspection-Scores/ecmv-9xxi) — rolling 3-year window, **live**
- [Dallas OpenData — Restaurant and Food Establishment Inspections](https://www.dallasopendata.com/Services/Restaurant-and-Food-Establishment-Inspections-Octo/dri5-wcct) — 2016-10 to 2024-01, **sunset** (Dallas migrated to a vendor portal that doesn't expose an API)

## Architecture

```
          ┌─────────────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
          │ data.texas.gov              │   │ data.austintexas.gov │   │ dallasopendata.com   │
          │ Mixed Beverage Receipts     │   │ Inspection Scores    │   │ Inspections + Viol.  │
          └──────────────┬──────────────┘   └──────────┬───────────┘   └──────────┬───────────┘
                         │                             │                          │
                         ▼                             ▼                          ▼
              ┌─────────────────────┐         ┌─────────────────────┐   ┌─────────────────────┐
              │ ingest_mixed_bev    │         │ austin ingest       │   │ dallas ingest       │
              │ (Airflow, monthly)  │         │ (Airflow, weekly)   │   │ (historical refresh)│
              └──────────┬──────────┘         └──────────┬──────────┘   └──────────┬──────────┘
                         │                               │                         │
                         └─────────────────┬─────────────┴─────────────────────────┘
                                           ▼
                              ┌────────────────────────┐
                              │  BRONZE (raw JSONB)    │
                              └───────────┬────────────┘
                                          ▼
                              ┌────────────────────────┐
                              │  SILVER (cleaned +     │
                              │  fuzzy-matched + per-  │
                              │  city dimensioning)    │
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
2. **Collapse inspections to distinct facilities** (one row per `facility_id`) — both cities publish repeated inspections per facility.
3. **Block by (city, ZIP)** — restaurants in different blocks are never the same establishment, dropping candidate pairs from O(n·m) to O(Σ nᵢ·mᵢ) over (city, ZIP) buckets.
4. Within each block, score `(name, address)` pairs with `rapidfuzz.token_set_ratio` under a weighted composite (`0.6·name + 0.4·address`).
5. **Greedy assignment with removal**: for each mixed-beverage row pick the best inspection candidate above threshold 78, then remove that candidate so it can't be double-matched.
6. Unmatched rows are still written — with `match_method = 'mb_only'` or `'inspection_only'` — so reports don't silently drop data. Match provenance (`match_score`, `match_method`) is persisted so confidence filters are trivial at the gold layer (and exposed as a slider on the Correlation tab).

**Match quality across 20,898 establishments:**

| City | Fuzzy-matched | MB-only | Inspection-only | Confident-match avg score |
| --- | --- | --- | --- | --- |
| Austin | 1,177 | 2,121 | 5,332 | 96.2 |
| Dallas | 1,656 | 3,113 | 7,499 | 96.4 |

## Dashboard

Seven views:

| Tab / Page | What's there |
| --- | --- |
| Overview | Headline KPIs; when **City = All** shows Austin and Dallas side-by-side |
| Revenue | Monthly time series (per-city colored), revenue by ZIP |
| Inspections | Score distribution (A/B/C/D), top violations (Dallas only), repeat low-score establishments (sortable) |
| Correlation | Log-y scatter with regression line + Pearson *r*, **match-confidence slider** to filter by provenance |
| Map | Leaflet ZIP-level circle map with side panel — click any ZIP to list its establishments and drill into individual records |
| Pipeline | Live OPS tab: row counts by layer, recent DAG runs with status |
| `/establishment/<id>` | Per-restaurant detail: inspection history line chart, stacked monthly receipts (liquor/wine/beer), full violation list |

There's also a **global search** in the header (2+ character typeahead against 20,898 establishments), and a **city selector** persisted in the URL so pages are shareable.

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
python -m pipeline.bronze.austin_inspections
python -m pipeline.bronze.dallas_inspections
python -m pipeline.silver.clean_mixed_beverage
python -m pipeline.silver.clean_inspections
python -m pipeline.silver.clean_violations
python -m pipeline.silver.match_establishments
python -m pipeline.gold.aggregates

# 4. Serve the dashboard
python run.py   # http://localhost:5000
```

### Optional: Socrata app token

Both TX and Austin/Dallas data portals throttle anonymous requests. Register a free token at [dev.socrata.com](https://dev.socrata.com/register) and set `SOCRATA_APP_TOKEN` in `.env` — ingests go noticeably faster.

### Running with Airflow

```bash
pip install -r requirements-airflow.txt
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

## Tests

```bash
.venv/bin/python -m pytest tests/
```

Covers the fuzzy matcher normalization and scoring — the component that would be hardest to debug if it silently broke.

## Project layout

```
texas-restaurants/
├── sql/                  # Schema definitions (applied by docker-compose on first boot)
├── pipeline/
│   ├── bronze/           # Raw ingest scripts (mixed beverage, austin, dallas)
│   ├── silver/           # Clean + fuzzy match
│   ├── gold/             # Analytics aggregates
│   ├── config.py, db.py, ops.py, socrata.py
├── airflow/dags/         # DAG definitions
├── app/                  # Flask dashboard
│   ├── routes.py
│   ├── templates/
│   └── static/{css,js}/
├── tests/
├── docker-compose.yml
├── requirements.txt
└── run.py
```

## License

MIT.
