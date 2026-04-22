# Austin Restaurant Industry Analytics

End-to-end data pipeline and analytics dashboard joining three public Texas datasets that nobody joins: **state mixed beverage gross receipts** (who sells liquor, and how much), **City of Austin food service inspections** (how clean those kitchens actually are), and **TABC license records** (when each bar/restaurant first got permitted and whether it's still active).

Built around a medallion (bronze → silver → gold) warehouse in PostgreSQL, orchestrated by Airflow (with a GitHub Actions mirror for hosted refresh), served through a Flask + Chart.js + Leaflet dashboard.

> **Serving model:** Postgres does the heavy lifting at *build time* — every query that backs a dashboard view is materialized into static JSON by the monthly ETL and committed alongside the code. Production only needs to serve Flask + static files; no hosted database. See [Serving architecture](#serving-architecture) below.

**Live:** https://texas-restaurants-production.up.railway.app/ *(first load takes ~20s — the server sleeps when idle to keep hosting costs near zero)*

---

## Headline finding

**Inspection scores do not meaningfully predict bar revenue in Austin.**

Across **1,177 matched establishments**:

- **Pearson *r* = 0.053** (raw), **0.027** (on log-transformed revenue)
- Regression slope: +$718 per inspection-score point — a rounding error against the $30k–$1M/month revenue range

Restaurants that score poorly on health inspections are not punished by their customers in revenue terms. Top earners span the full score range from 65 to 100.

This is a genuine null result, and I think that's more interesting than a contrived correlation. Inspection scores are a compliance signal, not a quality signal visible to diners — and the data shows that cleanly.

## Stack

| Layer | Tools |
| --- | --- |
| Ingestion | Python, `requests`, Socrata Open Data APIs |
| Storage | PostgreSQL 16 (schemas: `bronze`, `silver`, `gold`, `ops`) |
| Transform | `pandas`, `rapidfuzz`, SQL |
| Geocoding | `pgeocode` (offline US postal-code centroids) |
| Orchestration | Apache Airflow (+ GitHub Actions cron for hosted refresh) |
| Serving | Flask (static-only — no DB at runtime) |
| Frontend | Chart.js, Leaflet, vanilla JS, PWA (manifest + service worker) |
| Infra | Docker Compose (Postgres, local) · GitHub Actions (ETL) · Railway (Flask hosting) |

## Data sources

- [Texas Open Data Portal — Mixed Beverage Gross Receipts](https://data.texas.gov/dataset/Mixed-Beverage-Gross-Receipts/naix-2893) — statewide, monthly; filtered to Austin
- [City of Austin — Food Establishment Inspection Scores](https://data.austintexas.gov/Health-and-Community-Services/Food-Establishment-Inspection-Scores/ecmv-9xxi) — rolling 3-year window, live
- [Texas Open Data Portal — TABC License Information](https://data.texas.gov/resource/7hf9-qc9f) — statewide, refreshed continuously; filtered to Austin Retail tier

## Architecture

```
     ┌─────────────────────────────┐        ┌──────────────────────┐
     │ data.texas.gov              │        │ data.austintexas.gov │
     │ Mixed Beverage Receipts     │        │ Inspection Scores    │
     └──────────────┬──────────────┘        └──────────┬───────────┘
                    │                                  │
                    ▼                                  ▼
         ┌─────────────────────┐           ┌─────────────────────┐
         │ ingest_mixed_bev    │           │ ingest_inspections  │
         │ (Airflow, monthly)  │           │ (Airflow, weekly)   │
         └──────────┬──────────┘           └──────────┬──────────┘
                    │                                 │
                    └─────────────┬───────────────────┘
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
                     │  export_static_json    │
                     │  (materializes every   │
                     │   view as JSON)        │
                     └───────────┬────────────┘
                                 │  git commit + push
                                 ▼
                     ┌────────────────────────┐
                     │  Flask + Chart.js      │
                     │  + Leaflet dashboard   │
                     │  (no DB at runtime)    │
                     └────────────────────────┘
```

### Serving architecture

Most dashboards pay to keep Postgres running 24/7 just to answer the same handful of aggregate queries over and over. For a monthly-refreshed dataset, that's backwards — the data only changes 12 times a year, so the serving-time queries are pure waste.

Instead, the final pipeline step is [`pipeline/export/static_json.py`](pipeline/export/static_json.py), which runs every query that backs a dashboard view and writes the result to `app/static/data/*.json` — one file per `(endpoint, time-window)` variant. GitHub Actions commits the refreshed JSON back to `main`, Railway redeploys Flask, and the browser fetches directly from `/static/data/...`. Postgres never serves a request in production.

What this means practically:

- **$0 database hosting** — the only monthly compute cost is the ~5 minutes of GitHub Actions runtime that rebuilds the warehouse from scratch.
- **Sub-millisecond response times** — every dashboard query is a static file.
- **Full warehouse still intact**: all the SQL in [`pipeline/gold/aggregates.py`](pipeline/gold/aggregates.py) and [`pipeline/export/static_json.py`](pipeline/export/static_json.py) (window functions, CTEs, `FILTER` clauses, fuzzy-match array joins, correlated subqueries for per-establishment stats) runs against real Postgres every refresh — it's just that the *result* gets shipped instead of the DB.
- **Tradeoff**: data is only as fresh as the last commit. For a dataset whose source itself updates monthly, this is a non-issue.

Because every response is a static file, turning the site into a PWA is essentially free. A [manifest](app/static/manifest.webmanifest) + [service worker](app/static/sw.js) precache the shell on install and serve pages and JSON stale-while-revalidate. The site is installable on desktop and iOS/Android, and once loaded works fully offline.

### Why medallion?

- **Bronze** is append-on-truncate raw. If a field name changes upstream, nothing downstream breaks — I re-ingest, inspect the JSONB, and patch silver.
- **Silver** is where the engineering lives: typing, deduping, and the **fuzzy join** across the two datasets.
- **Gold** is pre-computed aggregates shaped for specific dashboard queries. The dashboard never reads silver directly (except for windowed recomputes).

### The fuzzy match (the interview-worthy bit)

The two sources don't share keys. A single establishment shows up in mixed-beverage receipts as `"MAIKO L.P. / MAIKO JAPANESE RESTAURANT"` and in inspections as `"Maiko Sushi Lounge"`. Matching them is where the value is created.

Approach in [`pipeline/silver/match_establishments.py`](pipeline/silver/match_establishments.py):

1. **Normalize** names (strip business suffixes like LLC/INC, punctuation, casing) and addresses (abbreviate street/directional suffixes) into comparable keys. See [`pipeline/silver/keys.py`](pipeline/silver/keys.py).
2. **Collapse inspections to distinct facilities** (one row per `facility_id`).
3. **Block by ZIP** — restaurants in different ZIPs are never the same establishment, dropping candidate pairs from O(n·m) to O(Σ nᵢ·mᵢ) over ZIP buckets.
4. Within each block, score `(name, address)` pairs with `rapidfuzz.token_set_ratio` under a weighted composite (`0.6·name + 0.4·address`).
5. **Greedy assignment with removal**: for each mixed-beverage row pick the best inspection candidate above threshold 78, then remove that candidate so it can't be double-matched.
6. Unmatched rows are still written — with `match_method = 'mb_only'` or `'inspection_only'` — so reports don't silently drop data. Match provenance (`match_score`, `match_method`) is persisted so confidence filters are trivial at the gold layer (and exposed as a slider on the Correlation tab).

**Match quality across 8,630 establishments:**

| Bucket | Count | Avg match score |
| --- | --- | --- |
| Fuzzy-matched | 1,177 | 96.2 |
| MB-only (no inspection pair found) | 2,121 | 58.2 |
| Inspection-only (no MB liquor permit) | 5,332 | — |

TABC licenses are attached to the resulting establishments in a second ZIP-blocked fuzzy pass ([`pipeline/silver/match_licenses.py`](pipeline/silver/match_licenses.py)) using the same scoring. Matches are written to `silver.establishment_licenses` rather than stuffing an array into `silver.establishments`, so the matcher stays narrow and a single establishment can carry multiple license rows (common for businesses that hold both on-premise and off-premise permits).

## Dashboard

Six views plus per-establishment detail:

| Tab / Page | What's there |
| --- | --- |
| Overview | Headline KPIs (window-scoped) + top/bottom ZIPs |
| Revenue | Monthly time series, revenue by ZIP |
| Inspections | Score distribution (A/B/C/D), repeat low-score establishments (sortable) |
| Correlation | Score-vs-revenue log-y scatter with regression line + Pearson *r* (with **match-confidence slider** to filter by provenance), plus **tenure-vs-score** scatter from TABC licenses, permit-status distribution, and ZIP-level permit churn |
| Map | Leaflet ZIP-level circle map with side panel — click any ZIP to list its establishments and drill into individual records |
| Browse | Sortable/filterable table of all 8,630 establishments |
| Pipeline | Live OPS tab: row counts by layer, recent DAG runs with status |
| `/establishment/<id>` | Per-restaurant detail: inspection history line chart, stacked monthly receipts (liquor/wine/beer), TABC license table with derived tenure |

Global header also has a **restaurant search** (2+ character typeahead) and a **time-window filter** (12 months / 3 years / 5 years / all time).

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
python -m pipeline.bronze.tabc_licenses
python -m pipeline.silver.clean_mixed_beverage
python -m pipeline.silver.clean_inspections
python -m pipeline.silver.clean_licenses
python -m pipeline.silver.match_establishments
python -m pipeline.silver.match_licenses
python -m pipeline.gold.aggregates
python -m pipeline.export.static_json   # bakes /static/data/*.json

# 4. Serve the dashboard
python run.py   # http://localhost:5000
```

### Optional: Socrata app token

Both TX and Austin portals throttle anonymous requests. Register a free token at [dev.socrata.com](https://dev.socrata.com/register) and set `SOCRATA_APP_TOKEN` in `.env` — ingests go noticeably faster.

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
| `ingest_tabc_licenses` | `@monthly` | → `build_silver_layer` |
| `build_silver_layer` | manual/trigger | → `build_gold_layer` |
| `build_gold_layer` | manual/trigger | `build_aggregates` → `export_static_json` |

For the hosted Railway deployment, a GitHub Actions workflow at [`.github/workflows/refresh-data.yml`](.github/workflows/refresh-data.yml) runs the same pipeline on a monthly cron.

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
│   ├── bronze/           # Raw ingest scripts
│   ├── silver/           # Clean + fuzzy match
│   ├── gold/             # Analytics aggregates
│   ├── config.py, db.py, ops.py, socrata.py
├── airflow/dags/         # DAG definitions
├── .github/workflows/    # Hosted refresh cron
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
