from flask import Blueprint, render_template, jsonify, request
from sqlalchemy import text
from pipeline.db import engine
from pipeline.config import TARGET_CITIES

bp = Blueprint("main", __name__)


def _fetch(sql: str, **params):
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(sql), params).mappings()]


def _city() -> str | None:
    """Return selected city (uppercase) or None for 'all'."""
    c = (request.args.get("city") or "").strip().upper()
    return c if c in TARGET_CITIES else None


WINDOWS = {
    "12m": ("Last 12 months", "11 months"),
    "3y":  ("Last 3 years",   "35 months"),
    "5y":  ("Last 5 years",   "59 months"),
    "all": ("All time (2007\u2013present)", None),
}


def _window() -> str:
    w = (request.args.get("window") or "").strip().lower()
    return w if w in WINDOWS else "12m"


def _window_params():
    w = _window()
    _, interval = WINDOWS[w]
    if interval is None:
        return "", {}
    return "AND month >= (SELECT max(month) - interval :iv FROM gold.revenue_by_zip_month)", {"iv": interval}


def _render(template: str, active: str):
    return render_template(
        template,
        active=active,
        cities=TARGET_CITIES,
        selected_city=_city() or "ALL",
        windows=WINDOWS,
        selected_window=_window(),
    )


# ---------- pages ----------
@bp.route("/")
def overview():
    return _render("overview.html", "overview")


@bp.route("/revenue")
def revenue():
    return _render("revenue.html", "revenue")


@bp.route("/inspections")
def inspections():
    return _render("inspections.html", "inspections")


@bp.route("/correlation")
def correlation():
    return _render("correlation.html", "correlation")


@bp.route("/map")
def map_view():
    return _render("map.html", "map")


@bp.route("/ops")
def ops():
    return _render("ops.html", "ops")


@bp.route("/establishment/<int:est_id>")
def establishment(est_id: int):
    return _render("establishment.html", "")


# ---------- JSON API ----------
# Convention: every query takes a :city param. If selected is None, we
# broaden the filter with `(:city IS NULL OR col = :city)`.

def _city_clause(col: str = "city") -> str:
    return f"(:city IS NULL OR {col} = :city)"


@bp.route("/api/overview")
def api_overview():
    c = _city()
    w_sql, w_params = _window_params()
    kpis = _fetch(
        f"""
        SELECT
          (SELECT count(*) FROM silver.establishments WHERE {_city_clause()}) AS establishments,
          (SELECT round(avg(score)::numeric, 2) FROM silver.inspections
              WHERE score IS NOT NULL AND {_city_clause()}) AS avg_score,
          (SELECT coalesce(sum(total_receipts), 0) FROM gold.revenue_by_zip_month
              WHERE {_city_clause()} {w_sql}) AS total_receipts,
          (SELECT count(*) FROM silver.inspections WHERE {_city_clause()}) AS inspections
        """,
        city=c, **w_params,
    )
    # Per-city KPIs for side-by-side view when no city filter is active.
    by_city = _fetch(
        """
        SELECT city,
          (SELECT count(*) FROM silver.establishments e WHERE e.city = c.city) AS establishments,
          (SELECT round(avg(score)::numeric, 2) FROM silver.inspections i
             WHERE i.city = c.city AND score IS NOT NULL) AS avg_score,
          (SELECT coalesce(sum(total_receipts), 0) FROM silver.mixed_beverage m
             WHERE m.city = c.city) AS total_receipts,
          (SELECT count(*) FROM silver.inspections i WHERE i.city = c.city) AS inspections
        FROM (SELECT DISTINCT city FROM silver.establishments) c
        ORDER BY city
        """,
    )
    top_zips = _fetch(
        f"""
        SELECT city, zip, sum(total_receipts) AS receipts
        FROM gold.revenue_by_zip_month WHERE {_city_clause()} {w_sql}
        GROUP BY city, zip ORDER BY receipts DESC LIMIT 10
        """,
        city=c, **w_params,
    )
    bottom_zips = _fetch(
        f"""
        SELECT city, zip, avg_score
        FROM gold.neighborhood_heat
        WHERE avg_score > 0 AND {_city_clause()}
        ORDER BY avg_score ASC LIMIT 10
        """,
        city=c,
    )
    return jsonify(kpis=kpis[0] if kpis else {}, by_city=by_city,
                   top_zips=top_zips, bottom_zips=bottom_zips)


@bp.route("/api/revenue")
def api_revenue():
    c = _city()
    w_sql, w_params = _window_params()
    monthly = _fetch(
        f"""
        SELECT month, city, sum(total_receipts) AS total
        FROM gold.revenue_by_zip_month WHERE {_city_clause()} {w_sql}
        GROUP BY month, city ORDER BY month
        """,
        city=c, **w_params,
    )
    by_zip = _fetch(
        f"""
        SELECT city, zip, sum(total_receipts) AS total
        FROM gold.revenue_by_zip_month WHERE {_city_clause()} {w_sql}
        GROUP BY city, zip ORDER BY total DESC LIMIT 15
        """,
        city=c, **w_params,
    )
    return jsonify(monthly=monthly, by_zip=by_zip)


@bp.route("/api/inspections")
def api_inspections():
    c = _city()
    dist = _fetch(
        f"""
        SELECT score_bucket, sum(inspections) AS inspections, avg(pct)::numeric(5,2) AS pct
        FROM gold.inspection_score_distribution WHERE {_city_clause()}
        GROUP BY score_bucket ORDER BY score_bucket
        """,
        city=c,
    )
    repeat = _fetch(
        f"""
        SELECT city, canonical_name, zip, inspection_count, low_score_count, avg_score, min_score
        FROM gold.repeat_offenders WHERE {_city_clause()}
        ORDER BY low_score_count DESC, avg_score ASC LIMIT 25
        """,
        city=c,
    )
    top_viol = _fetch(
        f"""
        SELECT city, description, occurrences, distinct_establishments
        FROM gold.top_violations WHERE {_city_clause()}
        ORDER BY occurrences DESC LIMIT 15
        """,
        city=c,
    )
    return jsonify(distribution=dist, repeat_offenders=repeat, top_violations=top_viol)


@bp.route("/api/correlation")
def api_correlation():
    c = _city()
    rows = _fetch(
        f"""
        SELECT c.city, c.canonical_name, c.zip, c.avg_score, c.avg_monthly_receipts,
               e.match_score
        FROM gold.score_revenue_correlation c
        JOIN silver.establishments e ON e.id = c.establishment_id
        WHERE c.avg_score IS NOT NULL AND c.avg_monthly_receipts IS NOT NULL
          AND {_city_clause("c.city")}
        """,
        city=c,
    )
    return jsonify(points=rows)


@bp.route("/api/map")
def api_map():
    c = _city()
    rows = _fetch(
        f"""
        SELECT city, zip, establishments, avg_score, total_receipts, latitude, longitude
        FROM gold.neighborhood_heat
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND {_city_clause()}
        """,
        city=c,
    )
    return jsonify(zips=rows)


@bp.route("/api/search")
def api_search():
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify(results=[])
    like = f"%{q.upper()}%"
    rows = _fetch(
        """
        SELECT id, city, canonical_name, canonical_address, zip, match_method
        FROM silver.establishments
        WHERE upper(canonical_name) LIKE :q
           OR upper(canonical_address) LIKE :q
        ORDER BY
          CASE match_method
            WHEN 'fuzzy_zip_block' THEN 1
            WHEN 'mb_only' THEN 2
            ELSE 3
          END,
          canonical_name
        LIMIT 15
        """,
        q=like,
    )
    return jsonify(results=rows)


@bp.route("/api/establishment/<int:est_id>")
def api_establishment(est_id: int):
    header = _fetch(
        """
        SELECT id, city, canonical_name, canonical_address, zip,
               mb_taxpayer_number, mb_location_number, facility_ids,
               match_score, match_method, latitude, longitude
        FROM silver.establishments WHERE id = :id
        """,
        id=est_id,
    )
    if not header:
        return jsonify(error="not found"), 404
    h = header[0]

    inspections = _fetch(
        """
        SELECT inspection_date, score, inspection_type
        FROM silver.inspections
        WHERE city = :city AND facility_id = ANY(:fids)
        ORDER BY inspection_date
        """,
        city=h["city"], fids=h["facility_ids"] or [],
    )
    revenue = _fetch(
        """
        SELECT obligation_end_date AS month, total_receipts, liquor_receipts,
               wine_receipts, beer_receipts
        FROM silver.mixed_beverage
        WHERE city = :city AND taxpayer_number = :tp AND location_number = :ln
        ORDER BY obligation_end_date
        """,
        city=h["city"], tp=h["mb_taxpayer_number"], ln=h["mb_location_number"],
    )
    violations = _fetch(
        """
        SELECT inspection_date, description, points, memo
        FROM silver.violations
        WHERE city = :city AND facility_id = ANY(:fids)
        ORDER BY inspection_date DESC, points DESC
        LIMIT 100
        """,
        city=h["city"], fids=h["facility_ids"] or [],
    )
    return jsonify(header=h, inspections=inspections, revenue=revenue, violations=violations)


@bp.route("/api/zip/<zip_code>")
def api_zip(zip_code: str):
    c = _city()
    establishments = _fetch(
        f"""
        SELECT e.id, e.canonical_name, e.canonical_address, e.city, e.match_method,
               e.match_score,
               (SELECT avg(score)::numeric(5,2) FROM silver.inspections i
                  WHERE i.city = e.city AND i.facility_id = ANY(e.facility_ids)) AS avg_score,
               (SELECT avg(mb.total_receipts)::numeric(14,2) FROM silver.mixed_beverage mb
                  WHERE mb.city = e.city
                    AND mb.taxpayer_number = e.mb_taxpayer_number
                    AND mb.location_number = e.mb_location_number) AS avg_monthly_receipts
        FROM silver.establishments e
        WHERE e.zip = :zip AND {_city_clause("e.city")}
        ORDER BY avg_monthly_receipts DESC NULLS LAST, e.canonical_name
        LIMIT 50
        """,
        zip=zip_code, city=c,
    )
    return jsonify(zip=zip_code, establishments=establishments)


@bp.route("/api/ops")
def api_ops():
    runs = _fetch("""
        SELECT dag_id, layer, started_at, finished_at, status, rows_written, notes
        FROM ops.pipeline_runs
        ORDER BY started_at DESC LIMIT 25
    """)
    counts = _fetch("""
        SELECT 'bronze.mixed_beverage' AS tbl, count(*) AS n FROM bronze.mixed_beverage
        UNION ALL SELECT 'bronze.inspections', count(*) FROM bronze.inspections
        UNION ALL SELECT 'bronze.dallas_violations', count(*) FROM bronze.dallas_violations
        UNION ALL SELECT 'silver.mixed_beverage', count(*) FROM silver.mixed_beverage
        UNION ALL SELECT 'silver.inspections', count(*) FROM silver.inspections
        UNION ALL SELECT 'silver.violations', count(*) FROM silver.violations
        UNION ALL SELECT 'silver.establishments', count(*) FROM silver.establishments
        UNION ALL SELECT 'gold.revenue_by_zip_month', count(*) FROM gold.revenue_by_zip_month
        UNION ALL SELECT 'gold.top_violations', count(*) FROM gold.top_violations
        UNION ALL SELECT 'gold.score_revenue_correlation', count(*) FROM gold.score_revenue_correlation
    """)
    return jsonify(runs=runs, counts=counts)
