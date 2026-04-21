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


def _render(template: str, active: str):
    return render_template(
        template,
        active=active,
        cities=TARGET_CITIES,
        selected_city=_city() or "ALL",
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


# ---------- JSON API ----------
# Convention: every query takes a :city param. If selected is None, we
# broaden the filter with `(:city IS NULL OR col = :city)`.

def _city_clause(col: str = "city") -> str:
    return f"(:city IS NULL OR {col} = :city)"


@bp.route("/api/overview")
def api_overview():
    c = _city()
    kpis = _fetch(
        f"""
        SELECT
          (SELECT count(*) FROM silver.establishments WHERE {_city_clause()}) AS establishments,
          (SELECT round(avg(score)::numeric, 2) FROM silver.inspections
              WHERE score IS NOT NULL AND {_city_clause()}) AS avg_score,
          (SELECT coalesce(sum(total_receipts), 0) FROM silver.mixed_beverage
              WHERE {_city_clause()}) AS total_receipts,
          (SELECT count(*) FROM silver.inspections WHERE {_city_clause()}) AS inspections
        """,
        city=c,
    )
    top_zips = _fetch(
        f"""
        SELECT city, zip, sum(total_receipts) AS receipts
        FROM gold.revenue_by_zip_month WHERE {_city_clause()}
        GROUP BY city, zip ORDER BY receipts DESC LIMIT 10
        """,
        city=c,
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
    return jsonify(kpis=kpis[0] if kpis else {}, top_zips=top_zips, bottom_zips=bottom_zips)


@bp.route("/api/revenue")
def api_revenue():
    c = _city()
    monthly = _fetch(
        f"""
        SELECT month, city, sum(total_receipts) AS total
        FROM gold.revenue_by_zip_month WHERE {_city_clause()}
        GROUP BY month, city ORDER BY month
        """,
        city=c,
    )
    by_zip = _fetch(
        f"""
        SELECT city, zip, sum(total_receipts) AS total
        FROM gold.revenue_by_zip_month WHERE {_city_clause()}
        GROUP BY city, zip ORDER BY total DESC LIMIT 15
        """,
        city=c,
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
        SELECT city, canonical_name, zip, avg_score, avg_monthly_receipts
        FROM gold.score_revenue_correlation
        WHERE avg_score IS NOT NULL AND avg_monthly_receipts IS NOT NULL
          AND {_city_clause()}
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
