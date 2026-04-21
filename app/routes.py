from flask import Blueprint, render_template, jsonify
from sqlalchemy import text
from pipeline.db import engine

bp = Blueprint("main", __name__)


def _fetch(sql: str, **params):
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(sql), params).mappings()]


# ---------- pages ----------
@bp.route("/")
def overview():
    return render_template("overview.html", active="overview")


@bp.route("/revenue")
def revenue():
    return render_template("revenue.html", active="revenue")


@bp.route("/inspections")
def inspections():
    return render_template("inspections.html", active="inspections")


@bp.route("/correlation")
def correlation():
    return render_template("correlation.html", active="correlation")


@bp.route("/map")
def map_view():
    return render_template("map.html", active="map")


@bp.route("/ops")
def ops():
    return render_template("ops.html", active="ops")


# ---------- JSON API ----------
@bp.route("/api/overview")
def api_overview():
    kpis = _fetch("""
        SELECT
          (SELECT count(*) FROM silver.establishments) AS establishments,
          (SELECT round(avg(score)::numeric, 2) FROM silver.inspections WHERE score IS NOT NULL) AS avg_score,
          (SELECT coalesce(sum(total_receipts), 0) FROM silver.mixed_beverage) AS total_receipts,
          (SELECT count(*) FROM silver.inspections) AS inspections
    """)
    top_zips = _fetch("""
        SELECT zip, sum(total_receipts) AS receipts
        FROM gold.revenue_by_zip_month
        GROUP BY zip ORDER BY receipts DESC LIMIT 10
    """)
    bottom_zips = _fetch("""
        SELECT zip, avg_score
        FROM gold.neighborhood_heat
        WHERE avg_score > 0
        ORDER BY avg_score ASC LIMIT 10
    """)
    return jsonify(kpis=kpis[0] if kpis else {}, top_zips=top_zips, bottom_zips=bottom_zips)


@bp.route("/api/revenue")
def api_revenue():
    monthly = _fetch("""
        SELECT month, sum(total_receipts) AS total
        FROM gold.revenue_by_zip_month
        GROUP BY month ORDER BY month
    """)
    by_zip = _fetch("""
        SELECT zip, sum(total_receipts) AS total
        FROM gold.revenue_by_zip_month
        GROUP BY zip ORDER BY total DESC LIMIT 15
    """)
    return jsonify(monthly=monthly, by_zip=by_zip)


@bp.route("/api/inspections")
def api_inspections():
    dist = _fetch("SELECT * FROM gold.inspection_score_distribution ORDER BY score_bucket")
    top_violations = _fetch("""
        SELECT violation_code, violation_description, occurrences
        FROM gold.top_violations ORDER BY occurrences DESC LIMIT 15
    """)
    repeat = _fetch("""
        SELECT canonical_name, zip, violation_count, avg_score
        FROM gold.repeat_offenders ORDER BY violation_count DESC LIMIT 25
    """)
    return jsonify(distribution=dist, top_violations=top_violations, repeat_offenders=repeat)


@bp.route("/api/correlation")
def api_correlation():
    rows = _fetch("""
        SELECT canonical_name, zip, avg_score, avg_monthly_receipts
        FROM gold.score_revenue_correlation
        WHERE avg_score IS NOT NULL AND avg_monthly_receipts IS NOT NULL
    """)
    return jsonify(points=rows)


@bp.route("/api/map")
def api_map():
    rows = _fetch("""
        SELECT zip, establishments, avg_score, total_receipts, latitude, longitude
        FROM gold.neighborhood_heat
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """)
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
        UNION ALL SELECT 'bronze.violations', count(*) FROM bronze.violations
        UNION ALL SELECT 'silver.mixed_beverage', count(*) FROM silver.mixed_beverage
        UNION ALL SELECT 'silver.inspections', count(*) FROM silver.inspections
        UNION ALL SELECT 'silver.violations', count(*) FROM silver.violations
        UNION ALL SELECT 'silver.establishments', count(*) FROM silver.establishments
        UNION ALL SELECT 'gold.revenue_by_zip_month', count(*) FROM gold.revenue_by_zip_month
        UNION ALL SELECT 'gold.score_revenue_correlation', count(*) FROM gold.score_revenue_correlation
    """)
    return jsonify(runs=runs, counts=counts)
