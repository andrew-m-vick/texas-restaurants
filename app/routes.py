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


def _window_predicate(col: str) -> tuple[str, dict]:
    """Returns ('AND <col> >= cutoff', params) or ('', {}) for 'all' window."""
    w = _window()
    _, interval = WINDOWS[w]
    if interval is None:
        return "", {}
    return f"AND {col} >= (current_date - interval :win_iv)", {"win_iv": interval}


def _window_params():
    """Back-compat shim: window predicate on gold.revenue_by_zip_month.month."""
    return _window_predicate("month")


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


@bp.route("/establishments")
def establishments():
    return _render("establishments.html", "establishments")


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
    w_mb_sql, w_params = _window_predicate("obligation_end_date")
    w_insp_sql, _ = _window_predicate("inspection_date")
    kpis = _fetch(
        f"""
        SELECT
          (SELECT count(*) FROM silver.establishments WHERE {_city_clause()}) AS establishments,
          (SELECT round(avg(score)::numeric, 2) FROM silver.inspections
              WHERE score IS NOT NULL AND {_city_clause()} {w_insp_sql}) AS avg_score,
          (SELECT coalesce(sum(total_receipts), 0) FROM silver.mixed_beverage
              WHERE {_city_clause()} {w_mb_sql}) AS total_receipts,
          (SELECT count(*) FROM silver.inspections
              WHERE {_city_clause()} {w_insp_sql}) AS inspections
        """,
        city=c, **w_params,
    )
    by_city = _fetch(
        f"""
        SELECT city,
          (SELECT count(*) FROM silver.establishments e WHERE e.city = c.city) AS establishments,
          (SELECT round(avg(score)::numeric, 2) FROM silver.inspections i
             WHERE i.city = c.city AND score IS NOT NULL {w_insp_sql}) AS avg_score,
          (SELECT coalesce(sum(total_receipts), 0) FROM silver.mixed_beverage m
             WHERE m.city = c.city {w_mb_sql}) AS total_receipts,
          (SELECT count(*) FROM silver.inspections i
             WHERE i.city = c.city {w_insp_sql}) AS inspections
        FROM (SELECT DISTINCT city FROM silver.establishments) c
        ORDER BY city
        """,
        **w_params,
    )
    w_month_sql, _ = _window_predicate("month")
    top_zips = _fetch(
        f"""
        SELECT city, zip, sum(total_receipts) AS receipts
        FROM gold.revenue_by_zip_month WHERE {_city_clause()} {w_month_sql}
        GROUP BY city, zip ORDER BY receipts DESC LIMIT 10
        """,
        city=c, **w_params,
    )
    bottom_zips = _fetch(
        f"""
        SELECT city, zip, avg(score)::numeric(5,2) AS avg_score
        FROM silver.inspections
        WHERE zip IS NOT NULL AND score IS NOT NULL
          AND {_city_clause()} {w_insp_sql}
        GROUP BY city, zip
        HAVING avg(score) > 0
        ORDER BY avg_score ASC LIMIT 10
        """,
        city=c, **w_params,
    )
    return jsonify(kpis=kpis[0] if kpis else {}, by_city=by_city,
                   top_zips=top_zips, bottom_zips=bottom_zips)


@bp.route("/api/revenue")
def api_revenue():
    c = _city()
    w_sql, w_params = _window_predicate("month")
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
    w_sql, w_params = _window_predicate("inspection_date")
    dist = _fetch(
        f"""
        SELECT
          CASE
            WHEN score >= 95 THEN 'A (95-100)'
            WHEN score >= 85 THEN 'B (85-94)'
            WHEN score >= 75 THEN 'C (75-84)'
            WHEN score >= 0  THEN 'D (<75)'
            ELSE 'Unscored'
          END AS score_bucket,
          count(*) AS inspections
        FROM silver.inspections
        WHERE {_city_clause()} {w_sql}
        GROUP BY score_bucket ORDER BY score_bucket
        """,
        city=c, **w_params,
    )
    repeat = _fetch(
        f"""
        SELECT
          e.id AS establishment_id,
          e.city, e.canonical_name, e.zip,
          count(i.id) AS inspection_count,
          count(*) FILTER (WHERE i.score < 85) AS low_score_count,
          avg(i.score)::numeric(5,2) AS avg_score,
          min(i.score)::numeric(5,2) AS min_score
        FROM silver.establishments e
        JOIN silver.inspections i
          ON i.city = e.city AND i.facility_id = ANY(e.facility_ids)
        WHERE {_city_clause("e.city")} {w_sql.replace("inspection_date", "i.inspection_date")}
        GROUP BY e.id, e.city, e.canonical_name, e.zip
        HAVING count(*) FILTER (WHERE i.score < 85) >= 2
        ORDER BY low_score_count DESC, avg_score ASC
        LIMIT 25
        """,
        city=c, **w_params,
    )
    top_viol = _fetch(
        f"""
        SELECT city, description, count(*) AS occurrences,
               count(DISTINCT facility_id) AS distinct_establishments
        FROM silver.violations
        WHERE {_city_clause()} {w_sql}
        GROUP BY city, description
        ORDER BY occurrences DESC LIMIT 15
        """,
        city=c, **w_params,
    )
    return jsonify(distribution=dist, repeat_offenders=repeat, top_violations=top_viol)


@bp.route("/api/correlation")
def api_correlation():
    c = _city()
    w_insp_sql, w_params = _window_predicate("i.inspection_date")
    w_mb_sql, _ = _window_predicate("mb.obligation_end_date")
    rows = _fetch(
        f"""
        WITH scores AS (
          SELECT e.id, avg(i.score) AS avg_score
          FROM silver.establishments e
          JOIN silver.inspections i
            ON i.city = e.city AND i.facility_id = ANY(e.facility_ids)
          WHERE TRUE {w_insp_sql}
          GROUP BY e.id
        ),
        revenue AS (
          SELECT e.id, avg(mb.total_receipts) AS avg_monthly_receipts
          FROM silver.establishments e
          JOIN silver.mixed_beverage mb
            ON mb.city = e.city
           AND mb.taxpayer_number = e.mb_taxpayer_number
           AND mb.location_number = e.mb_location_number
          WHERE TRUE {w_mb_sql}
          GROUP BY e.id
        )
        SELECT e.id AS establishment_id, e.city, e.canonical_name, e.zip,
               s.avg_score::numeric(5,2) AS avg_score,
               r.avg_monthly_receipts::numeric(14,2) AS avg_monthly_receipts,
               e.match_score
        FROM silver.establishments e
        JOIN scores s ON s.id = e.id
        JOIN revenue r ON r.id = e.id
        WHERE s.avg_score IS NOT NULL AND r.avg_monthly_receipts IS NOT NULL
          AND {_city_clause("e.city")}
        """,
        city=c, **w_params,
    )
    return jsonify(points=rows)


@bp.route("/api/map")
def api_map():
    c = _city()
    w_mb_sql, w_params = _window_predicate("mb.obligation_end_date")
    w_insp_sql, _ = _window_predicate("i.inspection_date")
    # Window-scoped aggregates from silver, joined to gold.neighborhood_heat
    # for the pgeocode-derived ZIP centroids.
    rows = _fetch(
        f"""
        WITH rev AS (
          SELECT mb.city, mb.location_zip AS zip,
                 count(DISTINCT (mb.taxpayer_number, mb.location_number)) AS establishments,
                 sum(mb.total_receipts) AS total_receipts
          FROM silver.mixed_beverage mb
          WHERE mb.location_zip IS NOT NULL {w_mb_sql}
          GROUP BY mb.city, mb.location_zip
        ),
        scr AS (
          SELECT i.city, i.zip, avg(i.score)::numeric(5,2) AS avg_score
          FROM silver.inspections i
          WHERE i.zip IS NOT NULL AND i.score IS NOT NULL {w_insp_sql}
          GROUP BY i.city, i.zip
        )
        SELECT COALESCE(rev.city, scr.city) AS city,
               COALESCE(rev.zip, scr.zip) AS zip,
               COALESCE(rev.establishments, 0) AS establishments,
               COALESCE(scr.avg_score, 0) AS avg_score,
               COALESCE(rev.total_receipts, 0) AS total_receipts,
               nh.latitude, nh.longitude
        FROM rev FULL OUTER JOIN scr USING (city, zip)
        JOIN gold.neighborhood_heat nh
          ON nh.city = COALESCE(rev.city, scr.city)
         AND nh.zip  = COALESCE(rev.zip, scr.zip)
        WHERE nh.latitude IS NOT NULL AND nh.longitude IS NOT NULL
          AND {_city_clause("COALESCE(rev.city, scr.city)")}
        """,
        city=c, **w_params,
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


@bp.route("/api/establishments")
def api_establishments():
    c = _city()
    q = (request.args.get("q") or "").strip()
    zip_ = (request.args.get("zip") or "").strip()
    match_method = (request.args.get("match") or "").strip()
    min_score = request.args.get("min_score", type=float)
    max_score = request.args.get("max_score", type=float)
    sort = (request.args.get("sort") or "name").lower()
    direction = "ASC" if request.args.get("dir", "asc").lower() == "asc" else "DESC"
    page = max(1, request.args.get("page", 1, type=int))
    per_page = 50

    # These refer to columns on the outer `base` CTE, not silver.establishments.
    allowed_sort = {
        "name": "canonical_name",
        "city": "city",
        "zip": "zip",
        "score": "avg_score",
        "inspections": "inspection_count",
        "revenue": "avg_monthly_receipts",
        "match": "match_score",
    }
    sort_col = allowed_sort.get(sort, "canonical_name")
    # Null-safe sorting: always push nulls to the end
    nulls = "NULLS LAST" if direction == "DESC" else "NULLS LAST"

    filters = [f"{_city_clause('e.city')}"]
    params = {"city": c}
    if q:
        filters.append("upper(e.canonical_name) LIKE :q")
        params["q"] = f"%{q.upper()}%"
    if zip_:
        filters.append("e.zip = :zip")
        params["zip"] = zip_
    if match_method:
        filters.append("e.match_method = :mm")
        params["mm"] = match_method

    score_filters = []
    if min_score is not None:
        score_filters.append("avg_score >= :min_score")
        params["min_score"] = min_score
    if max_score is not None:
        score_filters.append("avg_score <= :max_score")
        params["max_score"] = max_score

    where_sql = " AND ".join(filters) if filters else "TRUE"
    outer_where = ("WHERE " + " AND ".join(score_filters)) if score_filters else ""
    offset = (page - 1) * per_page

    w_insp_sql, w_params = _window_predicate("i.inspection_date")
    w_mb_sql, _ = _window_predicate("mb.obligation_end_date")
    params.update(w_params)

    rows = _fetch(
        f"""
        WITH base AS (
          SELECT
            e.id, e.canonical_name, e.canonical_address, e.city, e.zip,
            e.match_method, e.match_score,
            (SELECT count(*) FROM silver.inspections i
               WHERE i.city = e.city AND i.facility_id = ANY(e.facility_ids) {w_insp_sql}) AS inspection_count,
            (SELECT avg(score)::numeric(5,2) FROM silver.inspections i
               WHERE i.city = e.city AND i.facility_id = ANY(e.facility_ids) {w_insp_sql}) AS avg_score,
            (SELECT avg(mb.total_receipts)::numeric(14,2) FROM silver.mixed_beverage mb
               WHERE mb.city = e.city
                 AND mb.taxpayer_number = e.mb_taxpayer_number
                 AND mb.location_number = e.mb_location_number {w_mb_sql}) AS avg_monthly_receipts
          FROM silver.establishments e
          WHERE {where_sql}
        )
        SELECT * FROM base
        {outer_where}
        ORDER BY {sort_col} {direction} {nulls}, id
        LIMIT :limit OFFSET :offset
        """,
        limit=per_page, offset=offset, **params,
    )

    total = _fetch(
        f"""
        WITH base AS (
          SELECT e.id,
            (SELECT avg(score) FROM silver.inspections i
               WHERE i.city = e.city AND i.facility_id = ANY(e.facility_ids) {w_insp_sql}) AS avg_score
          FROM silver.establishments e
          WHERE {where_sql}
        )
        SELECT count(*) AS n FROM base {outer_where}
        """,
        **params,
    )
    return jsonify(
        rows=rows, total=total[0]["n"] if total else 0,
        page=page, per_page=per_page,
    )


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
