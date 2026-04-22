"""Export every dashboard API payload to static JSON under app/static/data/.

Railway hosting only needs to serve Flask + these files — Postgres stops
being a production dependency. One file per (endpoint, window) variant,
plus one file per establishment detail page.
"""
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import text

from ..db import engine
from ..ops import track_run

OUT_DIR = Path(__file__).resolve().parents[2] / "app" / "static" / "data"

WINDOWS = {
    "12m": "11 months",
    "3y":  "35 months",
    "5y":  "59 months",
    "all": None,
}


def _json_default(o):
    if isinstance(o, (Decimal,)):
        return float(o)
    if isinstance(o, (date, datetime)):
        return o.isoformat()
    raise TypeError(f"not serializable: {type(o)}")


def _write(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, default=_json_default, separators=(",", ":")))


def _rows(conn, sql: str, **params):
    return [dict(r) for r in conn.execute(text(sql), params).mappings()]


def _window_predicate(col: str, window: str) -> tuple[str, dict]:
    interval = WINDOWS[window]
    if interval is None:
        return "", {}
    return f"AND {col} >= (current_date - interval :win_iv)", {"win_iv": interval}


# ---------- endpoint builders ----------
def build_overview(conn, window: str) -> dict:
    w_mb, p = _window_predicate("obligation_end_date", window)
    w_insp, _ = _window_predicate("inspection_date", window)
    w_month, _ = _window_predicate("month", window)

    est_in_window = f"""
        e.id IN (
          SELECT DISTINCT e2.id FROM silver.establishments e2
          LEFT JOIN silver.inspections i
            ON i.city = e2.city AND i.facility_id = ANY(e2.facility_ids)
                {w_insp}
          LEFT JOIN silver.mixed_beverage mb
            ON mb.city = e2.city
           AND mb.taxpayer_number = e2.mb_taxpayer_number
           AND mb.location_number = e2.mb_location_number
                {w_mb}
          WHERE i.id IS NOT NULL OR mb.id IS NOT NULL
        )
    """

    kpis = _rows(conn, f"""
        SELECT
          (SELECT count(*) FROM silver.establishments e WHERE {est_in_window}) AS establishments,
          (SELECT round(avg(score)::numeric, 2) FROM silver.inspections
              WHERE score IS NOT NULL {w_insp}) AS avg_score,
          (SELECT coalesce(sum(total_receipts), 0) FROM silver.mixed_beverage
              WHERE TRUE {w_mb}) AS total_receipts,
          (SELECT count(*) FROM silver.inspections WHERE TRUE {w_insp}) AS inspections
    """, **p)

    by_city = _rows(conn, f"""
        SELECT c.city,
          (SELECT count(*) FROM silver.establishments e
             WHERE e.city = c.city AND {est_in_window}) AS establishments,
          (SELECT round(avg(score)::numeric, 2) FROM silver.inspections i
             WHERE i.city = c.city AND score IS NOT NULL {w_insp}) AS avg_score,
          (SELECT coalesce(sum(total_receipts), 0) FROM silver.mixed_beverage m
             WHERE m.city = c.city {w_mb}) AS total_receipts,
          (SELECT count(*) FROM silver.inspections i
             WHERE i.city = c.city {w_insp}) AS inspections
        FROM (SELECT DISTINCT city FROM silver.establishments) c
        ORDER BY c.city
    """, **p)

    top_zips = _rows(conn, f"""
        WITH ranked AS (
          SELECT city, zip, sum(total_receipts) AS receipts,
            row_number() OVER (PARTITION BY city
              ORDER BY sum(total_receipts) DESC) AS rn
          FROM gold.revenue_by_zip_month
          WHERE TRUE {w_month}
          GROUP BY city, zip
        )
        SELECT city, zip, receipts FROM ranked WHERE rn <= 5
        ORDER BY receipts DESC
    """, **p)

    bottom_zips = _rows(conn, f"""
        WITH ranked AS (
          SELECT city, zip, avg(score)::numeric(5,2) AS avg_score,
            row_number() OVER (PARTITION BY city ORDER BY avg(score) ASC) AS rn
          FROM silver.inspections
          WHERE zip IS NOT NULL AND score IS NOT NULL {w_insp}
          GROUP BY city, zip
          HAVING avg(score) > 0
        )
        SELECT city, zip, avg_score FROM ranked WHERE rn <= 5
        ORDER BY avg_score ASC
    """, **p)

    return {
        "kpis": kpis[0] if kpis else {},
        "by_city": by_city,
        "top_zips": top_zips,
        "bottom_zips": bottom_zips,
    }


def build_revenue(conn, window: str) -> dict:
    w, p = _window_predicate("month", window)
    monthly = _rows(conn, f"""
        SELECT month, city, sum(total_receipts) AS total
        FROM gold.revenue_by_zip_month WHERE TRUE {w}
        GROUP BY month, city ORDER BY month
    """, **p)
    by_zip = _rows(conn, f"""
        SELECT city, zip, sum(total_receipts) AS total
        FROM gold.revenue_by_zip_month WHERE TRUE {w}
        GROUP BY city, zip ORDER BY total DESC LIMIT 15
    """, **p)
    return {"monthly": monthly, "by_zip": by_zip}


def build_inspections(conn, window: str) -> dict:
    w, p = _window_predicate("inspection_date", window)
    dist = _rows(conn, f"""
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
        WHERE TRUE {w}
        GROUP BY score_bucket ORDER BY score_bucket
    """, **p)

    w_i = w.replace("inspection_date", "i.inspection_date")
    repeat = _rows(conn, f"""
        WITH grouped AS (
          SELECT
            e.id AS establishment_id, e.city, e.canonical_name, e.zip,
            count(i.id) AS inspection_count,
            count(*) FILTER (WHERE i.score < 85) AS low_score_count,
            avg(i.score)::numeric(5,2) AS avg_score,
            min(i.score)::numeric(5,2) AS min_score
          FROM silver.establishments e
          JOIN silver.inspections i
            ON i.city = e.city AND i.facility_id = ANY(e.facility_ids)
          WHERE TRUE {w_i}
          GROUP BY e.id, e.city, e.canonical_name, e.zip
          HAVING count(*) FILTER (WHERE i.score < 85) >= 2
        ),
        ranked AS (
          SELECT *, row_number() OVER (
            PARTITION BY city ORDER BY low_score_count DESC, avg_score ASC
          ) AS rn
          FROM grouped
        )
        SELECT establishment_id, city, canonical_name, zip,
               inspection_count, low_score_count, avg_score, min_score
        FROM ranked WHERE rn <= 15
        ORDER BY low_score_count DESC, avg_score ASC
    """, **p)
    return {"distribution": dist, "repeat_offenders": repeat}


def build_correlation(conn, window: str) -> dict:
    w_i, p = _window_predicate("i.inspection_date", window)
    w_m, _ = _window_predicate("mb.obligation_end_date", window)
    points = _rows(conn, f"""
        WITH scores AS (
          SELECT e.id, avg(i.score) AS avg_score
          FROM silver.establishments e
          JOIN silver.inspections i
            ON i.city = e.city AND i.facility_id = ANY(e.facility_ids)
          WHERE TRUE {w_i}
          GROUP BY e.id
        ),
        revenue AS (
          SELECT e.id, avg(mb.total_receipts) AS avg_monthly_receipts
          FROM silver.establishments e
          JOIN silver.mixed_beverage mb
            ON mb.city = e.city
           AND mb.taxpayer_number = e.mb_taxpayer_number
           AND mb.location_number = e.mb_location_number
          WHERE TRUE {w_m}
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
    """, **p)
    return {"points": points}


def build_map(conn, window: str) -> dict:
    w_m, p = _window_predicate("mb.obligation_end_date", window)
    w_i, _ = _window_predicate("i.inspection_date", window)
    zips = _rows(conn, f"""
        WITH rev AS (
          SELECT mb.city, mb.location_zip AS zip,
                 count(DISTINCT (mb.taxpayer_number, mb.location_number)) AS establishments,
                 sum(mb.total_receipts) AS total_receipts
          FROM silver.mixed_beverage mb
          WHERE mb.location_zip IS NOT NULL {w_m}
          GROUP BY mb.city, mb.location_zip
        ),
        scr AS (
          SELECT i.city, i.zip, avg(i.score)::numeric(5,2) AS avg_score
          FROM silver.inspections i
          WHERE i.zip IS NOT NULL AND i.score IS NOT NULL {w_i}
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
    """, **p)
    return {"zips": zips}


def build_establishments_list(conn, window: str) -> list[dict]:
    """All establishments with their window-scoped stats. Frontend filters/sorts/paginates."""
    w_i, p = _window_predicate("i.inspection_date", window)
    w_m, _ = _window_predicate("mb.obligation_end_date", window)
    return _rows(conn, f"""
        SELECT
          e.id, e.canonical_name, e.canonical_address, e.city, e.zip,
          e.match_method, e.match_score,
          (SELECT count(*) FROM silver.inspections i
             WHERE i.city = e.city AND i.facility_id = ANY(e.facility_ids) {w_i}) AS inspection_count,
          (SELECT avg(score)::numeric(5,2) FROM silver.inspections i
             WHERE i.city = e.city AND i.facility_id = ANY(e.facility_ids) {w_i}) AS avg_score,
          (SELECT avg(mb.total_receipts)::numeric(14,2) FROM silver.mixed_beverage mb
             WHERE mb.city = e.city
               AND mb.taxpayer_number = e.mb_taxpayer_number
               AND mb.location_number = e.mb_location_number {w_m}) AS avg_monthly_receipts
        FROM silver.establishments e
        ORDER BY e.canonical_name, e.id
    """, **p)


def build_search_index(conn) -> list[dict]:
    """Slim rows for client-side typeahead. Window-independent."""
    return _rows(conn, """
        SELECT id, city, canonical_name, canonical_address, zip, match_method
        FROM silver.establishments
        ORDER BY
          CASE match_method
            WHEN 'fuzzy_zip_block' THEN 1
            WHEN 'mb_only' THEN 2
            ELSE 3
          END,
          canonical_name
    """)


def build_establishment_detail(conn, row: dict) -> dict:
    inspections = _rows(conn, """
        SELECT inspection_date, score, inspection_type
        FROM silver.inspections
        WHERE city = :city AND facility_id = ANY(:fids)
        ORDER BY inspection_date
    """, city=row["city"], fids=row["facility_ids"] or [])
    revenue = _rows(conn, """
        SELECT obligation_end_date AS month, total_receipts, liquor_receipts,
               wine_receipts, beer_receipts
        FROM silver.mixed_beverage
        WHERE city = :city AND taxpayer_number = :tp AND location_number = :ln
        ORDER BY obligation_end_date
    """, city=row["city"], tp=row["mb_taxpayer_number"], ln=row["mb_location_number"])
    licenses = _rows(conn, """
        SELECT l.license_id, l.license_type, l.tier, l.primary_status,
               l.original_issue_date, l.current_issued_date,
               l.expiration_date, l.status_change_date, l.gun_sign,
               l.master_file_id, l.owner
        FROM silver.establishment_licenses el
        JOIN silver.licenses l ON l.license_id = el.license_id
        WHERE el.establishment_id = :eid
        ORDER BY l.original_issue_date
    """, eid=row["id"])
    return {
        "header": row,
        "inspections": inspections,
        "revenue": revenue,
        "violations": [],
        "licenses": licenses,
    }


def build_ops(conn) -> dict:
    runs = _rows(conn, """
        SELECT dag_id, layer, started_at, finished_at, status, rows_written, notes
        FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 25
    """)
    counts = _rows(conn, """
        SELECT 'bronze.mixed_beverage' AS tbl, count(*) AS n FROM bronze.mixed_beverage
        UNION ALL SELECT 'bronze.inspections', count(*) FROM bronze.inspections
        UNION ALL SELECT 'bronze.licenses', count(*) FROM bronze.licenses
        UNION ALL SELECT 'silver.mixed_beverage', count(*) FROM silver.mixed_beverage
        UNION ALL SELECT 'silver.inspections', count(*) FROM silver.inspections
        UNION ALL SELECT 'silver.establishments', count(*) FROM silver.establishments
        UNION ALL SELECT 'silver.licenses', count(*) FROM silver.licenses
        UNION ALL SELECT 'silver.establishment_licenses', count(*) FROM silver.establishment_licenses
        UNION ALL SELECT 'gold.revenue_by_zip_month', count(*) FROM gold.revenue_by_zip_month
        UNION ALL SELECT 'gold.score_revenue_correlation', count(*) FROM gold.score_revenue_correlation
    """)
    return {"runs": runs, "counts": counts}


def build_lifecycle(conn) -> dict:
    """Permit lifecycle aggregates: status distribution, tenure x score points,
    gun-sign breakdown. Window-independent — licenses reflect current state."""
    status = _rows(conn, """
        SELECT primary_status AS status, count(*) AS n
        FROM silver.licenses
        GROUP BY primary_status ORDER BY n DESC
    """)
    tier = _rows(conn, """
        SELECT COALESCE(gun_sign, 'UNKNOWN') AS gun_sign, count(*) AS n
        FROM silver.licenses
        GROUP BY gun_sign ORDER BY n DESC
    """)
    # One point per establishment: tenure (years since earliest license
    # original_issue_date) vs avg inspection score. Powers the
    # "does tenure predict cleanliness?" scatter.
    tenure_vs_score = _rows(conn, """
        WITH est_tenure AS (
          SELECT e.id AS establishment_id, e.canonical_name, e.city, e.zip,
                 min(l.original_issue_date) AS first_issued
          FROM silver.establishments e
          JOIN silver.establishment_licenses el ON el.establishment_id = e.id
          JOIN silver.licenses l ON l.license_id = el.license_id
          WHERE l.original_issue_date IS NOT NULL
          GROUP BY e.id, e.canonical_name, e.city, e.zip
        ),
        est_score AS (
          SELECT e.id, avg(i.score) AS avg_score, count(i.id) AS n
          FROM silver.establishments e
          JOIN silver.inspections i
            ON i.city = e.city AND i.facility_id = ANY(e.facility_ids)
          GROUP BY e.id
        )
        SELECT t.establishment_id, t.canonical_name, t.zip,
               t.first_issued,
               extract(year from age(current_date, t.first_issued))::int AS tenure_years,
               s.avg_score::numeric(5,2) AS avg_score,
               s.n AS inspection_count
        FROM est_tenure t
        JOIN est_score s ON s.id = t.establishment_id
        WHERE s.avg_score IS NOT NULL AND s.n >= 2
    """)
    # Most-expired / most-active ZIPs — churn proxy.
    status_by_zip = _rows(conn, """
        SELECT zip,
               count(*) AS total,
               count(*) FILTER (WHERE primary_status ILIKE 'Active%%') AS active,
               count(*) FILTER (WHERE primary_status ILIKE 'Expired%%') AS expired,
               count(*) FILTER (WHERE primary_status ILIKE 'Cancel%%'
                             OR primary_status ILIKE 'Inactive%%') AS cancelled
        FROM silver.licenses
        WHERE zip IS NOT NULL
        GROUP BY zip
        HAVING count(*) >= 5
        ORDER BY total DESC
    """)
    return {
        "status": status,
        "gun_sign": tier,
        "tenure_vs_score": tenure_vs_score,
        "status_by_zip": status_by_zip,
    }


# ---------- orchestration ----------
def run():
    with track_run("export_static_json", "export") as state, engine.connect() as conn:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        files = 0

        for w in WINDOWS:
            _write(OUT_DIR / f"overview-{w}.json", build_overview(conn, w)); files += 1
            _write(OUT_DIR / f"revenue-{w}.json", build_revenue(conn, w)); files += 1
            _write(OUT_DIR / f"inspections-{w}.json", build_inspections(conn, w)); files += 1
            _write(OUT_DIR / f"correlation-{w}.json", build_correlation(conn, w)); files += 1
            _write(OUT_DIR / f"map-{w}.json", build_map(conn, w)); files += 1
            _write(OUT_DIR / f"establishments-{w}.json",
                   {"rows": build_establishments_list(conn, w)}); files += 1

        _write(OUT_DIR / "search.json", {"results": build_search_index(conn)}); files += 1
        _write(OUT_DIR / "ops.json", build_ops(conn)); files += 1
        _write(OUT_DIR / "lifecycle.json", build_lifecycle(conn)); files += 1

        # Per-establishment detail
        est_rows = _rows(conn, """
            SELECT id, city, canonical_name, canonical_address, zip,
                   mb_taxpayer_number, mb_location_number, facility_ids,
                   match_score, match_method, latitude, longitude
            FROM silver.establishments
        """)
        detail_dir = OUT_DIR / "establishment"
        detail_dir.mkdir(parents=True, exist_ok=True)
        for row in est_rows:
            _write(detail_dir / f"{row['id']}.json",
                   build_establishment_detail(conn, row))
            files += 1

        state["rows"] = files
    print(f"wrote {files} JSON files under {OUT_DIR}")


if __name__ == "__main__":
    run()
