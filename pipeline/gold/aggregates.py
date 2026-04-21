"""Build gold-layer aggregates from silver. Pure SQL, idempotent, per-city.

ZIP centroids for the map are computed once from pgeocode (offline US postal
data) for any ZIP missing real lat/long in the source data.
"""
import pandas as pd
import pgeocode
from sqlalchemy import text
from ..db import engine
from ..ops import track_run

SQL_STATEMENTS = [
    # revenue_by_zip_month
    """
    TRUNCATE gold.revenue_by_zip_month;
    INSERT INTO gold.revenue_by_zip_month
    SELECT
        city,
        location_zip AS zip,
        date_trunc('month', obligation_end_date)::date AS month,
        count(DISTINCT (taxpayer_number, location_number)) AS establishments,
        sum(total_receipts),
        sum(liquor_receipts),
        sum(wine_receipts),
        sum(beer_receipts)
    FROM silver.mixed_beverage
    WHERE location_zip IS NOT NULL
    GROUP BY 1, 2, 3;
    """,
    # inspection_score_distribution — per city
    """
    TRUNCATE gold.inspection_score_distribution;
    WITH buckets AS (
        SELECT
            city,
            CASE
                WHEN score >= 95 THEN 'A (95-100)'
                WHEN score >= 85 THEN 'B (85-94)'
                WHEN score >= 75 THEN 'C (75-84)'
                WHEN score >= 0  THEN 'D (<75)'
                ELSE 'Unscored'
            END AS score_bucket
        FROM silver.inspections
    ),
    totals AS (SELECT city, count(*)::numeric AS n FROM buckets GROUP BY city)
    INSERT INTO gold.inspection_score_distribution
    SELECT
        b.city,
        b.score_bucket,
        count(*),
        round(100 * count(*) / t.n, 2)
    FROM buckets b JOIN totals t USING (city)
    GROUP BY b.city, b.score_bucket, t.n;
    """,
    # top_violations (Dallas only, but query is city-scoped so Austin just returns 0 rows)
    """
    TRUNCATE gold.top_violations;
    INSERT INTO gold.top_violations
    SELECT
        city,
        description,
        count(*) AS occurrences,
        count(DISTINCT facility_id) AS distinct_establishments,
        sum(coalesce(points, 0))::numeric(10,2) AS total_points
    FROM silver.violations
    GROUP BY city, description
    ORDER BY occurrences DESC;
    """,
    # repeat_offenders — score-based (works for both cities)
    """
    TRUNCATE gold.repeat_offenders;
    INSERT INTO gold.repeat_offenders
    SELECT
        e.id,
        e.city,
        e.canonical_name,
        e.canonical_address,
        e.zip,
        count(i.id) AS inspection_count,
        count(*) FILTER (WHERE i.score < 85) AS low_score_count,
        avg(i.score)::numeric(5,2) AS avg_score,
        min(i.score)::numeric(5,2) AS min_score
    FROM silver.establishments e
    JOIN silver.inspections i
      ON i.city = e.city AND i.facility_id = ANY(e.facility_ids)
    GROUP BY e.id, e.city, e.canonical_name, e.canonical_address, e.zip
    HAVING count(*) FILTER (WHERE i.score < 85) >= 2
    ORDER BY low_score_count DESC, avg_score ASC;
    """,
    # score_revenue_correlation
    """
    TRUNCATE gold.score_revenue_correlation;
    WITH est_scores AS (
        SELECT e.id, avg(i.score) AS avg_score,
               avg(i.latitude) AS lat, avg(i.longitude) AS lon
        FROM silver.establishments e
        JOIN silver.inspections i
          ON i.city = e.city AND i.facility_id = ANY(e.facility_ids)
        GROUP BY e.id
    ),
    est_revenue AS (
        SELECT e.id, avg(mb.total_receipts) AS avg_monthly_receipts
        FROM silver.establishments e
        JOIN silver.mixed_beverage mb
          ON mb.city = e.city
         AND mb.taxpayer_number = e.mb_taxpayer_number
         AND mb.location_number = e.mb_location_number
        GROUP BY e.id
    )
    INSERT INTO gold.score_revenue_correlation
    SELECT
        e.id, e.city, e.canonical_name, e.zip,
        s.avg_score::numeric(5,2),
        r.avg_monthly_receipts::numeric(14,2),
        COALESCE(e.latitude, s.lat)::numeric(9,6),
        COALESCE(e.longitude, s.lon)::numeric(9,6)
    FROM silver.establishments e
    JOIN est_scores s ON s.id = e.id
    JOIN est_revenue r ON r.id = e.id
    WHERE s.avg_score IS NOT NULL AND r.avg_monthly_receipts IS NOT NULL;
    """,
    # monthly_movers
    """
    TRUNCATE gold.monthly_movers;
    WITH monthly AS (
        SELECT
            e.id AS establishment_id,
            e.city,
            e.canonical_name,
            e.zip,
            date_trunc('month', mb.obligation_end_date)::date AS month,
            sum(mb.total_receipts) AS total_receipts
        FROM silver.establishments e
        JOIN silver.mixed_beverage mb
          ON mb.city = e.city
         AND mb.taxpayer_number = e.mb_taxpayer_number
         AND mb.location_number = e.mb_location_number
        GROUP BY 1, 2, 3, 4, 5
    ),
    lagged AS (
        SELECT
            *,
            lag(total_receipts) OVER (PARTITION BY establishment_id ORDER BY month) AS prev_receipts
        FROM monthly
    )
    INSERT INTO gold.monthly_movers
    SELECT
        establishment_id, city, canonical_name, zip, month, total_receipts,
        CASE WHEN prev_receipts > 0
             THEN round(100 * (total_receipts - prev_receipts) / prev_receipts, 2)
             ELSE NULL END,
        CASE WHEN prev_receipts IS NULL THEN 'new'
             WHEN total_receipts > prev_receipts THEN 'up'
             WHEN total_receipts < prev_receipts THEN 'down'
             ELSE 'flat' END
    FROM lagged;
    """,
]


def _rebuild_neighborhood_heat():
    """Per-city, per-ZIP rollups with real lat/long where available, pgeocode fallback."""
    df = pd.read_sql(
        """
        WITH z AS (
            SELECT city, location_zip AS zip,
                   count(DISTINCT (taxpayer_number, location_number)) AS establishments,
                   sum(total_receipts) AS total_receipts
            FROM silver.mixed_beverage
            WHERE location_zip IS NOT NULL
            GROUP BY city, location_zip
        ),
        s AS (
            SELECT city, zip, avg(score) AS avg_score,
                   avg(latitude) AS lat, avg(longitude) AS lon
            FROM silver.inspections
            WHERE zip IS NOT NULL
            GROUP BY city, zip
        )
        SELECT
            COALESCE(z.city, s.city) AS city,
            COALESCE(z.zip, s.zip) AS zip,
            COALESCE(z.establishments, 0) AS establishments,
            COALESCE(s.avg_score, 0)::numeric(5,2) AS avg_score,
            COALESCE(z.total_receipts, 0) AS total_receipts,
            s.lat AS latitude,
            s.lon AS longitude
        FROM z FULL OUTER JOIN s USING (city, zip)
        """,
        engine,
    )
    if df.empty:
        return 0
    # pgeocode fallback for ZIPs with no real coordinates
    needs_geo = df[df["latitude"].isna() | df["longitude"].isna()]
    if not needs_geo.empty:
        nomi = pgeocode.Nominatim("us")
        cents = nomi.query_postal_code(needs_geo["zip"].tolist())
        df.loc[needs_geo.index, "latitude"] = cents["latitude"].values
        df.loc[needs_geo.index, "longitude"] = cents["longitude"].values
    df = df.dropna(subset=["latitude", "longitude"])

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE gold.neighborhood_heat"))
    df.to_sql(
        "neighborhood_heat", engine, schema="gold",
        if_exists="append", index=False, method="multi", chunksize=1000,
    )
    return len(df)


def run():
    with track_run("build_gold_layer", "gold") as state:
        with engine.begin() as conn:
            for stmt in SQL_STATEMENTS:
                conn.execute(text(stmt))
        n_heat = _rebuild_neighborhood_heat()
        with engine.begin() as conn:
            rows = conn.execute(
                text("SELECT count(*) FROM gold.revenue_by_zip_month")
            ).scalar_one()
        state["rows"] = rows
    print(f"gold layer rebuilt ({rows} monthly rows, {n_heat} ZIP centroids)")


if __name__ == "__main__":
    run()
