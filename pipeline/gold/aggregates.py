"""Build gold-layer aggregates from silver. Pure SQL, idempotent.

ZIP centroids for the map are computed from pgeocode (offline US postal data)
rather than a per-inspection lat/long, since Austin's dataset doesn't publish
coordinates.
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
        location_zip AS zip,
        date_trunc('month', obligation_end_date)::date AS month,
        count(DISTINCT (taxpayer_number, location_number)) AS establishments,
        sum(total_receipts) AS total_receipts,
        sum(liquor_receipts) AS liquor_receipts,
        sum(wine_receipts) AS wine_receipts,
        sum(beer_receipts) AS beer_receipts
    FROM silver.mixed_beverage
    WHERE location_zip IS NOT NULL
    GROUP BY 1, 2;
    """,
    # inspection_score_distribution
    """
    TRUNCATE gold.inspection_score_distribution;
    WITH buckets AS (
        SELECT
            CASE
                WHEN score >= 95 THEN 'A (95-100)'
                WHEN score >= 85 THEN 'B (85-94)'
                WHEN score >= 75 THEN 'C (75-84)'
                WHEN score >= 0  THEN 'D (<75)'
                ELSE 'Unscored'
            END AS score_bucket
        FROM silver.inspections
    ),
    totals AS (SELECT count(*)::numeric AS n FROM buckets)
    INSERT INTO gold.inspection_score_distribution
    SELECT
        score_bucket,
        count(*) AS inspections,
        round(100 * count(*) / (SELECT n FROM totals), 2) AS pct
    FROM buckets
    GROUP BY score_bucket;
    """,
    # repeat_offenders — score-based, since Austin publishes no violation detail
    """
    TRUNCATE gold.repeat_offenders;
    INSERT INTO gold.repeat_offenders
    SELECT
        e.id,
        e.canonical_name,
        e.canonical_address,
        e.zip,
        count(i.id) AS inspection_count,
        count(*) FILTER (WHERE i.score < 85) AS low_score_count,
        avg(i.score)::numeric(5,2) AS avg_score,
        min(i.score)::numeric(5,2) AS min_score
    FROM silver.establishments e
    JOIN silver.inspections i ON i.facility_id = ANY(e.facility_ids)
    GROUP BY e.id, e.canonical_name, e.canonical_address, e.zip
    HAVING count(*) FILTER (WHERE i.score < 85) >= 2
    ORDER BY low_score_count DESC, avg_score ASC
    LIMIT 200;
    """,
    # score_revenue_correlation
    """
    TRUNCATE gold.score_revenue_correlation;
    WITH est_scores AS (
        SELECT e.id, avg(i.score) AS avg_score
        FROM silver.establishments e
        JOIN silver.inspections i ON i.facility_id = ANY(e.facility_ids)
        GROUP BY e.id
    ),
    est_revenue AS (
        SELECT e.id, avg(mb.total_receipts) AS avg_monthly_receipts
        FROM silver.establishments e
        JOIN silver.mixed_beverage mb
          ON mb.taxpayer_number = e.mb_taxpayer_number
         AND mb.location_number = e.mb_location_number
        GROUP BY e.id
    )
    INSERT INTO gold.score_revenue_correlation
    SELECT
        e.id,
        e.canonical_name,
        e.zip,
        s.avg_score::numeric(5,2),
        r.avg_monthly_receipts::numeric(14,2)
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
            e.canonical_name,
            e.zip,
            date_trunc('month', mb.obligation_end_date)::date AS month,
            sum(mb.total_receipts) AS total_receipts
        FROM silver.establishments e
        JOIN silver.mixed_beverage mb
          ON mb.taxpayer_number = e.mb_taxpayer_number
         AND mb.location_number = e.mb_location_number
        GROUP BY 1, 2, 3, 4
    ),
    lagged AS (
        SELECT
            *,
            lag(total_receipts) OVER (PARTITION BY establishment_id ORDER BY month) AS prev_receipts
        FROM monthly
    )
    INSERT INTO gold.monthly_movers
    SELECT
        establishment_id, canonical_name, zip, month, total_receipts,
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
    """Join MB + inspection aggregates per ZIP, then attach pgeocode centroids."""
    df = pd.read_sql(
        """
        WITH z AS (
            SELECT location_zip AS zip,
                   count(DISTINCT (taxpayer_number, location_number)) AS establishments,
                   sum(total_receipts) AS total_receipts
            FROM silver.mixed_beverage
            WHERE location_zip IS NOT NULL
            GROUP BY location_zip
        ),
        s AS (
            SELECT zip, avg(score) AS avg_score
            FROM silver.inspections
            WHERE zip IS NOT NULL
            GROUP BY zip
        )
        SELECT
            COALESCE(z.zip, s.zip) AS zip,
            COALESCE(z.establishments, 0) AS establishments,
            COALESCE(s.avg_score, 0)::numeric(5,2) AS avg_score,
            COALESCE(z.total_receipts, 0) AS total_receipts
        FROM z FULL OUTER JOIN s ON s.zip = z.zip
        """,
        engine,
    )
    if df.empty:
        return 0
    nomi = pgeocode.Nominatim("us")
    centroids = nomi.query_postal_code(df["zip"].tolist())
    df["latitude"] = centroids["latitude"].values
    df["longitude"] = centroids["longitude"].values
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
