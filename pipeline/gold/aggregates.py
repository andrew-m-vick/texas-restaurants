"""Build gold-layer aggregates from silver. Pure SQL, idempotent."""
from sqlalchemy import text
from ..db import engine
from ..ops import track_run

STATEMENTS = [
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
    # top_violations
    """
    TRUNCATE gold.top_violations;
    INSERT INTO gold.top_violations
    SELECT
        violation_code,
        max(violation_description) AS violation_description,
        count(*) AS occurrences,
        count(DISTINCT inspection_source_id) AS distinct_establishments
    FROM silver.violations
    WHERE violation_code IS NOT NULL
    GROUP BY violation_code
    ORDER BY occurrences DESC
    LIMIT 50;
    """,
    # repeat_offenders
    """
    TRUNCATE gold.repeat_offenders;
    INSERT INTO gold.repeat_offenders
    SELECT
        e.id,
        e.canonical_name,
        e.canonical_address,
        e.zip,
        count(v.id) AS violation_count,
        avg(i.score)::numeric(5,2) AS avg_score
    FROM silver.establishments e
    JOIN silver.inspections i ON i.source_id = ANY(e.inspection_source_ids)
    LEFT JOIN silver.violations v ON v.inspection_source_id = i.source_id
    GROUP BY e.id, e.canonical_name, e.canonical_address, e.zip
    HAVING count(v.id) >= 3
    ORDER BY violation_count DESC
    LIMIT 200;
    """,
    # score_revenue_correlation
    """
    TRUNCATE gold.score_revenue_correlation;
    WITH est_scores AS (
        SELECT e.id, avg(i.score) AS avg_score
        FROM silver.establishments e
        JOIN silver.inspections i ON i.source_id = ANY(e.inspection_source_ids)
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
        r.avg_monthly_receipts::numeric(14,2),
        e.latitude,
        e.longitude
    FROM silver.establishments e
    JOIN est_scores s ON s.id = e.id
    JOIN est_revenue r ON r.id = e.id
    WHERE s.avg_score IS NOT NULL AND r.avg_monthly_receipts IS NOT NULL;
    """,
    # neighborhood_heat
    """
    TRUNCATE gold.neighborhood_heat;
    INSERT INTO gold.neighborhood_heat
    SELECT
        z.zip,
        z.establishments,
        COALESCE(s.avg_score, 0) AS avg_score,
        COALESCE(z.total_receipts, 0) AS total_receipts,
        c.lat AS latitude,
        c.lon AS longitude
    FROM (
        SELECT location_zip AS zip,
               count(DISTINCT (taxpayer_number, location_number)) AS establishments,
               sum(total_receipts) AS total_receipts
        FROM silver.mixed_beverage
        WHERE location_zip IS NOT NULL
        GROUP BY location_zip
    ) z
    LEFT JOIN (
        SELECT zip, avg(score) AS avg_score
        FROM silver.inspections
        WHERE zip IS NOT NULL
        GROUP BY zip
    ) s ON s.zip = z.zip
    LEFT JOIN (
        SELECT zip, avg(latitude) AS lat, avg(longitude) AS lon
        FROM silver.inspections
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        GROUP BY zip
    ) c ON c.zip = z.zip;
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
        establishment_id,
        canonical_name,
        zip,
        month,
        total_receipts,
        CASE WHEN prev_receipts > 0
             THEN round(100 * (total_receipts - prev_receipts) / prev_receipts, 2)
             ELSE NULL END AS pct_change,
        CASE WHEN prev_receipts IS NULL THEN 'new'
             WHEN total_receipts > prev_receipts THEN 'up'
             WHEN total_receipts < prev_receipts THEN 'down'
             ELSE 'flat' END AS direction
    FROM lagged;
    """,
]


def run():
    with track_run("build_gold_layer", "gold") as state, engine.begin() as conn:
        for stmt in STATEMENTS:
            conn.execute(text(stmt))
        state["rows"] = conn.execute(
            text("SELECT count(*) FROM gold.revenue_by_zip_month")
        ).scalar_one()
    print("gold layer rebuilt")


if __name__ == "__main__":
    run()
