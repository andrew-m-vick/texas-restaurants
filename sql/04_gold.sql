-- Gold: analytics-ready aggregates.

DROP TABLE IF EXISTS gold.top_violations;

CREATE TABLE IF NOT EXISTS gold.revenue_by_zip_month (
    zip TEXT NOT NULL,
    month DATE NOT NULL,
    establishments INT,
    total_receipts NUMERIC(16,2),
    liquor_receipts NUMERIC(16,2),
    wine_receipts NUMERIC(16,2),
    beer_receipts NUMERIC(16,2),
    PRIMARY KEY (zip, month)
);

CREATE TABLE IF NOT EXISTS gold.inspection_score_distribution (
    score_bucket TEXT PRIMARY KEY,
    inspections INT,
    pct NUMERIC(5,2)
);

-- Establishments inspected repeatedly with persistently low scores (redefined
-- for Austin: violation-level detail isn't published, so we use score).
CREATE TABLE IF NOT EXISTS gold.repeat_offenders (
    establishment_id BIGINT PRIMARY KEY,
    canonical_name TEXT,
    canonical_address TEXT,
    zip TEXT,
    inspection_count INT,
    low_score_count INT,
    avg_score NUMERIC(5,2),
    min_score NUMERIC(5,2)
);

CREATE TABLE IF NOT EXISTS gold.score_revenue_correlation (
    establishment_id BIGINT PRIMARY KEY,
    canonical_name TEXT,
    zip TEXT,
    avg_score NUMERIC(5,2),
    avg_monthly_receipts NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS gold.neighborhood_heat (
    zip TEXT PRIMARY KEY,
    establishments INT,
    avg_score NUMERIC(5,2),
    total_receipts NUMERIC(16,2),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6)
);

CREATE TABLE IF NOT EXISTS gold.monthly_movers (
    establishment_id BIGINT,
    canonical_name TEXT,
    zip TEXT,
    month DATE,
    total_receipts NUMERIC(14,2),
    pct_change NUMERIC(6,2),
    direction TEXT,
    PRIMARY KEY (establishment_id, month)
);
