-- Gold: analytics-ready aggregates — city-dimensioned.

DROP TABLE IF EXISTS gold.monthly_movers;
DROP TABLE IF EXISTS gold.neighborhood_heat;
DROP TABLE IF EXISTS gold.score_revenue_correlation;
DROP TABLE IF EXISTS gold.repeat_offenders;
DROP TABLE IF EXISTS gold.inspection_score_distribution;
DROP TABLE IF EXISTS gold.top_violations;
DROP TABLE IF EXISTS gold.revenue_by_zip_month;

CREATE TABLE gold.revenue_by_zip_month (
    city TEXT NOT NULL,
    zip TEXT NOT NULL,
    month DATE NOT NULL,
    establishments INT,
    total_receipts NUMERIC(16,2),
    liquor_receipts NUMERIC(16,2),
    wine_receipts NUMERIC(16,2),
    beer_receipts NUMERIC(16,2),
    PRIMARY KEY (city, zip, month)
);

CREATE TABLE gold.inspection_score_distribution (
    city TEXT NOT NULL,
    score_bucket TEXT NOT NULL,
    inspections INT,
    pct NUMERIC(5,2),
    PRIMARY KEY (city, score_bucket)
);

-- Dallas only (Austin publishes no violation detail).
CREATE TABLE gold.top_violations (
    city TEXT NOT NULL,
    description TEXT NOT NULL,
    occurrences INT,
    distinct_establishments INT,
    total_points NUMERIC(10,2),
    PRIMARY KEY (city, description)
);

CREATE TABLE gold.repeat_offenders (
    establishment_id BIGINT PRIMARY KEY,
    city TEXT NOT NULL,
    canonical_name TEXT,
    canonical_address TEXT,
    zip TEXT,
    inspection_count INT,
    low_score_count INT,
    avg_score NUMERIC(5,2),
    min_score NUMERIC(5,2)
);

CREATE INDEX idx_gold_repeat_city ON gold.repeat_offenders (city);

CREATE TABLE gold.score_revenue_correlation (
    establishment_id BIGINT PRIMARY KEY,
    city TEXT NOT NULL,
    canonical_name TEXT,
    zip TEXT,
    avg_score NUMERIC(5,2),
    avg_monthly_receipts NUMERIC(14,2),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6)
);

CREATE INDEX idx_gold_corr_city ON gold.score_revenue_correlation (city);

CREATE TABLE gold.neighborhood_heat (
    city TEXT NOT NULL,
    zip TEXT NOT NULL,
    establishments INT,
    avg_score NUMERIC(5,2),
    total_receipts NUMERIC(16,2),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    PRIMARY KEY (city, zip)
);

CREATE TABLE gold.monthly_movers (
    establishment_id BIGINT,
    city TEXT NOT NULL,
    canonical_name TEXT,
    zip TEXT,
    month DATE,
    total_receipts NUMERIC(14,2),
    pct_change NUMERIC(10,2),
    direction TEXT,
    PRIMARY KEY (establishment_id, month)
);

CREATE INDEX idx_gold_movers_city ON gold.monthly_movers (city);
