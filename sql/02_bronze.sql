-- Bronze: raw landing tables. Unified where schemas overlap; separate where they diverge.

DROP TABLE IF EXISTS bronze.violations;
DROP TABLE IF EXISTS bronze.inspections;
DROP TABLE IF EXISTS bronze.dallas_violations;

CREATE TABLE IF NOT EXISTS bronze.mixed_beverage (
    taxpayer_number TEXT,
    taxpayer_name TEXT,
    taxpayer_address TEXT,
    taxpayer_city TEXT,
    taxpayer_state TEXT,
    taxpayer_zip TEXT,
    location_number TEXT,
    location_name TEXT,
    location_address TEXT,
    location_city TEXT,
    location_state TEXT,
    location_zip TEXT,
    location_county TEXT,
    obligation_end_date_yyyymmdd TEXT,
    liquor_receipts TEXT,
    wine_receipts TEXT,
    beer_receipts TEXT,
    cover_charge_receipts TEXT,
    total_receipts TEXT,
    raw JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bronze_mb_city ON bronze.mixed_beverage (location_city);
CREATE INDEX IF NOT EXISTS idx_bronze_mb_date ON bronze.mixed_beverage (obligation_end_date_yyyymmdd);

-- Inspections unified across cities. Only the common fields are typed;
-- city-specific payload (violations, lat/long, points) stays in `raw` JSONB.
CREATE TABLE IF NOT EXISTS bronze.inspections (
    city TEXT NOT NULL,
    facility_id TEXT,
    restaurant_name TEXT,
    address TEXT,
    zip_code TEXT,
    inspection_date TEXT,
    score TEXT,
    inspection_type TEXT,
    latitude TEXT,
    longitude TEXT,
    raw JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bronze_insp_city ON bronze.inspections (city);
CREATE INDEX IF NOT EXISTS idx_bronze_insp_facility ON bronze.inspections (facility_id);

-- Dallas publishes up to 15 violations per inspection row (denormalized).
-- We explode them during ingest into this bronze landing table.
CREATE TABLE IF NOT EXISTS bronze.dallas_violations (
    facility_id TEXT,
    inspection_date TEXT,
    violation_number INT,
    description TEXT,
    points TEXT,
    tfer_text TEXT,
    memo TEXT,
    raw JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bronze_dviol_facility ON bronze.dallas_violations (facility_id);
