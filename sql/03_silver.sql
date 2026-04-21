-- Silver: cleaned, typed, deduped.

DROP TABLE IF EXISTS silver.violations;

CREATE TABLE IF NOT EXISTS silver.mixed_beverage (
    id BIGSERIAL PRIMARY KEY,
    taxpayer_number TEXT,
    location_number TEXT,
    location_name TEXT NOT NULL,
    location_address TEXT,
    location_city TEXT,
    location_zip TEXT,
    obligation_end_date DATE NOT NULL,
    liquor_receipts NUMERIC(14,2) DEFAULT 0,
    wine_receipts NUMERIC(14,2) DEFAULT 0,
    beer_receipts NUMERIC(14,2) DEFAULT 0,
    cover_charge_receipts NUMERIC(14,2) DEFAULT 0,
    total_receipts NUMERIC(14,2) DEFAULT 0,
    name_key TEXT,
    address_key TEXT,
    UNIQUE (taxpayer_number, location_number, obligation_end_date)
);

CREATE INDEX IF NOT EXISTS idx_silver_mb_keys ON silver.mixed_beverage (name_key, address_key);
CREATE INDEX IF NOT EXISTS idx_silver_mb_zip_date ON silver.mixed_beverage (location_zip, obligation_end_date);

-- One row per inspection event. Austin provides a score per inspection, no violation detail.
CREATE TABLE IF NOT EXISTS silver.inspections (
    id BIGSERIAL PRIMARY KEY,
    facility_id TEXT NOT NULL,
    restaurant_name TEXT NOT NULL,
    address TEXT,
    zip TEXT,
    inspection_date DATE,
    score NUMERIC(5,2),
    process_description TEXT,
    name_key TEXT,
    address_key TEXT,
    UNIQUE (facility_id, inspection_date, process_description)
);

CREATE INDEX IF NOT EXISTS idx_silver_insp_keys ON silver.inspections (name_key, address_key);
CREATE INDEX IF NOT EXISTS idx_silver_insp_zip_date ON silver.inspections (zip, inspection_date);
CREATE INDEX IF NOT EXISTS idx_silver_insp_facility ON silver.inspections (facility_id);

-- Unified establishment identity produced by fuzzy matcher.
CREATE TABLE IF NOT EXISTS silver.establishments (
    id BIGSERIAL PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    canonical_address TEXT,
    zip TEXT,
    mb_taxpayer_number TEXT,
    mb_location_number TEXT,
    facility_ids TEXT[],
    match_score NUMERIC(5,2),
    match_method TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_est_zip ON silver.establishments (zip);
