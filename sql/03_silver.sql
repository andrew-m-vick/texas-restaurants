-- Silver: cleaned, typed, deduped.

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

CREATE TABLE IF NOT EXISTS silver.inspections (
    id BIGSERIAL PRIMARY KEY,
    source_id TEXT UNIQUE,
    establishment_name TEXT NOT NULL,
    address TEXT,
    zip TEXT,
    inspection_date DATE,
    inspection_type TEXT,
    score NUMERIC(5,2),
    grade TEXT,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    name_key TEXT,
    address_key TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_insp_keys ON silver.inspections (name_key, address_key);
CREATE INDEX IF NOT EXISTS idx_silver_insp_zip_date ON silver.inspections (zip, inspection_date);

CREATE TABLE IF NOT EXISTS silver.violations (
    id BIGSERIAL PRIMARY KEY,
    source_id TEXT UNIQUE,
    inspection_source_id TEXT,
    violation_code TEXT,
    violation_description TEXT,
    violation_date DATE,
    severity TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_viol_insp ON silver.violations (inspection_source_id);

-- Unified establishment identity produced by fuzzy matcher.
CREATE TABLE IF NOT EXISTS silver.establishments (
    id BIGSERIAL PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    canonical_address TEXT,
    zip TEXT,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    mb_taxpayer_number TEXT,
    mb_location_number TEXT,
    inspection_source_ids TEXT[],
    match_score NUMERIC(5,2),
    match_method TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_est_zip ON silver.establishments (zip);
