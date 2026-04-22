-- Silver: cleaned, typed, deduped.

DROP TABLE IF EXISTS silver.violations;
DROP TABLE IF EXISTS silver.establishments;
DROP TABLE IF EXISTS silver.inspections;
DROP TABLE IF EXISTS silver.mixed_beverage;
DROP TABLE IF EXISTS silver.licenses;

CREATE TABLE silver.mixed_beverage (
    id BIGSERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    taxpayer_number TEXT,
    location_number TEXT,
    location_name TEXT NOT NULL,
    location_address TEXT,
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

CREATE INDEX idx_silver_mb_keys ON silver.mixed_beverage (name_key, address_key);
CREATE INDEX idx_silver_mb_zip_date ON silver.mixed_beverage (location_zip, obligation_end_date);

CREATE TABLE silver.inspections (
    id BIGSERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    facility_id TEXT NOT NULL,
    restaurant_name TEXT NOT NULL,
    address TEXT,
    zip TEXT,
    inspection_date DATE,
    score NUMERIC(5,2),
    inspection_type TEXT,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    name_key TEXT,
    address_key TEXT,
    UNIQUE (facility_id, inspection_date, inspection_type)
);

CREATE INDEX idx_silver_insp_keys ON silver.inspections (name_key, address_key);
CREATE INDEX idx_silver_insp_zip_date ON silver.inspections (zip, inspection_date);
CREATE INDEX idx_silver_insp_facility ON silver.inspections (facility_id);

CREATE TABLE silver.establishments (
    id BIGSERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    canonical_address TEXT,
    zip TEXT,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    mb_taxpayer_number TEXT,
    mb_location_number TEXT,
    facility_ids TEXT[],
    match_score NUMERIC(5,2),
    match_method TEXT
);

CREATE INDEX idx_silver_est_zip ON silver.establishments (zip);

CREATE TABLE silver.licenses (
    id BIGSERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    master_file_id TEXT,
    license_id TEXT NOT NULL,
    license_type TEXT,
    tier TEXT,
    primary_status TEXT,
    trade_name TEXT,
    owner TEXT,
    address TEXT,
    zip TEXT,
    original_issue_date DATE,
    current_issued_date DATE,
    expiration_date DATE,
    status_change_date DATE,
    gun_sign TEXT,
    name_key TEXT,
    address_key TEXT,
    UNIQUE (license_id)
);

CREATE INDEX idx_silver_lic_keys ON silver.licenses (name_key, address_key);
CREATE INDEX idx_silver_lic_zip ON silver.licenses (zip);
CREATE INDEX idx_silver_lic_master ON silver.licenses (master_file_id);

-- Join table: which TABC licenses belong to which matched establishment.
-- Populated by match_establishments.py so licenses and establishments stay
-- loosely coupled (a single establishment can hold several license rows).
CREATE TABLE silver.establishment_licenses (
    establishment_id BIGINT NOT NULL,
    license_id TEXT NOT NULL,
    match_score NUMERIC(5,2),
    PRIMARY KEY (establishment_id, license_id)
);

CREATE INDEX idx_silver_el_license ON silver.establishment_licenses (license_id);
