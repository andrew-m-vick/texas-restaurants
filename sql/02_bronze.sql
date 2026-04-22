-- Bronze: raw landing tables.

DROP TABLE IF EXISTS bronze.dallas_violations;
DROP TABLE IF EXISTS bronze.violations;

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

-- Austin inspection ingest target. Score-only (Austin doesn't publish
-- itemized violations), so we don't keep a violations table.
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

-- TABC license ingest target. Retail tier only (the slice that overlaps
-- with restaurants/bars and MB receipts). Dates come in as ISO strings.
CREATE TABLE IF NOT EXISTS bronze.licenses (
    master_file_id TEXT,
    license_id TEXT,
    license_type TEXT,
    tier TEXT,
    primary_status TEXT,
    license_status TEXT,
    trade_name TEXT,
    owner TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    county TEXT,
    original_issue_date TEXT,
    current_issued_date TEXT,
    expiration_date TEXT,
    status_change_date TEXT,
    gun_sign TEXT,
    raw JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bronze_lic_city ON bronze.licenses (city);
CREATE INDEX IF NOT EXISTS idx_bronze_lic_master ON bronze.licenses (master_file_id);
