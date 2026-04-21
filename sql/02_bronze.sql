-- Bronze: raw landing tables. Schema-on-read where possible; JSONB for flexibility.

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

CREATE TABLE IF NOT EXISTS bronze.inspections (
    facility_id TEXT,
    restaurant_name TEXT,
    address TEXT,
    zip_code TEXT,
    inspection_date TEXT,
    score TEXT,
    process_description TEXT,
    raw JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bronze_insp_facility ON bronze.inspections (facility_id);
CREATE INDEX IF NOT EXISTS idx_bronze_insp_zip ON bronze.inspections (zip_code);
