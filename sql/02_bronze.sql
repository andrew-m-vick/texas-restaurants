-- Bronze: raw landing tables. Schema-on-read where possible; JSONB for flexibility.

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
    source_id TEXT,
    establishment_name TEXT,
    address TEXT,
    city TEXT,
    zip TEXT,
    inspection_date TEXT,
    inspection_type TEXT,
    score TEXT,
    grade TEXT,
    latitude TEXT,
    longitude TEXT,
    raw JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bronze_insp_src ON bronze.inspections (source_id);

CREATE TABLE IF NOT EXISTS bronze.violations (
    source_id TEXT,
    inspection_source_id TEXT,
    establishment_name TEXT,
    address TEXT,
    violation_code TEXT,
    violation_description TEXT,
    violation_date TEXT,
    severity TEXT,
    raw JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bronze_viol_insp ON bronze.violations (inspection_source_id);
