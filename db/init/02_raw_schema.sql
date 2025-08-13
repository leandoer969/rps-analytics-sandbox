-- 02_raw_schema.sql
-- Purpose: Create a dedicated RAW landing area that emulates real-world messy inputs.
-- This keeps raw data separate from modeled/cleaned tables in schema "rps".

BEGIN;

-- 1) Raw schema
CREATE SCHEMA IF NOT EXISTS rps_raw;

-- Note: We keep raw tables permissive (TEXT everywhere), include lineage fields,
-- and store an ingest timestamp for CDC/late-arrival handling.

-- 2) SALES (raw)
CREATE TABLE IF NOT EXISTS rps_raw.sales_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    date_id TEXT,
    product_id TEXT,
    region_id TEXT,
    channel_id TEXT,
    units TEXT,
    list_price_chf TEXT,
    gross_sales_chf TEXT,
    -- lineage / metadata
    source_system TEXT,
    source_file TEXT,
    raw_ingest_ts TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_sales_raw_ingest_ts ON rps_raw.sales_raw (raw_ingest_ts);
CREATE INDEX IF NOT EXISTS ix_sales_raw_source_sys ON rps_raw.sales_raw (source_system);

-- 3) REBATES (raw)
CREATE TABLE IF NOT EXISTS rps_raw.rebates_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    date_id TEXT,
    product_id TEXT,
    payer_id TEXT,
    region_id TEXT,
    rebate_chf TEXT,
    -- lineage / metadata
    source_system TEXT,
    source_file TEXT,
    raw_ingest_ts TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_rebates_raw_ingest_ts ON rps_raw.rebates_raw (raw_ingest_ts);

-- 4) PROMO (raw)
CREATE TABLE IF NOT EXISTS rps_raw.promo_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    date_id TEXT,
    product_id TEXT,
    region_id TEXT,
    channel_id TEXT,
    spend_chf TEXT,
    touchpoints TEXT,
    -- lineage / metadata
    source_system TEXT,
    source_file TEXT,
    raw_ingest_ts TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_promo_raw_ingest_ts ON rps_raw.promo_raw (raw_ingest_ts);

-- 5) FORECAST (raw)
CREATE TABLE IF NOT EXISTS rps_raw.forecast_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    date_id TEXT,
    product_id TEXT,
    region_id TEXT,
    baseline_units TEXT,
    uplift_units TEXT,
    forecast_units TEXT,
    -- lineage / metadata
    source_system TEXT,
    source_file TEXT,
    raw_ingest_ts TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_forecast_raw_ingest_ts ON rps_raw.forecast_raw (raw_ingest_ts);

COMMIT;

-- Rollback helper (manual):
-- DROP SCHEMA rps_raw CASCADE;
