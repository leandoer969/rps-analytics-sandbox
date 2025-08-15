CREATE SCHEMA IF NOT EXISTS rps_core;

-- Dimensions
CREATE TABLE IF NOT EXISTS rps_core.dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    week INT,
    month_start DATE,
    week_start DATE
);

CREATE TABLE IF NOT EXISTS rps_core.dim_product (
    product_id SERIAL PRIMARY KEY,
    brand TEXT,
    molecule TEXT,
    atc_code TEXT,
    indication TEXT,
    launch_date DATE
);

CREATE TABLE IF NOT EXISTS rps_core.dim_region (
    region_id SERIAL PRIMARY KEY,
    canton TEXT,
    language_region TEXT
);

CREATE TABLE IF NOT EXISTS rps_core.dim_payer (
    payer_id SERIAL PRIMARY KEY,
    payer_name TEXT,
    payer_type TEXT
);

CREATE TABLE IF NOT EXISTS rps_core.dim_channel (
    channel_id SERIAL PRIMARY KEY,
    channel_name TEXT
);

-- Facts
CREATE TABLE IF NOT EXISTS rps_core.fct_sales (
    sales_id BIGSERIAL PRIMARY KEY,
    date_id DATE REFERENCES rps_core.dim_date (date_id),
    product_id INT REFERENCES rps_core.dim_product (product_id),
    region_id INT REFERENCES rps_core.dim_region (region_id),
    channel_id INT REFERENCES rps_core.dim_channel (channel_id),
    units INT,
    list_price_chf NUMERIC(12, 2),
    gross_sales_chf NUMERIC(14, 2)
);

CREATE TABLE IF NOT EXISTS rps_core.fct_rebates (
    rebate_id BIGSERIAL PRIMARY KEY,
    date_id DATE REFERENCES rps_core.dim_date (date_id),
    product_id INT REFERENCES rps_core.dim_product (product_id),
    payer_id INT REFERENCES rps_core.dim_payer (payer_id),
    region_id INT REFERENCES rps_core.dim_region (region_id),
    rebate_chf NUMERIC(14, 2)
);

CREATE TABLE IF NOT EXISTS rps_core.fct_promo (
    promo_id BIGSERIAL PRIMARY KEY,
    date_id DATE REFERENCES rps_core.dim_date (date_id),
    product_id INT REFERENCES rps_core.dim_product (product_id),
    region_id INT REFERENCES rps_core.dim_region (region_id),
    channel_id INT REFERENCES rps_core.dim_channel (channel_id),
    spend_chf NUMERIC(14, 2),
    touchpoints INT
);

CREATE TABLE IF NOT EXISTS rps_core.fct_forecast (
    forecast_id BIGSERIAL PRIMARY KEY,
    date_id DATE REFERENCES rps_core.dim_date (date_id),
    product_id INT REFERENCES rps_core.dim_product (product_id),
    region_id INT REFERENCES rps_core.dim_region (region_id),
    baseline_units NUMERIC(12, 2),
    uplift_units NUMERIC(12, 2),
    forecast_units NUMERIC(12, 2)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_sales_date ON rps_core.fct_sales (date_id);
CREATE INDEX IF NOT EXISTS idx_sales_product ON rps_core.fct_sales (product_id);
CREATE INDEX IF NOT EXISTS idx_sales_region ON rps_core.fct_sales (region_id);
CREATE INDEX IF NOT EXISTS idx_rebates_date ON rps_core.fct_rebates (date_id);
CREATE INDEX IF NOT EXISTS idx_rebates_product ON rps_core.fct_rebates (product_id);

-- Channels seed
INSERT INTO rps_core.dim_channel (channel_name)
VALUES ('Retail'), ('Hospital'), ('Specialty')
ON CONFLICT DO NOTHING;
