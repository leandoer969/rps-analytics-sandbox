{{ config(materialized='view') }}

WITH src AS (
    SELECT *
    FROM {{ source('rps_raw','sales_raw') }}
),

norm AS (
    SELECT
    -- tolerant date parsing: 2024-06-23 | 23.06.2024 | 06/23/2024
        source_system,
        source_file,
        raw_ingest_ts,
        CASE
            WHEN date_id ~ '^\d{4}-\d{2}-\d{2}$' THEN date_id::date
            WHEN date_id ~ '^\d{2}\.\d{2}\.\d{4}$' THEN to_date(date_id, 'DD.MM.YYYY')
            WHEN date_id ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(date_id, 'MM/DD/YYYY')
        END AS date_id,

        -- numbers with commas/dots/empty → numeric
        nullif(product_id, '') AS product_id,

        nullif(region_id, '') AS region_id,

        nullif(channel_id, '') AS channel_id,

        CASE
            WHEN units ~ '^-?\d+(\.\d+)?$' THEN units::numeric
            WHEN units ~ '^-?\d+,\d+$' THEN replace(units, ',', '.')::numeric
        END AS units,
        CASE
            WHEN list_price_chf ~ '^-?\d+(\.\d+)?$' THEN list_price_chf::numeric
            WHEN list_price_chf ~ '^-?\d+,\d+$' THEN replace(list_price_chf, ',', '.')::numeric
        END AS list_price_chf,
        CASE
            WHEN gross_sales_chf ~ '^-?\d+(\.\d+)?$' THEN gross_sales_chf::numeric
            WHEN gross_sales_chf ~ '^-?\d+,\d+$' THEN replace(gross_sales_chf, ',', '.')::numeric
        END AS gross_sales_chf
    FROM src
)

SELECT *
FROM norm
