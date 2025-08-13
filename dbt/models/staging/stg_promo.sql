{{ config(materialized='view') }}

WITH src AS (
    SELECT *
    FROM {{ source('rps_raw','promo_raw') }}
),

norm AS (
    SELECT
        source_system,
        source_file,
        raw_ingest_ts,
        CASE
            WHEN date_id ~ '^\d{4}-\d{2}-\d{2}$' THEN date_id::date
            WHEN date_id ~ '^\d{2}\.\d{2}\.\d{4}$' THEN to_date(date_id, 'DD.MM.YYYY')
            WHEN date_id ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(date_id, 'MM/DD/YYYY')
        END AS date_id,
        nullif(product_id, '') AS product_id,
        nullif(region_id, '') AS region_id,
        nullif(channel_id, '') AS channel_id,
        CASE
            WHEN spend_chf ~ '^-?\d+(\.\d+)?$' THEN spend_chf::numeric
            WHEN spend_chf ~ '^-?\d+,\d+$' THEN replace(spend_chf, ',', '.')::numeric
        END AS spend_chf,
        nullif(touchpoints, '') AS touchpoints
    FROM src
)

SELECT *
FROM norm
