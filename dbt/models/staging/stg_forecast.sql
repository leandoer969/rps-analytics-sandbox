{{ config(materialized='view') }}

WITH src AS (
    SELECT *
    FROM {{ source('rps_raw','forecast_raw') }}
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

        CASE
            WHEN baseline_units ~ '^-?\d+(\.\d+)?$' THEN baseline_units::numeric
            WHEN baseline_units ~ '^-?\d+,\d+$' THEN replace(baseline_units, ',', '.')::numeric
        END AS baseline_units,
        CASE
            WHEN uplift_units ~ '^-?\d+(\.\d+)?$' THEN uplift_units::numeric
            WHEN uplift_units ~ '^-?\d+,\d+$' THEN replace(uplift_units, ',', '.')::numeric
        END AS uplift_units,
        CASE
            WHEN forecast_units ~ '^-?\d+(\.\d+)?$' THEN forecast_units::numeric
            WHEN forecast_units ~ '^-?\d+,\d+$' THEN replace(forecast_units, ',', '.')::numeric
        END AS forecast_units
    FROM src
)

SELECT *
FROM norm
