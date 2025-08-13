{{ config(materialized='view') }}

WITH src AS (
    SELECT
        {{ clean_date('date_id') }} AS date_id,
        {{ clean_text('product_id') }} AS product_id,
        {{ clean_text('region_id') }} AS region_id,
        {{ clean_numeric('baseline_units') }} AS baseline_units,
        {{ clean_numeric('uplift_units') }} AS uplift_units,
        {{ clean_numeric('forecast_units') }} AS forecast_units
    FROM {{ source('rps_raw', 'forecast_raw') }}
),

final AS (
    SELECT
        date_id,
        product_id,
        region_id,
        baseline_units,
        uplift_units,
        forecast_units
    FROM src
)

SELECT * FROM final
