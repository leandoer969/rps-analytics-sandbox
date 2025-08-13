{{ config(materialized='view') }}

WITH src AS (
    SELECT
        {{ clean_date('date_id') }} AS date_id,
        {{ clean_text('product_id') }} AS product_id,
        {{ clean_text('region_id') }} AS region_id,
        {{ clean_text('channel_id') }} AS channel_id,
        {{ clean_numeric('spend_chf') }} AS spend_chf,
        {{ clean_text('touchpoints') }} AS touchpoints
    FROM {{ source('rps_raw', 'promo_raw') }}
),

final AS (
    SELECT
        date_id,
        product_id,
        region_id,
        channel_id,
        spend_chf,
        touchpoints
    FROM src
)

SELECT * FROM final
