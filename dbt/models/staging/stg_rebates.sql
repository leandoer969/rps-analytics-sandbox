{{ config(materialized='view') }}

WITH src AS (
    SELECT
        {{ clean_date('date_id') }} AS date_id,
        {{ clean_text('product_id') }} AS product_id,
        {{ clean_text('payer_id') }} AS payer_id,
        {{ clean_text('region_id') }} AS region_id,
        {{ clean_numeric('rebate_chf') }} AS rebate_chf
    FROM {{ source('rps_raw', 'rebates_raw') }}
),

final AS (
    SELECT
        date_id,
        product_id,
        payer_id,
        region_id,
        rebate_chf
    FROM src
)

SELECT * FROM final
