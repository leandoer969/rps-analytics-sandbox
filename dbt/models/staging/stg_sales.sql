{{ config(materialized='view') }}

WITH src AS (
    SELECT
        {{ clean_date('date_id') }} AS date_id,
        {{ clean_text('product_id') }} AS product_id,
        {{ clean_text('region_id') }} AS region_id,
        {{ clean_text('channel_id') }} AS channel_id,
        {{ clean_int('units') }} AS units,
        {{ clean_numeric('list_price_chf') }} AS list_price_chf,
        {{ clean_numeric('gross_sales_chf') }} AS gross_sales_chf
    FROM {{ source('rps_raw', 'sales_raw') }}
),

final AS (
    SELECT
        date_id,
        product_id,
        region_id,
        channel_id,
        units,
        list_price_chf,
        gross_sales_chf
    FROM src
)

SELECT * FROM final
