WITH f AS (
    SELECT *
    FROM rps.fct_forecast
),

d AS (
    SELECT
        date_id,
        year,
        month,
        week
    FROM rps.dim_date
),

p AS (
    SELECT
        product_id,
        brand,
        indication
    FROM rps.dim_product
),

r AS (
    SELECT
        region_id,
        canton,
        language_region
    FROM rps.dim_region
)

SELECT
    f.date_id,
    d.year,
    d.month,
    d.week,
    f.product_id,
    p.brand,
    p.indication,
    f.region_id,
    r.canton,
    r.language_region,
    f.baseline_units,
    f.uplift_units,
    f.forecast_units
FROM f
INNER JOIN d
    ON f.date_id = d.date_id
INNER JOIN p
    ON f.product_id = p.product_id
INNER JOIN r
    ON f.region_id = r.region_id;
