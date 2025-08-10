WITH r AS (
    SELECT *
    FROM rps.fct_rebates
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
        brand
    FROM rps.dim_product
),

rg AS (
    SELECT
        region_id,
        canton
    FROM rps.dim_region
),

py AS (
    SELECT
        payer_id,
        payer_name,
        payer_type
    FROM rps.dim_payer
)

SELECT
    r.date_id,
    d.year,
    d.month,
    d.week,
    r.product_id,
    p.brand,
    r.region_id,
    rg.canton,
    r.payer_id,
    py.payer_name,
    py.payer_type,
    r.rebate_chf
FROM r
INNER JOIN d
    ON r.date_id = d.date_id
INNER JOIN p
    ON r.product_id = p.product_id
INNER JOIN rg
    ON r.region_id = rg.region_id
INNER JOIN py
    ON r.payer_id = py.payer_id;
