WITH s AS (
    SELECT
        date_id,
        product_id,
        region_id,
        SUM(units) AS units,
        SUM(gross_sales_chf) AS gross_sales_chf
    FROM rps.fct_sales
    GROUP BY 1, 2, 3
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
),

d AS (
    SELECT
        date_id,
        year,
        month,
        week
    FROM rps.dim_date
),

promo AS (
    SELECT
        date_id,
        product_id,
        region_id,
        SUM(spend_chf) AS promo_spend,
        SUM(touchpoints) AS touches
    FROM rps.fct_promo
    GROUP BY 1, 2, 3
)

SELECT
    s.date_id,
    d.year,
    d.month,
    d.week,
    s.product_id,
    p.brand,
    p.indication,
    s.region_id,
    r.canton,
    r.language_region,
    s.units,
    s.gross_sales_chf,
    pr.promo_spend,
    pr.touches
FROM s
INNER JOIN p
    ON s.product_id = p.product_id
INNER JOIN r
    ON s.region_id = r.region_id
INNER JOIN d
    ON s.date_id = d.date_id
LEFT JOIN promo AS pr
    ON
        s.date_id = pr.date_id
        AND s.product_id = pr.product_id
        AND s.region_id = pr.region_id;
