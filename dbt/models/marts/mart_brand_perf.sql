WITH s AS (
    SELECT
        d.year,
        d.month,
        p.brand,
        r.canton,
        sum(s.units) AS units,
        sum(s.gross_sales_chf) AS gross_sales_chf
    FROM rps_core.fct_sales AS s
    INNER JOIN rps_core.dim_date AS d ON s.date_id = d.date_id
    INNER JOIN rps_core.dim_product AS p ON s.product_id = p.product_id
    INNER JOIN rps_core.dim_region AS r ON s.region_id = r.region_id
    GROUP BY 1, 2, 3, 4
),

pr AS (
    SELECT
        d.year,
        d.month,
        p.brand,
        r.canton,
        sum(pp.spend_chf) AS promo_spend
    FROM rps_core.fct_promo AS pp
    INNER JOIN rps_core.dim_date AS d ON pp.date_id = d.date_id
    INNER JOIN rps_core.dim_product AS p ON pp.product_id = p.product_id
    INNER JOIN rps_core.dim_region AS r ON pp.region_id = r.region_id
    GROUP BY 1, 2, 3, 4
)

SELECT
    s.year,
    s.month,
    s.brand,
    s.canton,
    s.units,
    s.gross_sales_chf,
    coalesce(pr.promo_spend, 0.0) AS promo_spend
FROM s
LEFT JOIN pr
    ON
        s.year = pr.year
        AND s.month = pr.month
        AND s.brand = pr.brand
        AND s.canton = pr.canton
