WITH sales AS (
    SELECT
        d.year,
        d.month,
        p.brand,
        r.canton,
        sum(s.gross_sales_chf) AS gross_sales_chf
    FROM rps.fct_sales AS s
    INNER JOIN rps.dim_date AS d ON s.date_id = d.date_id
    INNER JOIN rps.dim_product AS p ON s.product_id = p.product_id
    INNER JOIN rps.dim_region AS r ON s.region_id = r.region_id
    GROUP BY 1, 2, 3, 4
),

rebates AS (
    SELECT
        d.year,
        d.month,
        p.brand,
        r.canton,
        sum(rb.rebate_chf) AS rebates_chf
    FROM rps.fct_rebates AS rb
    INNER JOIN rps.dim_date AS d ON rb.date_id = d.date_id
    INNER JOIN rps.dim_product AS p ON rb.product_id = p.product_id
    INNER JOIN rps.dim_region AS r ON rb.region_id = r.region_id
    GROUP BY 1, 2, 3, 4
)

SELECT
    s.year,
    s.month,
    s.brand,
    s.canton,
    s.gross_sales_chf,
    coalesce(r.rebates_chf, 0.0) AS rebates_chf,
    s.gross_sales_chf - coalesce(r.rebates_chf, 0.0) AS net_sales_chf
FROM sales AS s
LEFT JOIN rebates AS r
    ON
        s.year = r.year
        AND s.month = r.month
        AND s.brand = r.brand
        AND s.canton = r.canton
