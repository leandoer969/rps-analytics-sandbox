WITH sales AS (
    SELECT
        date_id,
        product_id,
        region_id,
        SUM(gross_sales_chf) AS gross_sales_chf
    FROM rps.fct_sales
    GROUP BY 1, 2, 3
),

rebates AS (
    SELECT
        date_id,
        product_id,
        region_id,
        SUM(rebate_chf) AS rebates_chf
    FROM rps.fct_rebates
    GROUP BY 1, 2, 3
),

units AS (
    SELECT
        date_id,
        product_id,
        region_id,
        SUM(units) AS units
    FROM rps.fct_sales
    GROUP BY 1, 2, 3
),

joined AS (
    SELECT
        s.date_id,
        s.product_id,
        s.region_id,
        s.gross_sales_chf,
        u.units,
        COALESCE(r.rebates_chf, 0) AS rebates_chf
    FROM sales AS s
    LEFT JOIN rebates AS r
        ON
            s.date_id = r.date_id
            AND s.product_id = r.product_id
            AND s.region_id = r.region_id
    LEFT JOIN units AS u
        ON
            s.date_id = u.date_id
            AND s.product_id = u.product_id
            AND s.region_id = u.region_id
),

dim AS (
    SELECT
        j.date_id,
        j.product_id,
        j.region_id,
        j.gross_sales_chf,
        j.rebates_chf,
        j.units,
        p.brand,
        p.indication,
        rg.canton,
        d.year,
        d.month,
        d.week
    FROM joined AS j
    INNER JOIN rps.dim_product AS p
        ON j.product_id = p.product_id
    INNER JOIN rps.dim_region AS rg
        ON j.region_id = rg.region_id
    INNER JOIN rps.dim_date AS d
        ON j.date_id = d.date_id
)

SELECT
    dim.date_id,
    dim.product_id,
    dim.region_id,
    dim.brand,
    dim.indication,
    dim.canton,
    dim.year,
    dim.month,
    dim.week,
    dim.units,
    dim.gross_sales_chf,
    dim.rebates_chf,
    (dim.gross_sales_chf - dim.rebates_chf) AS net_sales_chf,
    CASE
        WHEN dim.units > 0 THEN (dim.gross_sales_chf - dim.rebates_chf) / dim.units
    END AS net_price_per_unit
FROM dim;
