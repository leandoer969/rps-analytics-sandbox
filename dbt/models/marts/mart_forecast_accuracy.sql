WITH actuals AS (
    SELECT
        d.year,
        d.month,
        p.brand,
        r.canton,
        sum(s.units) AS actual_units,
        sum(s.gross_sales_chf) AS actual_gross_chf
    FROM rps.fct_sales AS s
    INNER JOIN rps.dim_date AS d ON s.date_id = d.date_id
    INNER JOIN rps.dim_product AS p ON s.product_id = p.product_id
    INNER JOIN rps.dim_region AS r ON s.region_id = r.region_id
    GROUP BY 1, 2, 3, 4
),

fc AS (
    SELECT
        d.year,
        d.month,
        p.brand,
        r.canton,
        sum(f.forecast_units) AS forecast_units,
        sum(f.baseline_units) AS baseline_units,
        sum(f.uplift_units) AS uplift_units
    FROM rps.fct_forecast AS f
    INNER JOIN rps.dim_date AS d ON f.date_id = d.date_id
    INNER JOIN rps.dim_product AS p ON f.product_id = p.product_id
    INNER JOIN rps.dim_region AS r ON f.region_id = r.region_id
    GROUP BY 1, 2, 3, 4
)

SELECT
    a.year,
    a.month,
    a.brand,
    a.canton,
    a.actual_units,
    fc.forecast_units,
    fc.baseline_units,
    fc.uplift_units,
    (a.actual_units - fc.forecast_units) AS abs_error_units,
    CASE
        WHEN a.actual_units = 0 THEN null
        ELSE abs(a.actual_units - fc.forecast_units) / a.actual_units::numeric
    END AS mape_units
FROM actuals AS a
LEFT JOIN fc
    ON
        a.year = fc.year
        AND a.month = fc.month
        AND a.brand = fc.brand
        AND a.canton = fc.canton
