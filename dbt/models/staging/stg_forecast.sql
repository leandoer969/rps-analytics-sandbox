WITH f AS (
    SELECT
        f.date_id,
        d.year,
        d.month,
        f.product_id,
        p.brand,
        f.region_id,
        r.canton,
        f.baseline_units,
        f.uplift_units,
        f.forecast_units
    FROM rps.fct_forecast AS f
    INNER JOIN rps.dim_date AS d ON f.date_id = d.date_id
    INNER JOIN rps.dim_product AS p ON f.product_id = p.product_id
    INNER JOIN rps.dim_region AS r ON f.region_id = r.region_id
)

SELECT * FROM f
