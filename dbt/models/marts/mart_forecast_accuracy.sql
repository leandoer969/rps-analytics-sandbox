WITH actuals AS (
    SELECT
        date_id,
        product_id,
        region_id,
        SUM(units) AS actual_units
    FROM rps.fct_sales
    GROUP BY 1, 2, 3
),

forecast AS (
    SELECT
        date_id,
        product_id,
        region_id,
        SUM(forecast_units) AS forecast_units
    FROM rps.fct_forecast
    GROUP BY 1, 2, 3
),

joined AS (
    SELECT
        a.date_id,
        a.product_id,
        a.region_id,
        a.actual_units,
        COALESCE(f.forecast_units, 0) AS forecast_units
    FROM actuals AS a
    LEFT JOIN forecast AS f
        ON
            a.date_id = f.date_id
            AND a.product_id = f.product_id
            AND a.region_id = f.region_id
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
    j.date_id,
    d.year,
    d.month,
    d.week,
    j.product_id,
    p.brand,
    p.indication,
    j.region_id,
    r.canton,
    r.language_region,
    j.actual_units,
    j.forecast_units,
    CASE
        WHEN j.actual_units > 0 THEN ABS(j.actual_units - j.forecast_units) / j.actual_units
    END AS mape_component,
    (j.forecast_units - j.actual_units) AS bias_units
FROM joined AS j
INNER JOIN d
    ON j.date_id = d.date_id
INNER JOIN p
    ON j.product_id = p.product_id
INNER JOIN r
    ON j.region_id = r.region_id;
