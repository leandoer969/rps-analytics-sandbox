WITH r AS (
    SELECT
        r.date_id,
        d.year,
        d.month,
        r.product_id,
        p.brand,
        r.region_id,
        rg.canton,
        r.payer_id,
        py.payer_name,
        r.rebate_chf
    FROM rps.fct_rebates AS r
    INNER JOIN rps.dim_date AS d ON r.date_id = d.date_id
    INNER JOIN rps.dim_product AS p ON r.product_id = p.product_id
    INNER JOIN rps.dim_region AS rg ON r.region_id = rg.region_id
    INNER JOIN rps.dim_payer AS py ON r.payer_id = py.payer_id
)

SELECT * FROM r
