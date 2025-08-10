WITH s AS (
    SELECT
        s.date_id,
        d.year,
        d.month,
        s.product_id,
        p.brand,
        p.molecule,
        s.region_id,
        r.canton,
        s.channel_id,
        c.channel_name,
        s.units,
        s.list_price_chf,
        s.gross_sales_chf
    FROM rps.fct_sales AS s
    INNER JOIN rps.dim_date AS d ON s.date_id = d.date_id
    INNER JOIN rps.dim_product AS p ON s.product_id = p.product_id
    INNER JOIN rps.dim_region AS r ON s.region_id = r.region_id
    INNER JOIN rps.dim_channel AS c ON s.channel_id = c.channel_id
)

SELECT * FROM s
