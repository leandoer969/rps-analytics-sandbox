WITH s AS (
    SELECT *
    FROM rps.fct_sales
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
        molecule,
        atc_code,
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

c AS (
    SELECT
        channel_id,
        channel_name
    FROM rps.dim_channel
)

SELECT
    s.date_id,
    d.year,
    d.month,
    d.week,
    s.product_id,
    p.brand,
    p.molecule,
    p.atc_code,
    p.indication,
    s.region_id,
    r.canton,
    r.language_region,
    s.channel_id,
    c.channel_name,
    s.units,
    s.list_price_chf,
    s.gross_sales_chf
FROM s
INNER JOIN d
    ON s.date_id = d.date_id
INNER JOIN p
    ON s.product_id = p.product_id
INNER JOIN r
    ON s.region_id = r.region_id
INNER JOIN c
    ON s.channel_id = c.channel_id;
