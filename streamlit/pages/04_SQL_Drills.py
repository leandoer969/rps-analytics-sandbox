import streamlit as st
from lib.db import read_sql_df

st.title("🧪 SQL Drills")

tabs = st.tabs(
    [
        "Period over Period",
        "De-dupe latest snapshot",
        "Join facts→dims",
        "Stockout & DoS",
    ]
)

with tabs[0]:
    st.subheader("1) Period-over-period (LAG)")
    sql = """
        WITH monthly AS (
    SELECT
        make_date(year, month, 1) AS month_start,
        product_id,
        brand,
        SUM(gross_sales_chf) AS sales_chf
    FROM rps.stg_sales
    GROUP BY 1, 2, 3
    ),
    po AS (
    SELECT
        month_start,
        product_id,
        brand,
        sales_chf,
        LAG(sales_chf) OVER (PARTITION BY product_id ORDER BY month_start) AS lag_sales
    FROM monthly
    )
    SELECT
    month_start,
    product_id,
    brand,
    sales_chf,
    lag_sales,
    sales_chf - lag_sales                                    AS abs_chg,
    CASE
        WHEN lag_sales IS NULL OR lag_sales = 0 THEN NULL
        ELSE (sales_chf / lag_sales) - 1
    END                                                       AS pct_chg
    FROM po
    ORDER BY product_id, month_start;
    """
    st.code(sql, language="sql")
    st.dataframe(read_sql_df(sql).head(50), use_container_width=True)

with tabs[1]:
    st.subheader("2) De-dupe with ROW_NUMBER()")
    sql = """WITH raw AS (
  SELECT * FROM (VALUES
    (101, DATE '2025-05-01', 120, TIMESTAMP '2025-05-02 08:00'),
    (101, DATE '2025-05-01', 130, TIMESTAMP '2025-05-02 09:00'), -- later duplicate
    (102, DATE '2025-05-01',  50, TIMESTAMP '2025-05-01 12:00')
  ) AS t(product_id, as_of_date, units, load_ts)
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY product_id, as_of_date
      ORDER BY load_ts DESC
    ) AS rn
  FROM raw
)
SELECT *
FROM ranked
WHERE rn = 1
ORDER BY product_id, as_of_date;"""
    st.code(sql, language="sql")
    st.dataframe(read_sql_df(sql), use_container_width=True)

with tabs[2]:
    st.subheader("3) Join facts to dims")
    sql = """SELECT
  make_date(d.year, d.month, 1) AS month_start,
  s.date_id, s.product_id, s.region_id, s.channel_id,
  p.brand, p.molecule, r.canton, c.channel_name,
  s.units, s.gross_sales_chf
FROM rps.fct_sales AS s
LEFT JOIN rps.dim_date    AS d ON s.date_id    = d.date_id
LEFT JOIN rps.dim_product AS p ON s.product_id = p.product_id
LEFT JOIN rps.dim_region  AS r ON s.region_id  = r.region_id
LEFT JOIN rps.dim_channel AS c ON s.channel_id = c.channel_id
ORDER BY month_start, p.brand, r.canton;"""
    st.code(sql, language="sql")
    st.dataframe(read_sql_df(sql).head(100), use_container_width=True)

with tabs[3]:
    st.subheader("4) Stockout flag and Days of Supply")
    sql = """WITH daily_demand AS (
    SELECT
        d.date_id,
        d.date_actual,
        s.product_id,
        SUM(s.units) AS units
    FROM rps.fct_sales AS s
    JOIN rps.dim_date  AS d ON s.date_id = d.date_id
    GROUP BY 1,2,3
    ),
    rolling AS (
    SELECT
        date_actual,
        product_id,
        units,
        AVG(units) OVER (
        PARTITION BY product_id
        ORDER BY date_actual
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_7d_units
    FROM daily_demand
    ),
    inv AS (
    -- Fake inventory series: start at 2,000 and decay by demand; reorder point at 300
    SELECT
        r.date_actual,
        r.product_id,
        GREATEST(0, 2000
        - SUM(r.units) OVER (PARTITION BY r.product_id ORDER BY r.date_actual)) AS inventory_on_hand,
        300::int AS reorder_point,
        r.avg_7d_units
    FROM rolling r
    )
    SELECT
    date_actual,
    product_id,
    inventory_on_hand,
    reorder_point,
    avg_7d_units,
    CASE WHEN inventory_on_hand <= reorder_point THEN 1 ELSE 0 END AS stockout_flag,
    CASE
        WHEN COALESCE(avg_7d_units,0) = 0 THEN NULL
        ELSE inventory_on_hand / avg_7d_units
    END AS days_of_supply
    FROM inv
    ORDER BY product_id, date_actual;"""
    st.code(sql, language="sql")
    st.dataframe(read_sql_df(sql).head(100), use_container_width=True)
