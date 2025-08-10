import plotly.express as px
import streamlit as st
from lib.db import read_sql_df

st.title("📊 Executive Overview")


@st.cache_data(ttl=300)
def load_overview():
    kpi_sql = """
      WITH m AS (SELECT * FROM rps.mart_gtn_waterfall)
      SELECT
          year,
          month,
          brand,
          SUM(gross_sales_chf) AS gross_sales,
          SUM(rebates_chf)     AS rebates,
          SUM(net_sales_chf)   AS net_sales
      FROM m
      GROUP BY 1, 2, 3
      ORDER BY 1, 2, 3
    """
    df = read_sql_df(kpi_sql)
    return df


df = load_overview()
if df.empty:
    st.info("No data found. Run the generator and dbt build.")
else:
    latest = df.sort_values(["year", "month"]).groupby("brand").tail(1)
    c1, c2, c3 = st.columns(3)
    c1.metric("Gross Sales (latest month, CHF)", f"{latest['gross_sales'].sum():,.0f}")
    c2.metric("Rebates (latest month, CHF)", f"{latest['rebates'].sum():,.0f}")
    c3.metric("Net Sales (latest month, CHF)", f"{latest['net_sales'].sum():,.0f}")

    trend = df.groupby(["year", "month"], as_index=False)[
        ["gross_sales", "net_sales"]
    ].sum()
    trend["period"] = (
        trend["year"].astype(str) + "-" + trend["month"].astype(str).str.zfill(2)
    )
    fig = px.line(trend, x="period", y=["gross_sales", "net_sales"], markers=True)
    st.plotly_chart(fig, use_container_width=True)

    heat_sql = """
      SELECT canton, SUM(net_sales_chf) AS net_sales
      FROM rps.mart_gtn_waterfall
      GROUP BY canton
      ORDER BY net_sales DESC
    """
    heat = read_sql_df(heat_sql)
    st.subheader("Regional Net Sales (totals)")
    st.dataframe(heat, use_container_width=True)
