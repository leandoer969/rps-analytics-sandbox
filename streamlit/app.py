import os
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="RPS Dashboards", layout="wide")

db = os.getenv("POSTGRES_DB", "rps")
user = os.getenv("POSTGRES_USER", "rps_user")
pwd = os.getenv("POSTGRES_PASSWORD", "rps_password")
host = os.getenv("POSTGRES_HOST", "postgres")  # <-- service name from compose
port = os.getenv("POSTGRES_PORT", "5432")

engine = create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}")

st.sidebar.title("RPS Analytics")
page = st.sidebar.radio("View", ["Executive Overview", "Brand Performance"])


@st.cache_data(ttl=300)
def load_df(sql, params=None):
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


if page == "Executive Overview":
    st.title("Executive Overview")
    kpi_sql = """
      WITH m AS (SELECT * FROM rps.mart_gtn_waterfall)
      SELECT
          year,
          month,
          brand,
          SUM(gross_sales_chf) AS gross_sales,
          SUM(rebates_chf) AS rebates,
          SUM(net_sales_chf) AS net_sales
      FROM m
      GROUP BY 1, 2, 3
      ORDER BY 1, 2, 3
    """
    df = load_df(kpi_sql)
    if df.empty:
        st.info("No data found. Run the generator and dbt build.")
    else:
        latest = df.sort_values(["year", "month"]).groupby("brand").tail(1)
        c1, c2, c3 = st.columns(3)
        c1.metric(
            "Gross Sales (latest month, CHF)", f"{latest['gross_sales'].sum():,.0f}"
        )
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
        heat = load_df(heat_sql)
        st.subheader("Regional Net Sales (totals)")
        st.dataframe(heat)

else:
    st.title("Brand Performance")
    brands = load_df("SELECT DISTINCT brand FROM rps.mart_brand_perf ORDER BY 1")
    brand = st.sidebar.selectbox(
        "Brand", options=brands["brand"].tolist() if not brands.empty else []
    )
    if not brand:
        st.info("No brand available. Run the generator and dbt build.")
    else:
        perf_sql = """
          SELECT year, month, canton, units, gross_sales_chf, COALESCE(promo_spend, 0) AS promo_spend
          FROM rps.mart_brand_perf
          WHERE brand = :brand
          ORDER BY year, month, canton
        """
        df = load_df(perf_sql, params={"brand": brand})
        if df.empty:
            st.info("No data for selected brand.")
        else:
            trend = df.groupby(["year", "month"], as_index=False)[
                ["units", "gross_sales_chf", "promo_spend"]
            ].sum()
            trend["period"] = (
                trend["year"].astype(str)
                + "-"
                + trend["month"].astype(str).str.zfill(2)
            )
            c1, c2 = st.columns(2)
            c1.plotly_chart(
                px.line(
                    trend, x="period", y="units", markers=True, title=f"{brand} Units"
                ),
                use_container_width=True,
            )
            c2.plotly_chart(
                px.bar(
                    trend,
                    x="period",
                    y="gross_sales_chf",
                    title=f"{brand} Net Sales (approx)",
                ),
                use_container_width=True,
            )

            st.subheader("By Canton (latest month)")
            last = trend.tail(1)[["year", "month"]].iloc[0]
            y, m = int(last["year"]), int(last["month"])
            by_canton = (
                df[(df["year"] == y) & (df["month"] == m)]
                .groupby("canton", as_index=False)[
                    ["units", "gross_sales_chf", "promo_spend"]
                ]
                .sum()
                .sort_values("gross_sales_chf", ascending=False)
            )
            st.dataframe(by_canton)
