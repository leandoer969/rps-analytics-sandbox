import plotly.express as px
import streamlit as st
from lib.db import read_sql_df

st.title("🏷️ Brand Performance")


@st.cache_data(ttl=300)
def load_brands():
    return read_sql_df("SELECT DISTINCT brand FROM rps_mart.mart_brand_perf ORDER BY 1")


brands = load_brands()
brand = st.sidebar.selectbox(
    "Brand", options=brands["brand"].tolist() if not brands.empty else []
)

if not brand:
    st.info("No brand available. Run the generator and dbt build.")
else:
    perf_sql = """
      SELECT year, month, canton, units, gross_sales_chf, COALESCE(promo_spend, 0) AS promo_spend
      FROM rps_mart.mart_brand_perf
      WHERE brand = :brand
      ORDER BY year, month, canton
    """
    df = read_sql_df(perf_sql, params={"brand": brand})
    if df.empty:
        st.info("No data for selected brand.")
    else:
        trend = df.groupby(["year", "month"], as_index=False)[
            ["units", "gross_sales_chf", "promo_spend"]
        ].sum()
        trend["period"] = (
            trend["year"].astype(str) + "-" + trend["month"].astype(str).str.zfill(2)
        )

        c1, c2 = st.columns(2)
        c1.plotly_chart(
            px.line(trend, x="period", y="units", markers=True, title=f"{brand} Units"),
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
        st.dataframe(by_canton, use_container_width=True)
