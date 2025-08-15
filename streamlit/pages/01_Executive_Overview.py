import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from lib.db import read_sql_df

st.title("📊 Executive Overview")


@st.cache_data(ttl=300)
def load_overview():
    sql = """
      WITH m AS (SELECT * FROM rps_mart.mart_gtn_waterfall)
      SELECT
          year, month, brand, canton,
          SUM(gross_sales_chf) AS gross_sales,
          SUM(rebates_chf)     AS rebates,
          SUM(net_sales_chf)   AS net_sales
      FROM m
      GROUP BY 1,2,3,4
      ORDER BY 1,2,3,4
    """
    df = read_sql_df(sql)
    df["month_start"] = pd.to_datetime(dict(year=df["year"], month=df["month"], day=1))
    return df


df = load_overview()
if df.empty:
    st.info("No data found. Run the generator and dbt build.")
    st.stop()

# ---- Filters ----
brands = sorted(df["brand"].unique().tolist())
brand = st.sidebar.selectbox("Brand", brands)

min_dt = df["month_start"].min().to_pydatetime()
max_dt = df["month_start"].max().to_pydatetime()
date_range = st.sidebar.slider(
    "Date range", min_value=min_dt, max_value=max_dt, value=(min_dt, max_dt)
)

f = df[
    (df["brand"] == brand)
    & (
        df["month_start"].between(
            pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
        )
    )
]

# ---- KPI tiles on filtered range (latest month) ----
latest = f.sort_values(["year", "month"]).groupby("brand").tail(1)
c1, c2, c3 = st.columns(3)
c1.metric("Gross Sales (latest, CHF)", f"{latest['gross_sales'].sum():,.0f}")
c2.metric("Rebates (latest, CHF)", f"{latest['rebates'].sum():,.0f}")
c3.metric("Net Sales (latest, CHF)", f"{latest['net_sales'].sum():,.0f}")

# ---- Trend (filtered) ----
trend = f.groupby(["year", "month"], as_index=False)[["gross_sales", "net_sales"]].sum()
trend["period"] = (
    trend["year"].astype(str) + "-" + trend["month"].astype(str).str.zfill(2)
)
st.plotly_chart(
    px.line(
        trend,
        x="period",
        y=["gross_sales", "net_sales"],
        markers=True,
        title=f"{brand}: Gross vs Net over time",
    ),
    use_container_width=True,
)

# ---- GTN Waterfall for latest month in filter ----
st.subheader(f"{brand}: GTN Waterfall (latest month in range)")
last_period = trend["period"].max() if not trend.empty else None
if last_period:
    y, m = map(int, last_period.split("-"))
    snap = f[(f["year"] == y) & (f["month"] == m)]
    gross = float(snap["gross_sales"].sum())
    rebates = float(snap["rebates"].sum())
    net = float(snap["net_sales"].sum())

    wf = go.Figure(
        go.Waterfall(
            name="GTN",
            orientation="v",
            measure=["relative", "relative", "total"],
            x=["Gross", "Rebates", "Net"],
            text=[f"{gross:,.0f}", f"{-rebates:,.0f}", f"{net:,.0f}"],
            y=[gross, -rebates, net],
        )
    )
    wf.update_layout(showlegend=False, margin=dict(l=10, r=10, t=30, b=10))
    st.plotly_chart(wf, use_container_width=True)
else:
    st.info("No months in selected range.")

# ---- Regional table + download ----
heat = (
    f.groupby("canton", as_index=False)["net_sales"]
    .sum()
    .sort_values("net_sales", ascending=False)
)
st.subheader("Regional Net Sales (filtered total)")
st.dataframe(heat, use_container_width=True)

csv = heat.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download canton totals (CSV)",
    data=csv,
    file_name=f"{brand}_net_sales_by_canton.csv",
    mime="text/csv",
)
