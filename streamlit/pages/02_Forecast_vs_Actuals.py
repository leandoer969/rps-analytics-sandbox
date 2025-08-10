import pandas as pd
import streamlit as st

from lib.db import read_sql_df

st.set_page_config(page_title="Forecast vs Actuals", page_icon="📈", layout="wide")
st.title("📈 Forecast vs Actuals")


@st.cache_data(ttl=300)
def load_data():
    sql = """
        SELECT
            year, month, brand, canton,
            actual_units, forecast_units, baseline_units, uplift_units, mape_units
        FROM rps.mart_forecast_accuracy
        ORDER BY year, month, brand, canton
    """
    df = read_sql_df(sql)
    # Build a month_start date for charts
    df["month_start"] = pd.to_datetime(dict(year=df["year"], month=df["month"], day=1))
    return df


df = load_data()
if df.empty:
    st.warning(
        "No data found in rps.mart_forecast_accuracy. Run `make restart` to (re)build."
    )
    st.stop()

# Sidebar filters
brands = sorted(df["brand"].unique().tolist())
brand = st.sidebar.selectbox("Brand", brands)

min_date = df["month_start"].min()
max_date = df["month_start"].max()
date_range = st.sidebar.slider(
    "Date range", min_value=min_date, max_value=max_date, value=(min_date, max_date)
)

# Filtered frame
mask = (df["brand"] == brand) & (
    df["month_start"].between(date_range[0], date_range[1])
)
d = df.loc[mask].copy()

# Aggregate over cantons per month for topline
top = (
    d.groupby("month_start", as_index=False)
    .agg(
        actual_units=("actual_units", "sum"),
        forecast_units=("forecast_units", "sum"),
        baseline_units=("baseline_units", "sum"),
        uplift_units=("uplift_units", "sum"),
        # average MAPE (weighted by actuals would be nicer; keep simple)
        mape_units=("mape_units", "mean"),
    )
    .sort_values("month_start")
)

# KPI tiles (latest month)
latest = top.tail(1).iloc[0]
k1, k2, k3, k4 = st.columns(4)
k1.metric("Actual units (latest)", f"{int(latest.actual_units):,}")
k2.metric(
    "Forecast units (latest)",
    f"{int(latest.forecast_units):,}",
    delta=f"{int(latest.forecast_units - latest.actual_units):,}",
)
k3.metric("MAPE (units)", f"{latest.mape_units:.1%}")
k4.metric("Uplift vs Baseline (latest)", f"{int(latest.uplift_units):,}")

# Trend: Actual vs Forecast
st.subheader(f"{brand}: Actual vs Forecast over time")
trend = top.set_index("month_start")[["actual_units", "forecast_units"]]
st.line_chart(trend)

# Breakdown by canton for latest month in range
st.subheader(f"{brand}: By canton (latest month in selection)")
latest_month = top["month_start"].max()
by_canton = (
    d[d["month_start"] == latest_month]
    .groupby(["canton"], as_index=False)
    .agg(
        actual_units=("actual_units", "sum"),
        forecast_units=("forecast_units", "sum"),
        baseline_units=("baseline_units", "sum"),
        uplift_units=("uplift_units", "sum"),
        mape_units=("mape_units", "mean"),
    )
    .sort_values("actual_units", ascending=False)
)
st.dataframe(by_canton, use_container_width=True)

# Raw table toggle (handy in interviews)
with st.expander("Show raw rows (filtered)"):
    st.dataframe(d, use_container_width=True)
