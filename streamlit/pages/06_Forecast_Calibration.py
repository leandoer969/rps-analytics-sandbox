# pages/06_Forecast_Calibration.py
# Interactive alpha/beta calibration for the uplift component of forecasts.

import os
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.title("🔧 Forecast Calibration (α / β)")

# --- DB setup ----------------------------------------------------------------
DB = os.getenv("POSTGRES_DB", "rps")
USER = os.getenv("POSTGRES_USER", "rps_user")
PWD = os.getenv("POSTGRES_PASSWORD", "rps_password")
HOST = os.getenv("POSTGRES_HOST", "postgres")
PORT = os.getenv("POSTGRES_PORT", "5432")
ENGINE = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")


@st.cache_data(ttl=300)
def load_df(sql: str, params=None) -> pd.DataFrame:
    with ENGINE.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def ensure_params_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS rps.forecast_params (
        scope_level TEXT NOT NULL,
        scope_key1  TEXT NULL,
        scope_key2  TEXT NULL,
        alpha       DOUBLE PRECISION NOT NULL,
        beta        DOUBLE PRECISION NOT NULL,
        train_start DATE NOT NULL,
        train_end   DATE NOT NULL,
        updated_at  TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
        PRIMARY KEY (scope_level, scope_key1, scope_key2)
    );
    """
    with ENGINE.begin() as conn:
        conn.execute(text(ddl))


def upsert_params(scope_level, k1, k2, alpha, beta, t_start, t_end):
    ensure_params_table()
    sql = """
    INSERT INTO rps.forecast_params
      (scope_level, scope_key1, scope_key2, alpha, beta, train_start, train_end, updated_at)
    VALUES
      (:scope_level, :k1, :k2, :alpha, :beta, :train_start, :train_end, now())
    ON CONFLICT (scope_level, scope_key1, scope_key2)
    DO UPDATE SET
      alpha = EXCLUDED.alpha,
      beta  = EXCLUDED.beta,
      train_start = EXCLUDED.train_start,
      train_end   = EXCLUDED.train_end,
      updated_at  = now();
    """
    with ENGINE.begin() as conn:
        conn.execute(
            text(sql),
            {
                "scope_level": scope_level,
                "k1": k1,
                "k2": k2,
                "alpha": float(alpha),
                "beta": float(beta),
                "train_start": pd.to_datetime(t_start).date(),
                "train_end": pd.to_datetime(t_end).date(),
            },
        )


# --- Load monthly data from marts (brand × canton) ---------------------------
data_sql = """
WITH perf AS (
  SELECT year, month, brand, canton,
         units,
         COALESCE(promo_spend, 0) AS promo_spend
  FROM rps.mart_brand_perf
),
gtn AS (
  SELECT year, month, brand, canton,
         SUM(gross_sales_chf)  AS gross_sales_chf,
         SUM(rebates_chf)      AS rebates_chf
  FROM rps.mart_gtn_waterfall
  GROUP BY year, month, brand, canton
),
m AS (
  SELECT p.year, p.month, p.brand, p.canton,
         p.units,
         p.promo_spend,
         g.gross_sales_chf,
         g.rebates_chf,
         CASE WHEN g.gross_sales_chf > 0
              THEN g.rebates_chf / g.gross_sales_chf
              ELSE 0.0 END AS rebate_rate
  FROM perf p
  LEFT JOIN gtn g
    ON g.year  = p.year
   AND g.month = p.month
   AND g.brand = p.brand
   AND g.canton= p.canton
)
SELECT
  make_date(year, month, 1) AS period,
  brand,
  canton,
  units::double precision,
  promo_spend::double precision,
  rebate_rate::double precision
FROM m
ORDER BY brand, canton, period;
"""
df = load_df(data_sql)
if df.empty:
    st.info("No data found in marts. Run the generator and `dbt build` first.")
    st.stop()

# Ensure pandas datetime (fixes .to_pydatetime errors)
df["period"] = pd.to_datetime(df["period"])

# --- Sidebar controls ---------------------------------------------------------
brands = sorted(df["brand"].dropna().unique().tolist())
cantons = sorted(df["canton"].dropna().unique().tolist())

scope = st.sidebar.selectbox("Scope", ["Global", "Brand", "Brand + Canton"], index=0)

sel_brand = None
sel_canton = None
if scope in ("Brand", "Brand + Canton"):
    sel_brand = st.sidebar.selectbox("Brand", options=brands)
if scope == "Brand + Canton":
    c_opts = sorted(
        df.loc[df["brand"] == sel_brand, "canton"].dropna().unique().tolist()
    )
    sel_canton = st.sidebar.selectbox("Canton", options=c_opts or cantons)

# Filter dataset to scope
scoped = df.copy()
if sel_brand:
    scoped = scoped[scoped["brand"] == sel_brand]
if sel_canton:
    scoped = scoped[scoped["canton"] == sel_canton]

if scoped.empty:
    st.warning("No rows for the selected scope.")
    st.stop()

# Training window slider (uses Python datetimes)
pmin, pmax = scoped["period"].min(), scoped["period"].max()
# Default: last 12 months (clamped to available range)
default_start = max(pmin, pmax - pd.offsets.MonthBegin(12))
train_range = st.sidebar.slider(
    "Training window",
    min_value=pmin.to_pydatetime(),
    max_value=pmax.to_pydatetime(),
    value=(default_start.to_pydatetime(), pmax.to_pydatetime()),
    format="YYYY-MM",
)

# --- Baseline (rolling 4M, no leakage) --------------------------------------
scoped = scoped.sort_values(["brand", "canton", "period"]).reset_index(drop=True)
grp_keys = ["brand", "canton"]
scoped["baseline_units"] = scoped.groupby(grp_keys, dropna=False)["units"].transform(
    lambda s: s.shift(1).rolling(4, min_periods=1).mean()
)

# Features & target
scoped["rebate_pressure"] = scoped["rebate_rate"] * scoped["units"]
scoped["uplift_actual"] = scoped["units"] - scoped["baseline_units"]

# Train/valid split
t_start = pd.to_datetime(train_range[0])
t_end = pd.to_datetime(train_range[1])
train = scoped.loc[(scoped["period"] >= t_start) & (scoped["period"] <= t_end)]
valid = scoped.loc[(scoped["period"] > t_end)]

train = train.dropna(subset=["baseline_units"]).copy()
valid = valid.dropna(subset=["baseline_units"]).copy()


def fit_alpha_beta(df_fit: pd.DataFrame):
    X1 = df_fit["promo_spend"].to_numpy(dtype=float)
    X2 = df_fit["rebate_pressure"].to_numpy(dtype=float)
    y = df_fit["uplift_actual"].to_numpy(dtype=float)
    X = np.c_[X1, X2]

    if len(df_fit) < 12 or np.allclose(X, 0):
        return 0.003, -0.5, False

    try:
        theta, *_ = np.linalg.lstsq(X, y, rcond=None)
        a_hat, b_hat = float(theta[0]), float(theta[1])
        # Optional, sensible signs
        a_hat = max(0.0, a_hat)
        b_hat = min(0.0, b_hat)
        return a_hat, b_hat, True
    except Exception:
        return 0.003, -0.5, False


alpha_hat, beta_hat, fitted_ok = fit_alpha_beta(train)
if not fitted_ok:
    st.warning("Fallback α/β used (not enough signal in the selected training window).")

# Allow manual override via sliders
alpha_sel = st.sidebar.slider("α (promo impact)", 0.0, 0.02, float(alpha_hat), 0.0001)
beta_sel = st.sidebar.slider("β (rebate effect)", -1.0, 0.0, float(beta_hat), 0.01)


# --- Recompute forecast with selected α/β ------------------------------------
def forecast_with_params(df: pd.DataFrame, a: float, b: float) -> pd.DataFrame:
    out = df.copy()
    out["uplift_pred"] = a * out["promo_spend"] + b * out["rebate_pressure"]
    out["uplift_pred"] = out["uplift_pred"].clip(
        lower=-0.4 * out["units"], upper=0.5 * out["units"]
    )
    out["forecast_units"] = np.maximum(0.0, out["baseline_units"] + out["uplift_pred"])
    return out


scoped_fc = forecast_with_params(scoped, alpha_sel, beta_sel)


# --- Metrics -----------------------------------------------------------------
def mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    mask = y_true > 0
    if mask.sum() == 0:
        return np.nan
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask]) * 100.0))


def smape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = np.abs(y_true) + np.abs(y_pred)
    mask = denom > 0
    if mask.sum() == 0:
        return np.nan
    return float(
        np.mean(2.0 * np.abs(y_true[mask] - y_pred[mask]) / denom[mask] * 100.0)
    )


train_eval = scoped_fc.loc[
    (scoped_fc["period"] >= t_start) & (scoped_fc["period"] <= t_end)
]
valid_eval = scoped_fc.loc[(scoped_fc["period"] > t_end)]

c1, c2, c3 = st.columns(3)
c1.metric("α (selected)", f"{alpha_sel:.5f}")
c2.metric("β (selected)", f"{beta_sel:.5f}")
c3.metric("# Train rows", f"{len(train_eval):,}")

c4, c5 = st.columns(2)
c4.metric(
    "MAPE (Train)",
    f"{mape(train_eval['units'].to_numpy(), train_eval['forecast_units'].to_numpy()):.1f}%",
)
c5.metric(
    "MAPE (Valid)",
    f"{mape(valid_eval['units'].to_numpy(), valid_eval['forecast_units'].to_numpy()):.1f}%",
)

# --- Plot --------------------------------------------------------------------
st.subheader("Forecast vs Actuals (scoped selection)")
show = (
    scoped_fc.groupby("period", as_index=False)[["units", "forecast_units"]]
    .sum()
    .sort_values("period")
)
fig = px.line(show, x="period", y=["units", "forecast_units"], markers=True)
fig.update_layout(legend_title_text="")
st.plotly_chart(fig, use_container_width=True)

st.caption(
    "Baseline = rolling mean of last 4 months (excludes current). "
    "Forecast = baseline + α·promo_spend + β·(rebate_rate·units), clipped and non-negative."
)

# --- Save parameters ----------------------------------------------------------
scope_level = "global"
k1, k2 = None, None
if scope == "Brand":
    scope_level, k1 = "brand", sel_brand
elif scope == "Brand + Canton":
    scope_level, k1, k2 = "brand_canton", sel_brand, sel_canton

if st.button("💾 Save α/β for this scope"):
    upsert_params(scope_level, k1, k2, alpha_sel, beta_sel, t_start, t_end)
    st.success(f"Saved α/β for scope: {scope_level} | {k1 or '∅'} | {k2 or '∅'}")
