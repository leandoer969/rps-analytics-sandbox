# pages/06_Forecast_Calibration.py
import os
import time
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

from lib.forecast import (
    add_features,
    fit_ols,
    fit_bounded,
    fit_grid_mape,
    forecast_with_params,
    mape,
    smape,
    wape,
)

st.set_page_config(page_title="Forecast Calibration", layout="wide")
st.title("🔧 Forecast Calibration (α / β)")

# -------------------- DB ----------------------
DB = os.getenv("POSTGRES_DB", "rps")
USER = os.getenv("POSTGRES_USER", "rps_user")
PWD = os.getenv("POSTGRES_PASSWORD", "rps_password")
HOST = os.getenv("POSTGRES_HOST", "postgres")
PORT = os.getenv("POSTGRES_PORT", "5432")
ENGINE = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")


@st.cache_resource
def get_engine():
    return ENGINE


@st.cache_data(ttl=300)
def load_df(sql: str, params=None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


# ---------- Key canonicalization (avoid NULLs in PK) ----------
S_GLOBAL = "__GLOBAL__"
S_ALL = "__ALL__"


def canonical_keys(scope_level: str, k1, k2):
    if scope_level == "global":
        return (S_GLOBAL, S_GLOBAL)
    if scope_level == "brand":
        return (str(k1), S_ALL)
    return (str(k1), str(k2))


def ensure_params_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS rps.forecast_params (
        scope_level TEXT NOT NULL,
        scope_key1  TEXT NOT NULL,
        scope_key2  TEXT NOT NULL,
        alpha       DOUBLE PRECISION NOT NULL,
        beta        DOUBLE PRECISION NOT NULL,
        train_start DATE NOT NULL,
        train_end   DATE NOT NULL,
        updated_at  TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
        fit_method  TEXT,
        promo_lag   INTEGER,
        PRIMARY KEY (scope_level, scope_key1, scope_key2)
    );
    """
    with get_engine().begin() as conn:
        conn.execute(text(ddl))


def upsert_params(
    scope_level, k1, k2, alpha, beta, t_start, t_end, fit_method, promo_lag
):
    ensure_params_table()
    key1, key2 = canonical_keys(scope_level, k1, k2)
    sql = """
    INSERT INTO rps.forecast_params
      (scope_level, scope_key1, scope_key2, alpha, beta, train_start, train_end, updated_at, fit_method, promo_lag)
    VALUES
      (:scope_level, :k1, :k2, :alpha, :beta, :train_start, :train_end, now(), :fit_method, :promo_lag)
    ON CONFLICT (scope_level, scope_key1, scope_key2)
    DO UPDATE SET
      alpha = EXCLUDED.alpha,
      beta  = EXCLUDED.beta,
      train_start = EXCLUDED.train_start,
      train_end   = EXCLUDED.train_end,
      updated_at  = now(),
      fit_method  = EXCLUDED.fit_method,
      promo_lag   = EXCLUDED.promo_lag;
    """
    with get_engine().begin() as conn:
        conn.execute(
            text(sql),
            {
                "scope_level": scope_level,
                "k1": key1,
                "k2": key2,
                "alpha": float(alpha),
                "beta": float(beta),
                "train_start": pd.to_datetime(t_start).date(),
                "train_end": pd.to_datetime(t_end).date(),
                "fit_method": fit_method,
                "promo_lag": int(promo_lag),
            },
        )


def load_saved_params(scope_level, k1, k2):
    ensure_params_table()
    key1, key2 = canonical_keys(scope_level, k1, k2)
    sql = """
      SELECT alpha, beta, train_start, train_end, fit_method, promo_lag, updated_at
      FROM rps.forecast_params
      WHERE scope_level = :scope_level
        AND scope_key1 = :k1
        AND scope_key2 = :k2
      ORDER BY updated_at DESC
      LIMIT 1;
    """
    with get_engine().connect() as conn:
        row = (
            conn.execute(
                text(sql), {"scope_level": scope_level, "k1": key1, "k2": key2}
            )
            .mappings()
            .first()
        )
    return dict(row) if row else None


# -------------------- Data --------------------
DATA_SQL = """
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
df = load_df(DATA_SQL)
if df.empty:
    st.info("No data found in marts. Run the generator and `dbt build` first.")
    st.stop()
df["period"] = pd.to_datetime(df["period"])

brands = sorted(df["brand"].dropna().unique().tolist())
cantons = sorted(df["canton"].dropna().unique().tolist())

# -------------------- Scope selection --------------------
st.sidebar.header("Scope")
scope = st.sidebar.selectbox(
    "Level", ["Global", "Brand", "Brand + Canton"], index=0, key="scope_level_select"
)
sel_brand = (
    st.sidebar.selectbox("Brand", options=brands, key="scope_brand_select")
    if scope in ("Brand", "Brand + Canton")
    else None
)
if scope == "Brand + Canton":
    c_opts = sorted(
        df.loc[df["brand"] == sel_brand, "canton"].dropna().unique().tolist()
    )
    sel_canton = st.sidebar.selectbox(
        "Canton", options=c_opts or cantons, key="scope_canton_select"
    )
else:
    sel_canton = None


def scope_keys_human():
    if scope == "Global":
        return "global", None, None
    if scope == "Brand":
        return "brand", sel_brand, None
    return "brand_canton", sel_brand, sel_canton


# Filter to scope
scoped = df.copy()
if sel_brand:
    scoped = scoped[scoped["brand"] == sel_brand]
if sel_canton:
    scoped = scoped[scoped["canton"] == sel_canton]
if scoped.empty:
    st.warning("No rows for the selected scope.")
    st.stop()

# -------------------- Non-widget session state --------------------
st.session_state.setdefault("active_params", None)
st.session_state.setdefault("fit_candidate", None)
st.session_state.setdefault("scope_sig", None)
st.session_state.setdefault("auto_load", True)  # preference

# Auto-load toggle (widget owns its value; we read it, no manual writes)
st.sidebar.toggle("Auto-load saved on scope change", key="auto_load")

lvl_h, k1_h, k2_h = scope_keys_human()
current_sig = f"{lvl_h}|{k1_h}|{k2_h}"
if st.session_state.scope_sig != current_sig:
    st.session_state.scope_sig = current_sig
    if st.session_state.auto_load:
        saved = load_saved_params(lvl_h, k1_h, k2_h)
        st.session_state.active_params = (
            {
                "alpha": float(saved["alpha"]),
                "beta": float(saved["beta"]),
                "promo_lag": int(saved.get("promo_lag", 0)),
                "fit_method": saved.get("fit_method", "loaded"),
                "train_start": pd.to_datetime(saved["train_start"]),
                "train_end": pd.to_datetime(saved["train_end"]),
                "provenance": "Loaded from DB (auto)",
            }
            if saved
            else None
        )
    else:
        st.session_state.active_params = None


# -------------------- Callbacks (no widget-key mutation) --------------------
def cb_set_active_from_fit():
    cand = st.session_state.get("fit_candidate")
    if not cand:
        st.warning("No fit candidate available. Run a fit first.")
        return
    st.session_state.active_params = {**cand, "provenance": "Fit"}
    st.toast("Active parameters updated from Fit.", icon="✅")


def cb_set_active_from_manual(alpha, beta, promo_lag):
    st.session_state.active_params = {
        "alpha": float(alpha),
        "beta": float(beta),
        "promo_lag": int(promo_lag),
        "fit_method": "manual",
        "train_start": None,
        "train_end": None,
        "provenance": "Manual",
    }
    st.toast("Active parameters updated from Manual.", icon="✅")


def cb_load_saved_for_scope(scope_level, k1, k2):
    saved = load_saved_params(scope_level, k1, k2)
    if saved:
        st.session_state.active_params = {
            "alpha": float(saved["alpha"]),
            "beta": float(saved["beta"]),
            "promo_lag": int(saved.get("promo_lag", 0)),
            "fit_method": saved.get("fit_method", "loaded"),
            "train_start": pd.to_datetime(saved["train_start"]),
            "train_end": pd.to_datetime(saved["train_end"]),
            "provenance": "Loaded from DB",
        }
        st.toast("Loaded saved parameters.", icon="📥")
    else:
        st.warning("No saved parameters for this scope.")


def cb_reset_active():
    st.session_state.active_params = None
    st.toast("Active parameters cleared.", icon="🧹")


def cb_save_active(scope_level, k1, k2):
    ap = st.session_state.get("active_params")
    if not ap:
        st.warning("No active parameters to save.")
        return
    upsert_params(
        scope_level,
        k1,
        k2,
        ap["alpha"],
        ap["beta"],
        ap.get("train_start", pd.Timestamp("1900-01-01")),
        ap.get("train_end", pd.Timestamp("1900-01-01")),
        ap.get("fit_method", "manual"),
        ap.get("promo_lag", 0),
    )
    st.toast("Saved parameters for scope.", icon="💾")


def cb_run_fit(
    scoped_df, fit_method, promo_lag, weight_wls, train_range, a_max, b_min, steps
):
    t0 = time.time()
    feats = add_features(scoped_df, promo_lag=promo_lag)
    t_start = pd.to_datetime(train_range[0])
    t_end = pd.to_datetime(train_range[1])
    train_df = feats[(feats["period"] >= t_start) & (feats["period"] <= t_end)].dropna(
        subset=["baseline_units", "promo_feat"]
    )
    w_train = None
    if weight_wls and not train_df.empty:
        w_train = np.sqrt(np.clip(train_df["units"].to_numpy(float), 1.0, None))

    if fit_method == "OLS (least squares)":
        a_hat, b_hat, ok = fit_ols(train_df, w_train)
    elif fit_method == "Bounded LS (SciPy)":
        a_hat, b_hat, ok = fit_bounded(train_df, w_train)
    else:
        a_hat, b_hat, ok = fit_grid_mape(
            train_df, a_max=a_max, b_min=b_min, steps=steps
        )

    elapsed = time.time() - t0
    st.session_state.fit_candidate = {
        "alpha": float(a_hat),
        "beta": float(b_hat),
        "promo_lag": int(promo_lag),
        "fit_method": fit_method,
        "train_start": t_start,
        "train_end": t_end,
        "elapsed": elapsed,
        "weighted": bool(weight_wls),
    }
    st.toast("Fit complete", icon="✅")


# -------------------- Tabs: Fit vs Manual --------------------
tabs = st.tabs(["🎯 Fit parameters", "✍️ Manual parameters"])

# --- FIT TAB (reactive; no form; widgets own state) ---
with tabs[0]:
    # Availability of SciPy-based method
    bounded_available = True
    try:
        import scipy  # noqa: F401
    except Exception:
        bounded_available = False

    fit_choices = ["OLS (least squares)", "Grid search (min MAPE)"]
    if bounded_available:
        fit_choices.insert(1, "Bounded LS (SciPy)")

    pmin, pmax = scoped["period"].min(), scoped["period"].max()
    default_start = max(pmin, pmax - pd.offsets.MonthBegin(12))

    # Read (or default) widget values via locals, not session assignments
    c1, c2, c3 = st.columns(3)
    with c1:
        fit_method = st.selectbox(
            "Fit method", fit_choices, index=0, key="fit_method_select"
        )
        promo_lag = st.selectbox(
            "Promo lag (months)", [0, 1], index=0, key="fit_promo_lag"
        )
    with c2:
        weight_wls = st.checkbox("Weighted LS", value=False, key="fit_weight_wls")
        train_range = st.slider(
            "Training window",
            min_value=pmin.to_pydatetime(),
            max_value=pmax.to_pydatetime(),
            value=(default_start.to_pydatetime(), pmax.to_pydatetime()),
            format="YYYY-MM",
            key="fit_train_range",
        )
    with c3:
        if fit_method.startswith("Grid"):
            a_max = st.number_input(
                "α max",
                min_value=0.001,
                max_value=0.1,
                value=0.02,
                step=0.001,
                format="%.3f",
                key="fit_a_max",
            )
            b_min = st.number_input(
                "β min",
                min_value=-5.0,
                max_value=-0.001,
                value=-1.0,
                step=0.05,
                format="%.3f",
                key="fit_b_min",
            )
            steps = st.slider("Grid steps", 11, 151, 51, 10, key="fit_grid_steps")
        else:
            st.caption("Grid options hidden (method ≠ Grid).")
            a_max, b_min, steps = 0.02, -1.0, 51  # ignored

    st.button(
        "▶ Run fit",
        key="btn_run_fit",
        use_container_width=True,
        on_click=cb_run_fit,
        kwargs=dict(
            scoped_df=scoped,
            fit_method=fit_method,
            promo_lag=promo_lag,
            weight_wls=weight_wls,
            train_range=train_range,
            a_max=a_max,
            b_min=b_min,
            steps=steps,
        ),
    )

    if st.session_state.fit_candidate is not None:
        cand = st.session_state.fit_candidate
        st.markdown("#### Candidate result (from last fit)")
        c1m, c2m, c3m, c4m = st.columns(4)
        c1m.metric("α", f"{cand['alpha']:.5f}")
        c2m.metric("β", f"{cand['beta']:.5f}")
        c3m.metric("Promo lag", f"{cand['promo_lag']} mo")
        c4m.metric("Fit time", f"{cand['elapsed']:.2f}s")

        feats = add_features(scoped, promo_lag=cand["promo_lag"])
        fc = forecast_with_params(feats, cand["alpha"], cand["beta"])
        mask = (fc["period"] >= cand["train_start"]) & (
            fc["period"] <= cand["train_end"]
        )
        train_eval = fc[mask]
        valid_eval = fc[~mask & (fc["period"] > cand["train_end"])]

        def summarize(df_eval):
            if df_eval.empty:
                return {"rows": 0, "MAPE": np.nan, "SMAPE": np.nan, "WAPE": np.nan}
            y, yhat = (
                df_eval["units"].to_numpy(float),
                df_eval["forecast_units"].to_numpy(float),
            )
            return {
                "rows": len(df_eval),
                "MAPE": mape(y, yhat),
                "SMAPE": smape(y, yhat),
                "WAPE": wape(y, yhat),
            }

        mt, mv = summarize(train_eval), summarize(valid_eval)
        tcol, vcol = st.columns(2)
        with tcol:
            st.caption("Train")
            st.metric("Rows", f"{mt['rows']:,}")
            st.metric("MAPE", f"{mt['MAPE']:.1f}%" if pd.notna(mt["MAPE"]) else "—")
            st.metric("SMAPE", f"{mt['SMAPE']:.1f}%" if pd.notna(mt["SMAPE"]) else "—")
            st.metric("WAPE", f"{mt['WAPE']:.1f}%" if pd.notna(mt["WAPE"]) else "—")
        with vcol:
            st.caption("Validation (post-train window)")
            st.metric("Rows", f"{mv['rows']:,}")
            st.metric("MAPE", f"{mv['MAPE']:.1f}%" if pd.notna(mv["MAPE"]) else "—")
            st.metric("SMAPE", f"{mv['SMAPE']:.1f}%" if pd.notna(mv["SMAPE"]) else "—")
            st.metric("WAPE", f"{mv['WAPE']:.1f}%" if pd.notna(mv["WAPE"]) else "—")

        st.button(
            "✅ Set Active from Fit",
            key="btn_set_active_fit",
            use_container_width=True,
            on_click=cb_set_active_from_fit,
        )

# --- MANUAL TAB ---
with tabs[1]:
    mc1, mc2, mc3 = st.columns(3)
    alpha_manual = mc1.slider("α", 0.0, 0.05, 0.003, 0.0001, key="manual_alpha")
    beta_manual = mc2.slider("β", -2.0, 0.0, -0.5, 0.01, key="manual_beta")
    promo_lag_manual = mc3.selectbox(
        "Promo lag (months)", [0, 1], index=0, key="manual_lag"
    )
    st.button(
        "✅ Set Active from Manual",
        key="btn_set_active_manual",
        use_container_width=True,
        on_click=cb_set_active_from_manual,
        kwargs={
            "alpha": alpha_manual,
            "beta": beta_manual,
            "promo_lag": promo_lag_manual,
        },
    )

# -------------------- Forecast using ACTIVE ONLY + Active params card (at bottom) --------------------
chart_col, active_col = st.columns([3, 1.2], gap="large")

with chart_col:
    st.subheader("Forecast vs Actuals (uses ACTIVE parameters only)")
    ap = st.session_state.active_params
    if ap is None:
        st.info("Set Active parameters (from Fit or Manual) to view forecast.")
    else:
        feats = add_features(scoped, promo_lag=ap["promo_lag"])
        show = (
            forecast_with_params(feats, ap["alpha"], ap["beta"])
            .groupby("period", as_index=False)[["units", "forecast_units"]]
            .sum()
        )
        fig = px.line(
            show.sort_values("period"),
            x="period",
            y=["units", "forecast_units"],
            markers=True,
        )
        fig.update_layout(legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)

with active_col:
    st.subheader("Active Parameters")
    ap = st.session_state.active_params
    if ap is None:
        st.info("No active parameters yet. Fit or set manual, or load saved for scope.")
    else:
        st.metric("α", f"{ap['alpha']:.5f}")
        st.metric("β", f"{ap['beta']:.5f}")
        st.caption(
            f"Promo lag: {ap.get('promo_lag', 0)} mo | Source: {ap.get('provenance', '')}"
        )
        if ap.get("train_start") is not None and ap.get("train_end") is not None:
            st.caption(
                f"Train window: {ap['train_start'].date()} → {ap['train_end'].date()}"
            )

    scope_label = {
        "global": "Global",
        "brand": f"Brand={k1_h}",
        "brand_canton": f"Brand={k1_h}, Canton={k2_h}",
    }[lvl_h]

    b1, b2, b3 = st.columns(3)
    b1.button(
        f"📥 Load saved for scope ({scope_label})",
        key="btn_load_saved",
        use_container_width=True,
        on_click=cb_load_saved_for_scope,
        kwargs={"scope_level": lvl_h, "k1": k1_h, "k2": k2_h},
    )
    b2.button(
        "🧹 Reset Active",
        key="btn_reset_active",
        use_container_width=True,
        on_click=cb_reset_active,
    )
    b3.button(
        "💾 Save Active to DB",
        key="btn_save_active",
        use_container_width=True,
        on_click=cb_save_active,
        kwargs={"scope_level": lvl_h, "k1": k1_h, "k2": k2_h},
    )

st.caption(
    "Baseline = rolling mean of last 4 months (excludes current). "
    "Forecast = baseline + α·promo + β·(rebate_rate·units), clipped and non-negative. "
    "Active parameters drive the forecast, independent of what’s shown in tabs."
)
