# lib/forecast.py
# Small forecasting helpers used by Streamlit pages.

from __future__ import annotations
import numpy as np
import pandas as pd


# ---------- Baseline & features ----------
def add_baseline(
    df: pd.DataFrame, group_cols=("brand", "canton"), y="units", window=4
) -> pd.DataFrame:
    out = df.sort_values([*group_cols, "period"]).copy()
    out["baseline_units"] = out.groupby(list(group_cols), dropna=False)[y].transform(
        lambda s: s.shift(1).rolling(window, min_periods=1).mean()
    )
    return out


def lag_within_groups(
    df: pd.DataFrame, col: str, lag: int, group_cols=("brand", "canton")
) -> pd.Series:
    if lag == 0:
        return df[col]
    return df.groupby(list(group_cols), dropna=False)[col].transform(
        lambda s: s.shift(lag)
    )


def add_features(
    df: pd.DataFrame, promo_lag: int = 0, group_cols=("brand", "canton")
) -> pd.DataFrame:
    out = add_baseline(df, group_cols=group_cols, y="units", window=4)
    out["promo_feat"] = lag_within_groups(out, "promo_spend", promo_lag, group_cols)
    out["rebate_pressure"] = out["rebate_rate"] * out["units"]
    out["uplift_actual"] = out["units"] - out["baseline_units"]
    return out


# ---------- Metrics ----------
def mape(y_true, y_pred) -> float:
    y_true = np.asarray(y_true, float)
    y_pred = np.asarray(y_pred, float)
    m = y_true > 0
    return (
        np.nan
        if m.sum() == 0
        else float(np.mean(np.abs((y_true[m] - y_pred[m]) / y_true[m]) * 100.0))
    )


def smape(y_true, y_pred) -> float:
    y_true = np.asarray(y_true, float)
    y_pred = np.asarray(y_pred, float)
    denom = np.abs(y_true) + np.abs(y_pred)
    m = denom > 0
    return (
        np.nan
        if m.sum() == 0
        else float(np.mean(2.0 * np.abs(y_true[m] - y_pred[m]) / denom[m] * 100.0))
    )


def wape(y_true, y_pred) -> float:
    y_true = np.asarray(y_true, float)
    y_pred = np.asarray(y_pred, float)
    denom = np.sum(np.abs(y_true))
    return (
        np.nan if denom == 0 else float(np.sum(np.abs(y_true - y_pred)) / denom * 100.0)
    )


# ---------- Fitting ----------
def fit_ols(train_df: pd.DataFrame, weights=None):
    X1 = train_df["promo_feat"].to_numpy(float)
    X2 = train_df["rebate_pressure"].to_numpy(float)
    y = train_df["uplift_actual"].to_numpy(float)
    X = np.c_[X1, X2]
    if weights is not None:
        w = np.asarray(weights, float)
        X = X * w[:, None]
        y = y * w
    if len(train_df) < 6 or np.allclose(X, 0):
        return 0.003, -0.5, False
    theta, *_ = np.linalg.lstsq(X, y, rcond=None)
    return float(theta[0]), float(theta[1]), True


def fit_bounded(train_df: pd.DataFrame, weights=None):
    """α ≥ 0, β ≤ 0; falls back to OLS if SciPy missing."""
    try:
        from scipy.optimize import lsq_linear  # optional
    except Exception:
        a, b, ok = fit_ols(train_df, weights)
        return max(0.0, a), min(0.0, b), ok

    X1 = train_df["promo_feat"].to_numpy(float)
    X2 = train_df["rebate_pressure"].to_numpy(float)
    y = train_df["uplift_actual"].to_numpy(float)
    X = np.c_[X1, X2]
    if weights is not None:
        w = np.asarray(weights, float)
        X = X * w[:, None]
        y = y * w
    if len(train_df) < 6 or np.allclose(X, 0):
        return 0.003, -0.5, False

    res = lsq_linear(X, y, bounds=([0.0, -np.inf], [np.inf, 0.0]))
    return float(res.x[0]), float(res.x[1]), True


def fit_grid_mape(train_df: pd.DataFrame, a_max=0.02, b_min=-1.0, steps=51):
    """Brute-force grid minimizing MAPE on the TRAIN slice."""
    if len(train_df) < 6:
        return 0.003, -0.5, False
    a_grid = np.linspace(0.0, float(a_max), int(steps))
    b_grid = np.linspace(float(b_min), 0.0, int(steps))

    promo = train_df["promo_feat"].to_numpy(float)
    press = train_df["rebate_pressure"].to_numpy(float)
    base = train_df["baseline_units"].to_numpy(float)
    act = train_df["units"].to_numpy(float)

    best = (0.003, -0.5, np.inf)
    for a in a_grid:
        ap = a * promo
        for b in b_grid:
            uplift = np.clip(ap + b * press, -0.4 * act, 0.5 * act)
            fc = np.maximum(0.0, base + uplift)
            err = mape(act, fc)
            if err < best[2]:
                best = (float(a), float(b), float(err))
    return best[0], best[1], True


# ---------- Forecasting ----------
def forecast_with_params(df: pd.DataFrame, a: float, b: float) -> pd.DataFrame:
    out = df.copy()
    out["uplift_pred"] = a * out["promo_feat"] + b * out["rebate_pressure"]
    out["uplift_pred"] = out["uplift_pred"].clip(
        lower=-0.4 * out["units"], upper=0.5 * out["units"]
    )
    out["forecast_units"] = np.maximum(0.0, out["baseline_units"] + out["uplift_pred"])
    return out
