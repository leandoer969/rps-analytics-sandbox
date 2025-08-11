# Business Analytics Metrics & Forecasting — Pocket Guide

## Table of Contents

- [1) Core Commercial & Financial KPIs](#1-core-commercial-financial-kpis)
  - [Revenue & Margin](#revenue-margin)
  - [Net Sales & Deductions](#net-sales-deductions)
  - [Growth & Mix](#growth-mix)
  - [Customer & Product KPIs](#customer-product-kpis)
  - [Supply & Service](#supply-service)
- [2) Forecast Accuracy & Diagnostics](#2-forecast-accuracy-diagnostics)
- [3) Time Series & Forecasting Methods (When/Why)](#3-time-series-forecasting-methods-whenwhy)
  - [Baselines](#baselines)
  - [Smoothing](#smoothing)
  - [ARIMA / SARIMA](#arima-sarima)
  - [Regression with Exogenous Regressors (Rex/ARIMAX)](#regression-with-exogenous-regressors-rexarimax)
  - [ML Methods](#ml-methods)
  - [Intermittent Demand](#intermittent-demand)
  - [Hierarchical & Reconciliation](#hierarchical-reconciliation)
- [4) Feature Engineering (cheat list)](#4-feature-engineering-cheat-list)
- [5) Data Quality & Guardrails](#5-data-quality-guardrails)
- [6) Ready-to-paste Utilities](#6-ready-to-paste-utilities)
- [7) Production Notes](#7-production-notes)

**Scope:** General KPIs (finance, growth, product, supply), how to compute them (formulas + SQL), plus a concise tour of forecasting methods and model validation. Vendor-agnostic.

---

## 1) Core Commercial & Financial KPIs

### Revenue & Margin

- **Revenue** = Σ `price * quantity`
- **COGS** = Σ cost of goods sold
- **Gross Margin (CHF)** = Revenue − COGS
- **Gross Margin %** = (Revenue − COGS) / NULLIF(Revenue, 0)

**SQL (monthly):**

```sql
SELECT
  date_trunc('month', order_ts)::date AS month_start,
  SUM(price * qty) AS revenue,
  SUM(cogs) AS cogs,
  SUM(price * qty) - SUM(cogs) AS gross_margin_chf,
  CASE WHEN SUM(price * qty)=0 THEN NULL
       ELSE (SUM(price * qty) - SUM(cogs)) / SUM(price * qty) END AS gross_margin_pct
FROM fct_orders
GROUP BY 1
ORDER BY 1;
```

### Net Sales & Deductions

- **Net Sales** = Gross Sales − Deductions (rebates, discounts, returns).
- **Net Sales %** = Net Sales / NULLIF(Gross Sales, 0).

```sql
SELECT
  month_start,
  SUM(gross_sales_chf) AS gross,
  SUM(rebates_chf + discounts_chf + returns_chf) AS deductions,
  SUM(gross_sales_chf) - SUM(rebates_chf + discounts_chf + returns_chf) AS net
FROM mart_gtn -- a monthly mart
GROUP BY 1;
```

### Growth & Mix

- **MoM Growth** = (This − Last) / Last
- **YoY Growth** = (This_Yr_Same_Month − Last_Yr_Same_Month) / Last_Yr_Same_Month
- **Mix %** (share of total) = Segment / Σ Segment

```sql
WITH m AS (
  SELECT date_trunc('month', ts)::date AS month_start, segment, SUM(metric) AS x
  FROM fct_events GROUP BY 1,2
)
SELECT
  month_start, segment, x,
  LAG(x) OVER (PARTITION BY segment ORDER BY month_start) AS prev_x,
  CASE WHEN LAG(x) OVER (PARTITION BY segment ORDER BY month_start) IN (0,NULL) THEN NULL
       ELSE x / LAG(x) OVER (PARTITION BY segment ORDER BY month_start) - 1 END AS mom_growth,
  x / NULLIF(SUM(x) OVER (PARTITION BY month_start), 0) AS mix_pct
FROM m
ORDER BY segment, month_start;
```

### Customer & Product KPIs

- **ARPU** = Revenue / Active Users
- **CAC** = (Sales + Marketing Spend) / New Customers
- **LTV (simplified)** = ARPU × Gross Margin % × Avg Customer Lifetime (periods)
- **Conversion Rate** = Conversions / Visitors
- **Churn Rate** = Churned Customers / Starting Customers

### Supply & Service

- **Fill Rate** = Shipped / Ordered
- **Service Level** = 1 − Stockouts / Opportunities
- **Days of Supply** = Inventory_on_hand / Avg_daily_demand

```sql
-- 7-day rolling demand and DoS
WITH d AS (
  SELECT date::date AS d, product_id, SUM(units) AS units
  FROM fct_shipments GROUP BY 1,2
),
r AS (
  SELECT d, product_id, units,
         AVG(units) OVER (PARTITION BY product_id ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_7d
  FROM d
)
SELECT d, product_id, inventory_on_hand,
       CASE WHEN COALESCE(avg_7d,0)=0 THEN NULL ELSE inventory_on_hand/avg_7d END AS days_of_supply
FROM inv_daily JOIN r USING (d, product_id);
```

---

## 2) Forecast Accuracy & Diagnostics

- **MAE** = AVG(|y − ŷ|)
- **RMSE** = sqrt(AVG((y − ŷ)^2))
- **MAPE** = AVG(|y − ŷ| / NULLIF(y, 0))
- **sMAPE** = AVG( |y − ŷ| / ((|y| + |ŷ|)/2) )
- **MASE** = MAE / MAE_naive (scale-free)
- **Bias (ME)** = AVG(y − ŷ)

**SQL (monthly, actual vs forecast):**

```sql
SELECT
  month_start,
  AVG(ABS(actual - forecast)) AS mae,
  SQRT(AVG(POWER(actual - forecast, 2))) AS rmse,
  AVG(ABS(actual - forecast) / NULLIF(actual, 0)) AS mape,
  AVG(actual - forecast) AS bias
FROM mart_forecast_accuracy
GROUP BY 1
ORDER BY 1;
```

**Backtesting tips**

- Use **time-series CV**: rolling origin or expanding window.
- Keep validation periods representative (seasonality, events).
- Track **stability** across windows, not just one split.

---

## 3) Time Series & Forecasting Methods (When/Why)

### Baselines

- **Naïve** (last value), **Seasonal Naïve** (last year’s same period).
  - Pros: simple, competitive; Cons: no trend/causal info.

### Smoothing

- **Moving Average** (window k): good for noise reduction.
- **Exponential Smoothing (SES)**: \( \hat{y}_t = lpha y_{t-1} + (1-\alpha)\hat{y}\_{t-1} \)
- **Holt (trend)** and **Holt–Winters (trend+seasonality)**: handles drift and seasonality.

### ARIMA / SARIMA

- For stationary (SARIMA for seasonality). Needs differencing & diagnostics (ACF/PACF).
- Pros: strong univariate benchmarks; Cons: less interpretable for promos/price.

### Regression with Exogenous Regressors (Rex/ARIMAX)

- Linear or regularized regression with **promo, price, rebate rate, events, holidays**.
- Add **lags/rolling features** of target and drivers.
- Pros: interpretable; Cons: leakage risk if you use future covariates.

### ML Methods

- **Gradient Boosting / Random Forest / XGBoost / LightGBM** with engineered features.
- **Neural nets** (RNN/LSTM, Temporal Fusion Transformer) for complex patterns.
- Pros: flexible; Cons: need more data, careful validation.

### Intermittent Demand

- **Croston**, **SBA**, **TSB** for sparse sales (many zeros).

### Hierarchical & Reconciliation

- Products → Brands → Regions: fit at multiple levels; reconcile with **MinT** or top-down/bottom-up rules.

**When to choose what**

- Short history, clear seasonality → Holt–Winters.
- Strong causal drivers → regression with exogenous vars.
- Many series, limited signal per series → pooled ML with shared features.
- Need explainability → regression/smoothing baselines.

---

## 4) Feature Engineering (cheat list)

- **Calendrical:** month, dow, holiday, end-of-quarter, school breaks.
- **Lags:** y*{t-1}, y*{t-4}, y\_{t-12}.
- **Rollings:** mean/median over 3/4/12 windows (use past only).
- **Price/Promo:** promo flag & spend, rebate rate, discount depth.
- **Censoring/stockouts:** zero-truncate or flags to avoid bias.
- **Interactions:** promo × season, price × channel.

**SQL lag/rolling example**

```sql
SELECT
  month_start,
  brand,
  net_sales,
  LAG(net_sales) OVER (PARTITION BY brand ORDER BY month_start) AS lag_1,
  AVG(net_sales) OVER (PARTITION BY brand ORDER BY month_start
                       ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ma_3
FROM brand_monthly;
```

---

## 5) Data Quality & Guardrails

- **Grain alignment:** aggregate daily facts to monthly before joining monthly drivers.
- **No lookahead:** only past information in features.
- **Duplicates:** de-dupe with `ROW_NUMBER()`; enforce unique keys in marts.
- **Missing periods:** build a **date spine** so LAG/rolling don’t skip.
- **Outliers:** winsorize/cap for robust fits; track anomaly flags.
- **Constraints:** non-negativity for forecasts where appropriate.

---

## 6) Ready-to-paste Utilities

**A) Month spine (36 months)**

```sql
SELECT generate_series(
  date_trunc('month', CURRENT_DATE) - interval '35 months',
  date_trunc('month', CURRENT_DATE),
  interval '1 month'
)::date AS month_start;
```

**B) De-dupe latest snapshot**

```sql
WITH r AS (
  SELECT t.*, ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY load_ts DESC, file_seq DESC) rn
  FROM raw_table t
)
SELECT * FROM r WHERE rn = 1;
```

**C) Contribution analysis (variance)**

```sql
SELECT brand,
       SUM(actual - forecast) AS variance_units,
       SUM(actual - forecast) / NULLIF(SUM(actual) OVER (), 0) AS contribution_pct
FROM mart_forecast_accuracy
GROUP BY 1
ORDER BY variance_units DESC;
```

**D) sMAPE in SQL**

```sql
SELECT AVG( CASE
  WHEN (ABS(actual) + ABS(forecast)) = 0 THEN 0
  ELSE ABS(actual - forecast) / ((ABS(actual) + ABS(forecast))/2.0) END ) AS smape
FROM x;
```

---

## 7) Production Notes

- **Backtesting** with rolling origin; log your splits & metrics.
- **Parameter store** for α/β or model options (DB table).
- **Monitoring**: drift in demand, promo mix, error spikes (weekly).
- **Explainability**: keep simple benchmarks as a yardstick.
- **Handoff**: docs for data sources, refresh schedule, and KPIs.
