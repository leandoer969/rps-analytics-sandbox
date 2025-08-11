# BA Metrics & Forecasting Cheat Sheet

**Quick links:** [KPI](#kpi) · [GTN](#gtn) · [ASP](#asp) · [DOS](#dos) · [MAPE](#mape) · [RMSE](#rmse) · [WAPE](#wape) · [MAE](#mae) · [ARIMA](#arima) · [ETS](#ets) · [Prophet](#prophet) · [OLS](#ols) · [CTE](#cte) · [ETL / ELT](#etl--elt) · [dbt](#dbt) · [SSoT](#ssot) · [SKU](#sku)

This sheet summarizes the **metrics, SQL patterns, and forecasting methods** you’re most likely to use in a Business Data Analyst role. It’s designed for fast recall during cases and on the job.

---

## Table of Contents

1. [Core Commercial KPIs](#core-commercial-kpis)
2. [Pricing / GTN & Profitability](#pricing--gtn--profitability)
3. [Supply & Operations KPIs](#supply--operations-kpis)
4. [Time-Series Forecasting Playbook](#time-series-forecasting-playbook)
5. [Error Metrics (choose wisely)](#error-metrics-choose-wisely)
6. [SQL Patterns (Spines, Rolling, De-dupe, Joins)](#sql-patterns-spines-rolling-de-dupe-joins)
7. [Feature Engineering (Promo, Price, Calendar)](#feature-engineering-promo-price-calendar)
8. [Validation & Experimentation](#validation--experimentation)
9. [Common Pitfalls & Checklists](#common-pitfalls--checklists)
10. [Glossary of Abbreviations](#glossary-of-abbreviations)

---

## Core Commercial KPIs

**Units**

- Raw volume at the chosen grain (brand/sku × region × period).
- _Careful:_ changes in pack size or SKU mix can mask trends.

**Net Sales (≈ Net Revenue)**
\(\textbf{Net Sales} = \text{Gross Sales} - \text{Rebates/Discounts}\)

**ASP (Average Selling Price)**
\(\textbf{ASP} = \frac{\text{Net Sales}}{\text{Units}}\)

- _Careful:_ segment/pack mix affects ASP; consider mix-adjusted ASP.

**Share & Growth**

- **Market Share** = Your Units ÷ Market Units (or Net Sales ÷ Market Net Sales).
- **Growth vs LY** = \(\frac{\text{This} - \text{LY}}{\text{LY}}\). Align calendars; handle missing months.

**Promo Efficiency**

- **Lift** = (Units during promo − Baseline) ÷ Baseline.
- **ROI** = Incremental Net Sales ÷ Promo Spend.

**SQL snippet (MoM growth with LAG):**

```sql
WITH m AS (
  SELECT make_date(year, month, 1) AS month_start, brand,
         SUM(net_sales_chf) AS net_sales
  FROM rps.mart_gtn_waterfall
  GROUP BY 1,2
)
SELECT month_start, brand, net_sales,
       LAG(net_sales) OVER (PARTITION BY brand ORDER BY month_start) AS prev,
       CASE WHEN LAG(net_sales) OVER (PARTITION BY brand ORDER BY month_start)=0
            THEN NULL
            ELSE net_sales / NULLIF(LAG(net_sales) OVER (PARTITION BY brand ORDER BY month_start),0) - 1
       END AS mom_growth
FROM m
ORDER BY brand, month_start;
```

---

## Pricing / GTN & Profitability

**Gross-to-Net (GTN) Waterfall**
Gross → (−) Rebates/Discounts → **Net Sales**. Use a **single source of truth** (SSoT) for definitions.

**Gross Margin (GM)**
\(\textbf{GM} = \frac{\text{Net Sales} - \text{COGS}}{\text{Net Sales}}\)

**Net Sales per Unit / Contribution**

- Net Sales ÷ Units; use for promo ROI, payer/channel analysis.

**SQL: GTN at monthly grain**

```sql
WITH m AS (
  SELECT make_date(year, month, 1) AS month_start, brand, canton,
         SUM(gross_sales_chf) AS gross,
         SUM(rebates_chf)     AS rebates,
         SUM(net_sales_chf)   AS net
  FROM rps.mart_gtn_waterfall
  GROUP BY 1,2,3
)
SELECT month_start, brand, canton, gross, rebates, net,
       rebates / NULLIF(gross,0) AS rebate_rate
FROM m
ORDER BY month_start, brand, canton;
```

---

## Supply & Operations KPIs

**Days of Supply (DOS)**
\(\textbf{DOS} = \frac{\text{Inventory On Hand}}{\text{Avg Daily Demand}}\)

**Service Level / OOS Flags**

- **Stockout flag** when inventory ≤ reorder point.
- Track **late fill** or delay to customer.

**SQL (7-day rolling demand + DOS):**

```sql
WITH daily AS (
  SELECT d.date_actual, s.product_id, SUM(s.units) AS units
  FROM rps.fct_sales s
  JOIN rps.dim_date d ON d.date_id = s.date_id
  GROUP BY 1,2
),
roll AS (
  SELECT date_actual, product_id, units,
         AVG(units) OVER (PARTITION BY product_id ORDER BY date_actual
                          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_7d
  FROM daily
)
SELECT date_actual, product_id,
       GREATEST(0, 2000 - SUM(units) OVER (PARTITION BY product_id ORDER BY date_actual)) AS inventory,
       300 AS reorder_point,
       avg_7d,
       CASE WHEN (GREATEST(0, 2000 - SUM(units) OVER (PARTITION BY product_id ORDER BY date_actual))) <= 300 THEN 1 ELSE 0 END AS stockout_flag,
       CASE WHEN avg_7d = 0 THEN NULL ELSE (GREATEST(0, 2000 - SUM(units) OVER (PARTITION BY product_id ORDER BY date_actual)) / avg_7d) END AS dos
FROM roll
ORDER BY product_id, date_actual;
```

---

## Time-Series Forecasting Playbook

**1) Baseline**

- Naive seasonal, moving average, or last-4-week mean:
  \(\hat y*t = \text{mean}(y*{t-1..t-4})\)

**2) Simple Causal (our page model)**
\(\hat y_t = \alpha \cdot \text{PromoSpend}\_t + \beta \cdot \text{RebateRate}\_t + \text{Baseline}(y)\)

- Fit with **OLS**; grid search, or **scipy.optimize**.
- Add **promo lag** (effects show with delay).

**3) Classical TS**

- **ETS/Holt-Winters** (level/trend/seasonal)
- **ARIMA/SARIMA** (AR, differencing, MA; seasonal components)

**4) ML**

- **XGBoost/LightGBM**, Random Forests with calendar, price, promo, lags; careful with CV.

**5) Validation**

- **Rolling-origin** (time-aware CV). Avoid leakage: use past-only features for t.

**When to prefer which?**

- Short horizon + few features → ETS/ARIMA.
- Strong drivers (price/promo) → OLS/GLM/GBDT.
- Cold starts / new SKUs → hierarchy, pooling, similarity.

---

## Error Metrics (choose wisely)

- **MAE**: \(\frac{1}{n}\sum |y - \hat y|\) — robust, easy to interpret.
- **RMSE**: \(\sqrt{\frac{1}{n}\sum (y - \hat y)^2}\) — penalizes large errors.
- **MAPE**: \(\frac{100}{n}\sum \big|\frac{y - \hat y}{y}\big|\) — avoid when \(y \approx 0\).
- **sMAPE**: \(\frac{100}{n}\sum \frac{|y - \hat y|}{(|y| + |\hat y|)/2}\) — handles zeros better.
- **WAPE**: \(\frac{\sum |y - \hat y|}{\sum |y|}\) — good with skew.
- **MASE**: compares vs naive seasonal; unitless and comparable across series.

---

## SQL Patterns (Spines, Rolling, De-dupe, Joins)

**Month spine (3y)**

```sql
WITH months AS (
  SELECT generate_series(date_trunc('month', now()) - INTERVAL '35 months',
                         date_trunc('month', now()),
                         INTERVAL '1 month')::date AS month_start
)
SELECT * FROM months;
```

**Rolling 12 months**

```sql
SELECT month_start, brand, net_sales,
       SUM(net_sales) OVER (PARTITION BY brand ORDER BY month_start
                            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS net_sales_l12m
FROM rps.mart_gtn_waterfall;
```

**De-dupe to latest snapshot**

```sql
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY business_key ORDER BY load_ts DESC, file_seq DESC
  ) AS rn
  FROM raw_table
)
SELECT * FROM ranked WHERE rn = 1;
```

**Safe fact→dim joins**

```sql
SELECT s.*, p.brand, r.canton, c.channel_name
FROM rps.fct_sales s
LEFT JOIN rps.dim_product p ON s.product_id = p.product_id
LEFT JOIN rps.dim_region  r ON s.region_id  = r.region_id
LEFT JOIN rps.dim_channel c ON s.channel_id = c.channel_id;
```

---

## Feature Engineering (Promo, Price, Calendar)

- **Promo intensity**: spend, touchpoints; carryover with lags/decay.
- **Price/Net price**: ASP (net/units); price changes/elasticity.
- **Calendar**: month, week, ISO week, holidays, seasonality flags.
- **Interaction**: promo × season; price × payer; channel × region.
- **Outliers**: cap extreme uplifts; winsorize for stability.

---

## Validation & Experimentation

- **Train/valid splits**: last 2–3 months as validation (or rolling).
- **Backtest**: simulate how you would have predicted at each point.
- **A/B tests**: when feasible; ensure power and clean assignment.
- **Diagnostics**: residual plots, autocorrelation, feature importance.

---

## Common Pitfalls & Checklists

**Data & Joins**

- ❌ Fact-to-fact joins without aggregation → double counting.
- ✅ Align grains (daily vs monthly); aggregate before joining.

**Time**

- ❌ Missing months → broken LAG/rolling.
- ✅ Build a **date/month spine**, left join to fill gaps.

**Dims**

- ❌ Non-unique dim keys → duplicate matches.
- ✅ De-dupe in `stg_*` with `ROW_NUMBER()`; enforce constraints.

**Forecasting**

- ❌ Leakage (using future info for past predictions).
- ✅ Strictly past features for predicting time _t_; rolling CV.

**Presentation**

- ✅ Start with the decision → KPI tiles → trend → deep dive → recs + risks.

---

## Glossary of Abbreviations

| Abbrev                                                | Meaning                                                                   | Notes / Where used                                            |
| ----------------------------------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------- | ----------------------- | ---------------------------- | --- | ---------------------------- | ------ | ------- |
| <a id="kpi"></a>**KPI**                               | Key Performance Indicator                                                 | Core metric guiding decisions (e.g., Net Sales, Units, MAPE). |
| <a id="gtn"></a>**GTN**                               | Gross-to-Net                                                              | Gross sales minus rebates/discounts; see GTN waterfall.       |
| <a id="asp"></a>**ASP**                               | Average Selling Price                                                     | Net Sales ÷ Units; mind mix shifts and pack sizes.            |
| <a id="wac"></a>**WAC**                               | Wholesale Acquisition Cost                                                | U.S. list price; not equal to net.                            |
| <a id="nr"></a>**NR**                                 | Net Revenue                                                               | Revenue after deductions/rebates; ≈ Net Sales.                |
| <a id="gm"></a>**GM**                                 | Gross Margin                                                              | (Revenue − COGS) ÷ Revenue.                                   |
| <a id="cogs"></a>**COGS**                             | Cost of Goods Sold                                                        | Direct costs of production/distribution.                      |
| <a id="dos"></a>**DOS**                               | Days of Supply                                                            | Inventory ÷ Avg daily demand; stock risk KPI.                 |
| <a id="oos"></a>**OOS**                               | Out-of-Stock                                                              | Inventory below zero/threshold; often flagged in ops.         |
| <a id="sla"></a>**SLA**                               | Service Level Agreement                                                   | Target delivery/availability level.                           |
| <a id="sql"></a>**SQL**                               | Structured Query Language                                                 | Core querying language; see joins/CTEs.                       |
| <a id="cte"></a>**CTE**                               | Common Table Expression                                                   | `WITH` blocks to structure SQL pipelines.                     |
| <a id="etl--elt"></a>**ETL / ELT**                    | Extract-Transform-Load / Extract-Load-Transform                           | Pipeline patterns; dbt favors ELT.                            |
| <a id="dbt"></a>**dbt**                               | data build tool                                                           | SQL+Jinja transformations, tests, lineage.                    |
| <a id="yoy-qoq-mom-wow"></a>**YoY / QoQ / MoM / WoW** | Year-over-Year / Quarter-over-Quarter / Month-over-Month / Week-over-Week | Standard period deltas; use date spines.                      |
| <a id="ltm-l12m-l3m"></a>**LTM / L12M / L3M**         | Last Twelve Months / Last 12 / Last 3 Months                              | Rolling windows; ensure continuous time series.               |
| <a id="cagr"></a>**CAGR**                             | Compound Annual Growth Rate                                               | \(\left(\frac{\text{End}}{\text{Start}}\right)^{1/n} - 1\).   |
| <a id="mae"></a>**MAE**                               | Mean Absolute Error                                                       | \(\tfrac{1}{n}\sum                                            | y-\hat y                | \). Scale-dependent, robust. |
| <a id="rmse"></a>**RMSE**                             | Root Mean Squared Error                                                   | \(\sqrt{\tfrac{1}{n}\sum (y-\hat y)^2}\).                     |
| <a id="mape"></a>**MAPE**                             | Mean Absolute Percentage Error                                            | \(\tfrac{100}{n}\sum \big                                     | \tfrac{y-\hat y}{y}\big | \) (avoid when y≈0).         |
| <a id="smape"></a>**sMAPE**                           | Symmetric MAPE                                                            | \(\tfrac{100}{n}\sum \frac{                                   | y-\hat y                | }{(                          | y   | +                            | \hat y | )/2}\). |
| <a id="wape"></a>**WAPE**                             | Weighted APE                                                              | \(\tfrac{\sum                                                 | y-\hat y                | }{\sum                       | y   | }\). Good for skewed demand. |
| <a id="mase"></a>**MASE**                             | Mean Abs Scaled Error                                                     | Compares vs naive seasonal; unitless.                         |
| <a id="mad"></a>**MAD**                               | Mean Absolute Deviation                                                   | Mean(                                                         | y-\\hat y               | ); like MAE.                 |
| <a id="ape"></a>**APE**                               | Absolute Percentage Error                                                 | Per-observation                                               | y-\\hat y               | /                            | y   | .                            |
| <a id="ols"></a>**OLS**                               | Ordinary Least Squares                                                    | Linear regression fit method.                                 |
| <a id="glm"></a>**GLM**                               | Generalized Linear Model                                                  | e.g., Poisson/Log link for counts.                            |
| <a id="ets"></a>**ETS**                               | Exponential Smoothing family                                              | Trend/seasonality; e.g., Holt-Winters.                        |
| <a id="arima"></a>**ARIMA**                           | AutoRegressive Integrated Moving Average                                  | Time-series with differencing/AR/MA.                          |
| <a id="prophet"></a>**Prophet**                       | Additive trend/seasonality model                                          | Useful for holidays/seasonality; FB.                          |
| <a id="xgboost"></a>**XGBoost**                       | Gradient-boosted trees                                                    | Nonlinear features; needs careful CV.                         |
| <a id="cv"></a>**CV**                                 | Cross-Validation                                                          | Train/validation splits; rolling for time series.             |
| <a id="ab"></a>**A/B**                                | A/B Test                                                                  | Controlled experiment; check power/segmentation.              |
| <a id="cac-ltv"></a>**CAC / LTV**                     | Customer Acquisition Cost / Lifetime Value                                | General BA metrics; cohort-aware.                             |
| <a id="arpu-arr-mrr"></a>**ARPU / ARR / MRR**         | Avg Rev Per User / Annual / Monthly Recurring Rev                         | SaaS finance; sometimes used in analytics.                    |
| <a id="sku"></a>**SKU**                               | Stock Keeping Unit                                                        | Product identifier; grain-critical.                           |
| <a id="hcp-hco"></a>**HCP / HCO**                     | Healthcare Professional / Organization                                    | Pharma stakeholders.                                          |
| <a id="trx-nrx"></a>**TRx / NRx**                     | Total / New Prescriptions                                                 | Rx-based markets.                                             |
| <a id="rls"></a>**RLS**                               | Row-Level Security                                                        | BI filters by user (e.g., canton).                            |
| <a id="pii"></a>**PII**                               | Personally Identifiable Information                                       | Handle under GDPR; avoid in marts.                            |
| <a id="gdpr"></a>**GDPR**                             | EU data privacy regulation                                                | Switzerland aligned; mind cross-border.                       |
| <a id="ssot"></a>**SSoT**                             | Single Source of Truth                                                    | One governed definition (dbt + catalog).                      |
