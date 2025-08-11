# Zero‑Instruction Starter: Business Analytics Cheat Sheet

A fast, repeatable playbook for when you’re handed a dataset with **no prompt**. Use this to create signal quickly, surface issues, and drive a conversation toward decisions.

---

## Table of Contents

1. [KPI Snapshot + L12M Trend (with Reference Bands)](#kpi-snapshot--l12m-trend-with-reference-bands)
2. [Period‑over‑Period Growth + Seasonality Scan](#periodoverperiod-growth--seasonality-scan)
3. [Variance & Contribution (Waterfall / Pareto)](#variance--contribution-waterfall--pareto)
4. [Segmentation Deep Dive (Mix & ASP)](#segmentation-deep-dive-mix--asp)
5. [Data Quality & Coverage Checks](#data-quality--coverage-checks)
6. [Reference Bands: What & How](#reference-bands-what--how)
7. [SQL Snippets Library](#sql-snippets-library)
8. [Presenting the Findings (1‑slide Exec)](#presenting-the-findings-1slide-exec)

---

## Prerequisites & Data Checklist

This playbook assumes a minimal star-ish schema. Use this checklist to confirm you can run each section.

### Core tables (generic names — map to your schema)

- **fact_sales** _(required)_: `date`, `product_id`, `region_id` _(or customer/channel)_, measures: `units`, **either** `net` **or** (`gross` **and** `rebates` to compute `net`).
- **dim_date** _(recommended)_: `date` (or `date_id`), `year`, `month`, `week`, `month_start`. _(You can derive these on the fly but a date dim helps spines & joins.)_
- **dim_product** _(for segments)_: `product_id`, `brand`, `sku`, `pack_size`, `molecule`.
- **dim_region** _(for geography)_: `region_id`, `canton`/`region`/`country`.
- **fact_plan** / **fact_forecast** _(optional)_: monthly baseline series: `month_start`, `product_id/brand`, `region_id`, `net` (or units).
- **fact_promo** / **fact_rebates** _(optional drivers)_: `date`, `product_id`, `region_id`, `spend_chf`, `touchpoints`, `rebate_chf`.

> **Tip:** If you already have business-ready marts (e.g., `mart_gtn_waterfall`, `mart_brand_perf`), you can run most analyses directly off those.

### What each section needs

1. **KPI Snapshot + L12M Trend**
   - **Required:** `fact_sales(date, units, net)` _(or `gross` + `rebates` to compute `net`)_.
   - **Nice-to-have:** `dim_date` (for robust month spines), `brand/region` dims for slice filters.
   - **Checks:** At least 13 months of data for L12M; no duplicate (date, product, region) at the chosen grain.

2. **Period-over-Period Growth + Seasonality**
   - **Required:** Same as (1); continuous monthly coverage for `LAG()` to make sense.
   - **Nice-to-have:** ≥ 24 months for YoY, enough range for heatmaps.
   - **Checks:** Month spine present; handle missing months with `generate_series` + `LEFT JOIN`.

3. **Variance & Contribution (Waterfall / Pareto)**
   - **Required:** **Two series** at the same grain: _Actual_ and _Baseline_ (Plan/LY/Forecast), aligned on `month_start` and the same segment keys (e.g., brand).
   - **Nice-to-have:** Segment dims (brand, region, payer) for contribution breakdown.
   - **Checks:** Same currency/units, matching calendars; avoid double-counting by pre-aggregating to monthly grain before joining.

4. **Segmentation Deep Dive (Mix & ASP)**
   - **Required:** `fact_sales` + dims to label segments (e.g., `dim_product.brand`, `dim_region.canton`).
   - **Nice-to-have:** Unit conversions / pack size if ASP comparisons matter.
   - **Checks:** For **ASP = Net / Units**, ensure units > 0; consider mix-adjusted ASP where needed.

5. **Data Quality & Coverage**
   - **Required:** `fact_sales` and all joined dims you intend to use.
   - **Checks:** Orphan facts (FK coverage), duplicate business keys, date range/gaps, outliers, unexpected nulls.

### Quick schema probe (Postgres)

```sql
-- What tables do I have?
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog','information_schema')
ORDER BY 1,2;

-- Does fact_sales have the columns I need?
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'fact_sales'
ORDER BY ordinal_position;

-- Row counts by month (coverage check)
SELECT date_trunc('month', date)::date AS month_start, COUNT(*) AS rows
FROM fact_sales
GROUP BY 1 ORDER BY 1;

-- Orphans (facts missing product)
SELECT COUNT(*) AS orphan_rows
FROM fact_sales f
LEFT JOIN dim_product p ON f.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Duplicate business keys at daily grain (adjust keys to your grain)
SELECT date, product_id, region_id, COUNT(*) AS n
FROM fact_sales
GROUP BY 1,2,3
HAVING COUNT(*) > 1;
```

## 1) KPI Snapshot + L12M Trend (with Reference Bands)

**What:** Tiles for Units, Gross, Net, ASP; a 12‑month trend line and a faint **reference band** (last year, forecast, or ±1σ).
**Why:** Executives decide off direction + magnitude; bands make outliers obvious.

**SQL (monthly spine + KPIs):**

```sql
WITH m AS (
  SELECT date_trunc('month', date)::date AS month_start,
         SUM(units) AS units,
         SUM(gross) AS gross,
         SUM(net)   AS net
  FROM fact_sales
  GROUP BY 1
)
SELECT month_start,
       units,
       gross,
       net,
       CASE WHEN units=0 THEN NULL ELSE net::numeric/units END AS asp
FROM m
ORDER BY month_start;
```

**Visual:** Line for Net (bold), faint line/shaded band for LY or forecast; KPI tiles above.

---

## 2) Period‑over‑Period Growth + Seasonality Scan

**What:** MoM / YoY deltas via `LAG()` plus a **seasonality heatmap** (Year × Month colored by Net).
**Why:** Catches turning points and recurring patterns.

**SQL (MoM via `LAG`):**

```sql
WITH m AS (
  SELECT date_trunc('month', date)::date AS month_start,
         SUM(net) AS net
  FROM fact_sales
  GROUP BY 1
)
SELECT month_start,
       net,
       LAG(net) OVER (ORDER BY month_start) AS prev,
       CASE
         WHEN NULLIF(LAG(net) OVER (ORDER BY month_start), 0) IS NULL THEN NULL
         ELSE net / NULLIF(LAG(net) OVER (ORDER BY month_start), 0) - 1
       END AS mom_growth
FROM m
ORDER BY month_start;
```

**Visuals:** Line+bars (Net + MoM bars). Heatmap (Year rows × Month columns, fill by Net) to spot seasonality.

---

## 3) Variance & Contribution (Waterfall / Pareto)

**What:** Explain **why** things moved: Actual vs Plan (or vs LY) and **which segments** drove the delta.
**Why:** Turns “it changed” into “here’s where to act.”

**SQL (variance & share of delta):**

```sql
WITH actual AS (
  SELECT month_start, brand, SUM(net) AS net
  FROM fact_sales_mth GROUP BY 1,2
),
plan AS (
  SELECT month_start, brand, SUM(net) AS net
  FROM plan_sales_mth GROUP BY 1,2
)
SELECT a.month_start,
       a.brand,
       a.net - p.net AS variance,
       (a.net - p.net)
         / NULLIF(SUM(a.net - p.net) OVER (), 0) AS pct_of_delta
FROM actual a
JOIN plan   p USING (month_start, brand)
ORDER BY variance DESC;
```

**Visuals:** Waterfall for total variance; Pareto bar (brand/region contributions) with cumulative line.

---

## 4) Segmentation Deep Dive (Mix & ASP)

**What:** Top‑N segments, long tail, ASP distribution, outliers.
**Why:** Mix shifts (who/where) often explain KPI moves more than demand.

**SQL (Top‑N by contribution):**

```sql
SELECT segment,
       SUM(net) AS net,
       SUM(net) / SUM(SUM(net)) OVER () AS share
FROM fact_sales
GROUP BY segment
ORDER BY net DESC
LIMIT 20;
```

**Visuals:** Sorted bar + cumulative % (Pareto). Boxplot/histogram of ASP to spot price dispersion.

---

## 5) Data Quality & Coverage Checks

**What:** Nulls, duplicates, date gaps, referential integrity, outliers.
**Why:** Prevents “pretty chart, wrong answer.”

**SQL:**

```sql
-- Date coverage by month
SELECT date_trunc('month', date)::date AS month_start,
       COUNT(*) AS rows
FROM fact_sales
GROUP BY 1
ORDER BY 1;

-- Facts missing dims (FK coverage)
SELECT COUNT(*) AS orphan_rows
FROM fact_sales f
LEFT JOIN dim_product p ON f.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Duplicates on business key
SELECT date, product_id, region_id, COUNT(*) AS n
FROM fact_sales
GROUP BY 1,2,3
HAVING COUNT(*) > 1;
```

**Visuals:** Coverage line/heatmap; small table of QA flags.

---

## 6) Reference Bands: What & How

- **Reference band** = a contextual range to compare the main series against.
- Examples:
  - **Last‑Year band:** plot current year bold; add last year faint.
  - **Forecast band:** shaded P10–P90 around forecast.
  - **Volatility band:** ±1 standard deviation around a rolling mean.
- **Practice:** one bold line (Actual), one faint line/band (reference), annotate deviations.

---

## 7) SQL Snippets Library

**Monthly spine:**

```sql
WITH months AS (
  SELECT generate_series(
           date_trunc('month', MIN(date))::date,
           date_trunc('month', MAX(date))::date,
           interval '1 month'
         )::date AS month_start
  FROM fact_sales
)
SELECT m.month_start, COALESCE(SUM(f.net), 0) AS net
FROM months m
LEFT JOIN fact_sales f
  ON date_trunc('month', f.date)::date = m.month_start
GROUP BY 1
ORDER BY 1;
```

**YoY comparison join (align months across years):**

```sql
WITH m AS (
  SELECT date_trunc('month', date)::date AS month_start, SUM(net) AS net
  FROM fact_sales GROUP BY 1
)
SELECT this.month_start AS month_start,
       this.net         AS net_this,
       last.net         AS net_last,
       CASE WHEN NULLIF(last.net,0) IS NULL THEN NULL
            ELSE this.net / NULLIF(last.net,0) - 1 END AS yoy
FROM m this
LEFT JOIN m last
  ON last.month_start = (this.month_start - interval '1 year')::date
ORDER BY month_start;
```

**Pareto (cumulative share):**

```sql
WITH ranked AS (
  SELECT brand,
         SUM(net) AS net,
         RANK() OVER (ORDER BY SUM(net) DESC) AS rnk
  FROM fact_sales
  GROUP BY brand
),
tot AS (SELECT SUM(net) AS total FROM ranked)
SELECT brand, net,
       net / NULLIF(t.total,0) AS share,
       SUM(net) OVER (ORDER BY net DESC)
         / NULLIF(t.total,0) AS cum_share
FROM ranked, tot t
ORDER BY net DESC;
```

---

## 8) Presenting the Findings (1‑slide Exec)

**Slide title = your recommendation.**
Examples: “Prioritise Brand B in Süd; shift 15% budget from Brand A digital. Expected +X% share in Q4.”

**Layout:**

- **Top:** KPI tiles (Net, Units, ASP) with arrows vs LY/Plan.
- **Left:** L12M trend with reference band (LY or forecast).
- **Right:** Variance waterfall (Actual vs Plan/LY).
- **Bottom:** Segment deep dive (Pareto by brand/region).
- **Footer:** Risks & next steps (and data QA note if needed).

**Close:** Trade‑offs, what would change your mind, and how you’d automate/monitor going forward.
