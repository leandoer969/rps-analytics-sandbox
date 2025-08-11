# Tableau & SAP Analytics Cloud (SAC) — Quickstart Cheatsheet (for SQL-minded Analysts)

A practical, fast-start guide to build the right charts, avoid common traps, and translate **SQL thinking** into **Tableau** and **SAC**.

---

## Table of Contents

1. [TL;DR — When to use what](#tldr--when-to-use-what)
2. [Mental model: Dimensions, Measures, Discrete/Continuous](#mental-model-dimensions-measures-discretecontinuous)
3. [Data connections & modeling](#data-connections--modeling)
4. [Calculations: SQL ↔ Tableau ↔ SAC](#calculations-sql--tableau--sac)
5. [Tableau order of operations (critical for filters)](#tableau-order-of-operations-critical-for-filters)
6. [Time-series patterns (L12M, TY vs LY, reference bands)](#time-series-patterns-l12m-ty-vs-ly-reference-bands)
7. [Build GTN & Brand dashboards (step-by-step)](#build-gtn--brand-dashboards-step-by-step)
8. [Dashboard UX best practices](#dashboard-ux-best-practices)
9. [Performance tips](#performance-tips)
10. [Common pitfalls & how to avoid them](#common-pitfalls--how-to-avoid-them)
11. [Quick recipes](#quick-recipes)
12. [Shortcuts & UI tips](#shortcuts--ui-tips)
13. [Glossary](#glossary)

---

## TL;DR — When to use what

- **Tableau**: Quick exploration, beautiful visuals, ad‑hoc analysis, fast drill. Great for “what’s going on?” and proving value quickly.
- **SAC (SAP Analytics Cloud)**: Enterprise stories, **governed** content, **user & role** management, integration with SAP stack, **planning** (writeback, versions), and **Smart Predict**.

**Rule of thumb:** Prototype in **Tableau** → productionize in **SAC** if it becomes “business‑critical” with access control and planning workflows.

---

## Mental model: Dimensions, Measures, Discrete/Continuous

- **Dimensions** = categorical slice keys (brand, canton, payer). In SQL: GROUP BY columns.
- **Measures** = numeric fields to aggregate (units, net_sales). In SQL: SUM/AVG columns.
- **Discrete (blue pills in Tableau)** = **headers / categories** (e.g., Month name).
- **Continuous (green pills in Tableau)** = **axes / ranges** (e.g., continuous date).
  _Tip:_ Switching a date field from discrete to continuous changes from headers to a time axis.

In **SAC**, you similarly define **Dimensions** (attributes, time) and **Measures** (aggregations).

---

## Data connections & modeling

### Tableau

- **Relationships** (logical layer, noodle) — preferred for multiple tables; preserves detail until viz time. Less risk of row duplication than physical **Joins**.
- **Joins** (physical layer) — materializes a flat table; can duplicate rows if one‑to‑many dims are joined without dedupe.
- **Extract vs Live**: **Extract** (hyper) for speed & offline; **Live** to hit the source directly. For big datasets, extract with incremental refresh.

**SQL mindset:** If you’d do it as a **view** or **CTE**, consider pre‑building a **dbt mart** and point Tableau to it.

### SAC

- **Model**: the governed semantic layer. Define **Dimensions** (including **Time**) and **Measures**, data types, hierarchies.
- **Data source**: **Import** (pull & store) or **Live** (query remote, e.g., SAP HANA/BW). Planning models add **Versions** (Actual/Budget/Forecast) and writeback.
- **Stories / Analytic Applications**: visual layer on top of the model.

**SQL mindset:** A SAC **Model** ≈ a curated SQL view/mart + metadata (types, hierarchies, units).

---

## Calculations: SQL ↔ Tableau ↔ SAC

| Concept        | SQL (Postgres-style)                                        | Tableau                                           | SAC                                                               |
| -------------- | ----------------------------------------------------------- | ------------------------------------------------- | ----------------------------------------------------------------- |
| If/Else        | `CASE WHEN cond THEN a ELSE b END`                          | `IF cond THEN a ELSE b END` (Calculated Field)    | Restricted/Advanced Calculations with IF/CASE                     |
| Date Trunc     | `date_trunc('month', dt)`                                   | `DATETRUNC('month', [Date])`                      | `StartOfMonth([Date])` / time dimension granularity               |
| Date Part      | `extract(month from dt)`                                    | `DATEPART('month', [Date])`                       | `Month([Date])`                                                   |
| Ratio          | `SUM(net)/NULLIF(SUM(units),0)`                             | `SUM([Net]) / SUM([Units])`                       | Measure formula `Net/Units` with `NULLIF` logic                   |
| Window Sum     | `SUM(x) OVER (PARTITION BY k ORDER BY dt ROWS 6 PRECEDING)` | `WINDOW_SUM([x], -6, 0)` (Table Calc)             | `Running Sum` / `Moving Average` widgets; restricted calc windows |
| Dense Rank     | `DENSE_RANK() OVER (ORDER BY net DESC)`                     | `INDEX()` + sort, or Level of Detail (LOD) tricks | Rank function (in model or calculation)                           |
| Distinct Count | `COUNT(DISTINCT id)`                                        | `COUNTD([id])`                                    | Distinct Count aggregation                                        |

### Tableau LOD (Level of Detail) expressions ↔ SQL

- `{FIXED [Brand]: SUM([Net])}` ≈ `SELECT brand, SUM(net) FROM ... GROUP BY brand` then joined back to viz grain.
- `{INCLUDE [SKU]: SUM([Net])}` adds finer grain before aggregating up (like a subquery at SKU then SUM to viz level).
- `{EXCLUDE [Canton]: SUM([Net])}` aggregates ignoring a dimension present in the viz (akin to a window across that dim).

**Rule:** Use **LOD** when your aggregation grain is **not** the same as the viz grain. Use **Table Calcs** for **window/ordering** logic.

---

## Tableau order of operations (critical for filters)

1. **Extract Filters** → 2) **Data Source Filters** → 3) **Context Filters** → 4) **Top N / Conditional Dim Filters** → 5) **Dimension Filters** → 6) **Measure Filters** → 7) **Table Calc Filters**.

- Put **slow, selective** filters in **Context** so Top N and other filters compute **after** them.
- **Table calc filters** hide marks but do **not** change totals; beware when computing KPIs.

---

## Time-series patterns (L12M, TY vs LY, reference bands)

**L12M trend:** Continuous Date axis; filter last 12 months. Add **Reference Line** for mean or **Band** for ±1σ.

**This Year vs Last Year (YoY):**

- Duplicate measure, one filtered to **This Year**, one to **Last Year**; dual‑axis or color. Or build a calculated field `[Is This Year?]` using year([Date]).

**Reference bands:**

- Tableau: Analytics pane → **Distribution Band** (std dev) or add **Forecast** (simple exponential smoothing) and shade **P10–P90**.
- SAC: Use **Reference Lines/Bands** in charts; for forecast, use **Predictive Forecast** if enabled or create a baseline series in the model.

---

## Build GTN & Brand dashboards (step-by-step)

### GTN (Gross → Net) — Tableau

1. Data: point to a **mart** like `mart_gtn_waterfall` (month_start, brand, canton, gross, rebates, net).
2. Sheet 1 (Trend): Month on Columns (continuous), Net on Rows; add Gross as faint line; add **Reference Band** (±1σ).
3. Sheet 2 (Waterfall): Use **Gantt Bar** or **Waterfall** template (Gross → −Rebates → Net) per month.
4. Sheet 3 (Pareto): Brand on Rows, Net on Columns; **Quick Table Calc → Running Total**; add % of Total for cumulative share.
5. Dashboard: Tiles (KPI), Trend, Waterfall, Pareto; global filters for Month/Brand/Canton.

### Brand Performance — SAC

1. Model fields: Time (Month), Dimensions (Brand, Canton), Measures (Units, Net, Promo Spend).
2. Charts: **Time Series** (Units, Net), **Bar** by Canton, **Indicator** tiles for KPIs.
3. Filters: Month (single-select), Brand (single), Canton (multi).
4. Add **Input Controls** if planning/what-if; save as **Story** with bookmarks.

---

## Dashboard UX best practices

- Start with a **decision title** (“Prioritise Brand B in Süd; shift 15% budget from Brand A digital”).
- ≤ 3 primary visuals; everything else goes to drilldowns or tooltips.
- **Consistent time grain** (monthly vs weekly) across charts.
- **Color** = meaning (e.g., Net = dark, Rebates = amber). Keep a palette consistent with your org.
- Write **insight subtitles** (“Net +9% vs LY, driven by Brand B in ZH (+CHF 1.2m)”).

---

## Performance tips

- Pre‑aggregate in SQL/dbt (marts). Avoid row‑explosion joins in Tableau.
- Use **Extracts** with Incremental refresh (Tableau) for large sources.
- Limit high‑cardinality quick filters; prefer **parameter** or **context filter**.
- In SAC, keep model tidy: only needed attributes, define hierarchies; consider **Import** with scheduled refresh if Live is slow.

---

## Common pitfalls & how to avoid them

- **Double counting** after joins/relationships → Validate row counts; prefer **Relationships** in Tableau; dedupe dims in staging.
- **Grain mismatch** (daily vs monthly) → Aggregate to the **same grain** before joining.
- **Filter order confusion (Tableau)** → Use **Context** for upstream filters; remember table‑calc filters don’t change totals.
- **Discrete vs continuous date** → Wrong pill leads to header categories instead of a time axis.
- **SAC model mismatch** → Wrong time granularity or measure type; fix in the model, not each chart.

---

## Quick recipes

- **Bar‑in‑bar (LY vs TY):** Put TY and LY on Columns, Brand on Rows; dual axis; synchronize; color TY solid, LY light.
- **Reference band:** Analytics → Distribution Band → set to Std Dev around AVG(Net).
- **Pareto 80/20:** Sort by Net desc; add Running Total of Net / Total Net; add reference line at 80%.
- **Cohort heatmap:** Build cohort dimension (first purchase month) in SQL or as calc; pivot to heatmap (Month since start × Cohort).
- **Small multiples:** Duplicate axes by Brand; use **Trellis** layout (Tableau) or multiple charts (SAC) with shared filters.

---

## Shortcuts & UI tips

**Tableau**

- Drag **Measure Names/Values** for quick multi‑measure charts.
- Right‑click a pill → **Convert to Continuous/Discrete** to fix axes vs headers.
- **Show Filter** on a dimension; **Add to Context** to control order.

**SAC**

- Create **Story** → **Responsive** layout.
- Use **Linked Analysis** so one control filters multiple charts.
- For planning, add **Version** dimension (Actual, Plan, Forecast), and **Input Tasks**.

---

## Glossary

- **LOD (Tableau)**: Level of Detail expression to compute at a fixed grain ({FIXED}, {INCLUDE}, {EXCLUDE}).
- **Table Calc**: Post‑aggregate calculations (WINDOW_SUM, RUNNING_AVG) dependent on the viz layout.
- **Context Filter**: Upstream filter that affects Top N and dependent filters.
- **SAC Model**: Semantic layer defining dimensions, measures, time, and (optionally) planning versions.
- **Story (SAC)**: A curated dashboard/page collection built on one or more models.

---

### SQL vs UI: Quick reference

- **You** write a `GROUP BY` → **Tableau/SAC** default to aggregating measures by shown dimensions.
- **You** write a window function → **Tableau** table calc; **SAC** running/moving aggregates.
- **You** build a monthly view → Set **continuous date** at month granularity (Tableau) or **Time = Month** in SAC model.
