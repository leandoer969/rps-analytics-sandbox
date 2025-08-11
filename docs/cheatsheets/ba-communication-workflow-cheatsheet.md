# Business Analytics Communication & Workflow — Pocket Guide

## Table of Contents

- [1) Requirements & Framing](#1-requirements-framing)
  - [Discovery questions (use 10 minutes)](#discovery-questions-use-10-minutes)
- [2) Analysis Workflow (field-tested)](#2-analysis-workflow-field-tested)
- [3) Presentation Patterns](#3-presentation-patterns)
  - [The Pyramid / SCQA](#the-pyramid-scqa)
  - [Slide skeleton (4–5 slides)](#slide-skeleton-45-slides)
  - [Charts that work](#charts-that-work)
- [4) Dashboards That Get Used](#4-dashboards-that-get-used)
- [5) STAR Stories (interviews & performance reviews)](#5-star-stories-interviews-performance-reviews)
- [6) Running the Meeting](#6-running-the-meeting)
- [7) Writing for Decision-Makers](#7-writing-for-decision-makers)
- [8) Ethics, Bias & Data Privacy](#8-ethics-bias-data-privacy)
- [9) Interview Day Quick Script](#9-interview-day-quick-script)
- [10) Reusable Phrases (EN/DE)](#10-reusable-phrases-ende)

**Scope:** How to collect requirements, structure analyses, craft dashboards, and present insights so decisions get made. Includes templates, checklists, and talk tracks.

---

## 1) Requirements & Framing

### Discovery questions (use 10 minutes)

- **Decision**: What decision will this enable _today_? (prioritize brands? reset targets?)
- **KPIs**: Which 2–3 KPIs decide success? How defined? (Include edge cases.)
- **Time grain**: Daily/weekly/monthly? Time horizon?
- **Segments**: Products, regions, customers, channels?
- **Consumers**: Exec vs. analyst vs. ops—what do they need _differently_?
- **Refresh**: How often? Who owns data quality?
- **Constraints**: Tools, access, timing, privacy.

**Artifact:** a 1‑pager with decision, KPIs, grain, segments, filters, acceptance criteria, and “out of scope”.

---

## 2) Analysis Workflow (field-tested)

1. **Clarify the decision & KPIs** (write them down).
2. **Data sanity check** (ranges, nulls, duplicates, joins).
3. **Minimal model** (trend + variance + one deep dive).
4. **Iterate with stakeholder** (quick sketch; confirm cuts & definitions).
5. **Productionize** (dbt tests, schedule, ownership).
6. **Measure impact** (did behavior change; what outcome moved?).

**Checklists**

- Grain aligned? Date spine present? No fact↔fact joins at detail?
- Numbers reconcile to source? KPI formula signed off?
- Charts labeled (units, currency), axes sensible, colors accessible?

---

## 3) Presentation Patterns

### The Pyramid / SCQA

- **Situation** → **Complication** → **Question** → **Answer (recommendation)**.
- Start with the **answer**; support with three evidences; then risks & next steps.

### Slide skeleton (4–5 slides)

1. **Recommendation (title)**; subline: decision & KPI.
2. **KPI tiles + L12M trend** (annotate notable events).
3. **Variance / Forecast accuracy** (table; note bias/systematic error).
4. **Deep dive cut** (brand/region/customer).
5. **Risks & next steps** (owner, timeline).

### Charts that work

- Trends: line with reference band; limit colors.
- Composition: stacked bar (<= 5 components).
- Ranking: bar or lollipop.
- Variation: box/violin (only if your audience gets it).
- Maps: choropleth sparingly; label values.

**Anti-patterns:** pie charts for >3 slices, dual y-axes without clear normalization, 3D, rainbow palettes.

---

## 4) Dashboards That Get Used

- **One job per view** (overview vs. deep dive).
- **KPI tiles** + single trend + one breakdown.
- **Natural language titles** (“Where did net sales fall in Q3?”).
- **Filters match mental model** (period, brand, region).
- **Empty-state hints** (what to do when no data).
- **Performance**: pre-aggregate (marts), avoid N+1 live queries.

**Governance**

- Glossary for KPI definitions.
- Version notes/changelog.
- Owner + support channel.

---

## 5) STAR Stories (interviews & performance reviews)

- **S**ituation — context & stakes
- **T**ask — your role & goal
- **A**ction — 3–5 crisp steps (tools, stakeholder moves)
- **R**esult — outcome with metric; what changed; lesson

**Examples to prepare**

- Automated refresh reduced cycle time by X%; error rate ↓.
- Conflicting KPI definitions → led a workshop → single source of truth.
- Ad-hoc under time pressure → recommendation adopted.
- “Failed assumption” caught early → risk avoided.

---

## 6) Running the Meeting

**Open (30–60s)**: “Decision to make today is X; KPIs are A/B/C; here’s what I’ll show.”
**Demo (5–10m)**: evidence visuals, narrate insights, compare options.
**Ask (1m)**: what decision do we want today? what support is needed?
**Close (30s)**: decisions, owners, due dates; follow‑ups.

**Handling pushback**

- Clarify metric definitions; show reconciliation table.
- If data is messy: acknowledge, quantify impact, propose fix & timeline.
- Offer trade-offs: “To get real-time, we’d accept less history, is that okay?”

---

## 7) Writing for Decision-Makers

**One-pager template**

- Title = Recommendation (e.g., “Prioritize Brand B in ZH/VD”).
- Context (2–3 lines).
- Evidence (bullets with numbers).
- Risks & mitigations.
- Next steps (owner/date).

**Email update (100–150 words)**

- Executive summary (one sentence).
- Bullet list of changes/insights.
- Ask/decision request.
- Link to dashboard or appendix.

---

## 8) Ethics, Bias & Data Privacy

- Be explicit about uncertainties & assumptions.
- Avoid leakage (future info).
- Review segments for fairness; protect PII.
- Document data lineage and transformations.

---

## 9) Interview Day Quick Script

- Anchor the decision and KPIs.
- 3-min data sanity.
- Minimal model: L12M trend + variance + deep dive.
- Clear recommendation with trade-offs.
- Risks & next steps; how to automate.

---

## 10) Reusable Phrases (EN/DE)

- EN: “Let’s align on the decision and KPI first so we target the analysis.”
- DE: “Lassen Sie uns zuerst Entscheidung und KPI klären, damit wir zielgerichtet arbeiten.”

- EN: “We aggregated facts to monthly grain before joining to forecasts to avoid double counting.”
- DE: “Wir haben die Daten vor dem Join auf Monats‑Grain aggregiert, um Doppelzählungen zu vermeiden.”

- EN: “Here are the trade-offs between speed and depth; I recommend option B for this cadence.”
- DE: “Hier die Trade-offs zwischen Geschwindigkeit und Tiefe; für diese Taktung empfehle ich Option B.”
