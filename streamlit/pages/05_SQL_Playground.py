import re
import time
import pandas as pd
import streamlit as st
from sqlalchemy import text
from lib.db import get_engine

st.set_page_config(page_title="SQL Playground", page_icon="🧪", layout="wide")
st.title("🧪 SQL Playground")

# --- Settings ---
st.sidebar.header("Run settings")
timeout_sec = st.sidebar.slider("Statement timeout (seconds)", 1, 60, 10)
max_rows = st.sidebar.number_input("Max rows to display", 10, 100000, 5000, step=100)
allow_writes = st.sidebar.checkbox(
    "Allow write queries (DANGER)", value=False, help="Unchecked = SELECT/CTE only"
)

# --- Examples dropdown ---------------------------------------------------------
EXAMPLES = {
    "Period-over-period (LAG)": """
WITH monthly AS (
  SELECT make_date(year, month, 1) AS month_start, product_id, brand,
         SUM(gross_sales_chf) AS sales_chf
  FROM rps_stg.stg_sales
  GROUP BY 1,2,3
)
SELECT month_start, product_id, brand, sales_chf,
       LAG(sales_chf) OVER (PARTITION BY product_id ORDER BY month_start) AS lag_sales,
       sales_chf - LAG(sales_chf) OVER (PARTITION BY product_id ORDER BY month_start) AS abs_chg,
       CASE WHEN LAG(sales_chf) OVER (PARTITION BY product_id ORDER BY month_start) IN (0,NULL) THEN NULL
            ELSE (sales_chf / LAG(sales_chf) OVER (PARTITION BY product_id ORDER BY month_start)) - 1 END AS pct_chg
FROM monthly
ORDER BY product_id, month_start;
""".strip(),
    "Dedupe latest snapshot (ROW_NUMBER)": """
WITH raw AS (
  SELECT * FROM (VALUES
    (101, DATE '2025-05-01', 120, TIMESTAMP '2025-05-02 08:00'),
    (101, DATE '2025-05-01', 130, TIMESTAMP '2025-05-02 09:00'),
    (102, DATE '2025-05-01',  50, TIMESTAMP '2025-05-01 12:00')
  ) AS t(product_id, as_of_date, units, load_ts)
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY product_id, as_of_date ORDER BY load_ts DESC
  ) rn
  FROM raw
)
SELECT * FROM ranked WHERE rn = 1 ORDER BY product_id, as_of_date;
""".strip(),
    "Join facts → dims": """
SELECT make_date(d.year, d.month, 1) AS month_start, s.product_id, p.brand, r.canton,
       s.units, s.gross_sales_chf
FROM rps_core.fct_sales s
LEFT JOIN rps_core.dim_date d    ON s.date_id    = d.date_id
LEFT JOIN rps_core.dim_product p ON s.product_id = p.product_id
LEFT JOIN rps_core.dim_region r  ON s.region_id  = r.region_id
ORDER BY month_start, brand, canton;
""".strip(),
    "Stockout flag + Days of Supply": """
WITH daily AS (
  SELECT d.date_actual, s.product_id, SUM(s.units) AS units
  FROM rps_core.fct_sales s JOIN rps_core.dim_date d ON s.date_id = d.date_id
  GROUP BY 1,2
),
roll AS (
  SELECT date_actual, product_id, units,
         AVG(units) OVER (PARTITION BY product_id ORDER BY date_actual
                          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_7d
  FROM daily
),
inv AS (
  SELECT date_actual, product_id,
         GREATEST(0, 2000 - SUM(units) OVER (PARTITION BY product_id ORDER BY date_actual)) AS inventory_on_hand,
         300::int AS reorder_point, avg_7d
  FROM roll
)
SELECT date_actual, product_id, inventory_on_hand, reorder_point, avg_7d,
       CASE WHEN inventory_on_hand <= reorder_point THEN 1 ELSE 0 END AS stockout_flag,
       CASE WHEN COALESCE(avg_7d,0)=0 THEN NULL ELSE inventory_on_hand/avg_7d END AS days_of_supply
FROM inv
ORDER BY product_id, date_actual;
""".strip(),
}

example_name = st.selectbox("Insert example", ["(none)"] + list(EXAMPLES.keys()))
if example_name != "(none)":
    st.session_state["sql_text"] = EXAMPLES[example_name]

# --- SQL editor ----------------------------------------------------------------
default_sql = (
    "SELECT * FROM rps_mart.mart_gtn_waterfall ORDER BY year, month, brand LIMIT 100;"
)
sql_text = st.text_area(
    "SQL",
    key="sql_text",
    value=st.session_state.get("sql_text", default_sql),
    height=260,
    placeholder="Type a SELECT... or WITH ... SELECT ...",
    help="Read-only by default. Enable 'Allow write queries' to run INSERT/UPDATE/DELETE/DDL.",
)

run = st.button("▶ Run query")

# --- helpers -------------------------------------------------------------------
WRITE_RE = re.compile(
    r"\\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE)\\b",
    re.IGNORECASE,
)


def is_select_like(q: str) -> bool:
    q = q.strip().lstrip("(")
    return q.lower().startswith("select") or q.lower().startswith("with")


def first_statement(q: str) -> str:
    # prevent multi-statement; take first semicolon-chunk
    parts = [p for p in q.split(";") if p.strip()]
    return parts[0] if parts else ""


def maybe_add_limit(q: str, n: int) -> str:
    if not is_select_like(q):
        return q
    # crude check: if it already has LIMIT, don't add; else append
    if re.search(r"\\blimit\\b", q, flags=re.IGNORECASE):
        return q
    return q.rstrip() + f" LIMIT {int(n)}"


# --- execute -------------------------------------------------------------------
if run and sql_text.strip():
    q = first_statement(sql_text)

    if not allow_writes and not is_select_like(q):
        st.error(
            "Blocked: Only SELECT/CTE queries are allowed. Enable 'Allow write queries' to run this."
        )
        st.stop()
    if not allow_writes and WRITE_RE.search(q):
        st.error(
            "Blocked: Write/DDL keywords detected. Enable 'Allow write queries' to proceed."
        )
        st.stop()

    start = time.time()
    eng = get_engine()
    try:
        with eng.begin() as conn:
            # Set safe search_path + timeout for this transaction
            conn.execute(text("SET LOCAL search_path TO rps, public"))
            conn.execute(
                text("SET LOCAL statement_timeout = :tms"),
                {"tms": f"{int(timeout_sec * 1000)}"},
            )
            if allow_writes and not is_select_like(q):
                # Execute write/DDL
                rc = conn.execute(text(q)).rowcount
                st.success(
                    f"✅ Executed non-SELECT. Rowcount: {rc if rc is not None else 0}"
                )
            else:
                q_limit = maybe_add_limit(q, max_rows)
                df = pd.read_sql(text(q_limit), conn)
                dur = time.time() - start
                st.caption(f"Returned {len(df):,} rows in {dur:.2f}s")
                st.dataframe(df, use_container_width=True)
                st.download_button(
                    "Download CSV",
                    df.to_csv(index=False).encode("utf-8"),
                    file_name="query_results.csv",
                    mime="text/csv",
                )
    except Exception as e:
        st.exception(e)

# Footnote
st.info(
    "Tip: results are auto-LIMITed if no LIMIT is present. Timeout and row cap are in the sidebar."
)
