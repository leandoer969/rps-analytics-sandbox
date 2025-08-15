# streamlit/pages/00_Schema_Overview.py
import html
import re
import streamlit as st
import pandas as pd
from sqlalchemy import text
from lib.db import read_sql_df, get_engine

st.title("🗺️ Schema Overview")

SYSTEM_SCHEMAS = {
    "pg_catalog",
    "information_schema",
    "pg_toast",
    "pg_temp_1",
    "pg_toast_temp_1",
}

# ---------- Helpers ----------


def human_bytes(n: int | None) -> str:
    if n is None:
        return "-"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024.0:
            return f"{n:3.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def safe_ident(name: str) -> str:
    # allow identifiers [letters, numbers, underscore], starting with letter/underscore
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe identifier: {name}")
    return name


@st.cache_data(ttl=60)
def list_schemas(include_system: bool = False) -> list[str]:
    sql = "SELECT schema_name FROM information_schema.schemata ORDER BY 1;"
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    schemas = df["schema_name"].tolist()
    if not include_system:
        schemas = [s for s in schemas if s not in SYSTEM_SCHEMAS]
    return schemas


@st.cache_data(ttl=120)
def load_metadata(schema: str):
    # Tables & views + size/approx rows for base tables
    tables_sql = """
    WITH t AS (
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = :schema
    )
    SELECT
        t.table_name,
        t.table_type,
        c.reltuples::bigint           AS approx_rows,
        pg_total_relation_size(c.oid) AS total_bytes
    FROM t
    LEFT JOIN pg_class     c ON c.relname = t.table_name
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = :schema
    ORDER BY t.table_name;
    """

    cols_sql = """
    SELECT
      table_name, column_name, data_type, is_nullable, column_default, ordinal_position
    FROM information_schema.columns
    WHERE table_schema = :schema
    ORDER BY table_name, ordinal_position;
    """

    pks_sql = """
    SELECT
      tc.table_name, kcu.column_name, tc.constraint_name, kcu.ordinal_position
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema    = kcu.table_schema
    WHERE tc.table_schema = :schema AND tc.constraint_type = 'PRIMARY KEY'
    ORDER BY tc.table_name, kcu.ordinal_position;
    """

    fks_sql = """
    SELECT
      tc.table_name                AS table_name,
      kcu.column_name              AS column_name,
      ccu.table_name               AS foreign_table_name,
      ccu.column_name              AS foreign_column_name,
      tc.constraint_name           AS constraint_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema    = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = tc.constraint_name
     AND ccu.table_schema    = tc.table_schema
    WHERE tc.table_schema = :schema AND tc.constraint_type = 'FOREIGN KEY'
    ORDER BY table_name, column_name;
    """

    eng = get_engine()
    with eng.connect() as conn:
        tables = pd.read_sql(text(tables_sql), conn, params={"schema": schema})
        cols = pd.read_sql(text(cols_sql), conn, params={"schema": schema})
        pks = pd.read_sql(text(pks_sql), conn, params={"schema": schema})
        fks = pd.read_sql(text(fks_sql), conn, params={"schema": schema})

    # Flag PK/FK on columns
    cols["is_pk"] = False
    cols["is_fk"] = False
    if not pks.empty:
        pk_idx = set(zip(pks.table_name, pks.column_name))
        cols.loc[
            cols.apply(lambda r: (r.table_name, r.column_name) in pk_idx, axis=1),
            "is_pk",
        ] = True
    if not fks.empty:
        fk_idx = set(zip(fks.table_name, fks.column_name))
        cols.loc[
            cols.apply(lambda r: (r.table_name, r.column_name) in fk_idx, axis=1),
            "is_fk",
        ] = True

    return tables, cols, pks, fks


@st.cache_data(ttl=60, show_spinner=False)
def sample_rows(schema: str, table_name: str, limit: int = 10) -> pd.DataFrame:
    sch = safe_ident(schema)
    tbl = safe_ident(table_name)
    sql = f'SELECT * FROM "{sch}"."{tbl}" LIMIT {int(limit)};'
    return read_sql_df(sql)


def build_graph_dot(tables: pd.DataFrame, fks: pd.DataFrame) -> str:
    def node_shape(tbl: str) -> str:
        if tbl.startswith("dim_"):
            return "box"
        if tbl.startswith("fct_"):
            return "ellipse"
        if tbl.startswith("stg_"):
            return "note"
        return "box"

    lines = [
        "digraph G {",
        "rankdir=LR;",
        'node [fontsize=10, style="rounded,filled", fillcolor="#EEF5FF"];',
    ]
    for t in tables["table_name"]:
        shape = node_shape(t)
        lines.append(f'"{t}" [shape={shape}];')
    for _, r in fks.iterrows():
        src = r["table_name"]
        dst = r["foreign_table_name"]
        label = f"{r['column_name']} → {r['foreign_column_name']}"
        lines.append(f'"{src}" -> "{dst}" [label="{html.escape(label)}", fontsize=9];')
    lines.append("}")
    return "\n".join(lines)


# ---------- Sidebar ----------

with st.sidebar:
    st.header("Filters")
    all_schemas = list_schemas(include_system=False)
    default_schema = (
        "rps_core"
        if "rps_core" in all_schemas
        else (all_schemas[0] if all_schemas else "")
    )
    selected_schema = st.selectbox(
        "Schema",
        options=all_schemas,
        index=all_schemas.index(default_schema) if default_schema else 0,
    )
    refresh = st.button("🔄 Refresh metadata (clear cache)")
    show_samples = st.checkbox("Show sample rows", value=True)
    sample_n = st.number_input(
        "Sample rows (head)", min_value=1, max_value=200, value=10, step=1
    )

if refresh:
    st.cache_data.clear()

if not selected_schema:
    st.warning("No non-system schemas found.")
    st.stop()

# ---------- Tabs ----------

tab_overview, tab_browser = st.tabs(["Overview", "Browser"])

with tab_overview:
    tables, cols, pks, fks = load_metadata(selected_schema)

    # Summary
    n_tables = len(tables)
    n_dims = sum(t.startswith("dim_") for t in tables["table_name"])
    n_facts = sum(t.startswith("fct_") for t in tables["table_name"])
    st.caption(
        f"Schema: `{selected_schema}` • Tables/Views: **{n_tables}** • "
        f"Dims: **{n_dims}** • Facts: **{n_facts}**"
    )

    # Top: tables grid
    tbl_view = tables.copy()
    tbl_view["size"] = tbl_view["total_bytes"].map(human_bytes)
    tbl_view = tbl_view[["table_name", "table_type", "approx_rows", "size"]]
    st.subheader("Tables & Views")
    st.dataframe(tbl_view, use_container_width=True, hide_index=True)

    # ER-ish graph
    st.subheader("Relationships (FKs)")
    if fks.empty:
        st.info("No foreign keys found.")
    else:
        dot = build_graph_dot(tables, fks)
        st.graphviz_chart(dot, use_container_width=True)

    # Details per table
    st.subheader("Table details")
    selected = st.multiselect(
        "Select tables",
        options=tbl_view["table_name"].tolist(),
        default=[
            n for n in tbl_view["table_name"].tolist() if n.startswith(("dim_", "fct_"))
        ][:3],
    )

    for t in selected:
        st.markdown(f"### `{t}`")
        c = cols[cols["table_name"] == t].copy()
        if not c.empty:
            c["key"] = c.apply(
                lambda r: "PK" if r.is_pk else ("FK" if r.is_fk else ""), axis=1
            )
            c_view = c[
                [
                    "ordinal_position",
                    "column_name",
                    "data_type",
                    "is_nullable",
                    "key",
                    "column_default",
                ]
            ].rename(
                columns={
                    "ordinal_position": "#",
                    "column_name": "column",
                    "data_type": "type",
                    "is_nullable": "nullable",
                    "column_default": "default",
                }
            )
            st.dataframe(c_view, use_container_width=True, hide_index=True)
        else:
            st.write("_No columns found?_")

        if show_samples:
            try:
                st.caption(f"Sample rows ({sample_n})")
                st.dataframe(
                    sample_rows(selected_schema, t, sample_n), use_container_width=True
                )
            except Exception as e:
                st.warning(f"Could not sample rows from {t}: {e}")

with tab_browser:
    st.subheader("Database Browser")
    # Simple tree: pick table → see columns + preview
    tables, cols, _, _ = load_metadata(selected_schema)
    tnames = tables["table_name"].tolist()
    if not tnames:
        st.info(f"No tables/views in schema `{selected_schema}`.")
    else:
        col_l, col_r = st.columns([1, 2])
        with col_l:
            t_pick = st.selectbox("Table", options=tnames)
            show_defs = st.checkbox("Show column defaults", value=False)
        with col_r:
            if t_pick:
                c = cols[cols["table_name"] == t_pick].copy()
                if not c.empty:
                    c_view = c[
                        [
                            "ordinal_position",
                            "column_name",
                            "data_type",
                            "is_nullable",
                            "column_default",
                        ]
                    ].rename(
                        columns={
                            "ordinal_position": "#",
                            "column_name": "column",
                            "data_type": "type",
                            "is_nullable": "nullable",
                            "column_default": "default",
                        }
                    )
                    if not show_defs:
                        c_view = c_view.drop(columns=["default"])
                    st.caption(f"Columns in `{selected_schema}.{t_pick}`")
                    st.dataframe(c_view, use_container_width=True, hide_index=True)
                if show_samples:
                    try:
                        st.caption(f"Sample rows ({sample_n})")
                        st.dataframe(
                            sample_rows(selected_schema, t_pick, sample_n),
                            use_container_width=True,
                        )
                    except Exception as e:
                        st.warning(f"Could not sample rows from {t_pick}: {e}")
