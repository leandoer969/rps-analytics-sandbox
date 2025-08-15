import os
import time
import numpy as np
import pandas as pd
import psycopg2
import sqlalchemy as sa

# after existing imports:
from messy import (
    as_text,
    add_lineage,
    messy_numeric,
    messy_dates,
    inject_dupes,
    inject_fk_breaks,
)

WRITE_RAW = os.getenv("WRITE_RAW", "1") == "1"
SRC_SYSTEM = os.getenv("SRC_SYSTEM", "erp")
MESS_RATE_TYPES = float(os.getenv("MESS_RATE_TYPES", "0.15"))
MESS_RATE_DATES = float(os.getenv("MESS_RATE_DATES", "0.08"))
MESS_RATE_FK_BREAKS = float(os.getenv("MESS_RATE_FK_BREAKS", "0.02"))
MESS_RATE_DUPES = float(os.getenv("MESS_RATE_DUPES", "0.02"))
RAW_RNG = np.random.default_rng(20250813)


DB = os.getenv("POSTGRES_DB", "rps")
USER = os.getenv("POSTGRES_USER", "rps_user")
PWD = os.getenv("POSTGRES_PASSWORD", "rps_password")
HOST = os.getenv("POSTGRES_HOST", "postgres")
PORT = int(os.getenv("POSTGRES_PORT", "5432"))
SCALE = os.getenv("SCALE", "small").lower()
TZ = os.getenv("TZ", "Europe/Zurich")


# Add a helper to copy to rps_raw.* (TEXT tables)
def copy_df_raw(conn, df: pd.DataFrame, table: str):
    cols = ", ".join(df.columns)
    path = f"/tmp/{table.replace('.', '_')}.csv"
    df.to_csv(path, index=False, header=False)
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY;")
        with open(path, "r") as fh:
            cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV)", fh)
    print(f"Loaded {table}: {len(df)}")


def connect():
    """Connect to Postgres with retries; set timezone and search_path."""
    for _ in range(40):
        try:
            conn = psycopg2.connect(
                dbname=DB, user=USER, password=PWD, host=HOST, port=PORT
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET TIME ZONE %s;", (TZ,))
                cur.execute("SET search_path TO rps_core, public;")
            return conn
        except Exception:
            time.sleep(2)
    raise RuntimeError("Could not connect to Postgres")


def connect_engine():
    """SQLAlchemy engine for pandas read_sql (silences the warnings)."""
    url = f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}"
    return sa.create_engine(url)


def seed_dates(conn):
    import pandas as pd

    TZ = "Europe/Zurich"

    # Today in Zurich, then make it naïve (no tz) for storage
    today = pd.Timestamp.now(tz=TZ).normalize().tz_localize(None)
    start = (today - pd.DateOffset(years=3)).replace(day=1)  # 3-year spine
    end = today

    # Daily spine
    days = pd.date_range(start, end, freq="D")  # datetime64[ns], naïve
    df = pd.DataFrame({"date_id": days})

    # Simple parts
    df["year"] = df["date_id"].dt.year.astype(int)
    df["month"] = df["date_id"].dt.month.astype(int)
    df["week"] = df["date_id"].dt.isocalendar().week.astype(int)

    # Robust month/week boundaries
    df["month_start"] = df["date_id"].dt.to_period("M").dt.start_time.dt.date
    df["week_start"] = df["date_id"].dt.to_period("W-MON").dt.start_time.dt.date

    # Store date_id as DATE (not timestamp)
    df["date_id"] = df["date_id"].dt.date

    with conn.cursor() as cur:
        # Clear dim and anything depending on it (facts) to avoid FK errors
        cur.execute("TRUNCATE TABLE rps_core.dim_date RESTART IDENTITY CASCADE;")
        path = "/tmp/dim_date.csv"
        # Order must match COPY column list below
        df[["date_id", "year", "month", "week", "month_start", "week_start"]].to_csv(
            path, index=False, header=False, date_format="%Y-%m-%d"
        )
        with open(path, "r") as fh:
            cur.copy_expert(
                """
                COPY rps_core.dim_date (date_id, year, month, week, month_start, week_start)
                FROM STDIN WITH (FORMAT CSV)
                """,
                fh,
            )
    print(f"Seeded dim_date: {len(df)}")


def seed_regions(conn):
    cantons = [
        ("ZH", "Deutschschweiz"),
        ("BE", "Deutschschweiz"),
        ("LU", "Deutschschweiz"),
        ("UR", "Deutschschweiz"),
        ("SZ", "Deutschschweiz"),
        ("OW", "Deutschschweiz"),
        ("NW", "Deutschschweiz"),
        ("GL", "Deutschschweiz"),
        ("ZG", "Deutschschweiz"),
        ("FR", "Romandie"),
        ("SO", "Deutschschweiz"),
        ("BS", "Deutschschweiz"),
        ("BL", "Deutschschweiz"),
        ("SH", "Deutschschweiz"),
        ("AR", "Deutschschweiz"),
        ("AI", "Deutschschweiz"),
        ("SG", "Deutschschweiz"),
        ("GR", "Deutschschweiz"),
        ("AG", "Deutschschweiz"),
        ("TG", "Deutschschweiz"),
        ("TI", "Svizzera italiana"),
        ("VD", "Romandie"),
        ("VS", "Romandie"),
        ("NE", "Romandie"),
        ("GE", "Romandie"),
        ("JU", "Romandie"),
    ]
    df = pd.DataFrame(cantons, columns=["canton", "language_region"])
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE rps_core.dim_region RESTART IDENTITY CASCADE;")
        path = "/tmp/dim_region.csv"
        df.to_csv(path, index=False, header=False)
        with open(path, "r") as fh:
            cur.copy_expert(
                "COPY rps_core.dim_region (canton, language_region) FROM STDIN WITH (FORMAT CSV)",
                fh,
            )
    print("Seeded dim_region:", len(df))


def seed_payers(conn):
    payers = [
        ("Helsana", "Insurer"),
        ("CSS", "Insurer"),
        ("SWICA", "Insurer"),
        ("Sanitas", "Insurer"),
        ("Groupe Mutuel", "Insurer"),
        ("Concordia", "Insurer"),
        ("KPT", "Insurer"),
        ("Visana", "Insurer"),
    ]
    df = pd.DataFrame(payers, columns=["payer_name", "payer_type"])
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE rps_core.dim_payer RESTART IDENTITY CASCADE;")
        path = "/tmp/dim_payer.csv"
        df.to_csv(path, index=False, header=False)
        with open(path, "r") as fh:
            cur.copy_expert(
                "COPY rps_core.dim_payer (payer_name, payer_type) FROM STDIN WITH (FORMAT CSV)",
                fh,
            )
    print("Seeded dim_payer:", len(df))


def seed_products(conn):
    brands = [
        ("Avalimab", "avalimumab", "L04A", "Oncology"),
        ("Rimuxen", "rituximab", "L01X", "Oncology"),
        ("Glycora", "empagliflozin", "A10B", "Diabetes"),
        ("Cardevo", "sacubitril/valsartan", "C09X", "Cardio"),
        ("Neuroxim", "ocrelizumab", "L04A", "Neuro"),
        ("Pulmava", "pirfenidone", "R07X", "Respiratory"),
        ("Dermavax", "dupilumab", "D11X", "Immunology"),
        ("Ophtra", "ranibizumab", "S01L", "Ophthalmology"),
    ]
    rows = []
    today = pd.Timestamp.now(tz=TZ).normalize()
    rng = np.random.default_rng(11)
    for b, m, atc, ind in brands:
        launch = today - pd.DateOffset(months=int(rng.integers(12, 36)))
        rows.append([b, m, atc, ind, launch.date()])
    df = pd.DataFrame(
        rows, columns=["brand", "molecule", "atc_code", "indication", "launch_date"]
    )
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE rps_core.dim_product RESTART IDENTITY CASCADE;")
        path = "/tmp/dim_product.csv"
        df.to_csv(path, index=False, header=False, date_format="%Y-%m-%d")
        with open(path, "r") as fh:
            cur.copy_expert(
                "COPY rps_core.dim_product (brand, molecule, atc_code, indication, launch_date) "
                "FROM STDIN WITH (FORMAT CSV)",
                fh,
            )
    print("Seeded dim_product:", len(df))


def synthesize(conn):
    # scale knobs
    if SCALE == "medium":
        weeks_per_brand = 104
        regions_per = 26
        channels = ["Retail", "Hospital", "Specialty"]
        base_mu = 350
    else:
        weeks_per_brand = 78
        regions_per = 12
        channels = ["Retail", "Hospital"]
        base_mu = 180

    engine = connect_engine()

    # --- FIX: ensure Timestamp dtype for comparisons ---
    dates = pd.read_sql(
        "SELECT date_id FROM rps_core.dim_date ORDER BY date_id", engine
    )
    dates["date_id"] = pd.to_datetime(dates["date_id"])  # <- add this
    last_date = dates["date_id"].max()
    start_date = last_date - pd.Timedelta(weeks=weeks_per_brand)
    window = dates[dates["date_id"] >= start_date]

    prod = pd.read_sql("SELECT * FROM rps_core.dim_product ORDER BY product_id", engine)
    reg = pd.read_sql(
        "SELECT * FROM rps_core.dim_region ORDER BY region_id", engine
    ).sample(n=min(regions_per, 26), random_state=7)
    ch = pd.read_sql("SELECT * FROM rps_core.dim_channel ORDER BY channel_id", engine)
    pay = pd.read_sql("SELECT * FROM rps_core.dim_payer ORDER BY payer_id", engine)

    # SALES
    sales_rows = []
    rng = np.random.default_rng(42)
    for _, pr in prod.iterrows():
        launch = pd.to_datetime(pr["launch_date"]) + pd.Timedelta(weeks=4)
        w = window[window["date_id"] >= launch]
        for _, rg in reg.iterrows():
            for cname in channels:
                ch_id = int(ch[ch["channel_name"] == cname]["channel_id"].iloc[0])
                t = len(w)
                if t == 0:
                    continue
                season = np.sin(np.linspace(0, 4 * np.pi, t)) * 0.1
                base = rng.lognormal(mean=np.log(base_mu), sigma=0.4, size=t)
                ramp = np.clip(np.linspace(0.5, 1.2, t), 0.5, 1.2)
                noise = rng.normal(1.0, 0.08, size=t)
                units = np.maximum(0, (base * ramp * (1 + season) * noise)).astype(int)
                price = float(rng.uniform(120.0, 350.0))
                gross = (units * price).round(2)
                for dt, u, g in zip(w["date_id"].values, units, gross):
                    sales_rows.append(
                        [
                            pd.to_datetime(dt).date(),
                            int(pr["product_id"]),
                            int(rg["region_id"]),
                            ch_id,
                            int(u),
                            price,
                            float(g),
                        ]
                    )
    s_df = pd.DataFrame(
        sales_rows,
        columns=[
            "date_id",
            "product_id",
            "region_id",
            "channel_id",
            "units",
            "list_price_chf",
            "gross_sales_chf",
        ],
    )

    # REBATES (payer mix)
    rebate_rows = []
    for _, row in s_df.iterrows():
        payer = pay.sample(1).iloc[0]
        pct = float(np.random.uniform(0.05, 0.22))
        rebate = float(row["gross_sales_chf"] * pct)
        rebate_rows.append(
            [
                row["date_id"],
                row["product_id"],
                int(payer["payer_id"]),
                row["region_id"],
                rebate,
            ]
        )
    r_df = pd.DataFrame(
        rebate_rows,
        columns=["date_id", "product_id", "payer_id", "region_id", "rebate_chf"],
    )

    # PROMO
    promo_rows = []
    for pid, grp in s_df.groupby("product_id"):
        by_wr = grp.groupby(["date_id", "region_id"], as_index=False)["units"].sum()
        by_wr["spend_chf"] = (by_wr["units"] * np.random.uniform(0.5, 1.5)).round(2)
        by_wr["touchpoints"] = (by_wr["units"] * np.random.uniform(0.01, 0.03)).astype(
            int
        )
        for _, r in by_wr.iterrows():
            ch_id = int(ch.sample(1).iloc[0]["channel_id"])
            promo_rows.append(
                [
                    r["date_id"],
                    int(pid),
                    int(r["region_id"]),
                    ch_id,
                    float(r["spend_chf"]),
                    int(max(1, r["touchpoints"])),
                ]  # noqa: E501
            )
    p_df = pd.DataFrame(
        promo_rows,
        columns=[
            "date_id",
            "product_id",
            "region_id",
            "channel_id",
            "spend_chf",
            "touchpoints",
        ],
    )

    # FORECAST (baseline + uplift)
    s_agg = s_df.groupby(["date_id", "product_id", "region_id"], as_index=False).agg(
        units_total=("units", "sum"), gross_sales_chf=("gross_sales_chf", "sum")
    )
    r_agg = r_df.groupby(["date_id", "product_id", "region_id"], as_index=False).agg(
        rebate_chf=("rebate_chf", "sum")
    )
    merged = (
        pd.merge(s_agg, r_agg, how="left", on=["date_id", "product_id", "region_id"])
        .fillna({"rebate_chf": 0.0})
        .copy()
    )
    merged["rebate_rate"] = np.where(
        merged["gross_sales_chf"] > 0,
        merged["rebate_chf"] / merged["gross_sales_chf"],
        0.0,
    )
    p_agg = p_df.groupby(["date_id", "product_id", "region_id"], as_index=False).agg(
        spend_chf=("spend_chf", "sum"), touchpoints=("touchpoints", "sum")
    )
    m = (
        pd.merge(merged, p_agg, how="left", on=["date_id", "product_id", "region_id"])
        .fillna({"spend_chf": 0.0, "touchpoints": 0})
        .sort_values(["product_id", "region_id", "date_id"])
    )
    m["baseline_units"] = (
        m.groupby(["product_id", "region_id"])["units_total"]
        .transform(lambda s: s.shift(1).rolling(4, min_periods=1).mean())
        .fillna(0)
    )
    alpha, beta = 0.003, -0.5
    m["uplift_units"] = (
        alpha * m["spend_chf"] + beta * m["rebate_rate"] * m["units_total"]
    ).clip(lower=-0.4 * m["units_total"], upper=0.5 * m["units_total"])
    m["forecast_units"] = np.maximum(0, m["baseline_units"] + m["uplift_units"])
    f_df = m[
        [
            "date_id",
            "product_id",
            "region_id",
            "baseline_units",
            "uplift_units",
            "forecast_units",
        ]
    ].copy()

    # ---- loaders -----------------------------------------------------------

    def copy_df(conn, df, table):
        cols = ", ".join(df.columns)
        path = f"/tmp/{table.replace('.', '_')}.csv"
        df.to_csv(path, index=False, header=False, date_format="%Y-%m-%d")
        with conn.cursor() as cur:
            # facts don't have dependents – restart identity only
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY;")
            with open(path, "r") as fh:
                cur.copy_expert(
                    f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV)", fh
                )
        print(f"Loaded {table}: {len(df)}")

    # ---- loaders -----------------------------------------------------------

    # existing clean loads (unchanged)
    copy_df(conn, s_df, "rps_core.fct_sales")
    copy_df(conn, r_df, "rps_core.fct_rebates")
    copy_df(conn, p_df, "rps_core.fct_promo")
    copy_df(conn, f_df, "rps_core.fct_forecast")

    # --- NEW: write messy raw copies to rps_raw.* ---
    if WRITE_RAW:
        # SALES raw
        sr = s_df.copy()
        # Date to iso string first
        sr["date_id"] = pd.to_datetime(sr["date_id"]).dt.date.astype(str)
        # Make some numeric columns messy
        sr = messy_numeric(
            sr, ["units", "list_price_chf", "gross_sales_chf"], MESS_RATE_TYPES, RAW_RNG
        )
        # FK breaks
        sr = inject_fk_breaks(
            sr, ["product_id", "region_id", "channel_id"], MESS_RATE_FK_BREAKS, RAW_RNG
        )
        # Date format chaos
        sr = messy_dates(sr, "date_id", MESS_RATE_DATES, RAW_RNG)
        # Duplicate business keys
        sr = inject_dupes(
            sr,
            ["date_id", "product_id", "region_id", "channel_id"],
            MESS_RATE_DUPES,
            RAW_RNG,
        )
        # TEXT + lineage
        sr = as_text(sr)
        sr = add_lineage(sr, source_system=SRC_SYSTEM, source_file="sales_raw.csv")
        copy_df_raw(conn, sr, "rps_raw.sales_raw")

        # REBATES raw
        rr = r_df.copy()
        rr["date_id"] = pd.to_datetime(rr["date_id"]).dt.date.astype(str)
        rr = messy_numeric(rr, ["rebate_chf"], MESS_RATE_TYPES, RAW_RNG)
        rr = inject_fk_breaks(
            rr, ["product_id", "payer_id", "region_id"], MESS_RATE_FK_BREAKS, RAW_RNG
        )
        rr = messy_dates(rr, "date_id", MESS_RATE_DATES, RAW_RNG)
        rr = inject_dupes(
            rr, ["date_id", "product_id", "region_id"], MESS_RATE_DUPES, RAW_RNG
        )
        rr = as_text(rr)
        rr = add_lineage(rr, source_system=SRC_SYSTEM, source_file="rebates_raw.csv")
        copy_df_raw(conn, rr, "rps_raw.rebates_raw")

        # PROMO raw
        prw = p_df.copy()
        prw["date_id"] = pd.to_datetime(prw["date_id"]).dt.date.astype(str)
        prw = messy_numeric(prw, ["spend_chf", "touchpoints"], MESS_RATE_TYPES, RAW_RNG)
        prw = inject_fk_breaks(
            prw, ["product_id", "region_id", "channel_id"], MESS_RATE_FK_BREAKS, RAW_RNG
        )
        prw = messy_dates(prw, "date_id", MESS_RATE_DATES, RAW_RNG)
        prw = inject_dupes(
            prw,
            ["date_id", "product_id", "region_id", "channel_id"],
            MESS_RATE_DUPES,
            RAW_RNG,
        )
        prw = as_text(prw)
        prw = add_lineage(prw, source_system=SRC_SYSTEM, source_file="promo_raw.csv")
        copy_df_raw(conn, prw, "rps_raw.promo_raw")

        # FORECAST raw
        fr = f_df.copy()
        fr["date_id"] = pd.to_datetime(fr["date_id"]).dt.date.astype(str)
        fr = messy_numeric(
            fr,
            ["baseline_units", "uplift_units", "forecast_units"],
            MESS_RATE_TYPES,
            RAW_RNG,
        )
        fr = inject_fk_breaks(
            fr, ["product_id", "region_id"], MESS_RATE_FK_BREAKS, RAW_RNG
        )
        fr = messy_dates(fr, "date_id", MESS_RATE_DATES, RAW_RNG)
        fr = inject_dupes(
            fr, ["date_id", "product_id", "region_id"], MESS_RATE_DUPES, RAW_RNG
        )
        fr = as_text(fr)
        fr = add_lineage(fr, source_system=SRC_SYSTEM, source_file="forecast_raw.csv")
        copy_df_raw(conn, fr, "rps_raw.forecast_raw")


def main():
    conn = connect()
    with conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_user;")
        print("Connected to:", cur.fetchone())
        cur.execute("SELECT to_regclass('rps_core.dim_date');")
        print("regclass rps_core.dim_date =", cur.fetchone()[0])
    seed_dates(conn)
    seed_regions(conn)
    seed_payers(conn)
    seed_products(conn)
    synthesize(conn)
    print("Data generation complete.")


if __name__ == "__main__":
    main()
