import os
import time
import numpy as np
import pandas as pd
import psycopg2

DB = os.getenv("POSTGRES_DB", "rps")
USER = os.getenv("POSTGRES_USER", "rps_user")
PWD = os.getenv("POSTGRES_PASSWORD", "rps_password")
HOST = os.getenv("POSTGRES_HOST", "postgres")
PORT = int(os.getenv("POSTGRES_PORT", "5432"))
SCALE = os.getenv("SCALE", "small").lower()
TZ = os.getenv("TZ", "Europe/Zurich")


def connect():
    for _ in range(40):
        try:
            conn = psycopg2.connect(
                dbname=DB, user=USER, password=PWD, host=HOST, port=PORT
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET TIME ZONE %s;", (TZ,))
            return conn
        except Exception:
            time.sleep(2)
    raise RuntimeError("Could not connect to Postgres")


def seed_dates(conn):
    start = (
        (pd.Timestamp.now(tz=TZ) - pd.DateOffset(years=2)).normalize().replace(day=1)
    )
    end = pd.Timestamp.now(tz=TZ).normalize()
    days = pd.date_range(start, end, freq="D")
    df = pd.DataFrame({"date_id": days})
    df["year"] = df["date_id"].dt.year
    df["month"] = df["date_id"].dt.month
    df["week"] = df["date_id"].dt.isocalendar().week.astype(int)
    df["month_start"] = df["date_id"].values.astype("datetime64[M]")
    df["week_start"] = df["date_id"] - pd.to_timedelta(
        df["date_id"].dt.weekday, unit="D"
    )
    with conn.cursor() as cur:
        cur.execute("DELETE FROM rps.dim_date;")
        path = "/tmp/dim_date.csv"
        df.to_csv(path, index=False, header=False, date_format="%Y-%m-%d")
        cur.copy_from(open(path, "r"), "rps.dim_date", sep=",")
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
        cur.execute("DELETE FROM rps.dim_region;")
        path = "/tmp/dim_region.csv"
        df.to_csv(path, index=False, header=False)
        cur.copy_from(open(path, "r"), "rps.dim_region", sep=",")
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
        cur.execute("DELETE FROM rps.dim_payer;")
        path = "/tmp/dim_payer.csv"
        df.to_csv(path, index=False, header=False)
        cur.copy_from(open(path, "r"), "rps.dim_payer", sep=",")
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
        cur.execute("DELETE FROM rps.dim_product;")
        path = "/tmp/dim_product.csv"
        df.to_csv(path, index=False, header=False, date_format="%Y-%m-%d")
        cur.copy_from(open(path, "r"), "rps.dim_product", sep=",")
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

    dates = pd.read_sql("SELECT date_id FROM rps.dim_date ORDER BY date_id", conn)
    last_date = dates["date_id"].max()
    start_date = last_date - pd.Timedelta(weeks=weeks_per_brand)
    weekly = dates[dates["date_id"] >= start_date]

    prod = pd.read_sql("SELECT * FROM rps.dim_product ORDER BY product_id", conn)
    reg = pd.read_sql("SELECT * FROM rps.dim_region ORDER BY region_id", conn).sample(
        n=min(regions_per, 26), random_state=7
    )
    ch = pd.read_sql("SELECT * FROM rps.dim_channel ORDER BY channel_id", conn)
    pay = pd.read_sql("SELECT * FROM rps.dim_payer ORDER BY payer_id", conn)

    # SALES
    sales_rows = []
    rng = np.random.default_rng(42)
    for _, pr in prod.iterrows():
        launch = pd.to_datetime(pr["launch_date"]) + pd.Timedelta(weeks=4)
        w = weekly[weekly["date_id"] >= launch]
        for _, rg in reg.iterrows():
            for cname in channels:
                ch_id = int(ch[ch["channel_name"] == cname]["channel_id"].iloc[0])
                t = len(w)
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

    def copy_df(df, table):
        path = f"/tmp/{table.replace('.', '_')}.csv"
        df.to_csv(path, index=False, header=False, date_format="%Y-%m-%d")
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table};")
            with open(path, "r") as fh:
                cur.copy_from(fh, table, sep=",")
        print(f"Loaded {table}: {len(df)}")

    copy_df(s_df, "rps.fct_sales")
    copy_df(r_df, "rps.fct_rebates")
    copy_df(p_df, "rps.fct_promo")
    copy_df(f_df, "rps.fct_forecast")


def main():
    conn = connect()
    seed_dates(conn)
    seed_regions(conn)
    seed_payers(conn)
    seed_products(conn)
    synthesize(conn)
    print("Data generation complete.")


if __name__ == "__main__":
    main()
