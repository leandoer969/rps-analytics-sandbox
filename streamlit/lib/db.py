import os
import pandas as pd
from sqlalchemy import create_engine, text
from functools import lru_cache

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "rps")
POSTGRES_USER = os.getenv("POSTGRES_USER", "rps_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "rps_password")


@lru_cache(maxsize=1)
def get_engine():
    url = (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    # pool_pre_ping helps in dev when containers restart
    return create_engine(url, pool_pre_ping=True)


def read_sql_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        conn.execute(
            text("SET LOCAL search_path TO rps_mart, rps_stg, rps_core, public")
        )
        return pd.read_sql(text(sql), conn, params=params or {})
