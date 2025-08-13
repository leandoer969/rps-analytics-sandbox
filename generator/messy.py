# generator/messy.py
import numpy as np
import pandas as pd
from typing import Iterable

DEFAULT_SRC = "erp"


def _rand_mask(n: int, rate: float, rng: np.random.Generator) -> np.ndarray:
    k = int(np.clip(rate, 0.0, 1.0) * n)
    idx = rng.choice(n, size=k, replace=False) if k > 0 else np.array([], dtype=int)
    mask = np.zeros(n, dtype=bool)
    mask[idx] = True
    return mask


def as_text(df: pd.DataFrame) -> pd.DataFrame:
    """Return a TEXT-only copy (all columns cast to string) for rps_raw.* sinks."""
    out = df.copy()
    for c in out.columns:
        out[c] = out[c].astype("string")
    return out


def add_lineage(
    df: pd.DataFrame, source_system: str = DEFAULT_SRC, source_file: str | None = None
) -> pd.DataFrame:
    out = df.copy()
    out["source_system"] = source_system
    out["source_file"] = source_file or f"{source_system}_batch.csv"
    # raw_ingest_ts is defaulted in DB; we don’t set it here
    return out


def messy_numeric(
    df: pd.DataFrame, cols: Iterable[str], rate: float, rng: np.random.Generator
) -> pd.DataFrame:
    """Turn some numerics into messy strings: thousands sep, comma decimals, blanks, 'NULL'."""
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            continue
        n = len(out)
        m = _rand_mask(n, rate, rng)
        noisy = out.loc[m, col].astype(float)
        formatted = noisy.map(
            lambda x: np.random.choice(
                [
                    f"{x:.2f}",
                    f"{x:,.2f}",  # thousands separator
                    str(int(round(x))) if x == x else "0",
                    f"{x:.2f}".replace(".", ","),  # comma decimal
                    "",  # empty
                    "NULL",
                ]
            )
        )
        out.loc[m, col] = formatted
    return out


def messy_dates(
    df: pd.DataFrame, col: str, rate: float, rng: np.random.Generator
) -> pd.DataFrame:
    """Shuffle date formats: YYYY-MM-DD, DD.MM.YYYY, MM/DD/YYYY, or timestamp-like."""
    if col not in df.columns:
        return df
    out = df.copy()
    n = len(out)
    m = _rand_mask(n, rate, rng)

    def fmt(x):
        try:
            d = pd.to_datetime(x).date()
        except Exception:
            return x
        choices = [
            d.strftime("%Y-%m-%d"),
            d.strftime("%d.%m.%Y"),
            d.strftime("%m/%d/%Y"),
            f"{d} 00:00:00",
        ]
        return np.random.choice(choices)

    out.loc[m, col] = out.loc[m, col].map(fmt)
    return out


def inject_dupes(
    df: pd.DataFrame, key_cols: list[str], rate: float, rng: np.random.Generator
) -> pd.DataFrame:
    """Copy a % of rows and slightly perturb non-key values to emulate late-arriving/conflicting dupes."""
    if len(df) == 0 or rate <= 0:
        return df
    n = len(df)
    k = max(1, int(rate * n))
    sample = df.iloc[rng.choice(n, size=min(k, n), replace=False)].copy()
    # Add small noise to the first non-key numeric col we find
    num_cols = [
        c
        for c in df.columns
        if c not in key_cols and pd.api.types.is_numeric_dtype(df[c])
    ]
    if num_cols:
        c = num_cols[0]
        sample[c] = (
            pd.to_numeric(sample[c], errors="coerce")
            * (1 + rng.normal(0, 0.05, size=len(sample)))
        ).round(2)
    # Return concat → same keys duplicated
    return pd.concat([df, sample], ignore_index=True)


def inject_fk_breaks(
    df: pd.DataFrame, fk_cols: list[str], rate: float, rng: np.random.Generator
) -> pd.DataFrame:
    """Replace some FK values with invalid ones (e.g., 999999) as TEXT."""
    out = df.copy()
    for col in fk_cols:
        if col not in out.columns:
            continue
        n = len(out)
        m = _rand_mask(n, rate, rng)
        out.loc[m, col] = rng.integers(900000, 999999, size=m.sum()).astype(str)
    return out
