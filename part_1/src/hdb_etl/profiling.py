"""Requirement 2: data profiling, run before any cleaning so it describes
the data as published, not a version with its inconsistencies already fixed."""
import re

import pandas as pd


def profile_columns(df: pd.DataFrame) -> pd.DataFrame:
    profile_rows = []
    for col in df.columns:
        series = df[col]
        numeric = pd.to_numeric(series, errors="coerce")
        non_null_count = series.notna().sum()
        is_numeric = non_null_count > 0 and numeric.notna().sum() == non_null_count

        distinct_count = series.nunique()
        cardinality_ratio = (distinct_count / non_null_count) if non_null_count else None
        value_lengths = series.dropna().astype(str).str.len().unique()
        consistent_length = len(value_lengths) == 1

        if is_numeric:
            suggested_check = "numeric, validate with a range/bound, not a list"
        elif distinct_count <= 50 and cardinality_ratio < 0.01:
            suggested_check = "low cardinality, candidate for a fixed accepted-value list"
        elif consistent_length:
            suggested_check = "consistent length, candidate for a regex/format check"
        else:
            suggested_check = "high cardinality, no fixed shape, free text"

        profile_rows.append({
            "column": col,
            "dtype": series.dtype,
            "non_null_count": non_null_count,
            "null_count": series.isna().sum(),
            "distinct_count": distinct_count,
            "cardinality_ratio": round(cardinality_ratio, 6) if cardinality_ratio is not None else None,
            "min": numeric.min() if is_numeric else None,
            "max": numeric.max() if is_numeric else None,
            "sample_values": series.dropna().unique()[:5].tolist(),
            "suggested_check": suggested_check,
        })
    return pd.DataFrame(profile_rows)


def find_hidden_nulls(
    df: pd.DataFrame,
    column: str,
    known_placeholders: tuple[str, ...] = ("na", "n/a", "null", "none", "nil", "unknown", "-", ".", "?"),
) -> list[str]:
    """Catches values that look missing but isna() will not see: whitespace-only
    strings and common placeholder tokens. Limitation: only catches tokens
    listed here, a dataset-specific one not on this list still slips through."""
    escaped = [re.escape(token) for token in known_placeholders]
    pattern = re.compile(r"^\s*$|^(" + "|".join(escaped) + r")\s*$", re.IGNORECASE)
    distinct_values = df[column].dropna().astype(str).unique()
    return [v for v in distinct_values if pattern.match(v)]


def scan_hidden_nulls(df: pd.DataFrame) -> dict[str, list[str]]:
    """{column: [suspicious values]} for every column that has any."""
    return {col: found for col in df.columns if (found := find_hidden_nulls(df, col))}
