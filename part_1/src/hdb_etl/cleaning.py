"""Cleaning in support of requirement 3. Must run before validation, or
cosmetic variation (casing) gets mistaken for a genuine domain violation."""
import pandas as pd

DBSS_MODEL_NAME = "DBSS"


def clean_flat_model(df: pd.DataFrame) -> pd.DataFrame:
    """flat_model has a real casing inconsistency in the raw data
    ('2-room', 'Adjoined flat'). Title-cases it, with DBSS explicitly
    protected since .title() would otherwise turn it into 'Dbss'."""
    df = df.copy()
    df["flat_model"] = df["flat_model"].str.strip().str.title()
    df.loc[df["flat_model"].str.upper() == DBSS_MODEL_NAME, "flat_model"] = DBSS_MODEL_NAME
    return df
