"""Requirements 11 and 12: assembling and writing the five output groups.

A row "passes" when none of its gating flags are set. Cleaned, Transformed
and Hashed all come from the same passing set, just with different columns
attached. Quarantined separately pulls back in the duplicate rows dropped
during dedup, since those never made it into `deduplicated` at all."""
import os
from typing import Any

import pandas as pd

from .validation import ALL_COMPUTED_FLAGS, COMPOSITE_KEY_COLUMNS


def gate_quality(deduplicated: pd.DataFrame, config: dict[str, Any]) -> tuple[pd.DataFrame, pd.Series]:
    """Returns (quality_passed, any_flagged). gating_flags in config.yaml
    decides which computed flags actually exclude a row, every flag is
    still computed and reported regardless."""
    gating_flags = config.get("validation", {}).get("gating_flags") or ALL_COMPUTED_FLAGS
    any_flagged = deduplicated[gating_flags].any(axis=1)
    quality_passed = deduplicated.loc[~any_flagged].reset_index(drop=True)
    return quality_passed, any_flagged


def assemble_quarantined(
    windowed_sorted: pd.DataFrame, deduplicated: pd.DataFrame, any_flagged: pd.Series
) -> pd.DataFrame:
    """duplicate_composite_key rows plus every gate-flagged row, each
    labelled with a human-readable quarantine_reason (not just the 14
    boolean flag columns, which stay too for programmatic filtering)."""
    dropped_duplicates = windowed_sorted[windowed_sorted.duplicated(subset=COMPOSITE_KEY_COLUMNS, keep="first")].copy()
    dropped_duplicates["quarantine_reason"] = "duplicate_composite_key"

    flagged_rows = deduplicated.loc[any_flagged].copy()
    flagged_rows["quarantine_reason"] = flagged_rows[ALL_COMPUTED_FLAGS].apply(
        lambda row: ", ".join(flag for flag in ALL_COMPUTED_FLAGS if row[flag]), axis=1
    )
    return pd.concat([dropped_duplicates, flagged_rows], ignore_index=True, sort=False)


def assemble_output_groups(quality_passed: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Cleaned: passes every check, no diagnostic columns. Transformed:
    Cleaned + resale_identifier + the storey_range split + the month
    split (analyst-facing additions, safe unconditionally here since every
    row already passed flag_storey_range_bounds and the month format/
    window check). Hashed: Cleaned + resale_identifier + both hash
    columns, resale_identifier_short_hash still collides (same rate as
    resale_identifier itself), resale_identifier_hash does not, kept side
    by side on purpose so the contrast is visible directly in the file.
    Cleaned deliberately keeps the single storey_range string and the
    un-split month, matching its own narrow spec definition."""
    extra_columns = ALL_COMPUTED_FLAGS + [
        "resale_identifier", "resale_identifier_key", "resale_identifier_hash", "resale_identifier_short_hash",
    ]
    cleaned = quality_passed.drop(columns=[c for c in extra_columns if c in quality_passed.columns])

    transformed = cleaned.copy()
    transformed["resale_identifier"] = quality_passed["resale_identifier"].values
    storey_split = transformed["storey_range"].str.extract(r"^(\d+) TO (\d+)$").astype(int)
    transformed["storey_lower"] = storey_split[0]
    transformed["storey_upper"] = storey_split[1]
    month_split = pd.PeriodIndex(transformed["month"], freq="M")
    transformed["transaction_year"] = month_split.year
    transformed["transaction_month"] = month_split.month

    hashed = cleaned.copy()
    hashed["resale_identifier"] = quality_passed["resale_identifier"].values
    hashed["resale_identifier_short_hash"] = quality_passed["resale_identifier_short_hash"].values
    hashed["resale_identifier_hash"] = quality_passed["resale_identifier_hash"].values

    return cleaned, transformed, hashed


def reconcile(windowed: pd.DataFrame, cleaned: pd.DataFrame, quarantined: pd.DataFrame) -> None:
    """Every windowed row must be counted exactly once: Cleaned, or
    Quarantined for a documented reason. Fails loudly here, before writing
    anything, rather than leaving a plausible-looking but wrong output."""
    assert len(windowed) == len(cleaned) + len(quarantined), (
        f"{len(windowed):,} windowed rows should reconcile to {len(cleaned):,} cleaned + "
        f"{len(quarantined):,} quarantined ({len(cleaned) + len(quarantined):,} total) but does not, "
        "a row was lost or double-counted somewhere above"
    )


def write_output_groups(
    output_groups: dict[str, pd.DataFrame], output_dir: str = "data/output"
) -> dict[str, tuple[str, str]]:
    """All five (Raw is written separately, during extraction) as CSV and
    Parquet, from data already fully computed in memory, so the files stay
    mutually consistent."""
    os.makedirs(output_dir, exist_ok=True)
    written: dict[str, tuple[str, str]] = {}
    for name, df in output_groups.items():
        csv_path = os.path.join(output_dir, f"{name}.csv")
        parquet_path = os.path.join(output_dir, f"{name}.parquet")
        df.to_csv(csv_path, index=False)
        df.to_parquet(parquet_path)
        written[name] = (csv_path, parquet_path)
    return written
