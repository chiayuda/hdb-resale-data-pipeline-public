"""Requirements 3, 6 and 7: every validation rule and quality flag. Every
threshold, accepted-value list and on/off switch is read from config,
nothing hardcoded here.
"""
import re
from typing import Any, Optional

import pandas as pd

REFERENCE_COLUMNS = ["town", "flat_type", "flat_model", "storey_range"]
CONFIG_DEFINED_DOMAINS = {"town", "flat_type", "flat_model"}  # storey_range stays live-derived
COMPOSITE_KEY_COLUMNS = [
    "month", "town", "flat_type", "block", "street_name",
    "storey_range", "floor_area_sqm", "flat_model",
    "lease_commence_date",
]
ALL_COMPUTED_FLAGS = [
    "flag_town", "flag_flat_type", "flag_flat_model", "flag_storey_range",
    "flag_storey_range_bounds", "flag_missing_required_field",
    "flag_block_format", "flag_non_numeric", "flag_floor_area",
    "flag_lease_commence", "flag_lease_commence_range",
    "flag_resale_price_range", "flag_remaining_lease_range", "flag_anomaly",
]


def apply_month_window(master: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Date validation: format first (a raw string range check alone would
    accept a malformed value like '2012-13'), then the configured window.
    A row outside the window never reaches profiling, cleaning or
    validation at all."""
    validation_cfg = config.get("validation", {})
    month_cfg = validation_cfg.get("month", {})
    enabled = month_cfg.get("enabled", True)
    pattern = re.compile(month_cfg.get("pattern", r"^\d{4}-(0[1-9]|1[0-2])$"))

    if enabled:
        valid_format = master["month"].astype(str).str.match(pattern)
    else:
        valid_format = pd.Series(True, index=master.index)
    in_range = (master["month"] >= config["window_start"]) & (master["month"] <= config["window_end"])
    return master[valid_format & in_range].reset_index(drop=True)


def compute_reference_domains(
    master: pd.DataFrame, config: dict[str, Any]
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """'Jan 2012 as authoritative' means the whole published file (2000-01
    to 2012-02), not just the calendar month, so this reads from master,
    not the windowed data. town/flat_type/flat_model are config-defined
    (reviewed, explicit lists); storey_range stays live-derived since its
    banding convention changed mid-window."""
    from .cleaning import DBSS_MODEL_NAME  # local import avoids a cycle at module load

    validation_cfg = config.get("validation", {})
    authoritative_id = config["authoritative_dataset_id"]
    reference_month_data = master[master["source_dataset_id"] == authoritative_id].copy()

    # master is raw/uncleaned; apply the same normalisation as cleaning.py
    # so this stays an apples-to-apples comparison against the
    # already-cleaned-casing config lists.
    cleaned_flat_model = reference_month_data["flat_model"].str.strip().str.title()
    cleaned_flat_model.loc[cleaned_flat_model.str.upper() == DBSS_MODEL_NAME] = DBSS_MODEL_NAME
    reference_month_data["flat_model"] = cleaned_flat_model

    live_reference_domains = {
        col: set(reference_month_data[col].dropna().unique()) for col in REFERENCE_COLUMNS
    }

    configured_domains = validation_cfg.get("reference_domains", {}) or {}
    reference_domains: dict[str, set[str]] = {}
    for col in REFERENCE_COLUMNS:
        if col in CONFIG_DEFINED_DOMAINS:
            reference_domains[col] = set(configured_domains.get(col, []))
        else:
            reference_domains[col] = live_reference_domains[col]

    return reference_domains, live_reference_domains


def flag_reference_columns(
    windowed: pd.DataFrame, reference_domains: dict[str, set[str]], config: dict[str, Any]
) -> pd.DataFrame:
    """Domain membership for town/flat_type/flat_model/storey_range. An
    unseen value is flagged (and printed as UNKNOWN <COLUMN> DETECTED),
    not silently accepted, until it's reviewed into reference_domains or
    reference_exceptions in config.yaml."""
    windowed = windowed.copy()
    reference_exceptions = config.get("validation", {}).get("reference_exceptions", {}) or {}

    for col in REFERENCE_COLUMNS:
        exceptions = reference_exceptions.get(col) or []
        allowed_values = reference_domains[col] | set(exceptions)
        is_present = windowed[col].notna()
        in_domain = windowed[col].isin(allowed_values)
        windowed[f"flag_{col}"] = is_present & ~in_domain

    return windowed


def flag_missing_required(windowed: pd.DataFrame, config: dict[str, Any]) -> tuple[pd.DataFrame, list[str]]:
    """Generic completeness check across every published column
    (validation.nullable), not a bespoke rule per column. remaining_lease
    is the one column configured nullable: true, structurally absent from
    two of the three source files."""
    windowed = windowed.copy()
    nullable_cfg = config.get("validation", {}).get("nullable", {})
    required_columns = [col for col, is_nullable in nullable_cfg.items() if not is_nullable]

    missing_any = pd.Series(False, index=windowed.index)
    for col in required_columns:
        missing_any = missing_any | windowed[col].isna()
    windowed["flag_missing_required_field"] = missing_any
    return windowed, required_columns


def flag_block_format(windowed: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """block is digits, optionally one trailing letter (e.g. '123', '9A'),
    confirmed against every distinct real value before choosing this
    pattern."""
    windowed = windowed.copy()
    cfg = config.get("validation", {}).get("block_format", {})
    enabled = cfg.get("enabled", True)
    pattern = re.compile(cfg.get("pattern", r"^\d+[A-Z]?$"))

    if enabled:
        present = windowed["block"].notna()
        valid_format = windowed["block"].astype(str).str.match(pattern)
        windowed["flag_block_format"] = present & ~valid_format
    else:
        windowed["flag_block_format"] = False
    return windowed


def flag_non_numeric(deduplicated: pd.DataFrame, config: dict[str, Any]) -> tuple[pd.DataFrame, list[str]]:
    """Checked before any range/bound check below runs: pd.to_numeric()
    would otherwise raise on a non-numeric value and halt the whole
    pipeline instead of quarantining the row."""
    deduplicated = deduplicated.copy()
    numeric_cfg = config.get("validation", {}).get("numeric", {})
    numeric_columns = [col for col, is_numeric in numeric_cfg.items() if is_numeric]

    flag_any = pd.Series(False, index=deduplicated.index)
    for col in numeric_columns:
        is_present = deduplicated[col].notna()
        coerced = pd.to_numeric(deduplicated[col], errors="coerce")
        flag_any = flag_any | (is_present & coerced.isna())
    deduplicated["flag_non_numeric"] = flag_any
    return deduplicated, numeric_columns


def flag_floor_area(deduplicated: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Hard bound from business domain knowledge (smallest/largest real
    HDB flat type), not this window's observed range."""
    deduplicated = deduplicated.copy()
    cfg = config.get("validation", {}).get("floor_area", {})
    enabled = cfg.get("enabled", True)
    min_sqm = cfg.get("min_sqm", 0)
    max_sqm = cfg.get("max_sqm", 350)

    if enabled:
        floor_area = pd.to_numeric(deduplicated["floor_area_sqm"], errors="coerce")
        deduplicated["flag_floor_area"] = (floor_area <= min_sqm) | (floor_area > max_sqm)
    else:
        deduplicated["flag_floor_area"] = False
    return deduplicated


def flag_lease_commence(deduplicated: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Relative: cannot commence after its own transaction. Absolute:
    cannot be before HDB existed (1960, domain knowledge) or after today
    (computed at run time, never goes stale)."""
    deduplicated = deduplicated.copy()
    cfg = config.get("validation", {}).get("lease_commence", {})
    enabled = cfg.get("enabled", True)
    min_year = cfg.get("min_year", 1960)
    current_year = pd.Timestamp.now().year

    if enabled:
        transaction_year = pd.PeriodIndex(deduplicated["month"], freq="M").year
        lease_commence = pd.to_numeric(deduplicated["lease_commence_date"], errors="coerce")
        deduplicated["flag_lease_commence"] = lease_commence > transaction_year
        deduplicated["flag_lease_commence_range"] = (lease_commence < min_year) | (lease_commence > current_year)
    else:
        deduplicated["flag_lease_commence"] = False
        deduplicated["flag_lease_commence_range"] = False
    return deduplicated


def flag_resale_price_range(deduplicated: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """A price of zero or less is not a low price, it is not a sale."""
    deduplicated = deduplicated.copy()
    cfg = config.get("validation", {}).get("resale_price_bounds", {})
    enabled = cfg.get("enabled", True)
    price_min = cfg.get("min", 1)
    price_max = cfg.get("max", 10_000_000)

    if enabled:
        price = pd.to_numeric(deduplicated["resale_price"], errors="coerce")
        deduplicated["flag_resale_price_range"] = (price < price_min) | (price > price_max)
    else:
        deduplicated["flag_resale_price_range"] = False
    return deduplicated


def flag_remaining_lease_range(deduplicated: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Only meaningful where a source figure was published at all, absent
    for two of the three source files (structural, not a violation)."""
    deduplicated = deduplicated.copy()
    cfg = config.get("validation", {}).get("remaining_lease_bounds", {})
    enabled = cfg.get("enabled", True)
    min_years = cfg.get("min_years", 0)
    max_years = cfg.get("max_years", config.get("validation", {}).get("lease_years", 99))

    if enabled:
        published = pd.to_numeric(deduplicated["remaining_lease"], errors="coerce")
        deduplicated["flag_remaining_lease_range"] = published.notna() & (
            (published < min_years) | (published > max_years)
        )
    else:
        deduplicated["flag_remaining_lease_range"] = False
    return deduplicated


def flag_storey_range_bounds(deduplicated: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Independent of, and in addition to, flag_storey_range (domain
    membership): this parses the two storey numbers and applies a hard
    bound, deliberately not a band-width rule, since the real band width
    varies by era."""
    deduplicated = deduplicated.copy()
    cfg = config.get("validation", {}).get("storey_range_bounds", {})
    enabled = cfg.get("enabled", True)
    min_storey = cfg.get("min_storey", 1)
    max_storey = cfg.get("max_storey", 100)
    pattern = re.compile(r"^(\d+) TO (\d+)$")

    if enabled:
        parsed = deduplicated["storey_range"].astype(str).str.extract(pattern)
        start = pd.to_numeric(parsed[0])
        end = pd.to_numeric(parsed[1])
        is_present = deduplicated["storey_range"].notna()
        unparseable = is_present & start.isna()
        out_of_bounds = (start < min_storey) | (end > max_storey) | (end <= start)
        deduplicated["flag_storey_range_bounds"] = unparseable | out_of_bounds.fillna(False)
    else:
        deduplicated["flag_storey_range_bounds"] = False
    return deduplicated


def flag_anomaly(deduplicated: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Global 1.5x IQR fence, purely statistical (unusual relative to real
    peers), a different question from flag_resale_price_range (physically
    impossible)."""
    deduplicated = deduplicated.copy()
    cfg = config.get("validation", {}).get("anomaly", {})
    enabled = cfg.get("enabled", True)
    iqr_multiplier = cfg.get("iqr_multiplier", 1.5)

    if enabled:
        price = pd.to_numeric(deduplicated["resale_price"], errors="coerce")
        q1, q3 = price.quantile([0.25, 0.75])
        iqr = q3 - q1
        lower, upper = q1 - iqr_multiplier * iqr, q3 + iqr_multiplier * iqr
        deduplicated["flag_anomaly"] = (price < lower) | (price > upper)
    else:
        deduplicated["flag_anomaly"] = False
    return deduplicated


def cross_check_remaining_lease(
    deduplicated: pd.DataFrame, config: dict[str, Any]
) -> Optional[dict[str, float]]:
    """Compares this pipeline's computed remaining lease against HDB's own
    published figure. Returns (agreement_count, has_published_count,
    difference_months) for the caller to report."""
    cfg = config.get("validation", {}).get("remaining_lease_cross_check", {})
    if not cfg.get("enabled", True):
        return None

    tolerance_months = cfg.get("tolerance_months", 12)
    months_per_year = 12
    has_published = deduplicated["remaining_lease"].notna()
    computed_total_months = deduplicated["remaining_lease_years"] * months_per_year + deduplicated["remaining_lease_months"]
    published_total_months = deduplicated["remaining_lease"] * months_per_year
    difference_months = (computed_total_months - published_total_months).where(has_published)
    agree = (difference_months.abs() <= tolerance_months) & has_published

    return {
        "has_published": int(has_published.sum()),
        "agree": int(agree.sum()),
        "difference_min": difference_months.min(),
        "difference_max": difference_months.max(),
        "difference_mean": difference_months.mean(),
    }
