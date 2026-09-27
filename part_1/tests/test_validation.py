"""One test per validated column/check, same 15 cases as the notebook's own
synthetic-corruption demo, run here against the actual hdb_etl functions
instead of inline notebook code. Each copies one real, confirmed-clean row,
corrupts exactly one field, and asserts the flag(s) that should fire do,
and nothing else does.

Run from part_1/: pytest tests/
"""
import pandas as pd
import pytest

from hdb_etl.pipeline import recompute_all_flags
from hdb_etl.validation import ALL_COMPUTED_FLAGS, apply_month_window


def test_month_format_rejects_malformed_value(config):
    """A raw string range check alone would accept "2012-13" (lexically
    between the window bounds); the format regex has to catch it first."""
    master = pd.DataFrame({
        "month": ["2012-13", config["window_start"]],
        "source_dataset_id": ["x", "x"],
    })
    windowed = apply_month_window(master, config)
    assert list(windowed["month"]) == [config["window_start"]]


def fire(flagged_state, config, template_index, column, bad_value):
    """Corrupts one field on the real windowed data, recomputes every
    flag through the real pipeline function, returns the set of flags
    that fired on that one row."""
    windowed = flagged_state["windowed"].copy()
    windowed.loc[template_index, column] = bad_value
    _, _, deduplicated = recompute_all_flags(windowed, config, flagged_state["reference_domains"])
    row = deduplicated.loc[template_index]
    return {flag for flag in ALL_COMPUTED_FLAGS if bool(row[flag])}


def test_town_not_in_reference_domain(flagged_state, config, clean_template):
    fired = fire(flagged_state, config, clean_template.name, "town", "NOT_A_REAL_TOWN")
    assert fired == {"flag_town"}


def test_flat_type_not_in_reference_domain(flagged_state, config, clean_template):
    fired = fire(flagged_state, config, clean_template.name, "flat_type", "9 ROOM")
    assert fired == {"flag_flat_type"}


def test_flat_model_not_in_reference_domain(flagged_state, config, clean_template):
    fired = fire(flagged_state, config, clean_template.name, "flat_model", "Not A Real Model")
    assert fired == {"flag_flat_model"}


def test_storey_range_unrecognized_but_numerically_sane(flagged_state, config, clean_template):
    """Shaped like a real band, passes the numeric bounds, but is not one
    of the bands seen in the authoritative file: only the domain check
    should fire."""
    fired = fire(flagged_state, config, clean_template.name, "storey_range", "12 TO 16")
    assert fired == {"flag_storey_range"}


def test_storey_range_numerically_impossible(flagged_state, config, clean_template):
    """Starts below storey 1: fails the numeric bounds too, so both the
    domain check and the bounds check fire together."""
    fired = fire(flagged_state, config, clean_template.name, "storey_range", "00 TO 04")
    assert fired == {"flag_storey_range", "flag_storey_range_bounds"}


def test_missing_required_field(flagged_state, config, clean_template):
    fired = fire(flagged_state, config, clean_template.name, "street_name", None)
    assert fired == {"flag_missing_required_field"}


def test_block_format(flagged_state, config, clean_template):
    fired = fire(flagged_state, config, clean_template.name, "block", "12-A")
    assert fired == {"flag_block_format"}


def test_non_numeric_value(flagged_state, config, clean_template):
    """A garbage resale_price should trip flag_non_numeric alone, not
    flag_anomaly or flag_resale_price_range: both of those coerce with
    errors="coerce" and treat an unparseable value as absent."""
    fired = fire(flagged_state, config, clean_template.name, "resale_price", "abc")
    assert fired == {"flag_non_numeric"}


def test_floor_area_too_small(flagged_state, config, clean_template):
    min_sqm = config["validation"]["floor_area"]["min_sqm"]
    fired = fire(flagged_state, config, clean_template.name, "floor_area_sqm", min_sqm - 1)
    assert fired == {"flag_floor_area"}


def test_floor_area_too_large(flagged_state, config, clean_template):
    max_sqm = config["validation"]["floor_area"]["max_sqm"]
    fired = fire(flagged_state, config, clean_template.name, "floor_area_sqm", max_sqm + 1)
    assert fired == {"flag_floor_area"}


def test_lease_commence_after_transaction(flagged_state, config, clean_template):
    transaction_year = pd.Period(clean_template["month"], freq="M").year
    fired = fire(flagged_state, config, clean_template.name, "lease_commence_date", transaction_year + 1)
    assert fired == {"flag_lease_commence"}


def test_lease_commence_before_hdb_existed(flagged_state, config, clean_template):
    min_year = config["validation"]["lease_commence"]["min_year"]
    fired = fire(flagged_state, config, clean_template.name, "lease_commence_date", min_year - 1)
    assert fired == {"flag_lease_commence_range"}


def test_resale_price_impossible_also_trips_anomaly(flagged_state, config, clean_template):
    """A hard-bound violation is deliberately expected to also trip
    flag_anomaly: the hard bounds are set far wider than the statistical
    IQR fence, so anything outside them is, by construction, also
    statistically extreme relative to real peers."""
    fired = fire(flagged_state, config, clean_template.name, "resale_price", -100)
    assert fired == {"flag_resale_price_range", "flag_anomaly"}


def test_remaining_lease_out_of_range(flagged_state, config, clean_template):
    max_years = config["validation"]["remaining_lease_bounds"]["max_years"]
    fired = fire(flagged_state, config, clean_template.name, "remaining_lease", max_years + 5)
    assert fired == {"flag_remaining_lease_range"}


def test_resale_price_statistical_anomaly(flagged_state, config, clean_template):
    price = flagged_state["deduplicated"]["resale_price"]
    q1, q3 = price.quantile([0.25, 0.75])
    iqr = q3 - q1
    iqr_multiplier = config["validation"]["anomaly"]["iqr_multiplier"]
    upper = q3 + iqr_multiplier * iqr
    fired = fire(flagged_state, config, clean_template.name, "resale_price", upper + iqr * 10)
    assert fired == {"flag_anomaly"}


def test_clean_template_is_actually_clean(clean_template):
    """Sanity check on the fixture itself: the row every test above
    copies must not already be flagged for something before corruption."""
    fired = {flag for flag in ALL_COMPUTED_FLAGS if bool(clean_template[flag])}
    assert fired == set()
