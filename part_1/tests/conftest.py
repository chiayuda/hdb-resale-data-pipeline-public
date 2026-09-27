import os
import sys

import pytest

PART_1_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PART_1_DIR, "src"))

from hdb_etl.config import load_config  # noqa: E402
from hdb_etl.pipeline import build_flagged_dataset  # noqa: E402


@pytest.fixture(scope="session")
def config():
    return load_config(os.path.join(PART_1_DIR, "config.yaml"))


@pytest.fixture(scope="session")
def flagged_state(config):
    """Runs the real pipeline, extraction through every flag_* check,
    once per test session. Uses whatever is already cached in data/raw/
    (no network call if the manifest is already populated)."""
    return build_flagged_dataset(config)


@pytest.fixture(scope="session")
def clean_template(flagged_state):
    """One real row confirmed clean on every check, the baseline every
    corruption test copies and modifies exactly one field on."""
    from hdb_etl.validation import ALL_COMPUTED_FLAGS

    deduplicated = flagged_state["deduplicated"]
    clean_rows = deduplicated.loc[~deduplicated[ALL_COMPUTED_FLAGS].any(axis=1)]
    assert len(clean_rows) > 0, "no row in the real data is clean on every check, cannot build a test template"
    return clean_rows.iloc[0]
