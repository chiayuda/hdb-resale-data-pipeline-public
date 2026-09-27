"""Requirement 5: composite key, keep the higher price.

The composite key is every published column except resale_price.
source_dataset_id and every flag_* column are excluded (added by this
pipeline, not HDB attributes). remaining_lease is also excluded: null for
two of the three source files, including it would treat identical sales as
different purely because one file happened to report a lease figure."""
import pandas as pd

from .validation import COMPOSITE_KEY_COLUMNS


def deduplicate(windowed: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Sorting by price descending before dropping duplicates is what
    implements 'keep the higher price': the first row pandas keeps per key
    group is guaranteed to be the highest-priced one. Sorts by a
    numeric-coerced key, not the raw column: a single non-numeric
    resale_price would otherwise make sort_values raise (mixed str/float
    comparison) and crash here, before flag_non_numeric ever gets a chance
    to catch and quarantine that row. Returns (windowed_sorted,
    deduplicated); windowed_sorted is needed again later to recover the
    dropped duplicates for Quarantined."""
    sort_key = pd.to_numeric(windowed["resale_price"], errors="coerce")
    windowed_sorted = windowed.loc[sort_key.sort_values(ascending=False).index]
    deduplicated = windowed_sorted.drop_duplicates(subset=COMPOSITE_KEY_COLUMNS, keep="first").sort_index()
    return windowed_sorted, deduplicated
