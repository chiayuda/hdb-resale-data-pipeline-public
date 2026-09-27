"""Requirement 4: remaining lease. Computed after dedup, not before, since
the formula depends only on columns already in the composite key, so
computing it earlier or later can never change which rows are duplicates."""
from typing import Any

import pandas as pd

MONTHS_PER_YEAR = 12


def compute_remaining_lease(deduplicated: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Remaining lease as of the transaction, not today. The 99-year term
    is anchored to lease_commence_date; transaction month is only the
    point in time the balance is read, the same way HDB itself computes
    it. lease_commence_date only publishes a year, so this assumes the
    lease started 1 January of that year, an explicit assumption."""
    deduplicated = deduplicated.copy()
    lease_years = config.get("validation", {}).get("lease_years", 99)

    lease_commence_year = pd.to_numeric(deduplicated["lease_commence_date"], errors="coerce")
    transaction = pd.PeriodIndex(deduplicated["month"], freq="M")

    elapsed_months = (transaction.year - lease_commence_year) * MONTHS_PER_YEAR + (transaction.month - 1)
    total_lease_months = lease_years * MONTHS_PER_YEAR
    remaining_months_total = total_lease_months - elapsed_months

    deduplicated["remaining_lease_years"] = remaining_months_total // MONTHS_PER_YEAR
    deduplicated["remaining_lease_months"] = remaining_months_total % MONTHS_PER_YEAR
    return deduplicated
