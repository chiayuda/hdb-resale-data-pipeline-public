"""Orchestrates every module from extraction through every flag_* check
being computed. Shared by run_pipeline.py and by the test suite, so tests
exercise the exact code path the real pipeline runs, not a reimplementation."""
from typing import Any

import pandas as pd

from . import cleaning, dedup, extraction, identifier, lease, profiling, validation


def recompute_all_flags(
    windowed: pd.DataFrame, config: dict[str, Any], reference_domains: dict[str, set[str]]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Applies every flag_* check, in the same order the real pipeline
    does, to whatever windowed-shaped DataFrame is passed in. Used for the
    real run and, unchanged, by the test suite to recompute flags on a
    corrupted copy of real data, so a test exercises this exact function,
    not a second copy of its logic. Returns (windowed, windowed_sorted,
    deduplicated): windowed carries every requirement 3 flag
    (town/flat_type/flat_model/storey_range/missing_required/block_format),
    each function returns a new copy rather than mutating in place, so the
    flagged version has to be the one actually returned, not the caller's
    original reference."""
    windowed = validation.flag_reference_columns(windowed, reference_domains, config)
    windowed, _ = validation.flag_missing_required(windowed, config)
    windowed = validation.flag_block_format(windowed, config)

    windowed_sorted, deduplicated = dedup.deduplicate(windowed)
    deduplicated, _ = validation.flag_non_numeric(deduplicated, config)

    deduplicated = lease.compute_remaining_lease(deduplicated, config)
    deduplicated = validation.flag_anomaly(deduplicated, config)
    deduplicated = validation.flag_resale_price_range(deduplicated, config)
    deduplicated = validation.flag_floor_area(deduplicated, config)
    deduplicated = validation.flag_lease_commence(deduplicated, config)
    deduplicated = validation.flag_remaining_lease_range(deduplicated, config)
    deduplicated = validation.flag_storey_range_bounds(deduplicated, config)
    return windowed, windowed_sorted, deduplicated


def build_flagged_dataset(config: dict[str, Any]) -> dict[str, Any]:
    dataset_ids = extraction.discover_datasets(config)
    dataset_ids_in_window, dataset_last_updated = extraction.filter_to_window(config, dataset_ids)
    raw_frames = extraction.extract_raw_frames(config, dataset_ids_in_window, dataset_last_updated)

    master = extraction.combine_master(raw_frames)
    windowed = validation.apply_month_window(master, config)

    profile = profiling.profile_columns(windowed)
    hidden_nulls = profiling.scan_hidden_nulls(windowed)

    windowed = cleaning.clean_flat_model(windowed)

    reference_domains, live_reference_domains = validation.compute_reference_domains(master, config)
    _, required_columns = validation.flag_missing_required(windowed, config)
    numeric_cfg = config.get("validation", {}).get("numeric", {})
    numeric_columns = [col for col, is_numeric in numeric_cfg.items() if is_numeric]

    windowed, windowed_sorted, deduplicated = recompute_all_flags(windowed, config, reference_domains)

    cross_check = validation.cross_check_remaining_lease(deduplicated, config)

    deduplicated = identifier.build_resale_identifier(deduplicated)
    deduplicated = identifier.build_resale_identifier_hash(deduplicated)

    return {
        "master": master,
        "windowed": windowed,
        "windowed_sorted": windowed_sorted,
        "deduplicated": deduplicated,
        "profile": profile,
        "hidden_nulls": hidden_nulls,
        "reference_domains": reference_domains,
        "live_reference_domains": live_reference_domains,
        "required_columns": required_columns,
        "numeric_columns": numeric_columns,
        "cross_check": cross_check,
    }
