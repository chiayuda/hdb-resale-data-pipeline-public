"""Loads config.yaml and .env. Same file the notebook reads, so retuning
one applies to both."""
import os
from typing import Any

import yaml
from dotenv import load_dotenv

PUBLISHED_COLUMNS = {
    "month", "town", "flat_type", "block", "street_name", "storey_range",
    "floor_area_sqm", "flat_model", "lease_commence_date", "resale_price",
    "remaining_lease",
}
REFERENCE_COLUMNS = {"town", "flat_type", "flat_model", "storey_range"}

# One entry per validation.<key> that is itself a dict of settings (an
# enabled switch plus its own parameters). Anything under validation.* not
# listed here at all (nullable, numeric, gating_flags, reference_domains,
# reference_exceptions, lease_years) has its own, separate check below,
# since those aren't "enabled + parameters" blocks.
VALIDATION_BLOCKS: dict[str, set[str]] = {
    "month": {"enabled", "pattern"},
    "block_format": {"enabled", "pattern"},
    "floor_area": {"enabled", "min_sqm", "max_sqm"},
    "lease_commence": {"enabled", "min_year"},
    "storey_range_bounds": {"enabled", "min_storey", "max_storey"},
    "resale_price_bounds": {"enabled", "min", "max"},
    "remaining_lease_bounds": {"enabled", "min_years", "max_years"},
    "remaining_lease_cross_check": {"enabled", "tolerance_months"},
    "anomaly": {"enabled", "iqr_multiplier"},
}

TOP_LEVEL_KEYS = {
    "collection_id", "urls", "authoritative_dataset_id", "window_start",
    "window_end", "source", "s3_bucket", "validation",
}
VALIDATION_KEYS = set(VALIDATION_BLOCKS) | {
    "lease_years", "nullable", "numeric", "gating_flags",
    "reference_domains", "reference_exceptions",
}


def validate_config_schema(config: dict[str, Any]) -> None:
    """Catches a typo'd key loudly, at load time, instead of the key being
    silently ignored and its section quietly falling back to a default.
    `.get(key, default)` everywhere else in this codebase is convenient but
    can't tell "key genuinely absent, use the default" apart from "key
    misspelled, use the default" -- this is the one place that distinction
    is actually checked."""
    unknown_top_level = set(config) - TOP_LEVEL_KEYS - {"_api_key"}
    if unknown_top_level:
        raise ValueError(f"config.yaml has unrecognised top-level key(s): {sorted(unknown_top_level)}")

    validation_cfg = config.get("validation", {}) or {}
    unknown_validation = set(validation_cfg) - VALIDATION_KEYS
    if unknown_validation:
        raise ValueError(f"config.yaml's validation: has unrecognised key(s): {sorted(unknown_validation)}")

    for block_name, expected_keys in VALIDATION_BLOCKS.items():
        block = validation_cfg.get(block_name)
        if block is None:
            continue
        unknown_in_block = set(block) - expected_keys
        if unknown_in_block:
            raise ValueError(
                f"config.yaml's validation.{block_name} has unrecognised key(s): {sorted(unknown_in_block)}"
            )

    for section_name in ("nullable", "numeric"):
        section = validation_cfg.get(section_name) or {}
        unknown_columns = set(section) - PUBLISHED_COLUMNS
        if unknown_columns:
            raise ValueError(
                f"config.yaml's validation.{section_name} references unknown column(s): {sorted(unknown_columns)}"
            )

    for section_name in ("reference_domains", "reference_exceptions"):
        section = validation_cfg.get(section_name) or {}
        unknown_columns = set(section) - REFERENCE_COLUMNS
        if unknown_columns:
            raise ValueError(
                f"config.yaml's validation.{section_name} references unknown column(s): {sorted(unknown_columns)}"
            )


def load_config(path: str = "config.yaml") -> dict[str, Any]:
    load_dotenv()  # reads .env into the environment if present, never overrides a real shell export
    with open(path) as f:
        config = yaml.safe_load(f)
    validate_config_schema(config)
    config["_api_key"] = os.environ.get("DATA_GOV_SG_API_KEY", "")  # from .env, never from the YAML
    return config
