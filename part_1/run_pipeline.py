"""Entry point for the standalone ETL pipeline. Same config.yaml as the
notebook, same logic, same five output groups. Run from this folder:

    python run_pipeline.py

Logging is grouped by requirement number, matching the notebook's own
section headers, so a reviewer can follow this run without opening the
notebook. hdb_etl itself stays print-free (library code, no side effects);
all reporting below reads values it already returns.
"""
import logging
import os
import sys
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from hdb_etl import output, validation  # noqa: E402
from hdb_etl.config import load_config  # noqa: E402
from hdb_etl.pipeline import build_flagged_dataset  # noqa: E402

# Bare message only, no timestamp/level prefix: this mirrors the notebook's
# own print-based output exactly, so the two are easy to compare side by
# side. Real log level filtering (DEBUG/INFO/WARNING) still works, this
# just doesn't clutter the format for a single-run CLI script.
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("hdb_etl.run_pipeline")


def _section(title: str) -> None:
    logger.info("\n=== %s ===", title)


def _report_reference_domains(state: dict[str, Any]) -> None:
    reference_domains = state["reference_domains"]
    live_reference_domains = state["live_reference_domains"]
    for col in validation.REFERENCE_COLUMNS:
        accepted = reference_domains[col]
        live = live_reference_domains[col]
        if accepted == live:
            note = "matches the live authoritative file exactly"
        else:
            missing = sorted(live - accepted)
            extra = sorted(accepted - live)
            note = f"DIFFERS from live data, missing={missing}, extra={extra}"
        logger.info("  %s: %d accepted value(s) (%s)", col, len(accepted), note)

    windowed = state["windowed"]
    for col in validation.REFERENCE_COLUMNS + ["street_name", "block"]:
        flag_col = f"flag_{col}" if col in validation.REFERENCE_COLUMNS else (
            "flag_missing_required_field" if col == "street_name" else "flag_block_format"
        )
        if flag_col not in windowed.columns:
            continue
        flagged_values = sorted(windowed.loc[windowed[flag_col], col].dropna().unique())
        count = int(windowed[flag_col].sum())
        if flagged_values:
            logger.info("  UNKNOWN %s DETECTED: %s row(s), values=%s", col.upper(), f"{count:,}", flagged_values[:20])


def main() -> dict[str, Any]:
    config = load_config("config.yaml")
    _section("Setup")
    logger.info("Collection %s, window %s to %s", config["collection_id"], config["window_start"], config["window_end"])

    state = build_flagged_dataset(config)
    windowed = state["windowed"]
    windowed_sorted = state["windowed_sorted"]
    deduplicated = state["deduplicated"]

    _section("Requirement 1: extraction and combine into a master dataset")
    logger.info("Master: %s rows, %d columns (union of all source files)",
                f"{len(state['master']):,}", len(state["master"].columns))
    logger.info("Windowed (%s to %s, format + range checked): %s rows",
                config["window_start"], config["window_end"], f"{len(windowed):,}")

    _section("Requirement 2: data profiling")
    logger.info(state["profile"].to_string(index=False))
    if state["hidden_nulls"]:
        logger.info("Hidden nulls found: %s", state["hidden_nulls"])
    else:
        logger.info("No hidden nulls found in any column.")

    _section("Requirement 3: validate Date, Town, Flat Type, Flat Model, storey_range")
    logger.info("(Date already enforced above, by the windowing step itself)")
    _report_reference_domains(state)

    _section("Requirement 5: composite key, keep the higher price")
    logger.info("Deduplicated: %s rows (%s duplicate(s) dropped)",
                f"{len(deduplicated):,}", f"{len(windowed) - len(deduplicated):,}")

    _section("Requirement 4: remaining lease")
    cross_check = state["cross_check"]
    if cross_check:
        pct = cross_check["agree"] / cross_check["has_published"] * 100 if cross_check["has_published"] else 0
        logger.info("Rows with a published remaining_lease: %s", f"{cross_check['has_published']:,}")
        logger.info("Agree with computed value within tolerance: %s (%.1f%%)", f"{cross_check['agree']:,}", pct)
        logger.info("Difference range: %.0f to %.0f months (mean %+.2f)",
                    cross_check["difference_min"], cross_check["difference_max"], cross_check["difference_mean"])
    else:
        logger.info("Cross-check disabled in config.yaml")

    _section("Requirement 6 and 7: anomaly detection and additional rules")
    gating_flags = config.get("validation", {}).get("gating_flags") or validation.ALL_COMPUTED_FLAGS
    for col in validation.ALL_COMPUTED_FLAGS:
        gating = "gating" if col in gating_flags else "reported only"
        logger.info("  %s: %s row(s) flagged (%s)", col, f"{deduplicated[col].sum():,}", gating)

    _section("Requirement 9: the resale identifier")
    distinct_codes = deduplicated["resale_identifier"].nunique()
    total_rows = len(deduplicated)
    collision_rate = 1 - distinct_codes / total_rows
    logger.info("%s distinct codes for %s rows (%.1f%% collision rate)",
                f"{distinct_codes:,}", f"{total_rows:,}", collision_rate * 100)
    logger.info("Expected, not a bug, see README.md: block digits and the group-average "
                "price digits both discard distinguishing information by construction.")

    _section("Requirement 10: hash it while preserving uniqueness")
    distinct_short_hashes = deduplicated["resale_identifier_short_hash"].nunique()
    logger.info("resale_identifier_short_hash (hash of resale_identifier itself): %s distinct for %s rows "
                "(collision found: %s, inherited from resale_identifier)",
                f"{distinct_short_hashes:,}", f"{total_rows:,}", distinct_short_hashes < total_rows)
    distinct_hashes = deduplicated["resale_identifier_hash"].nunique()
    logger.info("resale_identifier_hash (hash of the composite key): %s distinct for %s rows (collision found: %s)",
                f"{distinct_hashes:,}", f"{total_rows:,}", distinct_hashes < total_rows)

    _section("Requirements 11 and 12: assemble and write the five output groups")
    quality_passed, any_flagged = output.gate_quality(deduplicated, config)
    quarantined = output.assemble_quarantined(windowed_sorted, deduplicated, any_flagged)
    cleaned, transformed, hashed = output.assemble_output_groups(quality_passed)

    output.reconcile(windowed, cleaned, quarantined)
    logger.info("Reconciliation OK: %s windowed = %s cleaned + %s quarantined",
                f"{len(windowed):,}", f"{len(cleaned):,}", f"{len(quarantined):,}")
    logger.info("Quarantine reasons:\n%s", quarantined["quarantine_reason"].value_counts().to_string())

    output_groups = {"cleaned": cleaned, "transformed": transformed, "quarantined": quarantined, "hashed": hashed}
    written = output.write_output_groups(output_groups)
    for name, (csv_path, parquet_path) in written.items():
        logger.info("  %s: %s rows -> %s, %s", name, f"{len(output_groups[name]):,}", csv_path, parquet_path)

    retention = len(cleaned) / len(windowed)
    logger.info("\nRetention: %.2f%% cleaned, %.2f%% quarantined (of %s windowed rows)",
                retention * 100, (1 - retention) * 100, f"{len(windowed):,}")

    return output_groups


if __name__ == "__main__":
    main()
