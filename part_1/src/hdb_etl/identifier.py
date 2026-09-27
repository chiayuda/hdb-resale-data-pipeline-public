"""Requirements 9 and 10: the resale identifier, and hashing it while
actually preserving uniqueness.

resale_identifier is not, and cannot be, unique (block digits strip any
letter suffix, price digits come from a group average). Hashing it would
not fix that: SHA-256 is deterministic, so a code that already collides
just produces the same hash for the colliding rows too, that is exactly
what resale_identifier_short_hash demonstrates. resale_identifier_hash is
computed over the full composite key instead, the thing deduplication
already guarantees is unique per row. Both hashes ship in the Hashed
output group, side by side, so the contrast is visible in the deliverable
itself, not only here."""
import hashlib

import pandas as pd

from .validation import COMPOSITE_KEY_COLUMNS

BLOCK_CODE_LENGTH = 3
IDENTIFIER_PREFIX = "S"


def build_resale_identifier(deduplicated: pd.DataFrame) -> pd.DataFrame:
    """S + 3 block digits (zero-padded/truncated) + 2 average-price digits
    (grouped by month, town, flat_type) + 2 month digits + town initial."""
    deduplicated = deduplicated.copy()

    block_digits = deduplicated["block"].astype(str).str.replace(r"\D", "", regex=True)
    block_code = block_digits.str.zfill(BLOCK_CODE_LENGTH).str[:BLOCK_CODE_LENGTH]

    group_avg_price = deduplicated.groupby(["month", "town", "flat_type"])["resale_price"].transform("mean")
    price_code = group_avg_price.astype(int).astype(str).str[:2]

    month_code = deduplicated["month"].str[-2:]
    town_code = deduplicated["town"].str[0]

    deduplicated["resale_identifier"] = IDENTIFIER_PREFIX + block_code + price_code + month_code + town_code
    return deduplicated


def _canonical_key(row: pd.Series, columns: list[str]) -> str:
    return "|".join(str(row[col]) for col in columns)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_resale_identifier_hash(deduplicated: pd.DataFrame) -> pd.DataFrame:
    """resale_identifier_key is kept as its own column, not thrown away,
    since the hash is irreversible by design, this is the only way to see
    later what was actually hashed for a given row.

    resale_identifier_short_hash hashes resale_identifier itself (the
    short, colliding code): it inherits every one of those collisions,
    equal input always produces equal SHA-256 output. Kept deliberately,
    as the direct counterexample sitting next to resale_identifier_hash,
    not as a substitute for it."""
    deduplicated = deduplicated.copy()
    deduplicated["resale_identifier_key"] = deduplicated.apply(
        lambda row: _canonical_key(row, COMPOSITE_KEY_COLUMNS), axis=1
    )
    deduplicated["resale_identifier_hash"] = deduplicated["resale_identifier_key"].apply(_sha256)
    deduplicated["resale_identifier_short_hash"] = deduplicated["resale_identifier"].apply(_sha256)
    return deduplicated
