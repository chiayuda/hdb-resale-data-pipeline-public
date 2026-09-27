# HDB Resale Flat Prices: Part 1 ETL Notebook

A single, hand-written Jupyter notebook (`part1_notebook.ipynb`) that downloads HDB resale flat
transaction data from [data.gov.sg](https://data.gov.sg) (collection 189), combines the three
files covering January 2012 – December 2016, profiles and cleans it, validates it against a
January 2012 reference, computes remaining lease, deduplicates, screens for anomalous prices,
builds the prescribed resale identifier and hash, and writes the five required output groups.

No separate package, no framework: every stage is a plain pandas cell, run top to bottom. Every
threshold lives in `config.yaml`, not hardcoded in the notebook, so a reviewer can retune the
pipeline's behaviour without editing code.

This file covers setup, how to run both the notebook and the Python pipeline, what each stage and
output column means, the assumptions behind every judgement call, and the findings from actually
running this against real data.

## Contents

- [Prerequisites](#prerequisites), [Setup](#setup), [Running it](#running-it), [Running the Python ETL pipeline](#running-the-python-etl-pipeline), [Testing](#testing)
- [What the notebook does, in order](#what-the-notebook-does-in-order), [Data source](#data-source)
- [Resilience: rate limits, retries, and caching](#resilience-rate-limits-retries-and-caching)
- [Column-by-column validation](#column-by-column-validation), [Configuration](#configuration)
- [Remaining lease](#remaining-lease), [Resale identifier collision](#resale-identifier-collision), [Quarantine, not a hard stop](#quarantine-not-a-hard-stop)
- [Key design decisions](#key-design-decisions), [Findings from actually running this](#findings-from-actually-running-this), [Assumptions made explicitly](#assumptions-made-explicitly)
- [Outputs](#outputs) ([Output columns](#output-columns)), [Secrets](#secrets), [Reproducibility notes](#reproducibility-notes)
- [Software engineering practices](#software-engineering-practices)

## Prerequisites

- Python 3.10 or newer
- The packages in `requirements.txt` (pandas, pyarrow, requests, pyyaml, python-dotenv)

## Setup

From this folder (`part_1/`):

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

An API key is optional. data.gov.sg serves this collection unauthenticated at a lower rate limit
(2 calls/10s vs. 4/10s with a key), and the notebook's retry logic handles that either way. To use
a key: copy `.env.example` to `.env` and fill in `DATA_GOV_SG_API_KEY`. `.env` should never be
committed, see [Secrets](#secrets) below.

## Running it

Open `part1_notebook.ipynb` in Jupyter (`jupyter notebook` or `jupyter lab` from this folder),
select a Python 3.10+ kernel, and **Run All** from a fresh kernel (Kernel → Restart & Run All).
Running cell-by-cell out of order, or re-running a single cell after editing an earlier one, can
leave the saved file with non-sequential execution counts or stale output, if in doubt, restart
and run all before saving.

To execute headlessly instead:

```bash
jupyter nbconvert --to notebook --execute --inplace part1_notebook.ipynb
```

First run downloads three CSVs (~36 MB total) from data.gov.sg into `data/raw/` and takes under a
minute. Every subsequent run reuses those files (see
[Resilience](#resilience-rate-limits-retries-and-caching)) and finishes in a few seconds, with no
network calls at all for the download step.

## Running the Python ETL pipeline

The same logic is also available as a standalone pipeline, `src/hdb_etl/`, one module per
notebook section, plus `run_pipeline.py` as the entry point. Same `config.yaml`, same
`data/raw/`, same `data/output/`, same five groups, retuning one config file changes both the
notebook and the pipeline. This is not a second implementation to keep in sync by hand, it is a
port of the exact formulas in the notebook, verified to produce identical row counts, flag
counts, and output files.

From this folder (`part_1/`), with the same virtual environment active:

```bash
python run_pipeline.py
```

```
src/hdb_etl/
  config.py       load config.yaml + .env
  extraction.py   discover datasets, download/cache, Raw output group, combine into master
  profiling.py    requirement 2
  cleaning.py     flat_model casing fix
  validation.py   requirements 3, 6, 7, every flag_* check and the reference-domain logic
  lease.py        requirement 4
  dedup.py        requirement 5
  identifier.py   requirements 9 and 10
  output.py       requirements 11 and 12, gating, quarantine assembly, reconciliation, writing
```

The notebook additionally includes the synthetic-corruption demonstrations, the config-sensitivity
proofs, and the profiling heuristic comparison, none of that is duplicated here since it exists to
demonstrate correctness to a reviewer, not to run the pipeline itself.

### Testing

Three independent ways to see every check actually work, not just read that it should:

| Mechanism | Where | How to run |
|---|---|---|
| Notebook demonstration | `part1_notebook.ipynb`, the "Demonstrating the mechanism" and "Test cases" sections | Run the notebook end to end; the last cell is a pass/fail table |
| pytest suite | `tests/test_validation.py` (17 tests) | `python -m pytest tests/ -v` from `part_1/` |
| Excel workbook | `tests/test_scenarios.xlsx` | Open directly, no code required, same cases, human-readable |

**How the pytest suite works**: every test starts from one real row already confirmed clean on
every check (the `clean_template` fixture in `tests/conftest.py`), copies the real windowed data
with exactly one field on that row corrupted, runs it through the actual `hdb_etl` functions (the
same `recompute_all_flags` that `run_pipeline.py` itself calls, nothing mocked), and asserts the
flag(s) that fire match exactly what's expected, no more, no less.

```bash
python -m pytest tests/ -v
```

`-v` prints one line per test (`test_town_not_in_reference_domain PASSED`, etc.), so a failure
tells you exactly which scenario broke, not just that something did.

**What's actually tested**, one case per validated column or check:

| Test | Column | Bad value used | Expected result |
|---|---|---|---|
| Month format | `month` | `"2012-13"` | Excluded by windowing (not a row-level flag) |
| Town domain membership | `town` | `"NOT_A_REAL_TOWN"` | `flag_town` |
| Flat type domain membership | `flat_type` | `"9 ROOM"` | `flag_flat_type` |
| Flat model domain membership | `flat_model` | `"Not A Real Model"` | `flag_flat_model` |
| Storey range, unrecognized but numerically sane | `storey_range` | `"12 TO 16"` | `flag_storey_range` only |
| Storey range, numerically impossible | `storey_range` | `"00 TO 04"` | `flag_storey_range` and `flag_storey_range_bounds` together |
| Missing required field | `street_name` | null | `flag_missing_required_field` |
| Block format | `block` | `"12-A"` | `flag_block_format` |
| Non-numeric value | `resale_price` | `"abc"` | `flag_non_numeric` only, not the bound or anomaly checks |
| Floor area too small | `floor_area_sqm` | below `min_sqm` | `flag_floor_area` |
| Floor area too large | `floor_area_sqm` | above `max_sqm` | `flag_floor_area` |
| Lease commences after its own sale | `lease_commence_date` | transaction year + 1 | `flag_lease_commence` |
| Lease commences before HDB existed | `lease_commence_date` | below `min_year` | `flag_lease_commence_range` |
| Resale price impossible | `resale_price` | `-100` | `flag_resale_price_range` and `flag_anomaly` together (the hard bound is deliberately wider than the statistical fence, so anything outside it is also statistically extreme) |
| Remaining lease out of range | `remaining_lease` | above `max_years` | `flag_remaining_lease_range` |
| Resale price statistical anomaly | `resale_price` | far outside the real IQR fence | `flag_anomaly` only |

**No code, no problem**: `tests/test_scenarios.xlsx` has the same scenarios as a spreadsheet, a
pass/fail summary sheet and a sheet showing the actual row data for each case side by side. Open
it directly, nothing to run.

**Adding your own scenario**: to check a case not already covered, add a function to
`tests/test_validation.py` following the existing pattern:

```python
def test_my_new_scenario(flagged_state, config, clean_template):
    fired = fire(flagged_state, config, clean_template.name, "<column>", <bad_value>)
    assert fired == {"<expected_flag>"}
```

`fire()`, defined at the top of that file, does the corrupt-and-recompute step above, so a new
test is just picking a column, a bad value, and the flag(s) you expect back.

## What the notebook does, in order

| Section | Requirement | What it does |
|---|---|---|
| Extraction | 1 | Discovers dataset IDs from the collection's own metadata (never hardcoded), filters to the three datasets whose published coverage overlaps 2012-01–2016-12 |
| Config display | - | Prints the full, current `config.yaml` verbatim (comments included) right after loading it, so every tunable value and its reasoning is visible from the notebook alone |
| Resolve download URLs / Raw output group | - | Resolves a pre-signed download URL per dataset and saves each file byte-for-byte to `data/raw/`, with a manifest recording SHA-256 and fetch time. This *is* the "Raw" output group |
| Combine | 1 | Unions the three files' columns (`remaining_lease` is absent from two of them) rather than intersecting, so nothing is silently dropped |
| Scope to window | - | Filters to the 2012-01–2016-12 window **and** validates `month` actually matches `YYYY-MM` first, a raw string comparison alone would accept a malformed value like `"2012-13"`. Demonstrated on a synthetic value inline |
| Profiling | 2 | Null/blank counts, distinct counts and values for categorical columns, and a hidden-null scan (whitespace, placeholder tokens like `"NA"` or `"-"`) |
| Cleaning | 3 (support) | Normalises `flat_model` casing (`'2-room'` → `'2-Room'`), with `DBSS` protected from becoming `'Dbss'` |
| Validation | 3 | Checks Town, Flat Type, Flat Model and storey_range against the file that was current around January 2012, with configured exceptions for values that are genuine but absent from that reference. An unseen value prints as `UNKNOWN <COLUMN> DETECTED` with the exact values and an instruction to add it to `validation.reference_domains` or `validation.reference_exceptions` if it's legitimate |
| Required-field check | 3 (support) | A generic, config-driven completeness check across every published column (`validation.nullable`), not a bespoke rule per column, replaced an earlier `street_name`-only character-pattern check that never actually caught anything on real data |
| Numeric-safety check | 3 (support) | A generic, config-driven check (`validation.numeric`) that every numeric column actually parses as a number, checked *before* any range/bound cell runs against it. Without this, a single non-numeric value would make `pd.to_numeric()` / `.astype(int)` raise and halt the whole notebook, rather than being quarantined like everything else here |
| Block-format check | 3 (support) | `block` is checked structurally (`validation.block_format`): digits, optionally with one trailing letter (e.g. `"123"`, `"9A"`), confirmed against all 2,139 distinct real values in this window before choosing the pattern |
| Composite key / dedup | 5 | Key = every published column except `resale_price`. Highest price wins per key; every loser is kept, not dropped, for the Quarantined group |
| Remaining lease | 4 | 99-year term from `lease_commence_date` (year only, assumes 1 January), floored to whole years and months; cross-validated against HDB's own published figure where available |
| Anomaly detection | 6 | Global 1.5× IQR fence on `resale_price`, purely statistical, unusual relative to real peers in this dataset. The notebook explains what else was considered (mean±std, fixed thresholds, robust z-score, peer-grouped, ML) and why this was chosen |
| Hard bounds | - | A short section explaining why these bounds are set from **business domain knowledge**, not from data profiling (see [Why domain knowledge, not profiling](#why-domain-knowledge-not-profiling) below), then the first one: `resale_price` outside `[min, max]` |
| Additional rules | 7 | Floor area outside a physically plausible range (too small *or* too large), a lease commencing after its own sale date, a lease commencing before HDB existed or after today, a numerically impossible `storey_range` (independent of the domain-membership check above), an implausible published `remaining_lease`, and the lease cross-check against HDB's own published figure |
| Demonstrating the mechanism | 7 | Real 2012–2016 data trips none of the checks above, that shows the data is clean, not that the checks would catch a violation if one existed. This section builds fifteen synthetic rows, each with exactly one field corrupted, and confirms each one trips exactly (and only) the flag(s) it should |
| Resale identifier | 9 | `S` + 3 block digits + 2 average-price digits (grouped by month/town/flat_type) + 2 month digits + town initial |
| Hash | 10 | Demonstrates that hashing the identifier itself doesn't fix its collisions (SHA-256 is deterministic), then computes the real irreversible-and-unique hash over the full composite key instead |
| Output groups | 11, 12 | Assembles and writes Cleaned, Transformed, Quarantined and Hashed to `data/output/` as CSV and Parquet |
| Reconciliation | - | Asserts every windowed row is counted exactly once, as Cleaned or Quarantined, and fails loudly if not |
| Insights and assumptions | 8 | Design decisions, findings from the actual data, and explicit assumptions, see [Key design decisions](#key-design-decisions), [Findings](#findings-from-actually-running-this) and [Assumptions](#assumptions-made-explicitly) below |
| Tests (config-sensitivity) | - | Reruns three of the checks above (IQR multiplier, floor area minimum, flat_model exceptions) at alternate config values against data already in memory, to prove `config.yaml` actually controls behaviour rather than being read and ignored |
| Test cases (per column) | - | A single pass/fail table pulling together the month-format demo and all fifteen requirement-7 corruption cases, one row per validated column, in one place, at the very end of the notebook |

## Data source

The collection has 5 datasets. Only 3 overlap the 2012-01 to 2016-12 window and get downloaded:

| Dataset | Coverage | Used |
|---|---|---|
| 1990-1999 (Approval Date) | 1990-01 to 1999-12 | No, outside window |
| 2000-Feb 2012 (Approval Date) | 2000-01 to 2012-02 | Yes, also the Requirement 3 authoritative reference |
| Mar 2012-Dec 2014 (Registration Date) | 2012-03 to 2014-12 | Yes |
| Jan 2015-Dec 2016 (Registration Date) | 2015-01 to 2016-12 | Yes |
| 2017 onwards (Registration Date) | 2017-01 to present | No, outside window |

Dataset IDs are never hardcoded. The notebook reads the collection's own metadata and filters by
each dataset's published coverage dates, so this table is a result, not an input.

## Resilience: rate limits, retries, and caching

data.gov.sg's `poll-download` endpoint (the call that hands back a presigned download URL)
rate-limits at 2 calls per 10 seconds when unauthenticated, higher with an API key in `.env`. Two
separate mechanisms handle this, one to avoid hitting the limit at all, one to recover if it's hit
anyway:

- **Retry with exponential backoff.** Up to 4 attempts per dataset. First retry waits 10 seconds,
  then 20, then 40 (doubling each time). 10 seconds is not an arbitrary guess, it is the documented
  reset window, and data.gov.sg's own 429 response body says "try again in 10 seconds". If all 4
  attempts still fail, the pipeline raises and stops rather than continuing with partial data:
  exhausting 10+20+40 seconds of backoff already comfortably clears a single rate-limit window, so
  a failure past that point is more likely a real outage than transient throttling, and that's a
  case for a person to look at, not another automatic wait.
- **Manifest-based caching**, the bigger lever, since it avoids the rate-limited call entirely
  rather than retrying through it. `data/raw/_manifest.json` records each dataset's local path,
  SHA-256, and the `lastUpdatedAt` timestamp data.gov.sg reported at fetch time. On a re-run, a
  dataset is only re-downloaded if its manifest entry is missing, its local file is gone, or the
  checksum no longer matches, otherwise the cached copy is reused and no request is made to
  data.gov.sg at all, not even to resolve a fresh download URL, since that URL is itself the
  rate-limited step. Delete `data/raw/_manifest.json` (or an individual entry in it) to force a
  fresh download.
- `source.refresh_on_update` decides what happens if data.gov.sg later reports a *different*
  `lastUpdatedAt` for an already-cached dataset. Default `false`: keep the cached copy, print a
  note. These are finalised historical files, so a changed timestamp is far more likely a metadata
  edit (a description tweak) than the underlying rows changing, and re-downloading ~30MB on every
  such tick is wasteful. Set to `true` to treat a stale-timestamped cache as no cache at all.

## Column-by-column validation

Every check is config-driven (`config.yaml`), nothing hardcoded in a cell.

| Column | Nullable | Validation | Why |
|---|---|---|---|
| `month` | No | format `YYYY-MM` regex, then range `2012-01` to `2016-12` | A raw string range check alone accepts a malformed value like `2012-13`, format has to be checked first |
| `town` | No | domain membership | Closed set, 26 values in the authoritative file |
| `flat_type` | No | domain membership | Closed set, 7 values |
| `block` | No | format regex, digits plus an optional single trailing letter | Free text, but structured; 2,139+ distinct values is too high to validate as a fixed list |
| `street_name` | No | required-field check only | Free text, 522+ distinct values, no closed domain makes sense |
| `storey_range` | No | domain membership (live-derived, not a fixed config list) plus numeric bounds (1-60) | The authoritative file's own banding convention changed mid-window (3-storey pre-2012, 5-storey after), a fixed list would need hand-maintenance every time HDB's convention shifts. Numeric bounds catch a value that is not just unrecognized but genuinely impossible, independent of that convention |
| `floor_area_sqm` | No | numeric check plus hard bounds (20-350 sqm) | Physically bounded; bounds come from domain knowledge (smallest and largest real HDB flat types), not this window's observed range |
| `flat_model` | No | domain membership plus reviewed exceptions | Closed-ish set that genuinely changes over time (`DBSS` etc. did not exist in the Jan-2012 file); reviewed case by case, not silently absorbed |
| `lease_commence_date` | No | numeric check, hard bounds (>= 1960, <= this year), and a relative check (cannot be after its own transaction) | Year-only field; 1960 is HDB's founding year, "this year" is computed at run time so it never goes stale |
| `resale_price` | No | numeric check, hard bounds (> 0, <= 10,000,000), statistical anomaly check (1.5x IQR) | Price cannot be zero or negative by definition; IQR catches a value that is merely unusual relative to real peers, a separate question from "impossible" |
| `remaining_lease` | Yes | numeric check when present, hard bounds (0-99), cross-check against the computed value | Structurally absent from 2 of the 3 source files, that is a fact about the source, not a defect |

`nullable`, `numeric`, `block_format`, and every bound above are individually configured in
`config.yaml` and can be retuned without touching notebook code. The "Tests" section near the end
of the notebook proves this by changing a config value and rerunning the same formula against data
already in memory.

## Configuration

Every key below lives in `config.yaml`. Nothing in this list is hardcoded in the notebook or the
pipeline, every one of these can be changed without touching code, and the "Tests" section near
the end of the notebook (and `tests/test_validation.py`) proves several of them are actually read,
not just present.

| Key | Current value | Controls |
|---|---|---|
| `collection_id` | `"189"` | Which data.gov.sg collection to discover datasets from |
| `urls.collection_metadata` / `dataset_metadata` / `poll_download` | API URL templates | data.gov.sg's own endpoint shapes, fixed by their API, not this pipeline |
| `authoritative_dataset_id` | the Jan-2012-era file's ID | Which dataset is read as requirement 3's authoritative reference for Town/Flat Type/Flat Model/storey_range |
| `window_start` / `window_end` | `2012-01` / `2016-12` | The analysis window: both which datasets get downloaded and the Date range check |
| `source.refresh_on_update` | `false` | Whether a cached raw file is treated as stale when data.gov.sg reports a changed `lastUpdatedAt`, see [Resilience](#resilience-rate-limits-retries-and-caching) above |
| `s3_bucket` | placeholder | Only read when `PIPELINE_ENV=aws`; not exercised by a local run |
| `validation.lease_years` | `99` | The lease term requirement 4's formula assumes |
| `validation.month.enabled` / `pattern` | on, `'^\d{4}-(0[1-9]\|1[0-2])$'` | Whether the Date format check runs, and what shape it enforces before the range comparison |
| `validation.block_format.enabled` / `pattern` | on, `'^\d+[A-Z]?$'` | Structural check for `block`: digits, optionally one trailing letter. Verified against all 2,139 distinct real values before being chosen |
| `validation.floor_area.enabled` / `min_sqm` / `max_sqm` | on, 20 / 350 | Physically plausible floor area range, from domain knowledge (smallest/largest real HDB flat types), not this window's observed range |
| `validation.lease_commence.enabled` / `min_year` | on, 1960 | HDB's founding year, a lease cannot commence before HDB existed; the upper bound is always "this year", computed at run time, not configured |
| `validation.storey_range_bounds.enabled` / `min_storey` / `max_storey` | on, 1 / 60 | A hard numeric bound on the two storey numbers themselves, independent of, and in addition to, the domain-membership check above. Catches a numerically impossible band (e.g. `"-5 TO 0"`) that domain-membership alone would only lump in with an unrecognized-but-plausible one. Deliberately not a band-width rule (band width itself varies by era, see `reference_domains` below); `max_storey` reflects real upcoming BTO projects, not just today's tallest block |
| `validation.resale_price_bounds.enabled` / `min` / `max` | on, 1 / 10,000,000 | A price must be positive; the max is a loose sanity ceiling for data-entry errors |
| `validation.remaining_lease_bounds.enabled` / `min_years` / `max_years` | on, 0 / 99 | Hard bounds on the *published* `remaining_lease` figure |
| `validation.remaining_lease_cross_check.enabled` / `tolerance_months` | on, 12 | How much disagreement between computed and published remaining lease is tolerated before flagging |
| `validation.anomaly.enabled` / `iqr_multiplier` | on, 1.5 | The statistical anomaly fence width on `resale_price` |
| `validation.nullable.<column>` | `false` for every published column except `remaining_lease` (`true`) | Whether that column is allowed to be null. A row missing any column set to `false` gets `flag_missing_required_field`, one generic, config-driven check instead of a bespoke rule per column |
| `validation.numeric.<column>` | `true` for `floor_area_sqm`, `lease_commence_date`, `resale_price`, `remaining_lease` | Whether that column must actually parse as a number. A row with a non-numeric value in any of these gets `flag_non_numeric`, computed and gated *before* any range/bound cell touches the column, so a bad value is quarantined instead of crashing the run |
| `validation.gating_flags` | all 14 flags active | Which computed flags actually exclude a row from Cleaned into Quarantined. **Genuinely wired up**: the notebook reads this list at run time (`FLAG_COLUMNS = VALIDATION_CONFIG.get("gating_flags")`) rather than hardcoding it, every flag is still computed and reported regardless of whether it gates. Comment a flag out of this list to make it reported-only instead. A flagged row lands **only** in Quarantined: Cleaned, Transformed and Hashed are all built from the same "passed every gating flag" set, so a flagged row never reaches any of those three |
| `validation.reference_domains.town` / `flat_type` / `flat_model` | explicit lists, reviewed snapshot of the authoritative file | The accepted values for these three columns. Meant to be edited as review happens, not treated as fixed, the notebook/pipeline also computes the live equivalent and reports a diff if they ever disagree, so a stale list doesn't fail silently. `storey_range` deliberately stays live-derived, its real-world shape changed mid-window |
| `validation.reference_exceptions.<column>` | mostly empty | Values allowed even though they're absent from `reference_domains`. Starts empty for every column: the workflow is run first, read the `UNKNOWN <COLUMN> DETECTED` output, then either add a value to `reference_domains` (if it belongs in the accepted set) or here (if it's a deliberate, reviewed exception), never pre-populated with guesses before anything has actually been flagged |

Because every threshold, toggle, and accepted-value list lives in this one file and nothing is
hardcoded in the notebook or `src/hdb_etl/`, changing pipeline behaviour never requires touching
or redeploying code, only this file. A deployment could keep `config.yaml` outside the application
package entirely (in S3, or a config service) and let a reviewer change a threshold or approve a
new domain value by uploading a new file, no rebuild, no redeploy, and no code change for that
edit.

#### How to edit config.yaml

A few concrete recipes. All of these are edits to `config.yaml` only, `run_pipeline.py` and the
notebook both re-read it on every run, no code changes needed for any of them.

**Turn a check off entirely** (the flag still exists as a column, just always `False`, it no
longer fires or gates anything):

```yaml
block_format:
  enabled: false   # was: true
  pattern: '^\d+[A-Z]?$'
```

**Loosen or tighten a hard bound**, e.g. allow smaller flats down to 15 sqm:

```yaml
floor_area:
  enabled: true
  min_sqm: 15   # was: 20
  max_sqm: 350
```

**Make a flag reported-only instead of gating**, e.g. stop `flag_storey_range` from quarantining
rows, keep computing and reporting it: remove its line from `gating_flags`:

```yaml
gating_flags:
  - flag_anomaly
  - flag_floor_area
  # - flag_storey_range   <- removed: reported only now, no longer excludes a row from Cleaned
  - flag_missing_required_field
```

**Approve a new domain value**, after a run prints `UNKNOWN FLAT_MODEL DETECTED` and a person
confirms it's genuine: either add it to `reference_domains` if it belongs in the accepted set, or
to `reference_exceptions` if it's a deliberate, reviewed exception. `DBSS` is a real, currently
open case: `config.yaml`'s own comment above `reference_exceptions` says it's "the one exception
already populated", but `reference_exceptions.flat_model` is actually empty right now, and all 277
real `DBSS` rows in this window are quarantined for `flag_flat_model` as a result (verified against
`data/output/quarantined.csv`, zero `DBSS` rows reach `cleaned.csv`). To apply the exception the
comment describes:

```yaml
reference_exceptions:
  town: []
  flat_type: []
  flat_model:
    - "DBSS"
  storey_range: []
```

**Extend the analysis window**, once a newer source file covers more months:

```yaml
window_start: "2012-01"
window_end: "2017-12"   # was: "2016-12"
```

No manifest step needed for this one: a dataset that's newly inside the window has no manifest
entry yet (`_already_cached()` in `extraction.py` checks `manifest.get(dataset_id)` first, finds
nothing, and downloads it), so it's fetched automatically on the next run. Manifest deletion only
matters for forcing a re-download of a dataset that's already cached, see
[Resilience](#resilience-rate-limits-retries-and-caching) above.

#### Why domain knowledge, not profiling

The hard-bound settings above (`resale_price_bounds`, `lease_commence.min_year`, `remaining_lease_bounds`,
`storey_range_bounds`)
are deliberately **not** derived from this window's observed data range, even though the profiling
step already reports that range. Profiling describes what happened to be true for **2012–2016
specifically**, not what is structurally possible, a bound tightly derived from this window's
observed `lease_commence_date` maximum (2013) would silently make a future rerun against 2017+ data
reject a perfectly real, newly-built flat, just because this five-year slice never happened to
contain one. The same applies to `resale_price`: prices in any other period aren't bounded by what
2012–2016 happened to see. Business/domain facts (HDB's 1960 founding, a price can't be negative,
a 99-year lease term) hold regardless of which window of data is loaded, which is why they, not
the profiling output, set these particular bounds. `floor_area_sqm` is the one exception: the
observed range here is itself close to what's physically realistic, so it's used as a starting
point, padded generously, rather than set aside. See the notebook's own "Hard bounds" section for
the full reasoning, and `config.yaml`'s comments for the same, next to each value.

The **Tests** section at the end of the notebook demonstrates the config wiring directly: it
reruns the IQR, floor-area and flat_model checks at other values and shows the flagged counts
change accordingly.

## Remaining lease

Requirement 4 adds two new columns, `remaining_lease_years` and `remaining_lease_months`, computed
for every row. This is separate from `remaining_lease`, the column HDB itself publishes (in one of
the three source files only), which is used to cross-check the computed figure below, not to
produce it.

`remaining_months = 99 years - elapsed since lease_commence_date, measured at the transaction month`.

Two explicit assumptions, both because HDB only publishes a year for `lease_commence_date`, not a
month or day:

- The lease is assumed to commence 1 January of that year.
- The 99-year term is fixed, no adjustment by flat type.

Remaining lease is a property of the sale, not of today. It is anchored to `lease_commence_date`
and read at the transaction's own month, the same way HDB computes it for the one source file that
publishes it. For 36,678 rows with a published figure, 36,677 (100.0%) agree with this notebook's
computed value within 12 months. The difference runs from -13 to +11 months, not one-sided, which
shows HDB rounds to the nearest year where this notebook floors.

## Resale identifier collision

The prescribed 9-character identifier (`S` + block digits + price digits + month + town initial)
is not, and cannot be, unique. Verified: 77,255 distinct codes for 90,944 deduplicated rows, a
15.1% collision rate, worst case 11 different sales sharing one code.

Two things compound the collision, both by construction, not by accident:

- The price digits come from a group average (by month, town, flat_type), not the individual sale
  price. Different sales in the same group produce the same 2 digits.
- The block digits strip every non-numeric character. Blocks `1A`, `1C`, `1G` all reduce to `001`,
  the distinguishing letter is discarded entirely, not truncated.

Hashing this identifier does not fix it: SHA-256 is deterministic, so a code that already collides
just produces the same hash for the colliding rows too. `resale_identifier_short_hash` (SHA-256 of
`resale_identifier` itself) makes this concrete: 77,255 distinct hashes for 90,944 rows, the same
15.1% collision rate as the identifier it hashes. `resale_identifier_hash` is computed over the
full composite key instead, the thing deduplication already guarantees is unique per row.
Verified: 90,944 distinct hashes for 90,944 rows, zero collisions. Both columns ship in
`data/output/hashed.{csv,parquet}`, side by side, so this contrast is visible in the deliverable
itself, not only argued here.

## Quarantine, not a hard stop

Every row that fails a check is quarantined with a `quarantine_reason`, not dropped and not used to
halt the pipeline. The reasoning: quarantining preserves every row (the reconciliation assert
proves nothing is silently lost), and a human reviewing `config.yaml`'s
`reference_domains`/`reference_exceptions` is a better judge of "is this a new legitimate category"
than a hardcoded rule would be. There is a separate, row-count-based safety net (the pipeline
refuses nothing based on volume; if 99% of a run were bad, all 99% would be quarantined and
visible, not silently dropped).

Whether an automated alert should also exist on top of this (not implemented here, a genuine open
question) depends on the normal quarantine rate. This run quarantines 12.1% (11,182 of 92,544
windowed rows), almost entirely `flag_storey_range` (a known, expected reporting-convention split)
and `flag_anomaly` (expected statistical noise). A threshold meant to catch a genuinely broken run,
not ordinary variation, should sit meaningfully above that baseline, roughly double it. Something
like 25% is a reasonable starting point: high enough that a normal run never trips it, low enough
that a real schema break or bad source file (which would spike quarantine sharply, not by a few
points) still gets caught quickly.

## Key design decisions

- Jan 2012 authoritative set means the whole published file (2000-01 to 2012-02), not just the
  calendar month. HDB does not publish a file containing only January 2012.
- Date is validated by windowing, not a flag column. A row outside 2012-01 to 2016-12 never
  reaches profiling, cleaning, or validation.
- The composite key is every published column except `resale_price`. `source_dataset_id` (added
  during combine) and `remaining_lease` (absent from 2 of 3 files) are deliberately excluded, a
  judgement call, not the only defensible one.
- Deduplication runs before remaining lease is computed. The lease formula depends only on columns
  already in the key, so order cannot change which rows are duplicates, only whether the
  calculation is wasted on rows about to be dropped.
- Anomaly detection is a global 1.5x IQR rule, not peer-grouped. Peer-grouping (by town and flat
  type) is more analytically precise and documented in the notebook as a known upgrade path, not an
  oversight, but the flat global rule is the lower-effort option that still fully satisfies the
  requirement.

## Findings from actually running this

- `flat_model` had a real casing inconsistency (`'2-room'`, `'Adjoined flat'`), fixed with
  `.str.title()`, with `DBSS` explicitly protected from becoming `'Dbss'`.
- `storey_range` mixes two genuinely different banding conventions, not a casing problem, flagged
  as a domain mismatch rather than cleaned.
- The composite key cannot uniquely identify a transaction. HDB does not publish a unit number, so
  two different flats in the same block, sold the same month, are indistinguishable. "Keep the
  higher price" therefore also discards some real, distinct transactions, not just errors.
- Every numeric column (`floor_area_sqm`, `lease_commence_date`, `resale_price`, `remaining_lease`)
  previously converted with `pd.to_numeric()` without `errors="coerce"`, which would have raised
  and halted the whole notebook on a single bad value instead of quarantining it. Fixed, and a
  dedicated `flag_non_numeric` check now catches this class of problem directly.
- The deduplication step (requirement 5) sorted by raw `resale_price` before dropping duplicates. A
  single non-numeric value there would have made `sort_values` raise (comparing a string to a float
  has no defined order) and crash the pipeline before `flag_non_numeric` ever got a chance to catch
  it, the pytest suite (`tests/test_validation.py`) found this by corrupting data earlier in the
  pipeline than this notebook's own demo cell does. Fixed by sorting on a numeric-coerced key
  instead of the raw column.
- Found while adding requirement-labeled logging to `run_pipeline.py`: the standalone pipeline's
  `windowed` state was silently the pre-flag-computation version, not the flagged one, since each
  `hdb_etl.validation` function returns a new copy rather than mutating in place, and the
  orchestration code was discarding the flagged copy instead of keeping it. The row counts and
  output files were never affected (they're built from `deduplicated`, computed correctly), but the
  "UNKNOWN <COLUMN> DETECTED" report silently printed nothing. Fixed by returning and keeping the
  flagged `windowed`, not the stale reference.
- Found by running the pipeline in two separate locations and diffing the output byte-for-byte: the
  column *order* of `master` (and everything downstream) was not guaranteed stable run to run.
  Cached raw files were loaded into a Python `set` before being combined, and set iteration order
  depends on hash randomisation, not insertion order, so two otherwise-identical runs could produce
  the same data with columns in a different order. Row counts, values, and every check were
  unaffected either way, this never changed what got flagged or quarantined, only column position.
  Fixed by iterating the dataset ID list (stable order) instead of the set, in both the pipeline
  and the notebook.

## Assumptions made explicitly

- HDB lease commences 1 January of the year in `lease_commence_date`.
- The 99-year lease term is fixed, no adjustment for flat type.
- Remaining lease is floored to whole years and months, even though HDB's own published figure
  appears to round to the nearest year instead.
- IQR multiplier of 1.5 (standard boxplot convention), configurable rather than hardcoded.
- The composite key excludes `remaining_lease`, treating it as provenance-adjacent rather than a
  defining attribute of the sale.

## Outputs

All five mandatory groups, written under `data/`:

| Group | Location | Contents |
|---|---|---|
| Raw | `data/raw/*.csv`, `data/raw/_manifest.json` | Original files, byte-for-byte, plus SHA-256 checksums and fetch time |
| Cleaned | `data/output/cleaned.{csv,parquet}` | Rows passing every gating quality check |
| Transformed | `data/output/transformed.{csv,parquet}` | Cleaned + `resale_identifier` + `storey_lower`/`storey_upper` + `transaction_year`/`transaction_month` |
| Hashed | `data/output/hashed.{csv,parquet}` | Cleaned + `resale_identifier` + `resale_identifier_short_hash` (SHA-256 of the identifier, still collides) + `resale_identifier_hash` (SHA-256 of the composite key, does not) |
| Quarantined | `data/output/quarantined.{csv,parquet}` | Every dropped duplicate and every gate-flagged row, **with every flag column, `quarantine_reason`, `source_dataset_id`, and the identifier/hash columns kept**, unlike the other three groups, this one is deliberately a full diagnostic view so a reviewer can see exactly which rule(s) rejected each row, which source file it came from, and a single human-readable summary (`quarantine_reason`: e.g. `"flag_flat_model, flag_anomaly"` or `"duplicate_composite_key"`) rather than having to scan every flag column by hand |

Full column-by-column meaning for every group: [Output columns](#output-columns) below.

### Output columns

All five groups trace back to the same 14 base columns: the 11 HDB publishes, plus 3 this
pipeline adds (`source_dataset_id`, `remaining_lease_years`, `remaining_lease_months`).

**Base columns** (present in Cleaned, Transformed, Hashed, and, for rows that reached that far,
Quarantined):

| Column | Meaning |
|---|---|
| `month` | Transaction month, `YYYY-MM` |
| `town` | HDB town/estate |
| `flat_type` | e.g. `3 ROOM`, `EXECUTIVE` |
| `block` | Block number: digits, optionally one trailing letter |
| `street_name` | |
| `storey_range` | Floor band as published, e.g. `"10 TO 12"` |
| `floor_area_sqm` | |
| `flat_model` | |
| `lease_commence_date` | Year only |
| `resale_price` | |
| `source_dataset_id` | Which of the 3 source files this row came from (added during combine, requirement 1) |
| `remaining_lease` | HDB's own published figure. Present in one of the three source files only, null in the other two, a fact about the source, not a defect |
| `remaining_lease_years` / `remaining_lease_months` | This pipeline's own computed remaining lease (requirement 4), see [Remaining lease](#remaining-lease) above for the formula |

**Cleaned**: exactly the 14 base columns, nothing else. No flags, no identifier, just the
validated data.

**Transformed**: Cleaned, plus:

| Column | Meaning |
|---|---|
| `resale_identifier` | Requirement 9's 9-character code. Collides by construction, see [Resale identifier collision](#resale-identifier-collision) above |
| `storey_lower` / `storey_upper` | `storey_range` split into two integers, e.g. `"10 TO 12"` -> `10`, `12`. Safe unconditionally here, every row already passed `flag_storey_range_bounds` |
| `transaction_year` / `transaction_month` | `month` split into two integers, e.g. `"2012-01"` -> `2012`, `1`, for grouping or filtering without parsing a string |

**Hashed**: Cleaned, plus:

| Column | Meaning |
|---|---|
| `resale_identifier` | Same short code as above, included so the next two columns can be compared against it directly |
| `resale_identifier_short_hash` | SHA-256 of `resale_identifier` itself. Inherits every one of its collisions (equal input always produces equal output), included specifically to show hashing alone doesn't fix a colliding input |
| `resale_identifier_hash` | SHA-256 of the composite key (`month`, `town`, `flat_type`, `block`, `street_name`, `storey_range`, `floor_area_sqm`, `flat_model`, `lease_commence_date`), the thing deduplication already guarantees is unique per row. Zero collisions, this is the one that satisfies requirement 10 |

`resale_identifier_short_hash` colliding and `resale_identifier_hash` not is the point. Both are
kept in the same file so the contrast is visible directly, not just argued in
[Resale identifier collision](#resale-identifier-collision) above.

**Quarantined**: base columns, plus every one of the 14 `flag_*` boolean columns,
`quarantine_reason`, and the identifier/hash columns (`resale_identifier`,
`resale_identifier_key`, `resale_identifier_hash`, `resale_identifier_short_hash`). One real
wrinkle: a row quarantined for `quarantine_reason == "duplicate_composite_key"` was dropped
*before* remaining lease, the identifier, or most flags were computed (dedup runs first, see
[Key design decisions](#key-design-decisions) above), so those columns are blank for that row, not
a bug. A row quarantined for any `flag_*` reason instead has every column filled in, it made it
all the way through the pipeline and only failed a quality check at the end. Verified directly
against `data/output/quarantined.csv`: all 1,600 `duplicate_composite_key` rows are null in
`remaining_lease_years`, `resale_identifier`, `resale_identifier_hash` and every dedup-stage flag;
every gate-flagged row has all of them populated.

**Raw**: the three source CSVs, byte-for-byte as downloaded, plus `_manifest.json` (path,
SHA-256, fetch time). Column names and types are exactly what data.gov.sg publishes, nothing
added yet.

From the last verified run: 459,007 raw rows across all three files → 92,544 in the 2012–2016
window → 90,944 after deduplication (1,600 duplicates dropped) → 81,362 pass every gating check
(Cleaned) and 11,182 are quarantined. The notebook asserts this reconciles exactly on every run.
There is deliberately no run-level volume gate (e.g. "halt if Cleaned is unusually small"): every
row is fully accounted for by the reconciliation assert and the per-row `quarantine_reason`
either way, so quarantining is treated as sufficient on its own, the pipeline always writes
whatever it produces, however small Cleaned turns out to be.
All fourteen quality flags gate by default now (see [Configuration](#configuration)), including the
four domain-membership checks, an unseen `flat_model` or `storey_range` value is quarantined,
not silently accepted, until a person reviews it.

The exact Cleaned/Quarantined split above moves as `validation.reference_domains` and
`validation.reference_exceptions` get reviewed and edited (see `config.yaml`'s own comments next
to `flat_model` for the review already done there), that's expected, not a regression; the
reconciliation assert is what guarantees the split stays internally consistent regardless of
where those two numbers land.

## Secrets

`.env` (if you create one) is not committed, only `.env.example` is. Nothing in `config.yaml` or
the notebook itself contains a real credential; the API key is read from the environment only.

## Reproducibility notes

- Re-running with the same raw files and `config.yaml` reproduces identical row counts,
  identifiers and hashes.
- The prescribed `resale_identifier` is **not** unique by construction: measured on the
  90,944 deduplicated rows (before gating), roughly 15% share a code with at least one other row.
  This is expected, and visible directly in `data/output/hashed.{csv,parquet}` itself:
  `resale_identifier_short_hash` (SHA-256 of `resale_identifier`) has that same collision rate,
  hashing didn't remove it, which is exactly why `resale_identifier_hash` is computed over the
  composite key instead (zero collisions, see [Output columns](#output-columns) above).
- `data/output/` and `data/raw/` are regenerated by running the notebook or the pipeline, but are
  deliberately not excluded by `.gitignore`: requirement 14 asks for the notebook to be uploaded
  with its input files, and the five output groups are themselves a listed deliverable. `.gitignore`
  only excludes generated tooling clutter (`__pycache__/`, `.pytest_cache/`) and secrets (`.env`).

## Software engineering practices

**Separation of concerns.** `src/hdb_etl/` splits into 9 single-purpose modules (`extraction`,
`profiling`, `cleaning`, `validation`, `lease`, `dedup`, `identifier`, `output`, `pipeline`), each
one mapping to one notebook section. No module does more than one job.

**Library code has zero side effects.** `grep -rn "print(" src/hdb_etl/*.py` returns nothing.
Every `print()` in the project lives in `run_pipeline.py`, the one place reporting belongs.
Library functions take data in, return data out, nothing more.

**Tests exercise the real code path, not a copy of it.** `tests/conftest.py` and
`tests/test_validation.py` both import `hdb_etl.pipeline`, the exact module `run_pipeline.py`
itself calls. A change to validation logic can't silently leave a test suite that no longer
reflects what actually runs, because there is only one implementation to run.

**Defensive numeric handling, applied consistently.** `pd.to_numeric(..., errors="coerce")`
appears 9 times across the pipeline's numeric-facing code, each one the actual fix for a genuine
crash (see [Findings from actually running this](#findings-from-actually-running-this) above),
not a single patch applied once and left inconsistent elsewhere.

**Integrity-checked caching, not just existence-checked.** `_sha256_of()` in `extraction.py`
recomputes and compares a checksum before trusting a cached file. A file that exists on disk but
doesn't match its recorded hash is treated as not cached at all.

**Fail loud on an internal invariant, fail gracefully on an external one, deliberately not the
same mechanism for both.** `output.reconcile()` asserts row counts add up and raises immediately
if they don't. The download retry logic instead backs off and retries a known, documented,
transient external condition (a 429 rate limit). Two different failure modes, two different
responses.

**Every module documents itself at the point of use.** All 10 files in `src/hdb_etl/` open with a
docstring explaining what that module does and why.

**Config actually drives behaviour, demonstrated, not just claimed.** The notebook's "Tests"
section reruns the same formulas at different config values against data already in memory and
shows the flagged count changes accordingly, for the anomaly threshold, the floor-area minimum,
and the flat-model exceptions specifically.

**Pinned dependencies.** Every entry in `requirements.txt` is an exact version (`pandas==1.5.3`,
not `pandas>=1.5`). A rerun months from now installs the same library versions this was built and
tested against.

**Type hints throughout `src/hdb_etl/`.** Every function signature across all 9 modules is
annotated (parameters and return types). Config dicts are typed as `dict[str, Any]`,
DataFrame-in/DataFrame-out functions are typed as such, multi-value returns use `tuple[...]`.

**Logging instead of print, at the application layer only.** `run_pipeline.py` uses Python's
`logging` module, not bare `print()`. `hdb_etl` itself still has zero logging calls, same as it
has zero print calls, deliberately: a library function shouldn't decide how or where its caller's
log output goes, that's the entry point's job, not the library's.

**Config schema validation.** `hdb_etl.config.validate_config_schema()` runs automatically inside
`load_config()` and rejects an unrecognised key anywhere in `config.yaml`, at any nesting level,
with a `ValueError` naming the exact bad key, rather than the previous behaviour of silently
falling back to a default. Tested directly against a `min_sqmm` typo:
`config.yaml's validation.floor_area has unrecognised key(s): ['min_sqmm']`.
