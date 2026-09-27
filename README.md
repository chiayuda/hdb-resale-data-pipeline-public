# HDB resale flat data engineering submission

Two parts, each self-contained with its own README. Read Part 1, then Part 2.

| Part | What it is | Start here |
|---|---|---|
| [Part 1](part_1/README.md) | ETL pipeline: a Jupyter notebook and a standalone Python package, same logic, same `config.yaml` | `part_1/README.md`: setup, how to run both, what it does, validation choices, findings, the identifier collision |
| [Part 2](part_2/README.md) | Proposed AWS architecture for operating this pipeline at scale. Design only, nothing deployed | `part_2/README.md`, with the [ingestion](part_2/01_ingestion.png) and [exploitation](part_2/02_exploitation.png) diagrams |

## Part 1: ETL pipeline

Downloads HDB resale transaction data (Jan 2012 to Dec 2016), profiles and cleans it, validates it
against a January 2012 reference, computes remaining lease, deduplicates (keeping the higher price
on a duplicate), screens for anomalous prices, builds the prescribed resale identifier and a
collision-free hash of it, and writes five output groups: Raw, Cleaned, Transformed, Quarantined,
Hashed.

```
data.gov.sg
   -> Raw (byte-for-byte download)
   -> windowed (date format + range)
   -> profiled, cleaned (flat_model casing)
   -> validated (Town/Flat Type/Flat Model/storey_range domain membership, format and numeric checks)
   -> deduplicated (composite key, higher price wins)
   -> remaining lease computed and cross-checked
   -> anomaly + additional bound checks
   -> resale identifier + hash
   -> Cleaned / Transformed / Quarantined / Hashed
```

Two ways to run the exact same pipeline, same `config.yaml`, same `data/output/`:

- **Notebook** (`part1_notebook.ipynb`): the reference implementation. Includes the
  synthetic-corruption demonstrations and config-sensitivity proofs, so a reviewer can see the
  checks actually work, not just read that they should.
- **Python pipeline** (`run_pipeline.py` + `src/hdb_etl/`): the same logic as an importable
  package, one module per pipeline stage, with its own pytest suite (`tests/`).

Full write-up in [`part_1/README.md`](part_1/README.md): setup and run instructions for both,
per-column validation choices (what check, why, nullable or not), the remaining-lease formula and
its verification against HDB's own figures, the resale-identifier collision, and the
quarantine-not-halt design.

## Part 2: AWS architecture

Open the two PNGs for the submitted views and the draw.io file to inspect or edit the source. The
[Part 2 README](part_2/README.md) identifies what is already verified locally and what must be
implemented and tested before this could run on AWS. In particular, the notebook is not yet a
deployable ECS container, and the Athena-Tableau connection is a documented design rather than a
live integration test.
