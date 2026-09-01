# Dexwin Take-Home Solution Guide

Reviewer-facing guide to what was delivered and how to assess it. The
assignment specification itself is unchanged in `README.md`; design reasoning
is in `DESIGN_NOTES.md` and implementation detail in `IMPLEMENTATION_NOTES.md`.

## Executive Summary

A reproducible Python 3.10+ / SQLite pipeline that loads the five supplied
sources, resolves versioned duplicates and corrections, recognises order
revenue and reversals, computes store/day fulfilment metrics, emits current
delayed-delivery alerts as of the fixed assessment time, and reports every
data-quality finding. The pipeline itself uses only the Python standard
library (22 automated tests), and verified output totals for the supplied
dataset. A pinned `.venv` Jupyter environment supports the optional
investigation notebook.

## What Was Implemented

- `src/pipeline.py` — CLI, source loading, structural and row-level
  validation (timestamps, enums, `Decimal` finite/non-negative numerics,
  per-record JSONL checks), in-memory SQLite staging, orchestration,
  deterministic exports.
- `sql/transformations.sql` — version selection by latest `updated_at`,
  line/delivery facts, revenue lifecycle (SALE/REVERSAL), full-extract
  historical delivery metrics, as-of delayed-delivery alerts, and
  data-quality counts.
- `tests/test_pipeline.py` — 22 stdlib `unittest` tests on synthetic fixtures
  plus supplied-dataset end-to-end regression through the real CLI.
- `notebooks/data_investigation.ipynb` — investigation working paper.
- Generated artefacts committed under `output/`.

## Repository Structure

```
data/                    supplied sources (5 files)
output/                  generated results (4 files)
src/pipeline.py          pipeline + CLI
sql/transformations.sql  all business transformations
tests/test_pipeline.py   automated test suite
notebooks/data_investigation.ipynb  data investigation working paper
README.md                assignment specification (unchanged)
DESIGN_NOTES.md          assumptions, metrics, production design
IMPLEMENTATION_NOTES.md  how the implementation works
SOLUTION_GUIDE.md        this guide
requirements.txt         pinned notebooks/ environment only (pipeline has zero deps)
```

## How to Run

```bash
python3 src/pipeline.py --data-dir data --output-dir output
```

Reads the sources, validates and stages them, runs the SQL transformations,
and writes the four output files. The output directory is created if needed.
Fatal input problems (missing files, missing columns, invalid JSON) exit 1
with a clear message. Only `python3` (3.10+) is required; no installation
step is needed to run the pipeline or the tests.

The investigation notebook has an optional pinned environment (CPython
3.13.5):

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/jupyter notebook notebooks/data_investigation.ipynb
```

## How to Test

```bash
python3 -m unittest discover -s tests -v
```

22 tests, all passing. Tests use temporary directories and never write to
`output/`.

## Business Rules Implemented

- Latest valid `updated_at` is authoritative; duplicates and corrected
  versions are distinguished, never double-counted.
- First valid `COMPLETED` creates one SALE; a later `CANCELLED` creates an
  equal REVERSAL; cancellation before completion or `PROCESSING`-only
  produces no revenue; event dates use `event_ts`.
- Line value = `quantity * unit_price - discount_amount`.
- Unknown products keep revenue; known gross profit covers only lines with
  known costs (NULL, never zero, when none).
- First valid `DELIVERED` event determines delivery timing;
  `delivered_at <= promised_delivery_at` is on time.
- Delay alerts are evaluated as of `2026-07-15T09:00:00Z` (later deliveries
  excluded from alert state); historical metrics are not cutoff-limited.
- Orphans are quarantined and reported, never silently attached.
- Revenue is never cutoff by the assessment time (the assessment day's
  16:00 cancellation still reverses).

## Data Quality Handling

`data_quality_report.json` distinguishes rejected rows vs validation errors
(with file line numbers), duplicate extras vs corrected versions,
quarantined orphans (per source), unmatched unknown-product lines, and
raw/staged/authoritative row counts for reconciliation.

## Generated Outputs

- `revenue_events.csv` — auditable order-level SALE/REVERSAL events.
- `daily_store_metrics.csv` — metric_date x store_id revenue and fulfilment
  measures, including on-time rate.
- `delayed_delivery_alerts.jsonl` — one object per currently delayed order
  (`delivered_late` or `not_delivered_by_promise`).
- `data_quality_report.json` — the run's DQ and reconciliation document.

## Investigation Notebook

`notebooks/data_investigation.ipynb` is a working paper showing how the data
was explored: source inventory, duplicate/identifier analysis, orphan and
unknown-product detection, revenue and delivery investigation, execution of
the actual production SQL for result-layer inspection, and a cross-check of
the committed outputs. It uses only the standard library, writes no files,
and is not part of — nor a replacement for — the pipeline.

## Verification Results

Current run against the supplied dataset:

| Metric | Value |
|---|---|
| Revenue events | 32 |
| SALE / REVERSAL | 26 / 6 |
| Net revenue | 1675.00 |
| Known gross profit | 799.50 |
| Delivered orders | 26 |
| On-time deliveries | 20 |
| Delayed delivery alerts | 9 |
| Duplicate extra rows | 3 |
| Corrected versions | 3 |
| Quarantined orphan deliveries | 1 |
| Unknown product lines | 1 |
| Rejected rows | 0 |

## Key Design Decisions

- SQLite (`sqlite3`) keeps all business logic in visible, reviewable SQL
  with zero setup; no third-party packages, so reproduction needs only a
  Python 3.10+ interpreter.
- Python owns ingestion/validation/export; SQL owns transformation — the
  boundary is explicit and tested through the public interface.
- Version selection precedes business validation, so corrected versions
  repair records generically rather than via special cases.
- Two separate delivery views encode the two time semantics (historical
  metrics vs as-of alerts) instead of one overloaded rule.
- Known limitations (raw-text duplicate comparison, `updated_at` tie-break,
  whole-row rejects) are documented rather than silently accepted.

## Productionisation Path

Not implemented here; conceptually: object-storage landing zone, scheduled
orchestration (e.g. Azure Data Factory), batch-id idempotency, dead-letter
quarantine, atomic publication, stateful alert keys such as
`(order_id, assessment_window)`, monitoring on rejection/duration/row-count
trends, and replay from retained immutable sources. At ~100x volume, replace
full-reload SQLite with incremental loading, partitioned columnar storage
(DuckDB/warehouse), and per-partition parallelism. Details in
`DESIGN_NOTES.md`.

## AI / Tool Use

AI assistance (Cursor's coding assistant, with ChatGPT used for code review
and testing support) contributed to implementation assistance, review,
edge-case identification and documentation. All output was verified against
the assignment specification via the supplied-dataset run and the automated
test suite; the disclosures in `DESIGN_NOTES.md` name specific corrections
the review process made.
