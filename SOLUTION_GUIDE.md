# Dexwin Take-Home Solution Guide

## Overview

This repository contains a reproducible Python + SQLite pipeline for retail
order/revenue and fulfilment analytics. It deduplicates versioned source
records, recognises revenue from order lifecycle events, computes fulfilment
metrics and current delayed-delivery alerts, and reports data-quality
findings.

- Python (`src/pipeline.py`) handles file ingestion, record validation,
  SQLite staging, orchestration, and exports.
- SQL (`sql/transformations.sql`) handles the principal transformations:
  version selection, line/revenue facts, delivery state, metrics, alerts and
  data-quality counts.
- No third-party Python packages are required.
- The solution uses the fixed assessment timestamp
  `2026-07-15T09:00:00Z` from the assignment for delivery-delay evaluation
  only; revenue recognition is not cutoff-limited.

## Requirements

- Python 3.10+
- SQLite through Python's built-in `sqlite3` module
- No paid services
- No external database required

## Run the pipeline

```bash
python3 src/pipeline.py --data-dir data --output-dir output
```

This reads the five source files from `data/`, validates and stages them in
an in-memory SQLite database, executes the SQL transformations, and writes the
four result files to `output/`. Fatal schema/input problems stop the run with
an error message; row-level data-quality issues are reported in
`data_quality_report.json`.

## Run the tests

```bash
python3 -m unittest discover -s tests -v
```

The suite contains 22 tests covering the business rules (version selection,
revenue lifecycle, delivery/alert logic, unknown products, quarantined
orphans, validation and failure modes) plus end-to-end regression checks
against the supplied dataset. Tests run entirely in temporary directories and
do not write to `output/`.

## Generated outputs

- `output/revenue_events.csv` — order-level SALE and REVERSAL events with
  event date, store, net revenue and known gross profit; the auditable
  revenue record.
- `output/daily_store_metrics.csv` — revenue and fulfilment measures per
  metric_date x store_id, including on-time delivery counts and rate.
- `output/delayed_delivery_alerts.jsonl` — one JSON object per order currently
  delayed (delivered late, or not delivered by promise) as of the fixed
  assessment timestamp.
- `output/data_quality_report.json` — rejected rows and validation errors,
  quarantined orphans, duplicate (identical superseded) and corrected
  (differing superseded) version counts, unmatched unknown-product lines, and
  per-source row counts.

## Implementation structure

- `src/pipeline.py` — CLI entry point; loading, per-row validation, staging,
  running the SQL script, deterministic exports, and quality-report assembly.
- `sql/transformations.sql` — layered views and result tables: current
  authoritative versions by latest `updated_at`, line facts, revenue
  lifecycle, revenue events, delivery facts, as-of delivery state, daily
  metrics, delayed alerts, and data-quality counts.
- `tests/test_pipeline.py` — standard-library `unittest` suite using
  synthetic fixtures in temporary directories plus supplied-dataset
  regression tests via the real CLI.
- `DESIGN_NOTES.md` — assumptions, metric definitions, BI consumption, and
  the proposed (not implemented) productionised design.

## Verification

A current run against the supplied dataset produced:

| Result | Value |
|---|---|
| Revenue events | 32 (26 SALE, 6 REVERSAL) |
| Net revenue | 1675.00 |
| Known gross profit | 799.50 |
| Delivered orders | 26 |
| On-time deliveries | 20 |
| Delayed delivery alerts | 9 |
| Duplicate extra rows | 3 |
| Corrected versions | 3 |
| Quarantined orphan deliveries | 1 |
| Unmatched unknown-product lines | 1 |
| Rejected rows | 0 |

## Reproducibility

The pipeline creates the output directory when required and runs against an
in-memory SQLite database, so there is no persistent state to reset. Reruns
overwrite the outputs deterministically, and all data sources are plain
CSV/JSONL files included in the repository.

The original assignment `README.md` remains unchanged and contains the full
assessment requirements.
