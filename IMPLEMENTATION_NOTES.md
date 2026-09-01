# Implementation Notes

Describes how the submitted implementation works, based on the current
`src/pipeline.py`, `sql/transformations.sql` and `tests/test_pipeline.py`.

## 1. Architecture

```
Python ingestion/validation  ->  SQLite staging  ->  SQL transformations  ->  deterministic exports
```

`src/pipeline.py` is the orchestrator. It reads the five source files, fails
fast on structural problems, validates rows, inserts valid rows into
in-memory SQLite staging tables, executes `sql/transformations.sql`, then
queries the resulting tables and writes four output files. The SQLite
database is `:memory:`; nothing persists between runs.

## 2. Python Responsibilities

- CLI (`--data-dir`, `--output-dir`; defaults `data/`, `output/`).
- Source loading: CSV via `csv.DictReader`, JSONL via `json.loads` per line;
  missing files, missing headers, malformed JSON and non-object JSONL lines
  raise `PipelineError` (fatal, exit 1).
- Schema checks: every CSV must carry its required columns; every JSONL
  record is independently checked for the required fields.
- Row validation: identifiers non-blank; timestamps parsed with
  `datetime.fromisoformat` (`Z` suffix); status/`event_type` restricted to
  the allowed enums; numerics parsed with `Decimal`, required finite
  (rejecting `NaN`/`Infinity` spellings that `float()` would accept), and
  non-negative for `quantity`, `unit_price`, `discount_amount`, `unit_cost`.
- Staging: valid rows inserted into `staging_*` tables plus a one-row
  `pipeline_config` table carrying the assessment timestamp.
- Orchestration and export: executes the SQL script, then formats and writes
  outputs with stable ordering (documented sort keys), two-decimal money,
  six-decimal rates, and JSONL one object per line.

Row-level validation failures are rejections (reported), not fatal errors.

## 3. SQL Responsibilities

`sql/transformations.sql` (kept visible and unexecuted by Python logic) owns:

- version selection and authoritative records (`v_*_ranked`, `v_current_*`),
- line facts and order amounts (`v_line_facts`, `v_order_amounts`),
- the revenue lifecycle (`v_first_completed`, `v_first_cancelled`,
  `v_order_revenue`) and `result_revenue_events`,
- delivery state: full-extract facts (`v_delivery_facts`) for metrics and a
  separate as-of view (`v_asof_delivery`) for alerts,
- `result_daily_store_metrics`, `result_delayed_delivery_alerts`,
- `result_dq_counts` (duplicates, corrections, quarantined orphans,
  unmatched products, source/authoritative row counts).

All timestamps are ISO-8601 UTC text, so lexicographic comparison is valid
and is relied upon throughout.

## 4. Versioning

Each versioned source is ranked with
`ROW_NUMBER() OVER (PARTITION BY <business key> ORDER BY updated_at DESC, rowid ASC)`;
rank 1 is authoritative. `updated_at` is the source-system version clock and
is used only to pick versions — business timing always uses `event_ts`.
Version selection precedes validity checks (`event_ts >= ordered_at`, known
order), so a corrected later version can repair an originally invalid event
without special-casing any order.

Superseded rows are then classified against the kept row: identical in every
column → **duplicate extra** (never double-counts); differing → **corrected
version**. Limitations: equal-`updated_at` conflicts are tie-broken by
first-loaded row (safe only for exact duplicates), and the comparison is
raw-text, so `3` vs `3.0` classifies as a correction.

## 5. Revenue Logic

- Line value = `quantity * unit_price - discount_amount`; order revenue is
  the sum over the order's current lines (orphan lines excluded).
- First valid `COMPLETED` per order creates exactly one SALE, dated on its
  `event_ts`.
- A `CANCELLED` event later than that completion creates an equal REVERSAL
  (negated net revenue and known gross profit) on the cancellation date.
- `CANCELLED` before `COMPLETED` → no revenue; `CREATED`/`PROCESSING`-only →
  no revenue.
- Unknown products keep their revenue; `known_gross_profit` covers only lines
  with a known cost (NULL when none, never zero).
- The revenue extract has no assessment cutoff; `2026-07-15T09:00:00Z` is
  used only for delay-alert evaluation (the supplied 16:00 cancellation on
  the assessment day therefore still reverses).

## 6. Delivery Logic

Two views, two time semantics:

- **Historical** (`v_delivery_facts`): first valid `DELIVERED` event per
  known order over the full extract; `delivered_at <= promised_delivery_at`
  is on time (boundary inclusive). Daily metrics use this view and are not
  cutoff-limited.
- **As-of** (`v_asof_delivery`): identical logic restricted to
  `event_ts <= assessment_ts`, used only by
  `result_delayed_delivery_alerts`. Orders delivered late within the window
  alert as `delivered_late`; undelivered-with-passed-promise alert as
  `not_delivered_by_promise`; future promises do not alert. Cancelled orders
  are not excluded (documented open business question).

Orphan delivery events never join to orders; they are counted and reported
as quarantined.

## 7. Data Quality

The report keeps six distinct categories:

- **Rejected rows** — source lines failing validation (whole row excluded);
  **validation errors** — messages raised (one row can raise several); each
  cites source file and line number.
- **Duplicates** — superseded versions identical to the authoritative row.
- **Corrections** — superseded versions differing from it.
- **Quarantined** — current records referencing unknown orders (lines,
  status events, delivery events counted separately).
- **Unmatched** — current lines with unknown `product_id`; revenue stands,
  cost unknown.

Plus raw, staged and authoritative row counts for reconciliation.

## 8. Output Contracts

- `output/revenue_events.csv` — grain: order x recognised event. Columns:
  `event_date, order_id, store_id, event_type, net_revenue,
  known_gross_profit`. Money two decimals; empty GP means unknown.
- `output/daily_store_metrics.csv` — grain: metric_date x store_id. Columns:
  revenue measures, `completed_orders`, `reversed_orders`,
  `delivered_orders`, `on_time_deliveries`, `on_time_delivery_rate` (six
  decimals; empty when no deliveries).
- `output/delayed_delivery_alerts.jsonl` — grain: currently delayed order.
  Keys: `order_id, store_id, promised_delivery_at, delivered_at, reason`
  (`delivered_at` null when undelivered).
- `output/data_quality_report.json` — run document: `assessment_ts`,
  `rejected` (rows/validation_errors/by_source/errors), `quarantined`,
  `duplicate`, `corrected`, `unmatched`, and row-count sections.

## 9. Testing

`tests/test_pipeline.py` — 22 tests, standard-library `unittest`, no
third-party packages. Synthetic minimal fixtures run through `pipeline.run`
in temporary directories (repo `output/` is never used by tests), asserting
only on documented outputs. Coverage: supplied-dataset end-to-end via the
real CLI plus regression totals and format contracts; duplicate lines;
latest line/status/delivery version selection; single SALE per order; equal
reversal; cancel-before-complete and processing-only → no revenue; unknown
products; orphan quarantine; late/missing/future-promise alerts;
assessment-time exclusion for alerts vs historical metrics; inclusive
on-time boundary; NaN/Infinity/negative rejection; malformed JSONL rejection
(non-fatal); fatal invalid JSON/missing columns/missing files.

## 10. Reproducibility

```bash
python3 src/pipeline.py --data-dir data --output-dir output
python3 -m unittest discover -s tests -v
```

The output directory is created when required, the database is in-memory,
and outputs are fully regenerated and deterministic on every run. No
network, credentials or persistent state are involved.

## 11. Design Trade-offs

- **SQLite**: preinstalled with Python, real SQL, zero setup — matches the
  brief's requirement for visible SQL without new infrastructure. The
  in-memory choice removes cleanup and cross-run state entirely; dataset
  size makes the full reload cheap.
- **No third-party runtime dependencies**: the pipeline itself uses only the
  standard library. `requirements.txt` pins the optional notebook analysis
  environment (`notebook`, `ipykernel`, `pandas`, `matplotlib`, `seaborn`)
  for CPython 3.13.5; the committed notebook imports none of them directly
  beyond running under the pinned Jupyter kernel. Pipeline and tests need
  only `python3`.
  `csv`/`json`/`sqlite3`/`decimal` cover the job; fewer moving parts means
  the reviewer reproduces it with only Python 3.10+.
- **SQL stays in `sql/transformations.sql`** rather than embedded strings, so
  the transformation layer is diffable and reviewable on its own.
- **Row-level rejects over partial staging**: rejecting the whole row keeps
  staging uniform; field-level quarantine would add complexity the brief
  does not need.
- **No dashboard or automation flow**: the assignment asks for design
  documentation, not implementation (`DESIGN_NOTES.md` covers the proposed
  production architecture explicitly as non-implemented).
- **`notebooks/data_investigation.ipynb`** is a read-only working paper that
  reuses production helpers to inspect the same result layer; it is not part
  of the pipeline and writes no files.
