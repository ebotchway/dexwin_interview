# Implementation Notes

Engineering notes for the Dexwin take-home, describing the implementation
currently present in this repository (`src/pipeline.py`,
`sql/transformations.sql`, `tests/test_pipeline.py`). Sections that describe
production or deployment behaviour are explicitly labelled as proposals, not
implemented features.

## 1. Architecture and Responsibilities

Two layers with a clear boundary:

- **Python** (`src/pipeline.py`, standard library only, Python 3.10+):
  CLI argument handling, reading CSV/JSONL sources, fatal schema checks,
  per-row validation, staging of valid rows into an in-memory SQLite
  database, executing the SQL script, and writing the four output files with
  stable formatting and ordering.
- **SQLite SQL** (`sql/transformations.sql`, executed via built-in
  `sqlite3`): version selection, line and delivery facts, revenue lifecycle,
  revenue events, as-of delivery state, daily store metrics, delayed alerts,
  and data-quality counts.

All timestamps are ISO-8601 UTC text; lexicographic comparison is valid and
is relied upon in SQL. No paid tools or external runtime dependencies are
required; `requirements.txt` intentionally lists none.

## 2. Data Grain and Relationships

Orders are the hub. Every order-level record joins through `order_id`:

- `order_lines`: keyed and versioned by `line_id` + `updated_at`; facts at
  line grain, only attached to known orders.
- `order_status_events`: keyed and versioned by `event_id` + `updated_at`.
- `delivery_events` (JSONL): keyed and versioned by `event_id` + `updated_at`.
- `products`: joined to lines through `product_id` (left join; absence is a
  data-quality finding, not a rejection).

Output grains:

- `revenue_events.csv` — one row per recognised SALE or REVERSAL (order x
  event date).
- `daily_store_metrics.csv` — one row per metric_date x store_id.
- `delayed_delivery_alerts.jsonl` — one object per currently delayed order.
- `data_quality_report.json` — one run-level document.

## 3. Versioning and Corrections

`updated_at` is the source-system version clock. For each business key, a
`ROW_NUMBER()` window orders versions `updated_at DESC, rowid ASC`; rank 1 is
authoritative. Version selection happens **before** any business
interpretation, so a later corrected version can repair an originally invalid
event (for example, a `COMPLETED` status whose first version carried a
business timestamp before `ordered_at`, or a delivery whose corrected version
moved the delivery earlier) — handled generically, with no order-specific
logic in code or SQL.

Business timestamps come from `event_ts`, never `updated_at`; `updated_at`
only answers "which version is current".

Exact-duplicate superseded rows (identical in every column to the
authoritative row) are counted as duplicate extras and never double count.
Superseded rows that differ from the authoritative row are counted as
corrections. The two categories are computed by direct comparison against the
kept row, not by counting `ROW_NUMBER() > 1` twice.

Known limitations:

- Versions sharing an identical `updated_at` are tie-broken by first-loaded
  row (`rowid ASC`), which is only meaningful for exact duplicates; genuinely
  conflicting versions cannot be distinguished without a source sequence
  number.
- Duplicate-vs-correction classification compares raw staged text values, so
  semantically equivalent representations such as `3` and `3.0` are
  classified as a correction.

## 4. Revenue Recognition

- Line value = `quantity * unit_price - discount_amount`; order amount is the
  sum over current lines of the known order.
- The first **valid** `COMPLETED` event per order (valid = attached to a
  known order and `event_ts >= ordered_at`, evaluated on authoritative
  versions) creates exactly one SALE.
- A `CANCELLED` event later than that completion creates an equal REVERSAL
  (negated net revenue and known gross profit) dated on the cancellation's
  `event_ts`.
- `CANCELLED` before `COMPLETED` produces no revenue at all.
- `CREATED`/`PROCESSING`-only orders produce no revenue.
- Event dates are derived from `event_ts` (`substr(...,1,10)`), not load or
  version time.
- Unknown products still contribute net revenue; their cost is unknown, so
  `known_gross_profit` excludes those lines (NULL when no line has a known
  cost — never coerced to zero).
- The revenue extract is **not** cutoff at the assessment timestamp. In the
  supplied data, order O030's cancellation on 2026-07-15 at 16:00Z is later
  than the 09:00Z assessment instant yet still generates its reversal: the
  assessment timestamp is used exclusively for delay-alert evaluation, not as
  a global revenue cutoff. This is the implemented behaviour.

## 5. Delivery and Alert Logic

- Delivery performance uses the first valid `DELIVERED` event per known
  order (again after version selection, `event_ts >= ordered_at`).
- `delivered_at <= promised_delivery_at` is on time (boundary inclusive).
- Alerts are evaluated as of `2026-07-15T09:00:00Z` via a separate as-of view
  that ignores delivery events with `event_ts` after the assessment
  timestamp. An order actually delivered later than the assessment time is
  therefore alerted on the "not delivered by promise" branch as of the
  snapshot.
- Historical daily metrics are computed from the full-extract delivery facts
  and are **not** limited by the assessment timestamp, so later deliveries
  still appear in delivered/on-time measures on their business date.
- A missing delivery is alerted only once the promise has passed
  (`promised_delivery_at <= assessment_ts`).
- Orphan delivery events (unknown `order_id`) never attach to orders; they
  are counted and reported as quarantined.
- Alert reasons currently emitted: `delivered_late` and
  `not_delivered_by_promise`. Cancelled orders are not excluded from alerts
  (an open business question documented in `DESIGN_NOTES.md`).

## 6. Data Quality and Validation

Python validation:

- Header/schema checks for all four CSVs; missing source files, missing
  headers and missing required columns are fatal (`PipelineError`, exit 1).
- JSONL: invalid JSON or non-object lines are fatal; every record is
  independently checked for required fields, identifiers, allowed
  `event_type`, and parseable timestamps.
- Timestamps parsed with `datetime.fromisoformat` (`Z` suffix supported).
- Numerics parsed with `Decimal` and required to be finite (`NaN`,
  `Infinity`, `-Infinity` and other non-parsable spellings are rejected);
  `quantity`, `unit_price`, `discount_amount` and `unit_cost` must also be
  non-negative (revenue corrections are modelled as new versions and
  CANCELLED events, never signed amounts).
- Status values must be in `CREATED/PROCESSING/COMPLETED/CANCELLED`.

Reporting vocabulary (each is distinct):

- **Rejected rows** — source lines failing validation (whole row excluded
  from staging).
- **Validation errors** — messages raised; one row can raise several.
- **Duplicates** — superseded versions identical to the authoritative row.
- **Corrections** — superseded versions differing from the authoritative row.
- **Quarantined records** — orphan lines, status events or delivery events
  attached to unknown orders.
- **Unmatched** — current lines whose `product_id` is absent from products;
  revenue stands, cost is unknown.

## 7. Output Contracts

- `output/revenue_events.csv` — auditable order-level revenue events
  (`event_date, order_id, store_id, event_type, net_revenue,
  known_gross_profit`); money formatted to two decimals, empty GP means
  unknown.
- `output/daily_store_metrics.csv` — store/day aggregate: revenue measures,
  completed/reversed order counts, delivered and on-time counts, and
  `on_time_delivery_rate` (six decimals; empty when no deliveries).
- `output/delayed_delivery_alerts.jsonl` — one JSON object per delayed order
  with `order_id, store_id, promised_delivery_at, delivered_at, reason`
  (`delivered_at` null for undelivered orders).
- `output/data_quality_report.json` — run document with the six DQ
  categories above, per-source rejection detail with file line numbers, and
  raw/authoritative row counts, plus the assessment timestamp used.

Ordering in all files is deterministic (documented sort keys).

## 8. Testing Strategy

`tests/test_pipeline.py` uses standard-library `unittest` with synthetic
minimal fixtures in temporary directories (repo `output/` is never touched by
tests), asserting only on documented outputs. 22 tests cover: supplied-dataset
end-to-end run via the real CLI plus output-format contracts; exact-duplicate
lines; latest line/status/delivery version wins; single SALE from multiple
completions; equal reversal after cancellation; cancel-before-complete and
processing-only producing no revenue; unknown products keeping revenue
without GP; orphan delivery quarantine; late-delivery and missing-delivery
alerts; future promise producing no alert; deliveries after the assessment
time excluded from alert state but kept in historical metrics; inclusive
on-time boundary; NaN/Infinity/negative numerics rejected; malformed JSONL
records rejected non-fatally; fatal invalid JSON, missing columns and missing
files; and the full supplied-data regression totals.

## 9. Idempotency, Retries and Operational Considerations

The take-home run is effectively idempotent: outputs are regenerated
deterministically from the source snapshot, SQLite state is in-memory with no
persistent residue, the output directory and files are rewritten each run,
and version selection plus duplicate reporting prevent re-supplied source
versions from double counting. Operational design considerations (not
implemented here): production retries should key batches by file/batch id,
alert delivery needs a stable `(order_id, assessment_window)` business key
with an acknowledgement state store so reruns update rather than duplicate
alerts, and failures should distinguish transient (retry with backoff) from
structural (fail fast, quarantine).

## 10. Scaling to 100x Volume

Not implemented, but the natural escalation path if volumes grow: stream
source files rather than holding all parsed rows in memory; keep
`executemany` batching (already used) and add indexes on join/version keys
(`order_id`, business key + `updated_at`); process per-day or per-store
partitions incrementally instead of full reloads; persist state (SQLite file
or a client-server analytical database) if in-memory SQLite becomes the
bottleneck; and parallelise load/transform per partition with atomic
per-partition publication. The versioned-event model already supports
incremental merge semantics.

## 11. BI / Reporting Use

- `revenue_events` — transaction/event fact for drill-through and reversal
  auditing.
- `daily_store_metrics` — store/day aggregate serving management reporting,
  joinable on date and store dimensions.
- `delayed_delivery_alerts` — operational exception feed for store teams,
  not a history table.
- `data_quality_report` — pipeline monitoring metadata (rejection and
  quarantine trends, row-count reconciliation).

Business logic lives upstream in the pipeline so reports stay consistent.

## 12. Deployment Considerations

Proposed only — this assessment runs locally. In a Microsoft-oriented
environment: source and curated files in Azure Blob Storage / Data Lake; the
Python job containerised and scheduled by Azure Data Factory or Fabric
pipelines; result tables or files consumed by Power BI; identity via
Microsoft Entra ID with managed identities for least-privilege storage
access; secrets in Azure Key Vault; logs/metrics in Azure Monitor. Failure
handling, replay from retained immutable sources, and alert idempotency per
sections 3/9 would be configured at the orchestrator layer.

## 13. AI Assistance Disclosure

AI assistance (Cursor's coding assistant) was used during development for
code review, test design, edge-case identification, documentation refinement,
and general reasoning support. Every generated suggestion was reviewed against
the assignment requirements; the submitted implementation and documentation
were validated by running the supplied dataset end-to-end and the automated
test suite.

## 14. Known Limitations and Assumptions

Supported by the current implementation:

- Duplicate-vs-correction classification compares raw staged text, so
  `3` vs `3.0` counts as a correction.
- Conflicting versions with identical `updated_at` are tie-broken by
  first-loaded row, which cannot distinguish genuine disagreements.
- `customer_id` and `category` are required in headers but carry no value
  validation; they are not used by any rule, so validation is intentionally
  limited.
- The assessment timestamp is a Python constant injected into the SQL layer
  through a one-row `pipeline_config` table; changing the run date requires
  a code change (no CLI flag currently exposes it).
- A row failing any validation is rejected whole; its other valid fields are
  not field-level quarantined or partially staged.
- Supplied-dataset regression totals are hard-coded in the test suite by
  design (a regression lock); they must be revisited if `data/` changes.
- Revenue assumes order lines are complete per order; partial-line extracts
  are out of scope and undetectable from the sources.
