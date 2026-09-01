# Design notes

## Assumptions and business questions

**Version time vs business time.** `updated_at` is the source-system version
clock; the row with the latest `updated_at` per business key (`line_id`,
`event_id`) is authoritative. `event_ts` is when the business event happened
and drives revenue recognition and delivery timing. Version selection happens
first (SQL window functions); validity checks (`event_ts >= ordered_at`,
known order) run only on authoritative rows, so a corrected later version can
repair an originally invalid event without special-casing any order. Using
`updated_at` as a business date would wrongly recognise revenue on correction
days.


**Revenue lifecycle.** The first valid `COMPLETED` event creates one SALE; a
later `CANCELLED` event creates an equal REVERSAL on its own `event_ts` date;
cancellation before completion yields no revenue. The revenue extract covers
the whole extract with no assessment cutoff — that is why even the 16:00
cancellation on the assessment day itself still produces its reversal, while
`2026-07-15T09:00:00Z` is used only to evaluate current delivery delay, not as
a global revenue cutoff.

**Unknown products** keep their line revenue; cost and gross profit stay NULL
(never coerced to 0) and the line is reported as unmatched.

**Open questions.** Cancelled orders past promise with no delivery are
currently alerted as delayed because the brief does not exclude them; I would
confirm with operations. Conflicting versions with identical `updated_at` fall
back to first-loaded-wins, safe only for exact duplicates; production needs a
source version sequence. Duplicate-vs-corrected classification compares raw
representations, so `3` vs `3.0` counts as a correction; production should use
canonical typed comparison.

## Data model and metrics

Staging orders and products remain one row per order/product after Python
validation; current-version views (`v_current_*`) hold one authoritative
line/status/delivery row per business key. `v_line_facts` joins current lines
to known orders and products at line grain. `result_revenue_events` is
order-level event grain: one row per recognised SALE/REVERSAL.
`result_daily_store_metrics` aggregates to metric_date x store_id, including
delivery-only and revenue-only keys. `result_delayed_delivery_alerts` is one
row per currently delayed order. `result_dq_counts` reports duplicates,
corrections, quarantined orphans, unmatched products, and row counts; Python
merges these with validation rejections.

Definitions: `line_value = quantity * unit_price - discount_amount`;
`net_revenue` = sum of line values for the event's order (negative on
reversal); `known_gross_profit` = line value minus `quantity * unit_cost`,
summed only over lines with a known cost, NULL if none; `completed_orders` /
`reversed_orders` = SALE / REVERSAL event counts; `delivered_orders` = orders
whose first valid `DELIVERED` event falls on that date (full extract);
`on_time_deliveries` = those with `delivered_at <= promised_delivery_at`
(boundary inclusive); rate = on-time / delivered, NULL when no deliveries.
Revenue events stay at event grain so any daily figure can be audited and
traced to source; daily metrics exist only to make BI aggregation trivial and
consistent.

## BI consumption

Power BI would import all four outputs. `daily_store_metrics` is the primary
management dataset (net revenue, known GP, on-time rate by date x store).
`revenue_events` supports drill-through and reversal auditing.
`delayed_delivery_alerts` is an operational list for store teams, not a
history table. `data_quality_report.json` feeds a pipeline-monitoring page.
Shared dimensions: date and store. All business logic stays upstream in the
pipeline so KPIs cannot drift between reports.

## Automation, reliability, and recovery

The take-home runs locally: files in, SQLite in-memory transforms, files out.
The following is the proposed production design.

Flow: source arrival or schedule trigger -> ingestion (schema/format check)
-> validation -> staging -> SQL transforms -> data-quality gates -> atomic
publication (write then rename/merge so consumers never see partial files)
-> alert generation.

Idempotency: batches keyed by source file/batch id; reruns deterministically
overwrite the same partition. Alert idempotency uses the stable business key
`(order_id, assessment_window)` with an alert state store, so retries update or
acknowledge one alert per delayed order instead of duplicating it. Retries with
backoff for transient failures; permanent failures (schema mismatch, malformed
JSON) fail fast and route records to a dead-letter/quarantine location,
mirroring the take-home's fatal-versus-reported distinction. Structured logs
carry batch id and row counts; metrics track duration, rows in/out, rejection
and quarantine rates; a health alert fires on schedule misses, rising
rejections, or publication failure. Replay is safe from retained immutable
source files.

## Microsoft-first production mapping and scale

Proposed mapping (not used in this take-home): source and curated files in
Azure Data Lake Storage Gen2; execution orchestrated by Azure Data Factory
(or Fabric pipelines); the Python job containerised on Azure Container Apps;
transforms in Azure SQL Database or a Fabric warehouse depending on the
existing platform; Power BI for reporting; Microsoft Entra ID plus managed
identities for passwordless least-privilege access; Azure Key Vault for
secrets; Azure Monitor/Application Insights for observability. Encryption in
transit and at rest, audit-logged runs and publications, and backups with
immutable raw retention provide recovery.

At 100x volume, SQLite full reload is no longer suitable. Changes: incremental
ingestion keyed on arrival windows; partitioning analytical tables by business
date with indexes and partition pruning; columnar formats (Parquet) and
engines (DuckDB locally, or a managed warehouse/Fabric in Azure) for scan
efficiency; bulk loading (PolyBase/COPY INTO); incremental merges into
versioned and fact tables instead of re-deriving history; orchestrated
parallel runs per date/store partition with per-partition atomic publication;
raw-data retention tiering; and throughput/latency monitoring with
volume-anomaly alerts. No technology beyond what scale requires.

## AI and tool use

Cursor's AI coding assistant was used for implementation assistance, while
ChatGPT was used for code review and testing support.
The work relied on the assignment brief and the Python standard library.
All suggestions were reviewed against the brief and verified by running
the supplied dataset and the automated test suite (`python3 -m unittest discover -s tests`).

Rejected/corrected suggestions: adding pandas/DuckDB was rejected because
stdlib + SQLite met the brief with fewer reproducibility risks. Two real
defects were found and fixed during a review pass before tests were written:
`float()`-based numeric validation accepted `NaN`/`Infinity` (corrected to
`Decimal` with an `is_finite()` check), and JSONL schema checking inspected
only the first record (corrected to per-record validation).
