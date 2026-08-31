# Dexwin take-home: retail order and fulfilment analytics

## Context

Dexwin supports a retail distributor that receives order data, order-status
events, and delivery events from different operational systems. Management
needs trustworthy daily sales and fulfilment reporting. Operations also needs
an actionable file of delayed deliveries.

The current inputs contain duplicates, corrected events, late-arriving updates,
cancelled orders, missing product mappings, and an orphan delivery event.

## Time box and deadline

Please spend **no more than three hours** on this exercise. The submission
deadline will be supplied separately. We value clear prioritisation and
explicit trade-offs more than polish or extra features.

If you do not finish, submit what you have and explain what you would do next.

## Tooling

Use Python 3.10 or later and any free, locally runnable SQL engine. The starter
uses Python's built-in SQLite library, but you may replace it with DuckDB or
another free option. Do not use Power BI, Power Automate, Snowflake, Azure, or
any other paid account.

Documentation, internet search, and AI assistants are permitted. See the
required AI/tool-use note below.

## Source files

All timestamps are UTC and use ISO 8601 format.

| File | Grain |
|---|---|
| `data/orders.csv` | One row per order |
| `data/order_lines.csv` | One or more versions of an order line |
| `data/order_status_events.csv` | One or more versions of an order-status event |
| `data/products.csv` | One row per product |
| `data/delivery_events.jsonl` | One or more versions of a delivery event |

`updated_at` is the source-system version timestamp. When the same business
record identifier appears more than once, the row with the latest valid
`updated_at` is authoritative. Exact duplicate rows may also be present.

## Business rules

1. Deduplicate order lines by `line_id`, status events by `event_id`, and
   delivery events by `event_id`.
2. Line value is `quantity * unit_price - discount_amount`.
3. Recognise positive revenue on the first valid `COMPLETED` event for an
   order.
4. If that order is later `CANCELLED`, recognise an equal negative reversal on
   the cancellation date. An order cancelled before completion has no revenue.
5. An unknown `product_id` does not remove valid revenue. Its cost and gross
   profit are unknown and must be surfaced as a data-quality issue.
6. A delivery is on time when its first valid `DELIVERED` event is no later
   than the order's `promised_delivery_at`.
7. Use `2026-07-15T09:00:00Z` as the fixed assessment time. An order is delayed
   when it has no delivery by its promise time or when it was delivered late.
8. Orphan records must be quarantined or reported; do not silently attach or
   discard them.

## Required work

Build a small, reproducible pipeline that:

1. Loads and validates all source files.
2. Uses SQL for the principal transformations. Keep the SQL visible in
   `sql/` or clearly embedded and separated in the Python code.
3. Produces these files in `output/`:
   - `revenue_events.csv`: order-level positive and reversal events.
   - `daily_store_metrics.csv`: date and store-level revenue and fulfilment
     measures.
   - `delayed_delivery_alerts.jsonl`: one current alert per delayed order.
   - `data_quality_report.json`: rejected, quarantined, corrected, duplicate,
     and unmatched-record counts.
4. Includes automated tests for the most important business rules.
5. Can be run from a clean checkout using commands documented in your README.

You may add sensible output columns. At minimum:

- `revenue_events.csv`: `event_date`, `order_id`, `store_id`, `event_type`,
  `net_revenue`, `known_gross_profit`.
- `daily_store_metrics.csv`: `metric_date`, `store_id`, `net_revenue`,
  `known_gross_profit`, `completed_orders`, `reversed_orders`,
  `delivered_orders`, `on_time_deliveries`, `on_time_delivery_rate`.
- `delayed_delivery_alerts.jsonl`: `order_id`, `store_id`,
  `promised_delivery_at`, `delivered_at`, `reason`.

`known_gross_profit` should include only lines with a known product cost.
Explain this choice in the documentation.

## Design note

Complete `DESIGN_NOTES.md` in no more than 1,000 words. Cover:

- Assumptions and unresolved business questions.
- Data model, table grain, and metric definitions.
- How a BI tool would consume the result.
- A reliable trigger-to-alert automation design, including duplicate
  prevention, retries, failure handling, monitoring, and replay.
- How you would deploy, schedule, secure, monitor, and recover the pipeline in
  Dexwin's Microsoft-first environment.
- What you would change for 100 times the data volume.

You do not need to create a dashboard or a low-code flow.

## AI/tool-use note

In `DESIGN_NOTES.md`, name any AI or code-generation tools used, describe what
you used them for, explain how you verified their output, and identify at least
one suggestion you rejected or corrected. If you used none, state that.

You must be able to explain and modify every submitted component in a later
technical discussion.

## Submission

Submit a private Git repository containing the source code, SQL, tests,
documentation, and generated outputs. Do not commit secrets, virtual
environments, caches, or large generated databases.
