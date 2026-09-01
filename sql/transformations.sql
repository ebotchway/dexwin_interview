-- Dexwin pipeline transformations (SQLite).
-- Staging tables and pipeline_config are created by src/pipeline.py.
-- All timestamps are ISO-8601 UTC text; lexicographic comparison is valid.
-- updated_at is the source version clock. Business dates use event_ts.

DROP TABLE IF EXISTS result_dq_counts;
DROP TABLE IF EXISTS result_daily_store_metrics;
DROP TABLE IF EXISTS result_delayed_delivery_alerts;
DROP TABLE IF EXISTS result_revenue_events;
DROP VIEW IF EXISTS v_asof_delivery;
DROP VIEW IF EXISTS v_delivery_facts;
DROP VIEW IF EXISTS v_order_revenue;
DROP VIEW IF EXISTS v_first_cancelled;
DROP VIEW IF EXISTS v_first_completed;
DROP VIEW IF EXISTS v_order_amounts;
DROP VIEW IF EXISTS v_line_facts;
DROP VIEW IF EXISTS v_current_delivery_events;
DROP VIEW IF EXISTS v_delivery_ranked;
DROP VIEW IF EXISTS v_current_status_events;
DROP VIEW IF EXISTS v_status_ranked;
DROP VIEW IF EXISTS v_current_order_lines;
DROP VIEW IF EXISTS v_order_lines_ranked;

-- ---------------------------------------------------------------------------
-- Raw / current versioned records
-- Latest updated_at (source-system version clock) wins; rowid breaks ties so
-- the first loaded row is kept when two rows share the same version
-- timestamp (exact duplicates). Business timestamps (event_ts, ordered_at)
-- are never used for version selection.
-- Versioning happens FIRST, everywhere; business validity checks
-- (event_ts >= ordered_at, known order) run only on the authoritative rows
-- below, so a corrected later version can repair an originally invalid event
-- without special-casing any order.
-- ---------------------------------------------------------------------------

CREATE VIEW v_order_lines_ranked AS
SELECT
    line_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    updated_at,
    ROW_NUMBER() OVER (
        PARTITION BY line_id
        ORDER BY updated_at DESC, rowid ASC
    ) AS version_rank
FROM staging_order_lines;

CREATE VIEW v_current_order_lines AS
SELECT
    line_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    updated_at
FROM v_order_lines_ranked
WHERE version_rank = 1;

CREATE VIEW v_status_ranked AS
SELECT
    event_id,
    order_id,
    status,
    event_ts,
    updated_at,
    ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY updated_at DESC, rowid ASC
    ) AS version_rank
FROM staging_order_status_events;

CREATE VIEW v_current_status_events AS
SELECT
    event_id,
    order_id,
    status,
    event_ts,
    updated_at
FROM v_status_ranked
WHERE version_rank = 1;

CREATE VIEW v_delivery_ranked AS
SELECT
    event_id,
    order_id,
    event_type,
    event_ts,
    updated_at,
    ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY updated_at DESC, rowid ASC
    ) AS version_rank
FROM staging_delivery_events;

CREATE VIEW v_current_delivery_events AS
SELECT
    event_id,
    order_id,
    event_type,
    event_ts,
    updated_at
FROM v_delivery_ranked
WHERE version_rank = 1;

-- ---------------------------------------------------------------------------
-- Line facts
-- Orphan lines (order_id not in orders) are excluded from facts.
-- known gross profit is NULL when unit_cost is unknown, never coerced to 0.
-- ---------------------------------------------------------------------------

CREATE VIEW v_line_facts AS
SELECT
    l.line_id,
    l.order_id,
    l.product_id,
    CAST(l.quantity AS REAL) AS quantity,
    CAST(l.unit_price AS REAL) AS unit_price,
    CAST(l.discount_amount AS REAL) AS discount_amount,
    CAST(l.quantity AS REAL) * CAST(l.unit_price AS REAL)
        - CAST(l.discount_amount AS REAL) AS line_value,
    CAST(p.unit_cost AS REAL) AS unit_cost,
    CASE
        WHEN p.product_id IS NOT NULL
        THEN CAST(l.quantity AS REAL) * CAST(l.unit_price AS REAL)
            - CAST(l.discount_amount AS REAL)
            - CAST(l.quantity AS REAL) * CAST(p.unit_cost AS REAL)
        ELSE NULL
    END AS line_known_gp
FROM v_current_order_lines AS l
INNER JOIN staging_orders AS o
    ON o.order_id = l.order_id
LEFT JOIN staging_products AS p
    ON p.product_id = l.product_id;

CREATE VIEW v_order_amounts AS
SELECT
    order_id,
    SUM(line_value) AS net_revenue,
    SUM(line_known_gp) AS known_gross_profit
FROM v_line_facts
GROUP BY order_id;

-- ---------------------------------------------------------------------------
-- Order revenue
-- First valid COMPLETED / CANCELLED by business event_ts, after versioning.
-- Valid means the event attaches to a known order and is not before ordered_at.
-- ---------------------------------------------------------------------------

CREATE VIEW v_first_completed AS
SELECT
    s.order_id,
    MIN(s.event_ts) AS completed_ts
FROM v_current_status_events AS s
INNER JOIN staging_orders AS o
    ON o.order_id = s.order_id
WHERE s.status = 'COMPLETED'
  AND s.event_ts >= o.ordered_at
GROUP BY s.order_id;

CREATE VIEW v_first_cancelled AS
SELECT
    s.order_id,
    MIN(s.event_ts) AS cancelled_ts
FROM v_current_status_events AS s
INNER JOIN staging_orders AS o
    ON o.order_id = s.order_id
WHERE s.status = 'CANCELLED'
  AND s.event_ts >= o.ordered_at
GROUP BY s.order_id;

CREATE VIEW v_order_revenue AS
SELECT
    o.order_id,
    o.store_id,
    a.net_revenue,
    a.known_gross_profit,
    c.completed_ts,
    x.cancelled_ts,
    CASE
        WHEN c.completed_ts IS NOT NULL
         AND (x.cancelled_ts IS NULL OR x.cancelled_ts > c.completed_ts)
        THEN 1
        ELSE 0
    END AS recognise_sale,
    CASE
        WHEN c.completed_ts IS NOT NULL
         AND x.cancelled_ts IS NOT NULL
         AND x.cancelled_ts > c.completed_ts
        THEN 1
        ELSE 0
    END AS recognise_reversal
FROM staging_orders AS o
LEFT JOIN v_order_amounts AS a
    ON a.order_id = o.order_id
LEFT JOIN v_first_completed AS c
    ON c.order_id = o.order_id
LEFT JOIN v_first_cancelled AS x
    ON x.order_id = o.order_id;

-- ---------------------------------------------------------------------------
-- Revenue events
-- Entire extract: no assessment-time cutoff. Recognition date = event_ts date.
-- ---------------------------------------------------------------------------

CREATE TABLE result_revenue_events AS
SELECT
    substr(r.completed_ts, 1, 10) AS event_date,
    r.order_id,
    r.store_id,
    'SALE' AS event_type,
    r.net_revenue,
    r.known_gross_profit
FROM v_order_revenue AS r
WHERE r.recognise_sale = 1
UNION ALL
SELECT
    substr(r.cancelled_ts, 1, 10) AS event_date,
    r.order_id,
    r.store_id,
    'REVERSAL' AS event_type,
    -r.net_revenue AS net_revenue,
    CASE
        WHEN r.known_gross_profit IS NULL THEN NULL
        ELSE -r.known_gross_profit
    END AS known_gross_profit
FROM v_order_revenue AS r
WHERE r.recognise_reversal = 1;

-- ---------------------------------------------------------------------------
-- Delivery facts
-- First valid DELIVERED event per known order. Orphans stay out of this view.
-- ---------------------------------------------------------------------------

CREATE VIEW v_delivery_facts AS
SELECT
    o.order_id,
    o.store_id,
    o.promised_delivery_at,
    d.delivered_at,
    CASE
        WHEN d.delivered_at <= o.promised_delivery_at THEN 1
        ELSE 0
    END AS is_on_time
FROM staging_orders AS o
INNER JOIN (
    SELECT
        s.order_id,
        MIN(s.event_ts) AS delivered_at
    FROM v_current_delivery_events AS s
    INNER JOIN staging_orders AS ord
        ON ord.order_id = s.order_id
    WHERE s.event_type = 'DELIVERED'
      AND s.event_ts >= ord.ordered_at
    GROUP BY s.order_id
) AS d
    ON d.order_id = o.order_id;

CREATE VIEW v_asof_delivery AS
SELECT
    s.order_id,
    MIN(s.event_ts) AS delivered_at
FROM v_current_delivery_events AS s
INNER JOIN staging_orders AS o
    ON o.order_id = s.order_id
CROSS JOIN pipeline_config AS cfg
WHERE s.event_type = 'DELIVERED'
  AND s.event_ts >= o.ordered_at
  AND s.event_ts <= cfg.assessment_ts
GROUP BY s.order_id;

-- ---------------------------------------------------------------------------
-- Delayed alerts
-- Assessment cutoff applies here only. Promise must already have passed.
-- Cancelled orders are not excluded.
-- ---------------------------------------------------------------------------

CREATE TABLE result_delayed_delivery_alerts AS
SELECT
    o.order_id,
    o.store_id,
    o.promised_delivery_at,
    d.delivered_at,
    CASE
        WHEN d.delivered_at IS NOT NULL
         AND d.delivered_at > o.promised_delivery_at
        THEN 'delivered_late'
        WHEN d.delivered_at IS NULL
         AND o.promised_delivery_at <= cfg.assessment_ts
        THEN 'not_delivered_by_promise'
        ELSE NULL
    END AS reason
FROM staging_orders AS o
CROSS JOIN pipeline_config AS cfg
LEFT JOIN v_asof_delivery AS d
    ON d.order_id = o.order_id
WHERE
    (
        d.delivered_at IS NOT NULL
        AND d.delivered_at > o.promised_delivery_at
    )
    OR (
        d.delivered_at IS NULL
        AND o.promised_delivery_at <= cfg.assessment_ts
    );

-- ---------------------------------------------------------------------------
-- Daily store metrics
-- Revenue side from recognised events; delivery side from first valid delivery
-- (full extract, not as-of). Rate is NULL when delivered_orders = 0.
-- ---------------------------------------------------------------------------

CREATE TABLE result_daily_store_metrics AS
WITH keys AS (
    SELECT event_date AS metric_date, store_id
    FROM result_revenue_events
    UNION
    SELECT substr(delivered_at, 1, 10) AS metric_date, store_id
    FROM v_delivery_facts
),
rev AS (
    SELECT
        event_date AS metric_date,
        store_id,
        SUM(net_revenue) AS net_revenue,
        SUM(known_gross_profit) AS known_gross_profit,
        SUM(CASE WHEN event_type = 'SALE' THEN 1 ELSE 0 END) AS completed_orders,
        SUM(CASE WHEN event_type = 'REVERSAL' THEN 1 ELSE 0 END) AS reversed_orders
    FROM result_revenue_events
    GROUP BY event_date, store_id
),
del AS (
    SELECT
        substr(delivered_at, 1, 10) AS metric_date,
        store_id,
        COUNT(*) AS delivered_orders,
        SUM(is_on_time) AS on_time_deliveries
    FROM v_delivery_facts
    GROUP BY substr(delivered_at, 1, 10), store_id
)
SELECT
    k.metric_date,
    k.store_id,
    CASE WHEN r.store_id IS NULL THEN 0 ELSE r.net_revenue END AS net_revenue,
    CASE WHEN r.store_id IS NULL THEN 0 ELSE r.known_gross_profit END AS known_gross_profit,
    COALESCE(r.completed_orders, 0) AS completed_orders,
    COALESCE(r.reversed_orders, 0) AS reversed_orders,
    COALESCE(d.delivered_orders, 0) AS delivered_orders,
    COALESCE(d.on_time_deliveries, 0) AS on_time_deliveries,
    CASE
        WHEN COALESCE(d.delivered_orders, 0) = 0 THEN NULL
        ELSE CAST(d.on_time_deliveries AS REAL)
            / CAST(d.delivered_orders AS REAL)
    END AS on_time_delivery_rate
FROM keys AS k
LEFT JOIN rev AS r
    ON r.metric_date = k.metric_date
   AND r.store_id = k.store_id
LEFT JOIN del AS d
    ON d.metric_date = k.metric_date
   AND d.store_id = k.store_id;

-- ---------------------------------------------------------------------------
-- Data-quality counts (generic; no planted IDs)
-- Duplicate extras: superseded rows that match the kept row on every column.
-- Corrected: superseded rows that differ from the kept row (later version).
-- ---------------------------------------------------------------------------

CREATE TABLE result_dq_counts AS
SELECT 'duplicate_order_lines' AS metric, COUNT(*) AS value
FROM v_order_lines_ranked AS extra
INNER JOIN v_current_order_lines AS kept
    ON kept.line_id = extra.line_id
WHERE extra.version_rank > 1
  AND extra.order_id = kept.order_id
  AND extra.product_id = kept.product_id
  AND extra.quantity = kept.quantity
  AND extra.unit_price = kept.unit_price
  AND extra.discount_amount = kept.discount_amount
  AND extra.updated_at = kept.updated_at
UNION ALL
SELECT 'corrected_order_lines', COUNT(*)
FROM v_order_lines_ranked AS extra
INNER JOIN v_current_order_lines AS kept
    ON kept.line_id = extra.line_id
WHERE extra.version_rank > 1
  AND NOT (
        extra.order_id = kept.order_id
    AND extra.product_id = kept.product_id
    AND extra.quantity = kept.quantity
    AND extra.unit_price = kept.unit_price
    AND extra.discount_amount = kept.discount_amount
    AND extra.updated_at = kept.updated_at
  )
UNION ALL
SELECT 'duplicate_order_status_events', COUNT(*)
FROM v_status_ranked AS extra
INNER JOIN v_current_status_events AS kept
    ON kept.event_id = extra.event_id
WHERE extra.version_rank > 1
  AND extra.order_id = kept.order_id
  AND extra.status = kept.status
  AND extra.event_ts = kept.event_ts
  AND extra.updated_at = kept.updated_at
UNION ALL
SELECT 'corrected_order_status_events', COUNT(*)
FROM v_status_ranked AS extra
INNER JOIN v_current_status_events AS kept
    ON kept.event_id = extra.event_id
WHERE extra.version_rank > 1
  AND NOT (
        extra.order_id = kept.order_id
    AND extra.status = kept.status
    AND extra.event_ts = kept.event_ts
    AND extra.updated_at = kept.updated_at
  )
UNION ALL
SELECT 'duplicate_delivery_events', COUNT(*)
FROM v_delivery_ranked AS extra
INNER JOIN v_current_delivery_events AS kept
    ON kept.event_id = extra.event_id
WHERE extra.version_rank > 1
  AND extra.order_id = kept.order_id
  AND extra.event_type = kept.event_type
  AND extra.event_ts = kept.event_ts
  AND extra.updated_at = kept.updated_at
UNION ALL
SELECT 'corrected_delivery_events', COUNT(*)
FROM v_delivery_ranked AS extra
INNER JOIN v_current_delivery_events AS kept
    ON kept.event_id = extra.event_id
WHERE extra.version_rank > 1
  AND NOT (
        extra.order_id = kept.order_id
    AND extra.event_type = kept.event_type
    AND extra.event_ts = kept.event_ts
    AND extra.updated_at = kept.updated_at
  )
UNION ALL
SELECT 'quarantined_orphan_order_lines', COUNT(*)
FROM v_current_order_lines AS l
LEFT JOIN staging_orders AS o ON o.order_id = l.order_id
WHERE o.order_id IS NULL
UNION ALL
SELECT 'quarantined_orphan_status_events', COUNT(*)
FROM v_current_status_events AS s
LEFT JOIN staging_orders AS o ON o.order_id = s.order_id
WHERE o.order_id IS NULL
UNION ALL
SELECT 'quarantined_orphan_delivery_events', COUNT(*)
FROM v_current_delivery_events AS d
LEFT JOIN staging_orders AS o ON o.order_id = d.order_id
WHERE o.order_id IS NULL
UNION ALL
SELECT 'unmatched_order_lines_unknown_product', COUNT(*)
FROM v_current_order_lines AS l
INNER JOIN staging_orders AS o ON o.order_id = l.order_id
LEFT JOIN staging_products AS p ON p.product_id = l.product_id
WHERE p.product_id IS NULL
UNION ALL
SELECT 'authoritative_order_lines', COUNT(*)
FROM v_current_order_lines
UNION ALL
SELECT 'authoritative_status_events', COUNT(*)
FROM v_current_status_events
UNION ALL
SELECT 'authoritative_delivery_events', COUNT(*)
FROM v_current_delivery_events
UNION ALL
SELECT 'source_orders', COUNT(*) FROM staging_orders
UNION ALL
SELECT 'source_products', COUNT(*) FROM staging_products
UNION ALL
SELECT 'source_order_lines', COUNT(*) FROM staging_order_lines
UNION ALL
SELECT 'source_order_status_events', COUNT(*) FROM staging_order_status_events
UNION ALL
SELECT 'source_delivery_events', COUNT(*) FROM staging_delivery_events;
