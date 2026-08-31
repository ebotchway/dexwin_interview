"""Retail order and fulfilment reporting pipeline.

Python owns loading, validation, staging, and export. SQLite SQL in
sql/transformations.sql owns versioning, facts, and the reporting tables.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

ASSESSMENT_TS = "2026-07-15T09:00:00Z"
REPO_ROOT = Path(__file__).resolve().parent.parent
SQL_PATH = REPO_ROOT / "sql" / "transformations.sql"

ORDER_COLUMNS = (
    "order_id",
    "store_id",
    "customer_id",
    "ordered_at",
    "promised_delivery_at",
)
LINE_COLUMNS = (
    "line_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_amount",
    "updated_at",
)
STATUS_COLUMNS = ("event_id", "order_id", "status", "event_ts", "updated_at")
PRODUCT_COLUMNS = ("product_id", "category", "unit_cost")
DELIVERY_COLUMNS = ("event_id", "order_id", "event_type", "event_ts", "updated_at")

ALLOWED_STATUSES = frozenset({"CREATED", "PROCESSING", "COMPLETED", "CANCELLED"})
ALLOWED_DELIVERY_TYPES = frozenset({"DISPATCHED", "DELIVERED"})

REVENUE_COLUMNS = (
    "event_date",
    "order_id",
    "store_id",
    "event_type",
    "net_revenue",
    "known_gross_profit",
)
METRICS_COLUMNS = (
    "metric_date",
    "store_id",
    "net_revenue",
    "known_gross_profit",
    "completed_orders",
    "reversed_orders",
    "delivered_orders",
    "on_time_deliveries",
    "on_time_delivery_rate",
)
ALERT_COLUMNS = (
    "order_id",
    "store_id",
    "promised_delivery_at",
    "delivered_at",
    "reason",
)


class PipelineError(Exception):
    """Fatal input or schema problem; stop the run."""


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def require_columns(source: str, present: Iterable[str], required: Sequence[str]) -> None:
    missing = [c for c in required if c not in present]
    if missing:
        raise PipelineError(f"{source}: missing required columns: {', '.join(missing)}")


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file():
        raise PipelineError(f"source file not found: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise PipelineError(f"{path.name}: missing header row")
        rows = [{k: (v if v is not None else "") for k, v in row.items()} for row in reader]
    return list(reader.fieldnames), rows


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise PipelineError(f"source file not found: {path}")
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise PipelineError(f"{path.name} line {line_no}: invalid JSON ({exc})") from exc
            if not isinstance(payload, dict):
                raise PipelineError(f"{path.name} line {line_no}: expected a JSON object")
            records.append(payload)
    return records


def validate_timestamp(source: str, field: str, row: Mapping[str, Any], rejected: list[str]) -> bool:
    value = str(row.get(field, "")).strip()
    if is_blank(value):
        rejected.append(f"{source}: missing {field}")
        return False
    try:
        parse_timestamp(value)
    except ValueError:
        rejected.append(f"{source}: invalid {field}")
        return False
    return True


def validate_number(source: str, field: str, row: Mapping[str, Any], rejected: list[str]) -> bool:
    value = str(row.get(field, "")).strip()
    if is_blank(value):
        rejected.append(f"{source}: missing {field}")
        return False
    try:
        float(value)
    except ValueError:
        rejected.append(f"{source}: invalid {field}")
        return False
    return True


def require_id(source: str, field: str, row: Mapping[str, Any], rejected: list[str]) -> bool:
    if is_blank(row.get(field)):
        rejected.append(f"{source}: missing {field}")
        return False
    return True


def validate_orders(rows: list[dict[str, str]], rejected: list[str]) -> list[dict[str, str]]:
    kept: list[dict[str, str]] = []
    for row in rows:
        ok = True
        for field in ("order_id", "store_id"):
            ok = require_id("orders", field, row, rejected) and ok
        ok = validate_timestamp("orders", "ordered_at", row, rejected) and ok
        ok = validate_timestamp("orders", "promised_delivery_at", row, rejected) and ok
        if ok:
            kept.append(row)
    return kept


def validate_lines(rows: list[dict[str, str]], rejected: list[str]) -> list[dict[str, str]]:
    kept: list[dict[str, str]] = []
    for row in rows:
        ok = True
        for field in ("line_id", "order_id", "product_id"):
            ok = require_id("order_lines", field, row, rejected) and ok
        for field in ("quantity", "unit_price", "discount_amount"):
            ok = validate_number("order_lines", field, row, rejected) and ok
        ok = validate_timestamp("order_lines", "updated_at", row, rejected) and ok
        if ok:
            kept.append(row)
    return kept


def validate_status(rows: list[dict[str, str]], rejected: list[str]) -> list[dict[str, str]]:
    kept: list[dict[str, str]] = []
    for row in rows:
        ok = True
        for field in ("event_id", "order_id"):
            ok = require_id("order_status_events", field, row, rejected) and ok
        status = str(row.get("status", "")).strip()
        if status not in ALLOWED_STATUSES:
            rejected.append("order_status_events: unrecognised status")
            ok = False
        ok = validate_timestamp("order_status_events", "event_ts", row, rejected) and ok
        ok = validate_timestamp("order_status_events", "updated_at", row, rejected) and ok
        if ok:
            kept.append(row)
    return kept


def validate_products(rows: list[dict[str, str]], rejected: list[str]) -> list[dict[str, str]]:
    kept: list[dict[str, str]] = []
    for row in rows:
        ok = require_id("products", "product_id", row, rejected)
        ok = validate_number("products", "unit_cost", row, rejected) and ok
        if ok:
            kept.append(row)
    return kept


def validate_deliveries(rows: list[dict[str, Any]], rejected: list[str]) -> list[dict[str, str]]:
    kept: list[dict[str, str]] = []
    for row in rows:
        normalised = {key: "" if row.get(key) is None else str(row.get(key)) for key in DELIVERY_COLUMNS}
        ok = True
        for field in ("event_id", "order_id"):
            ok = require_id("delivery_events", field, normalised, rejected) and ok
        event_type = normalised["event_type"].strip()
        if event_type not in ALLOWED_DELIVERY_TYPES:
            rejected.append("delivery_events: unrecognised event_type")
            ok = False
        ok = validate_timestamp("delivery_events", "event_ts", normalised, rejected) and ok
        ok = validate_timestamp("delivery_events", "updated_at", normalised, rejected) and ok
        if ok:
            kept.append(normalised)
    return kept


def create_staging(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE pipeline_config (
            assessment_ts TEXT NOT NULL
        );
        CREATE TABLE staging_orders (
            order_id TEXT,
            store_id TEXT,
            customer_id TEXT,
            ordered_at TEXT,
            promised_delivery_at TEXT
        );
        CREATE TABLE staging_order_lines (
            line_id TEXT,
            order_id TEXT,
            product_id TEXT,
            quantity TEXT,
            unit_price TEXT,
            discount_amount TEXT,
            updated_at TEXT
        );
        CREATE TABLE staging_order_status_events (
            event_id TEXT,
            order_id TEXT,
            status TEXT,
            event_ts TEXT,
            updated_at TEXT
        );
        CREATE TABLE staging_products (
            product_id TEXT,
            category TEXT,
            unit_cost TEXT
        );
        CREATE TABLE staging_delivery_events (
            event_id TEXT,
            order_id TEXT,
            event_type TEXT,
            event_ts TEXT,
            updated_at TEXT
        );
        """
    )


def insert_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Mapping[str, str]],
) -> None:
    placeholders = ", ".join("?" for _ in columns)
    col_sql = ", ".join(columns)
    payload = [tuple(row[c] for c in columns) for row in rows]
    conn.executemany(f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})", payload)


def format_money(value: Any) -> str:
    if value is None:
        return ""
    return f"{float(value):.2f}"


def format_rate(value: Any) -> str:
    if value is None:
        return ""
    return f"{float(value):.6f}"


def write_csv(path: Path, columns: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row[column] for column in columns})


def export_revenue(conn: sqlite3.Connection, path: Path) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT event_date, order_id, store_id, event_type, net_revenue, known_gross_profit
        FROM result_revenue_events
        ORDER BY event_date, order_id, event_type
        """
    )
    rows = []
    for event_date, order_id, store_id, event_type, net_revenue, known_gp in cursor:
        rows.append(
            {
                "event_date": event_date,
                "order_id": order_id,
                "store_id": store_id,
                "event_type": event_type,
                "net_revenue": format_money(net_revenue),
                "known_gross_profit": format_money(known_gp),
            }
        )
    write_csv(path, REVENUE_COLUMNS, rows)
    return rows


def export_metrics(conn: sqlite3.Connection, path: Path) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT
            metric_date,
            store_id,
            net_revenue,
            known_gross_profit,
            completed_orders,
            reversed_orders,
            delivered_orders,
            on_time_deliveries,
            on_time_delivery_rate
        FROM result_daily_store_metrics
        ORDER BY metric_date, store_id
        """
    )
    rows = []
    for rec in cursor:
        (
            metric_date,
            store_id,
            net_revenue,
            known_gp,
            completed,
            reversed_,
            delivered,
            on_time,
            rate,
        ) = rec
        rows.append(
            {
                "metric_date": metric_date,
                "store_id": store_id,
                "net_revenue": format_money(net_revenue),
                "known_gross_profit": format_money(known_gp),
                "completed_orders": int(completed),
                "reversed_orders": int(reversed_),
                "delivered_orders": int(delivered),
                "on_time_deliveries": int(on_time),
                "on_time_delivery_rate": format_rate(rate),
            }
        )
    write_csv(path, METRICS_COLUMNS, rows)
    return rows


def export_alerts(conn: sqlite3.Connection, path: Path) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT order_id, store_id, promised_delivery_at, delivered_at, reason
        FROM result_delayed_delivery_alerts
        ORDER BY order_id
        """
    )
    rows = []
    with path.open("w", encoding="utf-8") as handle:
        for order_id, store_id, promised, delivered_at, reason in cursor:
            record = {
                "order_id": order_id,
                "store_id": store_id,
                "promised_delivery_at": promised,
                "delivered_at": delivered_at,
                "reason": reason,
            }
            rows.append(record)
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")
    return rows


def build_quality_report(
    conn: sqlite3.Connection,
    rejected: Sequence[str],
    raw_counts: Mapping[str, int],
) -> dict[str, Any]:
    dq = {metric: value for metric, value in conn.execute("SELECT metric, value FROM result_dq_counts")}
    rejected_by_source = Counter(item.split(":", 1)[0] for item in rejected)
    duplicate_total = (
        dq["duplicate_order_lines"]
        + dq["duplicate_order_status_events"]
        + dq["duplicate_delivery_events"]
    )
    corrected_total = (
        dq["corrected_order_lines"]
        + dq["corrected_order_status_events"]
        + dq["corrected_delivery_events"]
    )
    quarantined_total = (
        dq["quarantined_orphan_order_lines"]
        + dq["quarantined_orphan_status_events"]
        + dq["quarantined_orphan_delivery_events"]
    )
    return {
        "assessment_ts": ASSESSMENT_TS,
        "rejected": {
            "total": len(rejected),
            "by_source": dict(sorted(rejected_by_source.items())),
            "reasons": sorted(rejected),
        },
        "quarantined": {
            "total": quarantined_total,
            "orphan_order_lines": dq["quarantined_orphan_order_lines"],
            "orphan_status_events": dq["quarantined_orphan_status_events"],
            "orphan_delivery_events": dq["quarantined_orphan_delivery_events"],
        },
        "duplicate": {
            "total_extra_rows": duplicate_total,
            "order_lines": dq["duplicate_order_lines"],
            "order_status_events": dq["duplicate_order_status_events"],
            "delivery_events": dq["duplicate_delivery_events"],
        },
        "corrected": {
            "total": corrected_total,
            "order_lines": dq["corrected_order_lines"],
            "order_status_events": dq["corrected_order_status_events"],
            "delivery_events": dq["corrected_delivery_events"],
        },
        "unmatched": {
            "order_lines_unknown_product": dq["unmatched_order_lines_unknown_product"],
        },
        "source_row_counts": dict(raw_counts),
        "authoritative_row_counts": {
            "orders": dq["source_orders"],
            "products": dq["source_products"],
            "order_lines": dq["authoritative_order_lines"],
            "order_status_events": dq["authoritative_status_events"],
            "delivery_events": dq["authoritative_delivery_events"],
        },
        "staged_row_counts": {
            "orders": dq["source_orders"],
            "products": dq["source_products"],
            "order_lines": dq["source_order_lines"],
            "order_status_events": dq["source_order_status_events"],
            "delivery_events": dq["source_delivery_events"],
        },
    }


def run(data_dir: Path, output_dir: Path) -> None:
    if not SQL_PATH.is_file():
        raise PipelineError(f"SQL file not found: {SQL_PATH}")

    order_fields, order_rows = read_csv(data_dir / "orders.csv")
    line_fields, line_rows = read_csv(data_dir / "order_lines.csv")
    status_fields, status_rows = read_csv(data_dir / "order_status_events.csv")
    product_fields, product_rows = read_csv(data_dir / "products.csv")
    delivery_rows = read_jsonl(data_dir / "delivery_events.jsonl")

    require_columns("orders.csv", order_fields, ORDER_COLUMNS)
    require_columns("order_lines.csv", line_fields, LINE_COLUMNS)
    require_columns("order_status_events.csv", status_fields, STATUS_COLUMNS)
    require_columns("products.csv", product_fields, PRODUCT_COLUMNS)
    if delivery_rows:
        require_columns("delivery_events.jsonl", delivery_rows[0].keys(), DELIVERY_COLUMNS)

    raw_counts = {
        "orders": len(order_rows),
        "order_lines": len(line_rows),
        "order_status_events": len(status_rows),
        "products": len(product_rows),
        "delivery_events": len(delivery_rows),
    }

    rejected: list[str] = []
    orders = validate_orders(order_rows, rejected)
    lines = validate_lines(line_rows, rejected)
    status = validate_status(status_rows, rejected)
    products = validate_products(product_rows, rejected)
    deliveries = validate_deliveries(delivery_rows, rejected)

    output_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(":memory:")
    try:
        create_staging(conn)
        conn.execute("INSERT INTO pipeline_config (assessment_ts) VALUES (?)", (ASSESSMENT_TS,))
        insert_rows(conn, "staging_orders", ORDER_COLUMNS, orders)
        insert_rows(conn, "staging_order_lines", LINE_COLUMNS, lines)
        insert_rows(conn, "staging_order_status_events", STATUS_COLUMNS, status)
        insert_rows(conn, "staging_products", PRODUCT_COLUMNS, products)
        insert_rows(conn, "staging_delivery_events", DELIVERY_COLUMNS, deliveries)
        conn.executescript(SQL_PATH.read_text(encoding="utf-8"))

        export_revenue(conn, output_dir / "revenue_events.csv")
        export_metrics(conn, output_dir / "daily_store_metrics.csv")
        export_alerts(conn, output_dir / "delayed_delivery_alerts.jsonl")
        report = build_quality_report(conn, rejected, raw_counts)
        (output_dir / "data_quality_report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Dexwin retail order and fulfilment pipeline")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    args = parser.parse_args()
    try:
        run(args.data_dir, args.output_dir)
    except PipelineError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
