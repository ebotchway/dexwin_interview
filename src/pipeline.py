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
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

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


# A rejection is one (source, line number, message) triple. One malformed row
# can produce several messages; the report counts rejected rows and validation
# errors separately so "rejected" is never ambiguous.
Rejection = tuple[str, int, str]


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def require_columns(source: str, present: Iterable[str], required: Sequence[str]) -> None:
    missing = [c for c in required if c not in present]
    if missing:
        raise PipelineError(f"{source}: missing required columns: {', '.join(missing)}")


def read_csv(path: Path) -> tuple[list[str], list[tuple[int, dict[str, str]]]]:
    """Return (header, [(file line number, row)]); line 1 is the header."""
    if not path.is_file():
        raise PipelineError(f"source file not found: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise PipelineError(f"{path.name}: missing header row")
        rows = [
            (line_no, {k: (v if v is not None else "") for k, v in row.items()})
            for line_no, row in enumerate(reader, start=2)
        ]
    return list(reader.fieldnames), rows


def read_jsonl(path: Path) -> list[tuple[int, dict[str, Any]]]:
    """Return [(file line number, record)]; fatal error on malformed JSON."""
    if not path.is_file():
        raise PipelineError(f"source file not found: {path}")
    records: list[tuple[int, dict[str, Any]]] = []
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
            records.append((line_no, payload))
    return records


def check_id(row: Mapping[str, Any], field: str, errors: list[str]) -> None:
    if is_blank(row.get(field)):
        errors.append(f"missing {field}")


def check_timestamp(row: Mapping[str, Any], field: str, errors: list[str]) -> None:
    value = str(row.get(field, "")).strip()
    if is_blank(value):
        errors.append(f"missing {field}")
        return
    try:
        parse_timestamp(value)
    except ValueError:
        errors.append(f"invalid {field}")


def check_decimal(
    row: Mapping[str, Any],
    field: str,
    errors: list[str],
    *,
    non_negative: bool = False,
) -> None:
    """Strict numeric validation.

    float() would accept NaN/Infinity spellings; Decimal rejects odd
    literals (e.g. underscores) and is_finite() blocks NaN/+/-Inf.
    Monetary and quantity fields are non-negative in this domain because
    the assignment models corrections and cancellations through new versions
    and CANCELLED events, never through signed line amounts.
    """
    value = str(row.get(field, "")).strip()
    if is_blank(value):
        errors.append(f"missing {field}")
        return
    try:
        number = Decimal(value)
    except InvalidOperation:
        errors.append(f"invalid {field}")
        return
    if not number.is_finite():
        errors.append(f"non-finite {field}")
    elif non_negative and number < 0:
        errors.append(f"negative {field}")


def check_enum(row: Mapping[str, Any], field: str, allowed: frozenset[str], errors: list[str]) -> None:
    if str(row.get(field, "")).strip() not in allowed:
        errors.append(f"unrecognised {field}")


def check_order_row(row: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    check_id(row, "order_id", errors)
    check_id(row, "store_id", errors)
    check_timestamp(row, "ordered_at", errors)
    check_timestamp(row, "promised_delivery_at", errors)
    return errors


def check_line_row(row: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    check_id(row, "line_id", errors)
    check_id(row, "order_id", errors)
    check_id(row, "product_id", errors)
    for field in ("quantity", "unit_price", "discount_amount"):
        check_decimal(row, field, errors, non_negative=True)
    check_timestamp(row, "updated_at", errors)
    return errors


def check_status_row(row: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    check_id(row, "event_id", errors)
    check_id(row, "order_id", errors)
    check_enum(row, "status", ALLOWED_STATUSES, errors)
    check_timestamp(row, "event_ts", errors)
    check_timestamp(row, "updated_at", errors)
    return errors


def check_product_row(row: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    check_id(row, "product_id", errors)
    check_decimal(row, "unit_cost", errors, non_negative=True)
    return errors


def check_delivery_row(row: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    # Every record must independently carry the expected object structure;
    # only the first record used to be schema-checked.
    missing_keys = [key for key in DELIVERY_COLUMNS if key not in row]
    if missing_keys:
        errors.append(f"missing required field(s): {', '.join(missing_keys)}")
    check_id(row, "event_id", errors)
    check_id(row, "order_id", errors)
    check_enum(row, "event_type", ALLOWED_DELIVERY_TYPES, errors)
    check_timestamp(row, "event_ts", errors)
    check_timestamp(row, "updated_at", errors)
    return errors


def validate_records(
    source: str,
    rows: Sequence[tuple[int, Mapping[str, Any]]],
    check: Any,
    rejected: list[Rejection],
) -> list[dict[str, Any]]:
    """Keep structurally valid rows; record every failed row once per error.

    Rejections here are data-quality findings (bad timestamps, unknown
    status words, missing ids) and are reported, not fatal. Fatal problems
    (missing files, missing CSV columns, malformed JSON) raise upstream.
    """
    kept: list[dict[str, Any]] = []
    for line_no, row in rows:
        errors = check(row)
        if errors:
            rejected.extend((source, line_no, message) for message in errors)
        else:
            kept.append(dict(row))
    return kept


def normalise_rows(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> list[dict[str, str]]:
    return [
        {key: "" if row.get(key) is None else str(row.get(key)).strip() for key in columns}
        for row in rows
    ]


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
    rejected: Sequence[Rejection],
    raw_counts: Mapping[str, int],
) -> dict[str, Any]:
    dq = {metric: value for metric, value in conn.execute("SELECT metric, value FROM result_dq_counts")}
    rejected_rows = sorted({(source, line_no) for source, line_no, _ in rejected})
    rejected_by_source = Counter(source for source, _ in rejected_rows)
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
            "rows": len(rejected_rows),
            "validation_errors": len(rejected),
            "by_source": dict(sorted(rejected_by_source.items())),
            "errors": [
                {"source": source, "line": line_no, "message": message}
                for source, line_no, message in sorted(rejected)
            ],
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
    # delivery_events.jsonl has no header; its schema is checked per record.

    raw_counts = {
        "orders": len(order_rows),
        "order_lines": len(line_rows),
        "order_status_events": len(status_rows),
        "products": len(product_rows),
        "delivery_events": len(delivery_rows),
    }

    rejected: list[Rejection] = []
    orders = validate_records("orders", order_rows, check_order_row, rejected)
    lines = validate_records("order_lines", line_rows, check_line_row, rejected)
    status = validate_records("order_status_events", status_rows, check_status_row, rejected)
    products = validate_records("products", product_rows, check_product_row, rejected)
    deliveries = normalise_rows(
        validate_records("delivery_events", delivery_rows, check_delivery_row, rejected),
        DELIVERY_COLUMNS,
    )

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
