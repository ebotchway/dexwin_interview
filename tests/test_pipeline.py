"""Standard-library unittest suite for the Dexwin pipeline.

Tests exercise the public interface (pipeline.run and the CLI) against
synthetic fixtures in temporary directories, then assert on the documented
outputs only: revenue_events.csv, daily_store_metrics.csv,
delayed_delivery_alerts.jsonl and data_quality_report.json.
"""

from __future__ import annotations

import csv
import json
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

import pipeline  # noqa: E402

# Documented output schemas (from the assignment, not internals).
ORDER_COLUMNS = ["order_id", "store_id", "customer_id", "ordered_at", "promised_delivery_at"]
LINE_COLUMNS = ["line_id", "order_id", "product_id", "quantity", "unit_price",
                "discount_amount", "updated_at"]
STATUS_COLUMNS = ["event_id", "order_id", "status", "event_ts", "updated_at"]
PRODUCT_COLUMNS = ["product_id", "category", "unit_cost"]
DELIVERY_COLUMNS = ["event_id", "order_id", "event_type", "event_ts", "updated_at"]
REVENUE_COLUMNS = ["event_date", "order_id", "store_id", "event_type",
                   "net_revenue", "known_gross_profit"]
METRICS_COLUMNS = ["metric_date", "store_id", "net_revenue", "known_gross_profit",
                   "completed_orders", "reversed_orders", "delivered_orders",
                   "on_time_deliveries", "on_time_delivery_rate"]
ALERT_KEYS = ["order_id", "store_id", "promised_delivery_at", "delivered_at", "reason"]

MONEY_RE = re.compile(r"^-?\d+\.\d{2}$")


# --- fixture builders -------------------------------------------------------

def order_row(order_id="O1", store_id="S1", ordered_at="2026-07-01T00:00:00Z",
              promised_delivery_at="2026-07-05T00:00:00Z"):
    return {"order_id": order_id, "store_id": store_id, "customer_id": "C-" + order_id,
            "ordered_at": ordered_at, "promised_delivery_at": promised_delivery_at}


def line_row(line_id="L1", order_id="O1", product_id="P1", quantity="2",
             unit_price="10.00", discount_amount="0.00", updated_at="2026-07-01T01:00:00Z"):
    return {"line_id": line_id, "order_id": order_id, "product_id": product_id,
            "quantity": quantity, "unit_price": unit_price,
            "discount_amount": discount_amount, "updated_at": updated_at}


def status_row(event_id, order_id, status, event_ts, updated_at=None):
    return {"event_id": event_id, "order_id": order_id, "status": status,
            "event_ts": event_ts, "updated_at": updated_at or event_ts}


def product_row(product_id="P1", unit_cost="2.00", category="Test"):
    return {"product_id": product_id, "category": category, "unit_cost": unit_cost}


def delivery_row(event_id, order_id, event_type, event_ts, updated_at=None):
    return {"event_id": event_id, "order_id": order_id, "event_type": event_type,
            "event_ts": event_ts, "updated_at": updated_at or event_ts}


def write_dataset(root: Path, orders=(), lines=(), statuses=(), products=(),
                  deliveries=(), deliveries_raw=None):
    root.mkdir(parents=True, exist_ok=True)

    def write_csv(name, columns, rows):
        with (root / name).open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)

    write_csv("orders.csv", ORDER_COLUMNS, orders)
    write_csv("order_lines.csv", LINE_COLUMNS, lines)
    write_csv("order_status_events.csv", STATUS_COLUMNS, statuses)
    write_csv("products.csv", PRODUCT_COLUMNS, products)
    jsonl = (root / "delivery_events.jsonl")
    if deliveries_raw is not None:
        jsonl.write_text(deliveries_raw, encoding="utf-8")
    else:
        jsonl.write_text("".join(json.dumps(d) + "\n" for d in deliveries), encoding="utf-8")


# --- parsed outputs ----------------------------------------------------------

class PipelineResult:
    """Reads the four documented output files from one pipeline run."""

    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.revenue = self._read_csv(out_dir / "revenue_events.csv")
        self.metrics = self._read_csv(out_dir / "daily_store_metrics.csv")
        alert_lines = (out_dir / "delayed_delivery_alerts.jsonl").read_text(
            encoding="utf-8").splitlines()
        self.alerts = [json.loads(line) for line in alert_lines if line.strip()]
        self.report = json.loads(
            (out_dir / "data_quality_report.json").read_text(encoding="utf-8"))

    @staticmethod
    def _read_csv(path: Path):
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    @property
    def sales(self):
        return [r for r in self.revenue if r["event_type"] == "SALE"]

    @property
    def reversals(self):
        return [r for r in self.revenue if r["event_type"] == "REVERSAL"]

    @property
    def net_revenue(self):
        return round(sum(float(r["net_revenue"]) for r in self.revenue), 2)

    @property
    def known_gross_profit(self):
        values = [float(r["known_gross_profit"]) for r in self.revenue
                  if r["known_gross_profit"] != ""]
        return round(sum(values), 2)

    @property
    def delivered_total(self):
        return sum(int(r["delivered_orders"]) for r in self.metrics)

    @property
    def on_time_total(self):
        return sum(int(r["on_time_deliveries"]) for r in self.metrics)

    @property
    def rejected_rows(self):
        return self.report["rejected"]["rows"]


class PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="dexwin-tests-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def run_pipeline(self, case, *, orders=(), lines=(), statuses=(), products=(),
                     deliveries=(), deliveries_raw=None):
        data_dir = self.tmp / case / "data"
        out_dir = self.tmp / case / "out"
        write_dataset(data_dir, orders=orders, lines=lines, statuses=statuses,
                      products=products, deliveries=deliveries,
                      deliveries_raw=deliveries_raw)
        pipeline.run(data_dir, out_dir)  # also verifies the output dir is auto-created
        return PipelineResult(out_dir)


# --- 1 + 20: supplied dataset, via the real CLI ------------------------------

class SuppliedDatasetTests(unittest.TestCase):
    def test_supplied_dataset_end_to_end(self):
        tmp = Path(tempfile.mkdtemp(prefix="dexwin-cli-"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        out_dir = tmp / "output"
        proc = subprocess.run(
            [sys.executable, str(REPO_ROOT / "src" / "pipeline.py"),
             "--data-dir", str(REPO_ROOT / "data"),
             "--output-dir", str(out_dir)],
            capture_output=True, text=True, cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        result = PipelineResult(out_dir)

        # Documented regression expectations for the supplied dataset.
        self.assertEqual(len(result.revenue), 32)
        self.assertEqual(len(result.sales), 26)
        self.assertEqual(len(result.reversals), 6)
        self.assertEqual(result.net_revenue, 1675.00)
        self.assertEqual(result.known_gross_profit, 799.50)
        self.assertEqual(result.delivered_total, 26)
        self.assertEqual(result.on_time_total, 20)
        self.assertEqual(len(result.alerts), 9)
        self.assertEqual(result.report["duplicate"]["total_extra_rows"], 3)
        self.assertEqual(result.report["corrected"]["total"], 3)
        self.assertEqual(result.report["quarantined"]["orphan_delivery_events"], 1)
        self.assertEqual(result.report["unmatched"]["order_lines_unknown_product"], 1)
        self.assertEqual(result.rejected_rows, 0)

    def test_output_formats_on_supplied_dataset(self):
        tmp = Path(tempfile.mkdtemp(prefix="dexwin-fmt-"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        out_dir = tmp / "output"
        pipeline.run(REPO_ROOT / "data", out_dir)
        result = PipelineResult(out_dir)

        for name, expected in [("revenue_events.csv", REVENUE_COLUMNS),
                               ("daily_store_metrics.csv", METRICS_COLUMNS)]:
            with (out_dir / name).open(newline="", encoding="utf-8") as handle:
                header = next(csv.reader(handle))
            self.assertEqual(header, expected, msg=name)

        for row in result.revenue:
            self.assertRegex(row["net_revenue"], MONEY_RE)
            self.assertTrue(row["known_gross_profit"] == ""
                            or re.match(MONEY_RE, row["known_gross_profit"]))
            self.assertRegex(row["event_date"], r"^\d{4}-\d{2}-\d{2}$")
        for row in result.metrics:
            rate = row["on_time_delivery_rate"]
            self.assertTrue(rate == "" or re.match(r"^\d+\.\d{6}$", rate))

        alert_text = (out_dir / "delayed_delivery_alerts.jsonl").read_text(encoding="utf-8")
        lines = [line for line in alert_text.splitlines() if line.strip()]
        self.assertGreater(len(lines), 0)
        for line in lines:
            record = json.loads(line)
            self.assertEqual(sorted(record), sorted(ALERT_KEYS))
        self.assertTrue(alert_text.endswith("\n"))

        for key in ("rejected", "quarantined", "duplicate", "corrected", "unmatched"):
            self.assertIn(key, result.report)


# --- 2-6: versioning and revenue lifecycle -----------------------------------

class VersioningAndRevenueTests(PipelineTestCase):
    def test_exact_duplicate_line_does_not_double_count(self):
        # Behaviour 2: identical versions of one line count once.
        row = line_row()
        result = self.run_pipeline(
            "dup_line",
            orders=[order_row()],
            lines=[row, dict(row)],
            statuses=[status_row("SE1", "O1", "COMPLETED", "2026-07-02T00:00:00Z")],
            products=[product_row()],
        )
        self.assertEqual(len(result.sales), 1)
        self.assertEqual(result.sales[0]["net_revenue"], "20.00")
        self.assertEqual(result.report["duplicate"]["order_lines"], 1)
        self.assertEqual(result.report["corrected"]["order_lines"], 0)

    def test_latest_order_line_version_wins(self):
        # Behaviour 3: the version with the latest updated_at is authoritative.
        result = self.run_pipeline(
            "line_versions",
            orders=[order_row()],
            lines=[
                line_row(quantity="2", updated_at="2026-07-01T02:00:00Z"),
                line_row(quantity="5", updated_at="2026-07-02T02:00:00Z"),
            ],
            statuses=[status_row("SE1", "O1", "COMPLETED", "2026-07-02T00:00:00Z")],
            products=[product_row()],
        )
        self.assertEqual(result.net_revenue, 50.00)      # 5 * 10.00, not 2 * 10.00
        self.assertEqual(result.known_gross_profit, 40.00)  # 50 - 5 * 2.00
        self.assertEqual(result.report["corrected"]["order_lines"], 1)

    def test_latest_status_event_version_wins(self):
        # Behaviour 4: version selection uses updated_at, and corrected later
        # versions are handled generically (no special-casing any order).
        # O1: same event_id corrected to a later business timestamp -> SALE on
        # the corrected event_ts date.
        # O2: first version has an invalid business timestamp (before
        # ordered_at); the authoritative later version is valid -> SALE.
        result = self.run_pipeline(
            "status_versions",
            orders=[order_row("O1"), order_row("O2")],
            lines=[line_row("L1", "O1"), line_row("L2", "O2", quantity="1",
                                                  unit_price="5.00")],
            statuses=[
                status_row("SE-A", "O1", "COMPLETED", "2026-07-02T00:00:00Z",
                           updated_at="2026-07-02T01:00:00Z"),
                status_row("SE-A", "O1", "COMPLETED", "2026-07-03T00:00:00Z",
                           updated_at="2026-07-03T01:00:00Z"),
                status_row("SE-B", "O2", "COMPLETED", "2026-06-01T00:00:00Z",
                           updated_at="2026-06-01T01:00:00Z"),
                status_row("SE-B", "O2", "COMPLETED", "2026-07-04T00:00:00Z",
                           updated_at="2026-07-05T01:00:00Z"),
            ],
            products=[product_row()],
        )
        self.assertEqual(len(result.sales), 2)
        dates = {r["order_id"]: r["event_date"] for r in result.sales}
        self.assertEqual(dates, {"O1": "2026-07-03", "O2": "2026-07-04"})
        self.assertEqual(result.report["corrected"]["order_status_events"], 2)

    def test_first_valid_completed_creates_single_sale(self):
        # Behaviour 6: two valid COMPLETED events still produce one SALE,
        # dated on the first one.
        result = self.run_pipeline(
            "first_completed",
            orders=[order_row()],
            lines=[line_row()],
            statuses=[
                status_row("SE1", "O1", "COMPLETED", "2026-07-03T00:00:00Z"),
                status_row("SE2", "O1", "COMPLETED", "2026-07-02T00:00:00Z"),
            ],
            products=[product_row()],
        )
        self.assertEqual(len(result.sales), 1)
        self.assertEqual(result.sales[0]["event_date"], "2026-07-02")
        self.assertEqual(result.report["corrected"]["total"], 0)

    def test_completed_then_cancelled_creates_equal_reversal(self):
        # Behaviour 7: cancellation after completion reverses the exact
        # revenue and known gross profit on the cancellation date.
        result = self.run_pipeline(
            "sale_reversal",
            orders=[order_row()],
            lines=[line_row()],
            statuses=[
                status_row("SE1", "O1", "COMPLETED", "2026-07-02T00:00:00Z"),
                status_row("SE2", "O1", "CANCELLED", "2026-07-08T00:00:00Z"),
            ],
            products=[product_row()],
        )
        self.assertEqual(len(result.sales), 1)
        self.assertEqual(len(result.reversals), 1)
        self.assertEqual(result.reversals[0]["event_date"], "2026-07-08")
        self.assertEqual(result.reversals[0]["net_revenue"], "-20.00")
        self.assertEqual(result.reversals[0]["known_gross_profit"], "-16.00")
        self.assertEqual(result.net_revenue, 0.0)
        self.assertEqual(result.known_gross_profit, 0.0)

    def test_cancelled_before_completed_creates_no_revenue(self):
        # Behaviour 8.
        result = self.run_pipeline(
            "cancel_first",
            orders=[order_row()],
            lines=[line_row()],
            statuses=[
                status_row("SE1", "O1", "CANCELLED", "2026-07-02T00:00:00Z"),
                status_row("SE2", "O1", "COMPLETED", "2026-07-03T00:00:00Z"),
            ],
            products=[product_row()],
        )
        self.assertEqual(result.revenue, [])
        self.assertEqual(result.net_revenue, 0.0)

    def test_processing_only_order_creates_no_revenue(self):
        # Behaviour 9.
        result = self.run_pipeline(
            "processing_only",
            orders=[order_row()],
            lines=[line_row()],
            statuses=[
                status_row("SE1", "O1", "CREATED", "2026-07-01T00:00:00Z"),
                status_row("SE2", "O1", "PROCESSING", "2026-07-02T00:00:00Z"),
            ],
            products=[product_row()],
        )
        self.assertEqual(result.revenue, [])

    def test_unknown_product_keeps_revenue_but_excludes_cost(self):
        # Behaviour 10: unknown product contributes revenue, its gross profit
        # is unknown (excluded), and the line is reported as unmatched.
        result = self.run_pipeline(
            "unknown_product",
            orders=[order_row()],
            lines=[
                line_row("L1", product_id="P1", quantity="2", unit_price="10.00"),
                line_row("L2", product_id="P999", quantity="1", unit_price="50.00"),
            ],
            statuses=[status_row("SE1", "O1", "COMPLETED", "2026-07-02T00:00:00Z")],
            products=[product_row("P1")],
        )
        self.assertEqual(len(result.sales), 1)
        self.assertEqual(result.sales[0]["net_revenue"], "70.00")
        self.assertEqual(result.sales[0]["known_gross_profit"], "16.00")
        self.assertEqual(result.report["unmatched"]["order_lines_unknown_product"], 1)
        self.assertEqual(result.rejected_rows, 0)


# --- 5, 11-16: delivery and alert rules ---------------------------------------

class DeliveryAndAlertTests(PipelineTestCase):
    def _completed(self):
        return ([order_row()], [line_row()],
                [status_row("SE1", "O1", "COMPLETED", "2026-07-02T00:00:00Z")],
                [product_row()])

    def test_latest_delivery_event_version_wins(self):
        # Behaviour 5: the authoritative DELIVERED timestamp comes from the
        # version with the latest updated_at, turning a late delivery on time.
        orders, lines, statuses, products = self._completed()
        result = self.run_pipeline(
            "delivery_versions",
            orders=orders, lines=lines, statuses=statuses, products=products,
            deliveries=[
                delivery_row("DE1", "O1", "DELIVERED", "2026-07-06T00:00:00Z",
                             updated_at="2026-07-06T01:00:00Z"),
                delivery_row("DE1", "O1", "DELIVERED", "2026-07-04T00:00:00Z",
                             updated_at="2026-07-07T01:00:00Z"),
            ],
        )
        self.assertEqual(result.delivered_total, 1)
        self.assertEqual(result.on_time_total, 1)
        self.assertEqual(result.alerts, [])
        self.assertEqual(result.report["corrected"]["delivery_events"], 1)

    def test_orphan_delivery_is_quarantined_and_not_attached(self):
        # Behaviour 11.
        orders, lines, statuses, products = self._completed()
        result = self.run_pipeline(
            "orphan_delivery",
            orders=orders, lines=lines, statuses=statuses, products=products,
            deliveries=[
                delivery_row("DE-OK", "O1", "DELIVERED", "2026-07-04T00:00:00Z"),
                delivery_row("DE-ORPHAN", "X999", "DELIVERED", "2026-07-05T00:00:00Z"),
            ],
        )
        self.assertEqual(result.report["quarantined"]["orphan_delivery_events"], 1)
        self.assertEqual(result.delivered_total, 1)   # orphan not counted
        self.assertEqual(result.alerts, [])            # orphan not alerted
        self.assertNotIn("X999", json.dumps(result.alerts))

    def test_late_delivery_is_alerted(self):
        # Behaviour 12.
        orders, lines, statuses, products = self._completed()
        result = self.run_pipeline(
            "late_delivery",
            orders=orders, lines=lines, statuses=statuses, products=products,
            deliveries=[delivery_row("DE1", "O1", "DELIVERED", "2026-07-06T00:00:00Z")],
        )
        self.assertEqual(len(result.alerts), 1)
        alert = result.alerts[0]
        self.assertEqual(alert["order_id"], "O1")
        self.assertEqual(alert["store_id"], "S1")
        self.assertEqual(alert["reason"], "delivered_late")
        self.assertEqual(alert["delivered_at"], "2026-07-06T00:00:00Z")
        self.assertEqual(result.delivered_total, 1)
        self.assertEqual(result.on_time_total, 0)

    def test_missing_delivery_after_promise_is_alerted(self):
        # Behaviour 13.
        orders, lines, statuses, products = self._completed()
        orders[0] = order_row(promised_delivery_at="2026-07-10T00:00:00Z")
        result = self.run_pipeline(
            "undelivered",
            orders=orders, lines=lines, statuses=statuses, products=products,
        )
        self.assertEqual(len(result.alerts), 1)
        self.assertEqual(result.alerts[0]["reason"], "not_delivered_by_promise")
        self.assertEqual(result.alerts[0]["delivered_at"], None)

    def test_missing_delivery_before_promise_is_not_alerted(self):
        # Behaviour 14: the fixed assessment time is 2026-07-15T09:00:00Z.
        orders, lines, statuses, products = self._completed()
        orders[0] = order_row(promised_delivery_at="2026-07-20T00:00:00Z")
        result = self.run_pipeline(
            "promise_future",
            orders=orders, lines=lines, statuses=statuses, products=products,
        )
        self.assertEqual(result.alerts, [])

    def test_delivery_after_assessment_time_ignored_for_alerts(self):
        # Behaviour 15: the as-of 2026-07-15T09:00:00Z state has no delivery,
        # so the order is alerted as not delivered; but historical delivery
        # metrics still count the (late) delivery from 2026-07-20.
        orders, lines, statuses, products = self._completed()
        orders[0] = order_row(promised_delivery_at="2026-07-10T00:00:00Z")
        result = self.run_pipeline(
            "delivery_after_assessment",
            orders=orders, lines=lines, statuses=statuses, products=products,
            deliveries=[delivery_row("DE1", "O1", "DELIVERED", "2026-07-20T00:00:00Z")],
        )
        self.assertEqual(len(result.alerts), 1)
        self.assertEqual(result.alerts[0]["reason"], "not_delivered_by_promise")
        self.assertEqual(result.delivered_total, 1)
        self.assertEqual(result.on_time_total, 0)

    def test_on_time_boundary_is_inclusive(self):
        # Behaviour 16: delivered_at == promised_delivery_at is on time.
        orders, lines, statuses, products = self._completed()
        result = self.run_pipeline(
            "on_time_boundary",
            orders=orders, lines=lines, statuses=statuses, products=products,
            deliveries=[delivery_row("DE1", "O1", "DELIVERED", "2026-07-05T00:00:00Z")],
        )
        self.assertEqual(result.on_time_total, 1)
        self.assertEqual(result.alerts, [])


# --- 17-19: validation and failure modes --------------------------------------

class ValidationAndFailureTests(PipelineTestCase):
    def test_non_finite_and_negative_numbers_are_rejected(self):
        # Behaviour 17: NaN/Infinity spellings accepted by float() must not
        # reach the SQL layer; negative monetary fields are also rejected.
        result = self.run_pipeline(
            "bad_numbers",
            orders=[order_row()],
            lines=[
                line_row("L1"),
                line_row("L2", quantity="NaN"),
                line_row("L3", unit_price="Infinity"),
                line_row("L4", discount_amount="-5.00"),
                line_row("L5", quantity="-1"),
            ],
            statuses=[status_row("SE1", "O1", "COMPLETED", "2026-07-02T00:00:00Z")],
            products=[product_row()],
        )
        self.assertEqual(result.rejected_rows, 4)
        self.assertEqual(result.report["rejected"]["by_source"]["order_lines"], 4)
        # The valid line alone produces revenue.
        self.assertEqual(len(result.sales), 1)
        self.assertEqual(result.sales[0]["net_revenue"], "20.00")

    def test_malformed_jsonl_record_is_rejected_not_fatal(self):
        # Behaviour 18: a record missing required fields on any line is
        # reported, and the remaining valid records still flow through.
        orders, lines, statuses, products = (
            [order_row()], [line_row()],
            [status_row("SE1", "O1", "COMPLETED", "2026-07-02T00:00:00Z")],
            [product_row()],
        )
        broken = {"event_id": "DE-BAD", "event_type": "DELIVERED",
                  "event_ts": "2026-07-04T00:00:00Z"}  # missing order_id/updated_at
        result = self.run_pipeline(
            "bad_jsonl",
            orders=orders, lines=lines, statuses=statuses, products=products,
            deliveries=[broken, delivery_row("DE-OK", "O1", "DELIVERED",
                                             "2026-07-04T00:00:00Z")],
        )
        self.assertEqual(result.report["rejected"]["by_source"].get("delivery_events"), 1)
        self.assertEqual(result.delivered_total, 1)

    def test_invalid_json_line_is_fatal(self):
        with self.assertRaisesRegex(pipeline.PipelineError, "invalid JSON"):
            self.run_pipeline("bad_json_line", orders=[order_row()],
                              deliveries_raw='{"event_id": "X", broken json\n')

    def test_missing_required_columns_is_fatal(self):
        data_dir = self.tmp / "thin_schema" / "data"
        write_dataset(data_dir, orders=[order_row()])
        (data_dir / "orders.csv").write_text("order_id,store_id\nO1,S1\n",
                                             encoding="utf-8")
        with self.assertRaisesRegex(pipeline.PipelineError, "missing required columns"):
            pipeline.run(data_dir, self.tmp / "thin_schema" / "out")

    def test_missing_source_file_is_fatal(self):
        with self.assertRaisesRegex(pipeline.PipelineError, "not found"):
            pipeline.run(self.tmp / "does-not-exist", self.tmp / "unused-out")


if __name__ == "__main__":
    unittest.main()
